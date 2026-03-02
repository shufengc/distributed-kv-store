package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/schedulerpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

// HandleRaftReady is the central event-loop step for a Raft peer. It drains
// one pending Ready batch from the Raft state machine, durably persists it,
// delivers outgoing messages to peers, applies committed entries to the KV
// state machine, and finally calls Advance to let Raft know the batch is done.
//
// The ordering is deliberate:
//  1. SaveReadyState (disk) must happen before Send (network) so that the node
//     can recover its HardState even if it crashes right after voting.
//  2. Apply committed entries after messages so that reads on other nodes can
//     see the committed data as soon as possible.
func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if !d.RaftGroup.HasReady() {
		return
	}

	ready := d.RaftGroup.Ready()

	// Persist log entries and HardState to the Raft DB. Must complete before
	// any network messages are sent.
	applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
	if err != nil {
		log.Panicf("%s failed to save ready state: %v", d.Tag, err)
		return
	}

	// After a snapshot is applied the region metadata changes (new key range,
	// new peer list). We must update storeMeta so the router can find this
	// region under its new boundaries. (Primarily exercised in 2C.)
	if applySnapResult != nil {
		prevRegion := applySnapResult.PrevRegion
		newRegion := applySnapResult.Region
		storeMeta := d.ctx.storeMeta
		storeMeta.Lock()
		storeMeta.regions[newRegion.Id] = newRegion
		storeMeta.regionRanges.Delete(&regionItem{region: prevRegion})
		storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
		storeMeta.Unlock()
	}

	// Deliver outgoing Raft messages (AppendEntries, Vote responses, etc.) to
	// peer nodes. This happens after the disk write so HardState is safe.
	d.Send(d.ctx.trans, ready.Messages)

	// Apply any newly committed entries to the KV state machine. All writes
	// are batched into a single WriteBatch and flushed at the end to keep the
	// AppliedIndex and KV data consistent.
	if len(ready.CommittedEntries) > 0 {
		kvWriteBatch := new(engine_util.WriteBatch)

		for _, committedEntry := range ready.CommittedEntries {
			kvWriteBatch = d.applyCommittedEntry(committedEntry, kvWriteBatch)
			if d.stopped {
				// The peer may be destroyed mid-loop (e.g., by a ConfChange
				// removing it). Stop applying immediately.
				return
			}
		}

		// Record the last applied index and persist ApplyState atomically with
		// the KV writes so that a crash cannot leave AppliedIndex ahead of the
		// data that was actually written.
		lastCommittedEntry := ready.CommittedEntries[len(ready.CommittedEntries)-1]
		d.peerStorage.applyState.AppliedIndex = lastCommittedEntry.Index
		if err := kvWriteBatch.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState); err != nil {
			log.Panicf("%s failed to set apply state: %v", d.Tag, err)
		}
		kvWriteBatch.MustWriteToDB(d.peerStorage.Engines.Kv)
	}

	// Signal to the Raft state machine that the current Ready batch has been
	// fully processed. This advances the stable and applied indices inside
	// RaftLog and clears the messages list.
	d.RaftGroup.Advance(ready)
}

// applyCommittedEntry dispatches a single committed log entry to the
// appropriate handler based on its type and content.
//
// It returns the (possibly replaced) kvWriteBatch, because Get and Snap
// commands must flush pending writes before reading, which creates a new batch.
func (d *peerMsgHandler) applyCommittedEntry(entry eraftpb.Entry, kvWriteBatch *engine_util.WriteBatch) *engine_util.WriteBatch {
	if entry.EntryType == eraftpb.EntryType_EntryConfChange {
		return d.applyConfChange(entry, kvWriteBatch)
	}

	// The leader always appends a no-op entry when it first becomes leader.
	// This entry has no data and no associated client proposal; skip it.
	if len(entry.Data) == 0 {
		return kvWriteBatch
	}

	var raftCmdRequest raft_cmdpb.RaftCmdRequest
	if err := raftCmdRequest.Unmarshal(entry.Data); err != nil {
		log.Panicf("%s failed to unmarshal committed entry at index %d: %v", d.Tag, entry.Index, err)
	}

	if raftCmdRequest.AdminRequest != nil {
		return d.applyAdminCommand(entry, &raftCmdRequest, kvWriteBatch)
	}
	return d.applyNormalRequests(entry, &raftCmdRequest, kvWriteBatch)
}

// applyNormalRequests executes the KV operations (Get, Put, Delete, Snap)
// contained in a committed normal (non-admin) command entry, builds the
// response, and completes the matching client proposal callback.
func (d *peerMsgHandler) applyNormalRequests(
	entry eraftpb.Entry,
	msg *raft_cmdpb.RaftCmdRequest,
	kvWriteBatch *engine_util.WriteBatch,
) *engine_util.WriteBatch {
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			kvWriteBatch.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})

		case raft_cmdpb.CmdType_Delete:
			kvWriteBatch.DeleteCF(req.Delete.Cf, req.Delete.Key)
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})

		case raft_cmdpb.CmdType_Get:
			// A Get must see the latest committed state. Flush any buffered
			// writes from earlier entries in this Ready batch before reading.
			kvWriteBatch.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWriteBatch = new(engine_util.WriteBatch)

			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
			if err != nil {
				log.Panicf("%s Get failed for key %v: %v", d.Tag, req.Get.Key, err)
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get:     &raft_cmdpb.GetResponse{Value: value},
			})

		case raft_cmdpb.CmdType_Snap:
			// A Snap also needs a consistent read, so flush first.
			kvWriteBatch.MustWriteToDB(d.peerStorage.Engines.Kv)
			kvWriteBatch = new(engine_util.WriteBatch)

			// Re-validate the region epoch. A split can be committed in the
			// same Ready batch as this Snap, applied before it. If the region's
			// epoch changed (version bumped by split), the client's Snap epoch
			// is now stale. Return EpochNotMatch so it retries with the new
			// region, preventing iter.Seek from panicking on the wrong bounds.
			if err := util.CheckRegionEpoch(msg, d.Region(), true); err != nil {
				BindRespError(resp, err)
				d.completeProposalForEntry(entry, resp)
				return kvWriteBatch
			}

			// Attach a read-only BadgerDB transaction to the callback so the
			// caller can iterate over the snapshot of the KV state.
			snapProposal := d.findProposalForEntry(entry)
			if snapProposal != nil && snapProposal.cb != nil {
				snapProposal.cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
			}
			resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
			})
		}
	}

	d.completeProposalForEntry(entry, resp)
	return kvWriteBatch
}

// applyAdminCommand handles admin commands committed through the Raft log.
// For Project 2B only AdminCmdType_CompactLog is exercised; other admin
// command types (Split, ConfChange) are handled in 3B.
func (d *peerMsgHandler) applyAdminCommand(
	entry eraftpb.Entry,
	msg *raft_cmdpb.RaftCmdRequest,
	kvWriteBatch *engine_util.WriteBatch,
) *engine_util.WriteBatch {
	adminReq := msg.AdminRequest
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	switch adminReq.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactIndex := adminReq.CompactLog.CompactIndex
		compactTerm := adminReq.CompactLog.CompactTerm

		// Only advance the truncation point if this compact request is actually
		// newer than what we already have. Out-of-order requests can be dropped.
		if compactIndex > d.peerStorage.applyState.TruncatedState.Index {
			d.peerStorage.applyState.TruncatedState.Index = compactIndex
			d.peerStorage.applyState.TruncatedState.Term = compactTerm
		}

		// Enqueue a background task to physically delete the now-obsolete log
		// entries from the Raft BadgerDB. This is done asynchronously so as not
		// to block the apply loop.
		d.ScheduleCompactLog(compactIndex)

		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType:    raft_cmdpb.AdminCmdType_CompactLog,
			CompactLog: &raft_cmdpb.CompactLogResponse{},
		}

	case raft_cmdpb.AdminCmdType_Split:
		return d.applySplitRegion(entry, adminReq, kvWriteBatch)

	default:
		log.Warnf("%s unknown admin command type %v at index %d, skipping", d.Tag, adminReq.CmdType, entry.Index)
	}

	d.completeProposalForEntry(entry, resp)
	return kvWriteBatch
}

// applyConfChange applies a committed EntryConfChange entry. It updates the
// region peer list, increments ConfVer, persists the new RegionLocalState,
// notifies the Raft state machine, and—if we are being removed—destroys the peer.
func (d *peerMsgHandler) applyConfChange(entry eraftpb.Entry, kvWB *engine_util.WriteBatch) *engine_util.WriteBatch {
	var cc eraftpb.ConfChange
	if err := cc.Unmarshal(entry.Data); err != nil {
		log.Panicf("%s failed to unmarshal ConfChange at index %d: %v", d.Tag, entry.Index, err)
	}

	// The context carries the original RaftCmdRequest that initiated this change.
	var raftCmdReq raft_cmdpb.RaftCmdRequest
	if err := raftCmdReq.Unmarshal(cc.Context); err != nil {
		log.Panicf("%s failed to unmarshal ConfChange context at index %d: %v", d.Tag, entry.Index, err)
	}
	changePeer := raftCmdReq.AdminRequest.ChangePeer

	// Work on a shallow copy so we don't mutate the storage-owned region object
	// before we are ready to persist the change.
	region := new(metapb.Region)
	if err := util.CloneMsg(d.Region(), region); err != nil {
		log.Panicf("%s failed to clone region: %v", d.Tag, err)
	}

	// For RemoveNode, track whether this peer is actually being removed.
	// A replicated peer (one applying historic log entries) may encounter a
	// "RemoveNode self" entry that pre-dates its own addition to the cluster.
	// In that case the peer was NOT in the region list, so the remove is a
	// no-op and we must NOT destroy ourselves.
	removeSelf := false

	switch cc.ChangeType {
	case eraftpb.ConfChangeType_AddNode:
		// Idempotency guard: only add if the peer is not already in the list.
		if util.FindPeer(region, changePeer.Peer.StoreId) == nil {
			region.Peers = append(region.Peers, changePeer.Peer)
		}
		// Cache the new peer's address so sendRaftMessage can reach it.
		d.insertPeerCache(changePeer.Peer)
	case eraftpb.ConfChangeType_RemoveNode:
		// Only mark remove-self if this peer is currently in the region.
		if p := util.FindPeer(region, changePeer.Peer.StoreId); p != nil && p.GetId() == d.PeerId() {
			removeSelf = true
		}
		util.RemovePeer(region, changePeer.Peer.StoreId)
		// Purge the removed peer from our local send cache.
		d.removePeerCache(changePeer.Peer.Id)
	}

	region.RegionEpoch.ConfVer++

	// Persist the updated region to the KV engine.
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)

	// Update the in-memory region on the peer and the global region map.
	d.SetRegion(region)
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regions[region.Id] = region
	storeMeta.Unlock()

	// Notify the Raft state machine so it updates r.Prs accordingly.
	d.RaftGroup.ApplyConfChange(cc)

	// Build and deliver the response to the waiting client callback.
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:    raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{Region: region},
	}
	d.completeProposalForEntry(entry, resp)

	// If we were actually removed from the region, destroy this peer.
	// destroyPeer sets d.stopped, causing HandleRaftReady to exit early.
	// The Tombstone written inside destroyPeer supersedes the Normal-state
	// write above.
	if removeSelf {
		d.destroyPeer()
	}

	return kvWB
}

// applySplitRegion applies a committed Split admin command. It divides the
// current region at the given split key, creates the new region's peer on this
// store, and registers it with the router.
func (d *peerMsgHandler) applySplitRegion(
	entry eraftpb.Entry,
	req *raft_cmdpb.AdminRequest,
	kvWB *engine_util.WriteBatch,
) *engine_util.WriteBatch {
	splitReq := req.Split
	resp := newCmdResp()
	BindRespTerm(resp, d.Term())

	// Idempotency guard: if the split key is no longer within this region the
	// split has already been applied (or the key is invalid). Skip silently.
	if err := util.CheckKeyInRegion(splitReq.SplitKey, d.Region()); err != nil {
		log.Warnf("%s split key %v not in region, split likely already applied: %v", d.Tag, splitReq.SplitKey, err)
		d.completeProposalForEntry(entry, resp)
		return kvWB
	}

	// Build the peer list for the new region. Each new peer has the same StoreId
	// as the corresponding peer in the current region, but a fresh peer ID
	// allocated by the scheduler.
	currentRegion := d.Region()
	newPeers := make([]*metapb.Peer, len(currentRegion.Peers))
	for i, peer := range currentRegion.Peers {
		newPeers[i] = &metapb.Peer{
			Id:      splitReq.NewPeerIds[i],
			StoreId: peer.StoreId,
		}
	}

	newRegion := &metapb.Region{
		Id:       splitReq.NewRegionId,
		StartKey: splitReq.SplitKey,
		EndKey:   currentRegion.EndKey,
		RegionEpoch: &metapb.RegionEpoch{
			Version: currentRegion.RegionEpoch.Version + 1,
			ConfVer: currentRegion.RegionEpoch.ConfVer,
		},
		Peers: newPeers,
	}

	// Update the current region: shrink its key range and bump its version.
	// We clone first to avoid mutating the storage-owned object prematurely.
	updatedRegion := new(metapb.Region)
	if err := util.CloneMsg(currentRegion, updatedRegion); err != nil {
		log.Panicf("%s failed to clone region for split: %v", d.Tag, err)
	}
	updatedRegion.EndKey = splitReq.SplitKey
	updatedRegion.RegionEpoch.Version++

	// Persist the state of both regions atomically in the shared write batch.
	meta.WriteRegionState(kvWB, updatedRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)

	// Update the in-memory peer region.
	d.SetRegion(updatedRegion)

	// Create the peer object for the new region on this store before we take
	// the storeMeta lock, so we minimise the time spent holding it.
	newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion)
	if err != nil {
		log.Panicf("%s failed to create peer for new region %d after split: %v", d.Tag, newRegion.Id, err)
	}

	// Update storeMeta: replace the current region's range entry (end key
	// changed) and insert the brand-new region.
	storeMeta := d.ctx.storeMeta
	storeMeta.Lock()
	storeMeta.regions[d.regionId] = updatedRegion
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: updatedRegion})
	storeMeta.regions[newRegion.Id] = newRegion
	storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})

	// Register the new peer with the router and send it the start signal.
	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})

	// Re-dispatch any pending vote messages that arrived for the new region
	// before it was registered (buffered in storeMeta while the region was unknown).
	remaining := storeMeta.pendingVotes[:0]
	for _, vote := range storeMeta.pendingVotes {
		if vote.RegionId == newRegion.Id {
			_ = d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeRaftMessage, Data: vote})
		} else {
			remaining = append(remaining, vote)
		}
	}
	storeMeta.pendingVotes = remaining
	storeMeta.Unlock()

	// If we are the current leader, try to campaign for leadership of the new
	// region as well (the last peer mirrors the parent's leader by convention).
	newPeer.MaybeCampaign(d.IsLeader())

	// Synchronously notify the scheduler about both regions right after the
	// split. The order is critical: left region first (shrinks the old region's
	// EndKey in the scheduler), then right region (which no longer overlaps the
	// shrunken left and is inserted cleanly). Sending right before left would
	// cause the scheduler's overlap-removal logic to delete the left region
	// entirely, corrupting GetRegion for all left-region keys.
	if d.IsLeader() {
		_ = d.ctx.schedulerClient.RegionHeartbeat(&schedulerpb.RegionHeartbeatRequest{
			Region: updatedRegion,
			Leader: d.Meta,
		})
		_ = d.ctx.schedulerClient.RegionHeartbeat(&schedulerpb.RegionHeartbeatRequest{
			Region: newRegion,
			Leader: newPeer.Meta,
		})
	}

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{updatedRegion, newRegion},
		},
	}
	d.completeProposalForEntry(entry, resp)

	return kvWB
}

// findProposalForEntry returns the proposal whose index matches the given
// entry without consuming it from the queue. This is used for Snap requests
// which need the callback before the normal completeProposalForEntry call.
func (d *peerMsgHandler) findProposalForEntry(entry eraftpb.Entry) *proposal {
	for _, p := range d.proposals {
		if p.index == entry.Index {
			return p
		}
	}
	return nil
}

// completeProposalForEntry walks the proposal FIFO queue and resolves the
// callback that corresponds to the given committed entry.
//
// Proposals whose index is less than the entry's index were proposed in an
// earlier term and have been superseded (a new leader wrote a different entry
// at that slot). They are declared stale and their callbacks are notified with
// an ErrStaleCommand error before being discarded.
//
// If a matching proposal is found but its term differs from the entry's term,
// the entry was re-proposed by a different leader and the original proposal is
// also stale.
func (d *peerMsgHandler) completeProposalForEntry(entry eraftpb.Entry, resp *raft_cmdpb.RaftCmdResponse) {
	for len(d.proposals) > 0 {
		headProposal := d.proposals[0]

		if headProposal.index < entry.Index {
			// This proposal was submitted to an earlier log slot that was never
			// committed (a new leader overwrote it). Notify the client.
			NotifyStaleReq(d.Term(), headProposal.cb)
			d.proposals = d.proposals[1:]
			continue
		}

		if headProposal.index == entry.Index {
			if headProposal.term != entry.Term {
				// The same index was re-used by a new leader with a different
				// entry; the old proposal is stale.
				NotifyStaleReq(d.Term(), headProposal.cb)
			} else {
				// The proposal matches: deliver the response to the client.
				headProposal.cb.Done(resp)
			}
			d.proposals = d.proposals[1:]
		}

		// If headProposal.index > entry.Index, there is no proposal for this
		// entry (e.g., it was the leader no-op or a log GC admin command).
		break
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	if err != nil {
		return err
	}
	// For normal (non-admin) commands, verify that every requested key falls
	// within this region's key range. After a split the region's EndKey shrinks,
	// and clients may send requests for keys that now belong to the new region.
	if req.AdminRequest == nil {
		for _, r := range req.Requests {
			var key []byte
			switch r.CmdType {
			case raft_cmdpb.CmdType_Get:
				key = r.Get.Key
			case raft_cmdpb.CmdType_Put:
				key = r.Put.Key
			case raft_cmdpb.CmdType_Delete:
				key = r.Delete.Key
			}
			if key != nil {
				if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Handle admin commands that require special propose paths.
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			// TransferLeader is a local action; it is never replicated through the
			// Raft log. Call the RawNode directly and respond immediately.
			d.RaftGroup.TransferLeader(msg.AdminRequest.TransferLeader.Peer.Id)
			resp := newCmdResp()
			BindRespTerm(resp, d.Term())
			resp.AdminResponse = &raft_cmdpb.AdminResponse{
				CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
				TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
			}
			cb.Done(resp)
			return

		case raft_cmdpb.AdminCmdType_ChangePeer:
			// ChangePeer is proposed as an EntryConfChange. The full
			// RaftCmdRequest is stored in ConfChange.Context so we can recover
			// the peer metadata when the entry is applied.
			changePeer := msg.AdminRequest.ChangePeer
			data, err := msg.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			cc := eraftpb.ConfChange{
				ChangeType: changePeer.ChangeType,
				NodeId:     changePeer.Peer.Id,
				Context:    data,
			}
			proposalIndex := d.nextProposalIndex()
			proposalTerm := d.Term()
			if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
				cb.Done(ErrResp(err))
				return
			}
			d.proposals = append(d.proposals, &proposal{
				index: proposalIndex,
				term:  proposalTerm,
				cb:    cb,
			})
			return
		}
	}

	// Capture the index at which this entry will land BEFORE calling Propose,
	// because Propose grows the log by one. nextProposalIndex() returns
	// LastIndex()+1, which is exactly the slot our new entry will occupy.
	proposalIndex := d.nextProposalIndex()
	proposalTerm := d.Term()

	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Submit the command to the Raft state machine. If the node is not the
	// leader (e.g., leadership changed between the pre-check above and here),
	// Propose returns an error and we must reject the request.
	if err := d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	// Track the proposal so HandleRaftReady can match the committed entry back
	// to this client callback when the entry is applied.
	d.proposals = append(d.proposals, &proposal{
		index: proposalIndex,
		term:  proposalTerm,
		cb:    cb,
	})
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
