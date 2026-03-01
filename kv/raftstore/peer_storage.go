package raftstore

import (
	"bytes"
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
	"github.com/golang/protobuf/proto"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

type ApplySnapResult struct {
	// PrevRegion is the region before snapshot applied
	PrevRegion *metapb.Region
	Region     *metapb.Region
}

var _ raft.Storage = new(PeerStorage)

type PeerStorage struct {
	// current region information of the peer
	region *metapb.Region
	// current raft state of the peer
	raftState *rspb.RaftLocalState
	// current apply state of the peer
	applyState *rspb.RaftApplyState

	// current snapshot state
	snapState snap.SnapState
	// regionSched used to schedule task to region worker
	regionSched chan<- worker.Task
	// generate snapshot tried count
	snapTriedCnt int
	// Engine include two badger instance: Raft and Kv
	Engines *engine_util.Engines
	// Tag used for logging
	Tag string
}

// NewPeerStorage get the persist raftState from engines and return a peer storage
func NewPeerStorage(engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task, tag string) (*PeerStorage, error) {
	log.Debugf("%s creating storage for %s", tag, region.String())
	raftState, err := meta.InitRaftLocalState(engines.Raft, region)
	if err != nil {
		return nil, err
	}
	applyState, err := meta.InitApplyState(engines.Kv, region)
	if err != nil {
		return nil, err
	}
	if raftState.LastIndex < applyState.AppliedIndex {
		panic(fmt.Sprintf("%s unexpected raft log index: lastIndex %d < appliedIndex %d",
			tag, raftState.LastIndex, applyState.AppliedIndex))
	}
	return &PeerStorage{
		Engines:     engines,
		region:      region,
		Tag:         tag,
		raftState:   raftState,
		applyState:  applyState,
		regionSched: regionSched,
	}, nil
}

func (ps *PeerStorage) InitialState() (eraftpb.HardState, eraftpb.ConfState, error) {
	raftState := ps.raftState
	if raft.IsEmptyHardState(*raftState.HardState) {
		y.AssertTruef(!ps.isInitialized(),
			"peer for region %s is initialized but local state %+v has empty hard state",
			ps.region, ps.raftState)
		return eraftpb.HardState{}, eraftpb.ConfState{}, nil
	}
	return *raftState.HardState, util.ConfStateFromRegion(ps.region), nil
}

func (ps *PeerStorage) Entries(low, high uint64) ([]eraftpb.Entry, error) {
	if err := ps.checkRange(low, high); err != nil || low == high {
		return nil, err
	}
	buf := make([]eraftpb.Entry, 0, high-low)
	nextIndex := low
	txn := ps.Engines.Raft.NewTransaction(false)
	defer txn.Discard()
	startKey := meta.RaftLogKey(ps.region.Id, low)
	endKey := meta.RaftLogKey(ps.region.Id, high)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()
	for iter.Seek(startKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		if bytes.Compare(item.Key(), endKey) >= 0 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		var entry eraftpb.Entry
		if err = entry.Unmarshal(val); err != nil {
			return nil, err
		}
		// May meet gap or has been compacted.
		if entry.Index != nextIndex {
			break
		}
		nextIndex++
		buf = append(buf, entry)
	}
	// If we get the correct number of entries, returns.
	if len(buf) == int(high-low) {
		return buf, nil
	}
	// Here means we don't fetch enough entries.
	return nil, raft.ErrUnavailable
}

func (ps *PeerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}
	if err := ps.checkRange(idx, idx+1); err != nil {
		return 0, err
	}
	if ps.truncatedTerm() == ps.raftState.LastTerm || idx == ps.raftState.LastIndex {
		return ps.raftState.LastTerm, nil
	}
	var entry eraftpb.Entry
	if err := engine_util.GetMeta(ps.Engines.Raft, meta.RaftLogKey(ps.region.Id, idx), &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (ps *PeerStorage) LastIndex() (uint64, error) {
	return ps.raftState.LastIndex, nil
}

func (ps *PeerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *PeerStorage) Snapshot() (eraftpb.Snapshot, error) {
	var snapshot eraftpb.Snapshot
	if ps.snapState.StateType == snap.SnapState_Generating {
		select {
		case s := <-ps.snapState.Receiver:
			if s != nil {
				snapshot = *s
			}
		default:
			return snapshot, raft.ErrSnapshotTemporarilyUnavailable
		}
		ps.snapState.StateType = snap.SnapState_Relax
		if snapshot.GetMetadata() != nil {
			ps.snapTriedCnt = 0
			if ps.validateSnap(&snapshot) {
				return snapshot, nil
			}
		} else {
			log.Warnf("%s failed to try generating snapshot, times: %d", ps.Tag, ps.snapTriedCnt)
		}
	}

	if ps.snapTriedCnt >= 5 {
		err := errors.Errorf("failed to get snapshot after %d times", ps.snapTriedCnt)
		ps.snapTriedCnt = 0
		return snapshot, err
	}

	log.Infof("%s requesting snapshot", ps.Tag)
	ps.snapTriedCnt++
	ch := make(chan *eraftpb.Snapshot, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Generating,
		Receiver:  ch,
	}
	// schedule snapshot generate task
	ps.regionSched <- &runner.RegionTaskGen{
		RegionId: ps.region.GetId(),
		Notifier: ch,
	}
	return snapshot, raft.ErrSnapshotTemporarilyUnavailable
}

func (ps *PeerStorage) isInitialized() bool {
	return len(ps.region.Peers) > 0
}

func (ps *PeerStorage) Region() *metapb.Region {
	return ps.region
}

func (ps *PeerStorage) SetRegion(region *metapb.Region) {
	ps.region = region
}

func (ps *PeerStorage) checkRange(low, high uint64) error {
	if low > high {
		return errors.Errorf("low %d is greater than high %d", low, high)
	} else if low <= ps.truncatedIndex() {
		return raft.ErrCompacted
	} else if high > ps.raftState.LastIndex+1 {
		return errors.Errorf("entries' high %d is out of bound, lastIndex %d",
			high, ps.raftState.LastIndex)
	}
	return nil
}

func (ps *PeerStorage) truncatedIndex() uint64 {
	return ps.applyState.TruncatedState.Index
}

func (ps *PeerStorage) truncatedTerm() uint64 {
	return ps.applyState.TruncatedState.Term
}

func (ps *PeerStorage) AppliedIndex() uint64 {
	return ps.applyState.AppliedIndex
}

func (ps *PeerStorage) validateSnap(snap *eraftpb.Snapshot) bool {
	idx := snap.GetMetadata().GetIndex()
	if idx < ps.truncatedIndex() {
		log.Infof("%s snapshot is stale, generate again, snapIndex: %d, truncatedIndex: %d", ps.Tag, idx, ps.truncatedIndex())
		return false
	}
	var snapData rspb.RaftSnapshotData
	if err := proto.UnmarshalMerge(snap.GetData(), &snapData); err != nil {
		log.Errorf("%s failed to decode snapshot, it may be corrupted, err: %v", ps.Tag, err)
		return false
	}
	snapEpoch := snapData.GetRegion().GetRegionEpoch()
	latestEpoch := ps.region.GetRegionEpoch()
	if snapEpoch.GetConfVer() < latestEpoch.GetConfVer() {
		log.Infof("%s snapshot epoch is stale, snapEpoch: %s, latestEpoch: %s", ps.Tag, snapEpoch, latestEpoch)
		return false
	}
	return true
}

func (ps *PeerStorage) clearMeta(kvWB, raftWB *engine_util.WriteBatch) error {
	return ClearMeta(ps.Engines, kvWB, raftWB, ps.region.Id, ps.raftState.LastIndex)
}

// Delete all data that is not covered by `new_region`.
func (ps *PeerStorage) clearExtraData(newRegion *metapb.Region) {
	oldStartKey, oldEndKey := ps.region.GetStartKey(), ps.region.GetEndKey()
	newStartKey, newEndKey := newRegion.GetStartKey(), newRegion.GetEndKey()
	if bytes.Compare(oldStartKey, newStartKey) < 0 {
		ps.clearRange(newRegion.Id, oldStartKey, newStartKey)
	}
	if bytes.Compare(newEndKey, oldEndKey) < 0 || (len(oldEndKey) == 0 && len(newEndKey) != 0) {
		ps.clearRange(newRegion.Id, newEndKey, oldEndKey)
	}
}

// ClearMeta delete stale metadata like raftState, applyState, regionState and raft log entries
func ClearMeta(engines *engine_util.Engines, kvWB, raftWB *engine_util.WriteBatch, regionID uint64, lastIndex uint64) error {
	start := time.Now()
	kvWB.DeleteMeta(meta.RegionStateKey(regionID))
	kvWB.DeleteMeta(meta.ApplyStateKey(regionID))

	firstIndex := lastIndex + 1
	beginLogKey := meta.RaftLogKey(regionID, 0)
	endLogKey := meta.RaftLogKey(regionID, firstIndex)
	err := engines.Raft.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(beginLogKey)
		if it.Valid() && bytes.Compare(it.Item().Key(), endLogKey) < 0 {
			logIdx, err1 := meta.RaftLogIndex(it.Item().Key())
			if err1 != nil {
				return err1
			}
			firstIndex = logIdx
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i := firstIndex; i <= lastIndex; i++ {
		raftWB.DeleteMeta(meta.RaftLogKey(regionID, i))
	}
	raftWB.DeleteMeta(meta.RaftStateKey(regionID))
	log.Infof(
		"[region %d] clear peer 1 meta key 1 apply key 1 raft key and %d raft logs, takes %v",
		regionID,
		lastIndex+1-firstIndex,
		time.Since(start),
	)
	return nil
}

// Append writes new log entries to the Raft DB and deletes any entries that have
// been superseded by a conflicting suffix from a newer leader.
//
// After a leader election, the new leader may send entries that conflict with
// locally-stored entries from the old leader. Our in-memory RaftLog already
// truncates the entries slice at the conflict point, but the old entries still
// exist in BadgerDB. This function is responsible for overwriting them and
// deleting any tail that is no longer valid.
func (ps *PeerStorage) Append(entries []eraftpb.Entry, raftWB *engine_util.WriteBatch) error {
	if len(entries) == 0 {
		return nil
	}

	// Persist each valid entry to the Raft DB. Entries at or below the
	// truncated index have already been compacted into a snapshot and must
	// not be stored again.
	for i := range entries {
		if entries[i].Index <= ps.truncatedIndex() {
			continue
		}
		if err := raftWB.SetMeta(meta.RaftLogKey(ps.region.Id, entries[i].Index), &entries[i]); err != nil {
			return err
		}
	}

	newLastIndex := entries[len(entries)-1].Index
	newLastTerm := entries[len(entries)-1].Term

	// Delete any stale entries that used to be in the log beyond our new last
	// index. This happens when a new leader sends a shorter (or conflicting)
	// set of entries that replaces a longer local suffix.
	oldLastIndex := ps.raftState.LastIndex
	if newLastIndex < oldLastIndex {
		for staleTailIndex := newLastIndex + 1; staleTailIndex <= oldLastIndex; staleTailIndex++ {
			raftWB.DeleteMeta(meta.RaftLogKey(ps.region.Id, staleTailIndex))
		}
	}

	// Update the in-memory RaftLocalState so that callers writing the state to
	// the DB (e.g. SaveReadyState) see the correct values.
	ps.raftState.LastIndex = newLastIndex
	ps.raftState.LastTerm = newLastTerm

	return nil
}

// ApplySnapshot installs an incoming snapshot by:
//  1. Clearing all existing Raft metadata (log entries, raftState, applyState)
//     from BadgerDB via the supplied write-batches.
//  2. Writing the new applyState and RegionLocalState for the snapshot region
//     to the KV BadgerDB (flushed atomically here, because SaveReadyState only
//     flushes the raftWB).
//  3. Updating in-memory state (raftState, applyState, region pointer).
//  4. Scheduling the actual SST data ingestion via a RegionTaskApply worker task.
func (ps *PeerStorage) ApplySnapshot(snapshot *eraftpb.Snapshot, kvWB *engine_util.WriteBatch, raftWB *engine_util.WriteBatch) (*ApplySnapResult, error) {
	log.Infof("%v begin to apply snapshot", ps.Tag)

	snapData := new(rspb.RaftSnapshotData)
	if err := snapData.Unmarshal(snapshot.Data); err != nil {
		return nil, err
	}

	newRegion := snapData.Region
	snapshotIndex := snapshot.Metadata.Index
	snapshotTerm := snapshot.Metadata.Term

	// Save the old region boundaries so we can return them and so the region
	// worker knows which key range to purge before installing the snapshot data.
	prevRegion := ps.region

	// Enqueue deletions of all existing Raft log entries and metadata keys into
	// the two write-batches. clearMeta writes Raft-DB deletions to raftWB and
	// KV-DB metadata deletions (applyState, regionState) to kvWB.
	if err := ps.clearMeta(kvWB, raftWB); err != nil {
		return nil, err
	}

	// Schedule destruction of any KV data that falls outside the new region's
	// key range (happens asynchronously via the region worker).
	ps.clearExtraData(newRegion)

	// Advance raftState to the snapshot's index/term. SaveReadyState will write
	// this to raftWB and flush it to the Raft BadgerDB after we return.
	ps.raftState.LastIndex = snapshotIndex
	ps.raftState.LastTerm = snapshotTerm

	// Advance applyState: the snapshot covers all entries up to snapshotIndex.
	ps.applyState.AppliedIndex = snapshotIndex
	ps.applyState.TruncatedState.Index = snapshotIndex
	ps.applyState.TruncatedState.Term = snapshotTerm

	// Write the new applyState and the RegionLocalState for the snapshot region
	// into kvWB, then flush to the KV BadgerDB now. We must flush here because
	// SaveReadyState only flushes the raftWB; if we skip this flush, a crash
	// between SaveReadyState returning and the region worker finishing would
	// leave the KV metadata inconsistent.
	kvWB.SetMeta(meta.ApplyStateKey(ps.region.Id), ps.applyState)
	meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
	if err := kvWB.WriteToDB(ps.Engines.Kv); err != nil {
		return nil, err
	}

	// Update the in-memory region pointer so the peer serves the correct key range.
	ps.region = newRegion

	// Mark snapState as Applying so the peer knows a snapshot ingestion is in
	// flight. The region worker will signal the notifier channel when done.
	applyNotifier := make(chan bool, 1)
	ps.snapState = snap.SnapState{
		StateType: snap.SnapState_Applying,
	}

	// Send the actual data-ingestion task to the region worker. It will clean up
	// the old key range (using the *previous* region's boundaries) and then
	// apply the snapshot SST files.
	ps.regionSched <- &runner.RegionTaskApply{
		RegionId: newRegion.Id,
		Notifier: applyNotifier,
		SnapMeta: snapshot.Metadata,
		StartKey: prevRegion.GetStartKey(),
		EndKey:   prevRegion.GetEndKey(),
	}

	return &ApplySnapResult{
		PrevRegion: prevRegion,
		Region:     newRegion,
	}, nil
}

// SaveReadyState durably writes all state from the Raft Ready struct to disk.
// This must complete before any outgoing messages are delivered to peers,
// because the Raft protocol requires that HardState is on disk before vote
// grants or AppendEntries responses are sent.
//
// It writes to the Raft DB only (log entries + RaftLocalState). Applying
// committed entries to the KV DB is handled separately in HandleRaftReady.
func (ps *PeerStorage) SaveReadyState(ready *raft.Ready) (*ApplySnapResult, error) {
	raftWriteBatch := new(engine_util.WriteBatch)

	// Handle an incoming snapshot (implemented in 2C). For 2B this branch
	// will not be reached because the Raft layer does not produce snapshots yet.
	var applySnapshotResult *ApplySnapResult
	if !raft.IsEmptySnap(&ready.Snapshot) {
		var err error
		applySnapshotResult, err = ps.ApplySnapshot(&ready.Snapshot, new(engine_util.WriteBatch), raftWriteBatch)
		if err != nil {
			return nil, err
		}
	}

	// Persist any new log entries and update ps.raftState.LastIndex/LastTerm.
	if len(ready.Entries) > 0 {
		if err := ps.Append(ready.Entries, raftWriteBatch); err != nil {
			return nil, err
		}
	}

	// If the HardState changed (term, vote, or commit advanced), update the
	// in-memory pointer so it is included when we write RaftLocalState below.
	if !raft.IsEmptyHardState(ready.HardState) {
		ps.raftState.HardState = &ready.HardState
	}

	// Always write the full RaftLocalState. This atomically records the new
	// LastIndex, LastTerm, and HardState so that the node can reconstruct the
	// correct state after a crash.
	if err := raftWriteBatch.SetMeta(meta.RaftStateKey(ps.region.Id), ps.raftState); err != nil {
		return nil, err
	}

	// Flush the entire write batch to the Raft BadgerDB in one atomic write.
	if err := ps.Engines.WriteRaft(raftWriteBatch); err != nil {
		return nil, err
	}

	return applySnapshotResult, nil
}

func (ps *PeerStorage) ClearData() {
	ps.clearRange(ps.region.GetId(), ps.region.GetStartKey(), ps.region.GetEndKey())
}

func (ps *PeerStorage) clearRange(regionID uint64, start, end []byte) {
	ps.regionSched <- &runner.RegionTaskDestroy{
		RegionId: regionID,
		StartKey: start,
		EndKey:   end,
	}
}
