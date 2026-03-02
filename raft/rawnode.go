// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft

	// prevSoftState is the last SoftState we reported to the application via a
	// Ready struct. We compare against it to detect changes that need reporting.
	prevSoftState *SoftState

	// prevHardState is the last HardState we reported to the application via a
	// Ready struct. We compare against it to avoid writing identical hard state
	// to stable storage on every call.
	prevHardState pb.HardState
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	raft := newRaft(config)
	rn := &RawNode{
		Raft:          raft,
		prevSoftState: raft.softState(),
		prevHardState: raft.hardState(),
	}
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// isSoftStateChanged returns true if the current soft state differs from the
// previously reported soft state, meaning the application needs to be notified.
func isSoftStateChanged(current, previous *SoftState) bool {
	return current.Lead != previous.Lead || current.RaftState != previous.RaftState
}

// Ready returns the current point-in-time state of this RawNode.
// The returned Ready contains all information the application needs to:
//  1. Persist HardState and unstable log Entries to stable storage.
//  2. Send Messages to peer nodes.
//  3. Apply CommittedEntries to the state machine.
func (rn *RawNode) Ready() Ready {
	rd := Ready{}

	currentSoftState := rn.Raft.softState()
	if isSoftStateChanged(currentSoftState, rn.prevSoftState) {
		rd.SoftState = currentSoftState
	}

	currentHardState := rn.Raft.hardState()
	if !isHardStateEqual(currentHardState, rn.prevHardState) {
		rd.HardState = currentHardState
	}

	// Unstable entries must be written to stable storage before any messages
	// are delivered to peers.
	rd.Entries = rn.Raft.RaftLog.unstableEntries()

	// Committed entries that the state machine has not yet applied.
	rd.CommittedEntries = rn.Raft.RaftLog.nextEnts()

	// Outbound messages to deliver to peer nodes.
	rd.Messages = rn.Raft.msgs

	// Pending snapshot to install (used in 2C).
	if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
		rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
	}

	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
// Returns true if the application needs to process a new Ready batch.
func (rn *RawNode) HasReady() bool {
	r := rn.Raft

	if isSoftStateChanged(r.softState(), rn.prevSoftState) {
		return true
	}

	if currentHardState := r.hardState(); !IsEmptyHardState(currentHardState) &&
		!isHardStateEqual(currentHardState, rn.prevHardState) {
		return true
	}

	if len(r.RaftLog.unstableEntries()) > 0 {
		return true
	}

	if len(r.RaftLog.nextEnts()) > 0 {
		return true
	}

	if len(r.msgs) > 0 {
		return true
	}

	if !IsEmptySnap(r.RaftLog.pendingSnapshot) {
		return true
	}

	return false
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
// After calling Advance, the application must not use any of the slices from the previous
// Ready (they may be overwritten on the next call to Ready).
func (rn *RawNode) Advance(rd Ready) {
	// Update our cached soft state so future HasReady calls compare correctly.
	if rd.SoftState != nil {
		rn.prevSoftState = rd.SoftState
	}

	// Update our cached hard state.
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHardState = rd.HardState
	}

	// Advance the stabled pointer to the last entry the application has written
	// to stable storage. Future Ready calls will not re-include these entries.
	if len(rd.Entries) > 0 {
		rn.Raft.RaftLog.stabled = rd.Entries[len(rd.Entries)-1].Index
	}

	// Advance the applied pointer to the last entry the state machine has
	// processed. Future Ready calls will not re-include these entries.
	if len(rd.CommittedEntries) > 0 {
		rn.Raft.RaftLog.applied = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
	}

	// Clear the outbound message queue; these have been handed off to the
	// transport layer and must not be sent again.
	rn.Raft.msgs = nil

	// Discard the pending snapshot now that the application has installed it.
	if !IsEmptySnap(&rd.Snapshot) {
		rn.Raft.RaftLog.pendingSnapshot = nil
	}
}

// GetProgress return the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
