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
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower's progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// randomizedElectionTimeout is set to a random value in [electionTimeout, 2*electionTimeout)
	// each time the election timer is reset. This reduces split-vote probability across peers.
	randomizedElectionTimeout int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftLog := newLog(c.Storage)

	// Restore persisted hard state (term, vote, commit index).
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	// Determine the set of peer IDs. On a fresh cluster start, config.peers
	// is provided. On a node restart, peers come from the stored ConfState.
	peerIDs := c.peers
	if len(peerIDs) == 0 {
		peerIDs = confState.Nodes
	}

	r := &Raft{
		id:               c.ID,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]bool),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	// Initialise progress tracking for all peers. Match starts at 0 and Next
	// starts one past the last known log index (the leader will probe backwards
	// if the follower is behind).
	for _, peerID := range peerIDs {
		r.Prs[peerID] = &Progress{Next: raftLog.LastIndex() + 1}
	}

	// Apply persisted hard state.
	r.Term = hardState.Term
	r.Vote = hardState.Vote
	r.RaftLog.committed = hardState.Commit

	// If the caller provided a last-applied index (node restart), honour it so
	// we don't re-deliver already-applied entries to the state machine.
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
	}

	// Every node starts as a follower. becomeFollower also randomises the
	// election timeout so peers don't all expire at the same tick.
	r.becomeFollower(r.Term, None)

	return r
}

// -----------------------------------------------------------------------
// State transition helpers
// -----------------------------------------------------------------------

// resetRandomizedElectionTimeout picks a new election deadline uniformly at
// random from [electionTimeout, 2*electionTimeout). Randomisation reduces the
// probability of simultaneous elections (§5.2 of the Raft paper).
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

// softState returns the node's current volatile state (leader ID and role).
func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

// hardState returns the node's current durable state (term, vote, commit index).
// This must be persisted to stable storage before any messages are sent.
func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// becomeFollower transitions this node to the follower role.
// The vote is only cleared when we move to a strictly higher term; if we are
// reverting to follower within the same term (e.g., a candidate learns that
// another node already won), we keep the vote to prevent double-voting.
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Lead = lead
	r.leadTransferee = None

	if term > r.Term {
		// Entering a new term: clear any vote from the previous term.
		r.Term = term
		r.Vote = None
	}

	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transitions this node to the candidate role and immediately
// votes for itself. This is called at the start of every new election.
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Lead = None
	r.Term++           // Start a new election term.
	r.Vote = r.id      // Vote for ourselves.
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true // Record our own self-vote.

	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transitions this node to the leader role.
// Per §8 of the Raft paper, the leader must immediately append a no-op entry
// so that it can discover the committed index from previous terms.
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0

	lastLogIndex := r.RaftLog.LastIndex()

	// Reset progress for every peer. Followers start with Next = lastLogIndex + 1
	// (optimistic: assume they are up to date; the probe will back off on rejection).
	// The leader's own Match is immediately set to its last log index.
	for peerID := range r.Prs {
		if peerID == r.id {
			r.Prs[peerID] = &Progress{
				Match: lastLogIndex,
				Next:  lastLogIndex + 1,
			}
		} else {
			r.Prs[peerID] = &Progress{
				Match: 0,
				Next:  lastLogIndex + 1,
			}
		}
	}

	// Append the mandatory no-op entry. This entry causes all entries from
	// previous terms to become committed once a majority acknowledges it.
	noopEntry := pb.Entry{
		Term:  r.Term,
		Index: lastLogIndex + 1,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, noopEntry)

	// Advance the leader's own Match/Next past the noop entry.
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// Replicate the noop entry to all followers.
	r.broadcastAppend()

	// In a single-node cluster there are no followers to wait for, so we can
	// commit immediately.
	r.maybeAdvanceCommitIndex()
}

// -----------------------------------------------------------------------
// Tick (logical clock)
// -----------------------------------------------------------------------

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateFollower, StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			// MsgHup is the local signal that triggers a new election.
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}

	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			// MsgBeat is the local signal that tells the leader to send heartbeats.
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
		// If a leader transfer is in progress, track election time and abort if
		// the transferee has not caught up within one election timeout period.
		if r.leadTransferee != None {
			r.electionElapsed++
			if r.electionElapsed >= r.randomizedElectionTimeout {
				r.electionElapsed = 0
				r.leadTransferee = None
			}
		}
	}
}

// -----------------------------------------------------------------------
// Message sending helpers
// -----------------------------------------------------------------------

// sendTimeoutNow sends a MsgTimeoutNow to the given peer, instructing it to
// start an election immediately regardless of its election timeout.
func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// broadcastAppend sends MsgAppend to every follower to replicate new log entries.
func (r *Raft) broadcastAppend() {
	for peerID := range r.Prs {
		if peerID != r.id {
			r.sendAppend(peerID)
		}
	}
}

// broadcastHeartbeat sends MsgHeartbeat to every follower to maintain authority.
func (r *Raft) broadcastHeartbeat() {
	for peerID := range r.Prs {
		if peerID != r.id {
			r.sendHeartbeat(peerID)
		}
	}
}

// sendSnapshot sends the current snapshot to the given peer. This is used
// when the follower's log is so far behind that the required entries have
// already been compacted. The snapshot is obtained from storage; if it is
// not yet ready (being generated asynchronously), this is a no-op and the
// caller will retry on the next heartbeat response.
func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		// Snapshot is temporarily unavailable; will retry later.
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	// Advance Next past the snapshot so we don't resend log entries that
	// the snapshot already covers.
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	progress := r.Prs[to]
	prevLogIndex := progress.Next - 1

	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		// The entry at prevLogIndex has been compacted into a snapshot.
		// Fall back to sending a snapshot so the follower can catch up.
		r.sendSnapshot(to)
		return false
	}

	// Collect all log entries the follower is missing, starting at Next.
	var entriesToSend []*pb.Entry
	for logIndex := progress.Next; logIndex <= r.RaftLog.LastIndex(); logIndex++ {
		sliceIndex := logIndex - r.RaftLog.firstEntryIndex
		entry := r.RaftLog.entries[sliceIndex]
		entriesToSend = append(entriesToSend, &entry)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: entriesToSend,
		Commit:  r.RaftLog.committed,
	})
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// The commit index piggybacked on the heartbeat must not exceed the
	// follower's known Match, otherwise the follower might advance its commit
	// past entries it hasn't received yet.
	commitToSend := min(r.Prs[to].Match, r.RaftLog.committed)

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commitToSend,
	})
}

// -----------------------------------------------------------------------
// Log management helpers (leader-side)
// -----------------------------------------------------------------------

// appendEntriesToLog assigns the correct Term and Index to each proposed entry,
// appends them to the in-memory log, and advances the leader's own Progress.
func (r *Raft) appendEntriesToLog(proposedEntries []*pb.Entry) {
	nextIndex := r.RaftLog.LastIndex() + 1
	for i, entry := range proposedEntries {
		entry.Term = r.Term
		entry.Index = nextIndex + uint64(i)
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	// Advance the leader's own Match/Next so it counts itself in quorum checks.
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// quorum returns the minimum number of votes (or acknowledgements) required
// to form a majority in the current cluster.
func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

// maybeAdvanceCommitIndex checks whether a new commit index can be established
// on the leader. Per §5.4.2 of the Raft paper, the leader may only commit
// entries from its own current term by counting acknowledgements; it cannot
// retroactively commit entries from a previous term by count alone.
func (r *Raft) maybeAdvanceCommitIndex() {
	// Collect every peer's Match index and sort in descending order so we can
	// find the highest index replicated on a majority of nodes.
	matchIndexes := make([]uint64, 0, len(r.Prs))
	for _, progress := range r.Prs {
		matchIndexes = append(matchIndexes, progress.Match)
	}
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j] // descending
	})

	// The element at position (quorum - 1) is the highest index that at least
	// quorum nodes have matched.
	majorityMatchIndex := matchIndexes[r.quorum()-1]

	if majorityMatchIndex > r.RaftLog.committed {
		termAtMajorityIndex, err := r.RaftLog.Term(majorityMatchIndex)
		if err != nil {
			return
		}
		// Only advance commit for entries written in the current term.
		// This is the key safety rule from §5.4.2.
		if termAtMajorityIndex == r.Term {
			r.RaftLog.committed = majorityMatchIndex
			// Let followers know about the updated commit index.
			r.broadcastAppend()
		}
	}
}

// -----------------------------------------------------------------------
// Step — the central message handler
// -----------------------------------------------------------------------

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Global term check — handle stale (lower-term) messages first.
	// Messages without a term (Term == 0) are either local signals (MsgHup,
	// MsgBeat) or client requests (MsgPropose); they do not carry a peer term
	// and must bypass this filter. Only genuine peer messages with an explicit
	// nonzero term that is lower than ours are considered stale.
	if m.Term > 0 && m.Term < r.Term {
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To:      m.From,
				From:    r.id,
				Term:    r.Term,
				Reject:  true,
			})
		}
		return nil
	}

	// Global term check: if the incoming message carries a higher term we must
	// update our term and possibly step down before processing the message.
	if m.Term > r.Term {
		// Messages that arrive from a legitimate leader carry the leader's ID in
		// the From field; we record that as our current leader.
		if m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			// Vote requests and other messages: step down but don't yet record
			// the sender as leader because we haven't heard from an elected leader.
			r.becomeFollower(m.Term, None)
		}
	}

	// Dispatch based on message type. The state (follower/candidate/leader) is
	// consulted inside each handler where behaviour differs by role.
	switch m.MsgType {

	case pb.MessageType_MsgHup:
		// Local signal: our election timer expired. Only non-leaders react.
		//
		// Guard: a freshly-replicated peer (created via replicatePeer) starts
		// with an empty r.Prs AND Term=0. Allowing it to campaign would
		// cycle its term upward indefinitely (no vote requests are sent, but
		// becomeCandidate still increments Term), eventually causing it to
		// reject the real leader's heartbeats and stall the cluster.
		//
		// Split-created peers are exempt from this guard: they start with
		// r.Term = RaftInitLogTerm > 0 (from InitRaftLocalState), so they
		// are allowed to campaign immediately and become single-node leaders.
		if r.State != StateLeader {
			_, isMember := r.Prs[r.id]
			if isMember || r.Term > 0 {
				r.startElection()
			}
		}

	case pb.MessageType_MsgBeat:
		// Local signal: our heartbeat timer expired. Only the leader reacts.
		if r.State == StateLeader {
			r.broadcastHeartbeat()
		}

	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)

	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			r.handleVoteResponse(m)
		}

	case pb.MessageType_MsgPropose:
		switch r.State {
		case StateLeader:
			// Block new proposals while a leader transfer is in progress to
			// avoid the old leader committing entries after the transfer.
			if r.leadTransferee != None {
				return ErrProposalDropped
			}
			// Enforce at-most-one-pending-conf-change. If a conf change is
			// already in the log but not yet applied, convert any additional
			// conf change entries in this batch to normal no-ops.
			for i, entry := range m.Entries {
				if entry.EntryType == pb.EntryType_EntryConfChange {
					if r.PendingConfIndex > r.RaftLog.applied {
						// A conf change is already pending; demote to no-op.
						m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
					} else {
						// Record the index this conf change will occupy.
						r.PendingConfIndex = r.RaftLog.LastIndex() + uint64(i) + 1
					}
				}
			}
			r.appendEntriesToLog(m.Entries)
			r.broadcastAppend()
			// After appending, check whether we can commit immediately.
			// In a single-node cluster the leader is its own majority, so
			// every proposal can be committed in the same round without
			// waiting for any follower acknowledgement.
			r.maybeAdvanceCommitIndex()
		case StateCandidate:
			// Candidates cannot accept proposals; the proposer should retry.
			return ErrProposalDropped
		case StateFollower:
			// In 2B the follower will forward this to the leader. For 2A, drop.
			return ErrProposalDropped
		}

	case pb.MessageType_MsgAppend:
		// A candidate that receives a valid MsgAppend from a current-term leader
		// must recognise that leader and revert to follower.
		if r.State == StateCandidate {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)

	case pb.MessageType_MsgAppendResponse:
		if r.State == StateLeader {
			r.handleAppendResponse(m)
		}

	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)

	case pb.MessageType_MsgHeartbeatResponse:
		// The leader uses heartbeat responses as a trigger to check whether
		// the responder needs log entries that it hasn't received yet.
		if r.State == StateLeader {
			r.sendAppend(m.From)
		}

	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)

	case pb.MessageType_MsgTransferLeader:
		// Local message: the application wants to transfer leadership to m.From.
		// Only the current leader can drive this process; a follower forwards it.
		if r.State != StateLeader {
			if r.Lead != None {
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgTransferLeader,
					To:      r.Lead,
					From:    m.From,
					Term:    r.Term,
				})
			}
			return nil
		}
		transferee := m.From
		if transferee == r.id {
			return nil
		}
		if _, exists := r.Prs[transferee]; !exists {
			return nil
		}
		// If the same transfer is already in flight, nothing to do.
		if r.leadTransferee == transferee {
			return nil
		}
		r.leadTransferee = transferee
		r.electionElapsed = 0
		if r.Prs[transferee].Match == r.RaftLog.LastIndex() {
			// Transferee is already up-to-date; tell it to start an election now.
			r.sendTimeoutNow(transferee)
		} else {
			// Help the transferee catch up; once it does, handleAppendResponse
			// will send MsgTimeoutNow.
			r.sendAppend(transferee)
		}

	case pb.MessageType_MsgTimeoutNow:
		// The leader told us to start an election immediately.
		// Ignore if we are not yet a member of the cluster.
		if _, isMember := r.Prs[r.id]; isMember {
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}

	return nil
}

// -----------------------------------------------------------------------
// Election helpers
// -----------------------------------------------------------------------

// startElection begins a new election by transitioning to candidate and
// requesting votes from all peers.
func (r *Raft) startElection() {
	r.becomeCandidate()

	// In a single-node cluster there is nobody to ask; we win immediately.
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastLogIndex)

	for peerID := range r.Prs {
		if peerID == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      peerID,
			From:    r.id,
			Term:    r.Term,
			// LogTerm and Index describe the candidate's last log entry so that
			// voters can enforce the "at least as up-to-date" rule (§5.4.1).
			LogTerm: lastLogTerm,
			Index:   lastLogIndex,
		})
	}
}

// handleVoteRequest processes an incoming MsgRequestVote and sends a response.
// A vote is granted only when the candidate's log is at least as up-to-date
// as ours AND we haven't already voted for a different candidate this term.
func (r *Raft) handleVoteRequest(m pb.Message) {
	// If the message carries a lower term, deny immediately.
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
		return
	}

	ourLastLogIndex := r.RaftLog.LastIndex()
	ourLastLogTerm, _ := r.RaftLog.Term(ourLastLogIndex)

	// "At least as up-to-date": compare last log terms first, then last log
	// indices (§5.4.1).
	candidateLogIsAtLeastAsUpToDate := m.LogTerm > ourLastLogTerm ||
		(m.LogTerm == ourLastLogTerm && m.Index >= ourLastLogIndex)

	// Grant the vote if we haven't voted yet (or already voted for this candidate)
	// and the candidate's log meets the up-to-date requirement.
	voteNotYetGrantedToAnother := r.Vote == None || r.Vote == m.From
	shouldGrantVote := voteNotYetGrantedToAnother && candidateLogIsAtLeastAsUpToDate

	if shouldGrantVote {
		r.Vote = m.From
		// Reset our election timer: we have just vouched for a candidate, so we
		// give it time to win before we start our own election.
		r.electionElapsed = 0
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  false,
		})
	} else {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			To:      m.From,
			From:    r.id,
			Term:    r.Term,
			Reject:  true,
		})
	}
}

// handleVoteResponse processes an incoming MsgRequestVoteResponse while this
// node is a candidate. It tallies granted and denied votes and transitions
// to leader or follower once a majority is reached in either direction.
func (r *Raft) handleVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject

	grantedVoteCount := 0
	deniedVoteCount := 0
	for _, voteGranted := range r.votes {
		if voteGranted {
			grantedVoteCount++
		} else {
			deniedVoteCount++
		}
	}

	majority := r.quorum()
	if grantedVoteCount >= majority {
		r.becomeLeader()
	} else if deniedVoteCount >= majority {
		// We lost the election; revert to follower and wait for the winner to
		// announce itself via MsgAppend or MsgHeartbeat.
		r.becomeFollower(r.Term, None)
	}
}

// -----------------------------------------------------------------------
// AppendEntries RPC handlers
// -----------------------------------------------------------------------

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Reset the election timer: we've received a message from the current leader.
	r.electionElapsed = 0
	r.Lead = m.From

	// Build the base response that we'll populate and enqueue.
	response := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	}

	// Check 1: we must have the entry at prevLogIndex.
	if m.Index > r.RaftLog.LastIndex() {
		// We don't have the entry the leader wants to anchor on.
		// Tell the leader our last index so it can back up Next appropriately.
		response.Reject = true
		response.Index = r.RaftLog.LastIndex()
		r.msgs = append(r.msgs, response)
		return
	}

	// Check 2: the term at prevLogIndex must match the leader's LogTerm.
	existingTermAtPrevIndex, err := r.RaftLog.Term(m.Index)
	if err != nil || existingTermAtPrevIndex != m.LogTerm {
		response.Reject = true
		// Hint: the leader can skip back past our last index in this term to
		// find the point of divergence faster.
		if m.Index > 0 {
			response.Index = m.Index - 1
		}
		r.msgs = append(r.msgs, response)
		return
	}

	// The consistency check passed. Now reconcile the incoming entries with
	// our local log, truncating at the first conflict.
	for i, incomingEntry := range m.Entries {
		if incomingEntry.Index <= r.RaftLog.LastIndex() {
			existingTerm, termErr := r.RaftLog.Term(incomingEntry.Index)
			if termErr != nil || existingTerm != incomingEntry.Term {
				// Found a conflict: the leader's entry disagrees with ours.
				// Truncate our log at this point (remove the conflicting entry
				// and everything after it).
				conflictSliceIndex := incomingEntry.Index - r.RaftLog.firstEntryIndex
				r.RaftLog.entries = r.RaftLog.entries[:conflictSliceIndex]

				// Regress stabled if we just discarded entries that had been
				// reported as stable to the application.
				if incomingEntry.Index <= r.RaftLog.stabled {
					r.RaftLog.stabled = incomingEntry.Index - 1
				}

				// Append the remainder of the leader's entries starting from
				// the conflict position.
				for _, entryToAppend := range m.Entries[i:] {
					r.RaftLog.entries = append(r.RaftLog.entries, *entryToAppend)
				}
				break
			}
			// Entry matches; no action needed, keep scanning.
		} else {
			// The incoming entry is beyond our current last index; append the
			// rest of the leader's entries directly.
			for _, entryToAppend := range m.Entries[i:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *entryToAppend)
			}
			break
		}
	}

	// Advance our commit index. Per the Raft paper (§5.3), the follower sets
	// commitIndex = min(leaderCommit, index of last new entry). The "last new
	// entry" is the last entry in this specific AppendEntries RPC, which equals
	// prevLogIndex (m.Index) when the RPC carries no entries. We must NOT
	// simply cap at our own LastIndex because that could commit entries the
	// leader hasn't yet acknowledged replicating to a majority.
	if m.Commit > r.RaftLog.committed {
		lastNewEntryIndex := m.Index // prevLogIndex when there are no new entries
		if len(m.Entries) > 0 {
			lastNewEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, lastNewEntryIndex)
	}

	// Report success: the leader uses response.Index to advance Prs[us].Match.
	response.Reject = false
	response.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, response)
}

// handleAppendResponse processes an incoming MsgAppendResponse from a follower.
// On success, it updates the follower's progress and attempts to advance the
// commit index. On rejection, it backs up Next and retries.
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Reject {
		// Back off Next so the next sendAppend will include the entry the
		// follower is missing. m.Index is the hint the follower sent back.
		r.Prs[m.From].Next = m.Index + 1
		r.sendAppend(m.From)
		return
	}

	// The follower successfully stored entries up to m.Index.
	// Only advance Match/Next if this response is not stale.
	if m.Index > r.Prs[m.From].Match {
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index + 1
		r.maybeAdvanceCommitIndex()
		// If this follower is the transfer target and has now caught up,
		// send MsgTimeoutNow so it starts the election immediately.
		if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			r.sendTimeoutNow(m.From)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	r.electionElapsed = 0
	r.Lead = m.From

	// Advance our commit index if the leader's is higher.
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
	})
}

// handleSnapshot processes an incoming MsgSnapshot from the leader. The Raft
// layer's job is lightweight: record the snapshot as pending so it flows
// through the next Ready, then fast-forward all log pointers to the snapshot's
// index. The actual data ingestion (SST files) is handled by the raftstore
// layer after it reads the snapshot out of Ready.
func (r *Raft) handleSnapshot(m pb.Message) {
	snapshotMetadata := m.Snapshot.Metadata
	snapshotIndex := snapshotMetadata.Index
	snapshotTerm := snapshotMetadata.Term

	// If this snapshot is older than (or equal to) what we have already
	// committed, ignore it. Send an AppendResponse so the leader learns our
	// current index and stops sending stale snapshots.
	if snapshotIndex <= r.RaftLog.committed {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Index:   r.RaftLog.committed,
		})
		return
	}

	// Store the snapshot so RawNode.Ready() can include it for the raftstore
	// to persist. Once the raftstore calls Advance(), pendingSnapshot is cleared.
	// m.Snapshot is already a *pb.Snapshot, so assign it directly.
	r.RaftLog.pendingSnapshot = m.Snapshot

	// Fast-forward all log indices to the snapshot point. Every entry up to
	// snapshotIndex is now covered by the snapshot and no longer needed.
	r.RaftLog.committed = snapshotIndex
	r.RaftLog.applied = snapshotIndex
	r.RaftLog.stabled = snapshotIndex

	// Drop all in-memory entries — they are all superseded by the snapshot.
	// firstEntryIndex is set to the first slot AFTER the snapshot.
	r.RaftLog.entries = nil
	r.RaftLog.firstEntryIndex = snapshotIndex + 1

	// Rebuild the peer progress map from the snapshot's configuration so that
	// peers added or removed since our last known state are accounted for.
	r.Prs = make(map[uint64]*Progress)
	for _, peerID := range snapshotMetadata.ConfState.Nodes {
		r.Prs[peerID] = &Progress{
			Next:  snapshotIndex + 1,
			Match: 0,
		}
	}

	// Acknowledge the snapshot so the leader knows we are now caught up to
	// snapshotIndex and can update our progress entry.
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Index:   snapshotIndex,
		LogTerm: snapshotTerm,
	})
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, exists := r.Prs[id]; !exists {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	delete(r.Prs, id)
	// Removing a peer shrinks the quorum; re-check the commit index in case
	// entries that were just short of a majority are now committable.
	if r.State == StateLeader {
		r.maybeAdvanceCommitIndex()
	}
}
