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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// firstEntryIndex is the log index that entries[0] corresponds to.
	// All entries with log index < firstEntryIndex have been compacted into a snapshot.
	// Invariant: when entries is non-empty, entries[0].Index == firstEntryIndex.
	firstEntryIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// Load all stable entries from storage into our in-memory slice.
	// When lastIndex < firstIndex the storage is empty (only the dummy entry exists),
	// so there are no real entries to load.
	var loadedEntries []pb.Entry
	if lastIndex >= firstIndex {
		loadedEntries, err = storage.Entries(firstIndex, lastIndex+1)
		if err != nil {
			panic(err)
		}
	}

	return &RaftLog{
		storage: storage,
		// committed comes from the persisted hard state so we restore the
		// highest committed index that was known to a quorum before any crash.
		committed: hardState.Commit,
		// applied starts at the snapshot index (firstIndex - 1). It will be
		// overridden by config.Applied in newRaft when restarting from a saved state.
		applied:         firstIndex - 1,
		stabled:         lastIndex,
		entries:         loadedEntries,
		firstEntryIndex: firstIndex,
	}
}

// maybeCompact trims the in-memory entries slice to remove any entries whose
// log index now falls below storage.FirstIndex(). This happens when a snapshot
// has been applied: the storage layer advances its truncated index, so entries
// that predate the snapshot are no longer needed in memory.
//
// It is safe to call this when entries is already nil or already trimmed.
func (l *RaftLog) maybeCompact() {
	newFirstIndex, err := l.storage.FirstIndex()
	if err != nil {
		// If storage cannot report its first index, leave entries unchanged.
		return
	}

	// Nothing to trim if entries is empty or already starts at/after newFirstIndex.
	if len(l.entries) == 0 || l.firstEntryIndex >= newFirstIndex {
		return
	}

	// Compute how many entries at the front have been compacted into the snapshot.
	numberOfCompactedEntries := newFirstIndex - l.firstEntryIndex

	if numberOfCompactedEntries >= uint64(len(l.entries)) {
		// All in-memory entries are now below the storage's first index.
		l.entries = nil
	} else {
		// Keep only the entries starting at newFirstIndex.
		l.entries = l.entries[numberOfCompactedEntries:]
	}

	l.firstEntryIndex = newFirstIndex
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// The entries slice was loaded starting from storage.FirstIndex(), so it
	// never contains the storage dummy entry (which lives at index 0 or the
	// snapshot metadata index). Returning the slice directly is correct.
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}

	// Unstable entries are those whose index is strictly greater than stabled.
	// Because entries[0].Index == firstEntryIndex, the slice position of the
	// first unstable entry is (stabled + 1 - firstEntryIndex).
	if l.stabled+1 < l.firstEntryIndex {
		// This can only happen if stabled was regressed during a conflict
		// truncation; in that case, all entries are unstable.
		return l.entries
	}

	unstableStartSliceIndex := l.stabled + 1 - l.firstEntryIndex
	// Return a non-nil slice even when it is empty. Tests (and callers such as
	// RawNode.Ready) rely on reflect.DeepEqual comparisons where nil != []pb.Entry{}.
	return l.entries[unstableStartSliceIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed <= l.applied {
		return nil
	}

	// Entries in the range (applied, committed] need to be handed to the
	// application. Convert log indices to slice indices using firstEntryIndex.
	startSliceIndex := l.applied + 1 - l.firstEntryIndex
	endSliceIndex := l.committed + 1 - l.firstEntryIndex

	if endSliceIndex > uint64(len(l.entries)) {
		endSliceIndex = uint64(len(l.entries))
	}

	return l.entries[startSliceIndex:endSliceIndex]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	// If there are no in-memory entries but firstEntryIndex > 0, we have
	// compacted to a snapshot. The last index is firstEntryIndex - 1 (the
	// snapshot index). This handles the case where handleSnapshot cleared
	// entries and set firstEntryIndex before the storage was updated.
	if l.firstEntryIndex > 0 {
		return l.firstEntryIndex - 1
	}
	// Otherwise fall back to storage.
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return lastIndex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Fast path: check our in-memory entries first.
	if len(l.entries) > 0 && i >= l.firstEntryIndex {
		sliceIndex := i - l.firstEntryIndex
		if sliceIndex < uint64(len(l.entries)) {
			return l.entries[sliceIndex].Term, nil
		}
	}

	// If we have a pending snapshot, the term at the snapshot index is
	// available from the snapshot metadata. Indices below it are compacted.
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata != nil {
		snapIdx := l.pendingSnapshot.Metadata.Index
		if i == snapIdx {
			return l.pendingSnapshot.Metadata.Term, nil
		}
		if i < snapIdx {
			return 0, ErrCompacted
		}
	}

	// Fall back to storage for entries that have been stabilised there
	// (including the compaction dummy entry at firstIndex-1).
	term, err := l.storage.Term(i)
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	return term, err
}
