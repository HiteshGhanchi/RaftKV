package raft

// raftLog is the in-memory log: an optional snapshot followed by entries.
//
//	snapIndex                    stable           lastIndex
//	    |  entries[0] ... (durable)  |  (unstable)    |
//
// Entries up to stable have been written to disk by the caller; entries after
// it are handed out in the next Ready. committed and applied are the two
// indexes from Figure 2's "volatile state on all servers".
type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	entries   []Entry // entries[i].Index == snapIndex+1+i

	committed uint64
	applied   uint64
	stable    uint64

	snapshot        *Snapshot // latest snapshot, sent to followers that fell behind it
	pendingSnapshot *Snapshot // received from a leader, not yet handed to the caller
}

func newLog(snap *Snapshot, ents []Entry, commit uint64) *raftLog {
	l := &raftLog{}
	if snap != nil {
		l.snapIndex, l.snapTerm, l.snapshot = snap.Index, snap.Term, snap
	}
	for _, e := range ents {
		if e.Index > l.snapIndex {
			l.entries = append(l.entries, e)
		}
	}
	for i, e := range l.entries {
		invariant(e.Index == l.snapIndex+1+uint64(i), "LogContiguous", "restored log has a gap at index %d", e.Index)
	}
	l.stable = l.lastIndex()
	l.committed = max(min(commit, l.lastIndex()), l.snapIndex)
	l.applied = l.snapIndex
	return l
}

func (l *raftLog) lastIndex() uint64 { return l.snapIndex + uint64(len(l.entries)) }

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index i. ok is false when i is beyond
// the log or already compacted into the snapshot.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.snapIndex:
		return l.snapTerm, true
	case i < l.snapIndex || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.snapIndex-1].Term, true
	}
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	got, ok := l.term(i)
	return ok && got == t
}

// slice returns a copy of entries [lo, hi). lo must be past the snapshot.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo >= hi {
		return nil
	}
	invariant(lo > l.snapIndex && hi <= l.lastIndex()+1, "LogBounds", "slice [%d,%d) outside (%d,%d]", lo, hi, l.snapIndex, l.lastIndex())
	out := make([]Entry, hi-lo)
	copy(out, l.entries[lo-l.snapIndex-1:hi-l.snapIndex-1])
	return out
}

// isUpToDate implements §5.4.1: a log with a later last term is more
// up-to-date; with equal last terms, the longer log is.
func (l *raftLog) isUpToDate(lastTerm, lastIndex uint64) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

// append adds entries at the end (leader path).
func (l *raftLog) append(ents ...Entry) {
	l.entries = append(l.entries, ents...)
}

// maybeAppend is the AppendEntries receiver, Figure 2 rules 2-5. It returns
// the index of the last entry covered by the request and whether the
// consistency check passed.
func (l *raftLog) maybeAppend(prevIndex, prevTerm, leaderCommit uint64, ents []Entry) (uint64, bool) {
	// Rule 2: reply false if the log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm. An entry inside the snapshot is
	// committed, so it matches by Log Matching.
	if prevIndex >= l.snapIndex && !l.matchTerm(prevIndex, prevTerm) {
		return 0, false
	}
	lastNew := prevIndex + uint64(len(ents))
	for i, e := range ents {
		if e.Index <= l.snapIndex {
			continue
		}
		t, ok := l.term(e.Index)
		if ok && t == e.Term {
			continue // already have it
		}
		if ok {
			// Rule 3: an existing entry conflicts with a new one (same index,
			// different term): delete it and all that follow. A committed
			// entry must never be deleted (State Machine Safety).
			invariant(e.Index > l.committed, "CommittedNeverTruncated",
				"truncating index %d at or below commit %d", e.Index, l.committed)
			l.entries = l.entries[:e.Index-l.snapIndex-1]
			l.stable = min(l.stable, e.Index-1)
		}
		// Rule 4: append any new entries not already in the log.
		l.append(ents[i:]...)
		break
	}
	// Rule 5: if leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry).
	l.commitTo(min(leaderCommit, lastNew))
	return lastNew, true
}

// commitTo advances the commit index; it never moves backwards.
func (l *raftLog) commitTo(i uint64) {
	if i > l.committed {
		invariant(i <= l.lastIndex(), "CommitWithinLog", "commit %d beyond last index %d", i, l.lastIndex())
		l.committed = i
	}
}

// unstableEntries are the entries the caller has not persisted yet.
func (l *raftLog) unstableEntries() []Entry {
	lo := max(l.stable, l.snapIndex) + 1
	return l.slice(lo, l.lastIndex()+1)
}

// applyLimit is the highest index the state machine may apply.
func (l *raftLog) applyLimit(applyUncommitted bool) uint64 {
	if applyUncommitted {
		return l.lastIndex() // mutation: All Servers rule 1 broken
	}
	return l.committed
}

func (l *raftLog) nextCommitted(applyUncommitted bool) []Entry {
	lo := max(l.applied, l.snapIndex) + 1
	return l.slice(lo, l.applyLimit(applyUncommitted)+1)
}

// compact discards entries up to index, which the caller has already captured
// in a state machine snapshot (thesis §5.1).
func (l *raftLog) compact(index uint64, data []byte) *Snapshot {
	t, ok := l.term(index)
	invariant(ok && index <= l.applied, "CompactApplied", "compacting %d (applied %d, snap %d)", index, l.applied, l.snapIndex)
	l.entries = append([]Entry(nil), l.entries[index-l.snapIndex:]...)
	l.snapIndex, l.snapTerm = index, t
	l.snapshot = &Snapshot{Index: index, Term: t, Data: data}
	return l.snapshot
}

// restore installs a snapshot from the leader (Figure 13 receiver rules 6-7):
// if the log already has the snapshot's last entry, keep what follows it;
// otherwise throw the whole log away.
func (l *raftLog) restore(s *Snapshot) {
	if l.matchTerm(s.Index, s.Term) && s.Index > l.snapIndex {
		l.entries = append([]Entry(nil), l.entries[s.Index-l.snapIndex:]...)
	} else {
		l.entries = nil
		l.stable = s.Index
	}
	l.snapIndex, l.snapTerm = s.Index, s.Term
	l.stable = max(l.stable, s.Index)
	l.committed = max(l.committed, s.Index)
	l.snapshot = s
	l.pendingSnapshot = s
}
