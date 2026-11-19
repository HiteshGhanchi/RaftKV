package raft

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"raftlab/mutation"
	"raftlab/sim"
)

// testNet runs a few nodes in lock step: no clock, no randomness in delivery.
// Messages are delivered in FIFO order unless a link is cut.
type testNet struct {
	t       *testing.T
	nodes   map[NodeID]*Node
	ids     []NodeID
	cut     map[[2]NodeID]bool
	queue   []Message
	applied map[NodeID][]Entry
	stored  map[NodeID]HardState
}

func cfgFor(id NodeID, peers []NodeID) Config {
	return Config{ID: id, Peers: peers, ElectionTick: 10, HeartbeatTick: 1, Rand: sim.NewRand(id)}
}

func newTestNet(t *testing.T, n int, edit func(*Config)) *testNet {
	tn := &testNet{t: t, nodes: map[NodeID]*Node{}, cut: map[[2]NodeID]bool{},
		applied: map[NodeID][]Entry{}, stored: map[NodeID]HardState{}}
	for i := 1; i <= n; i++ {
		tn.ids = append(tn.ids, NodeID(i))
	}
	for _, id := range tn.ids {
		c := cfgFor(id, tn.ids)
		if edit != nil {
			edit(&c)
		}
		tn.nodes[id] = New(c)
	}
	return tn
}

func (tn *testNet) isolate(id NodeID) {
	for _, o := range tn.ids {
		if o != id {
			tn.cut[[2]NodeID{id, o}] = true
			tn.cut[[2]NodeID{o, id}] = true
		}
	}
}

func (tn *testNet) heal() { tn.cut = map[[2]NodeID]bool{} }

// drain processes Ready on every node and delivers messages until quiet.
func (tn *testNet) drain() {
	for range 10_000 {
		busy := false
		for _, id := range tn.ids {
			n := tn.nodes[id]
			for n.HasReady() {
				busy = true
				rd := n.Ready()
				tn.stored[id] = rd.HardState
				for _, m := range rd.Messages {
					if !tn.cut[[2]NodeID{m.From, m.To}] {
						tn.queue = append(tn.queue, m)
					}
				}
				tn.applied[id] = append(tn.applied[id], rd.CommittedEntries...)
				n.Advance(rd)
			}
		}
		q := tn.queue
		tn.queue = nil
		for _, m := range q {
			busy = true
			tn.nodes[m.To].Step(m)
		}
		if !busy {
			return
		}
	}
	tn.t.Fatal("network never went quiet")
}

func (tn *testNet) leader() NodeID {
	for _, id := range tn.ids {
		if tn.nodes[id].state == Leader {
			return id
		}
	}
	return None
}

func terms(ents []Entry) []uint64 {
	out := make([]uint64, len(ents))
	for i, e := range ents {
		out[i] = e.Term
	}
	return out
}

func logOf(ts ...uint64) []Entry {
	out := make([]Entry, len(ts))
	for i, t := range ts {
		out[i] = Entry{Term: t, Index: uint64(i + 1), Data: []byte(fmt.Sprint(i + 1))}
	}
	return out
}

// ------------------------------------------------------------ elections

// Figure 2, Followers: an election timeout with no leader starts an election.
func TestFollowerCampaignsAfterElectionTimeout(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	n := tn.nodes[1]
	for range 2 * n.cfg.ElectionTick {
		n.Tick()
		if n.state == Candidate {
			break
		}
	}
	if n.state != Candidate || n.term != 1 || n.vote != 1 {
		t.Fatalf("after timeout: state=%v term=%d vote=%d", n.state, n.term, n.vote)
	}
	tn.drain()
	if tn.leader() != 1 {
		t.Fatalf("leader = %d, want 1", tn.leader())
	}
}

// Figure 2, Candidates: a majority of votes makes a leader, which appends a
// no-op from its term (thesis §6.4).
func TestCandidateWithMajorityBecomesLeader(t *testing.T) {
	tn := newTestNet(t, 5, nil)
	tn.isolate(4)
	tn.isolate(5)
	tn.nodes[1].Campaign()
	tn.drain()
	l := tn.nodes[1]
	if l.state != Leader {
		t.Fatalf("node 1 with 3/5 votes is %v", l.state)
	}
	if e := l.Entries()[0]; e.Type != EntryNoop || e.Term != l.term {
		t.Fatalf("first leader entry = %+v, want a no-op in term %d", e, l.term)
	}
	if l.log.committed != 1 {
		t.Fatalf("no-op not committed by the majority: commit=%d", l.log.committed)
	}
}

func TestCandidateWithoutMajorityStaysCandidate(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.isolate(1)
	tn.nodes[1].Campaign()
	tn.drain()
	if tn.nodes[1].state != Candidate {
		t.Fatalf("isolated candidate is %v", tn.nodes[1].state)
	}
}

// RequestVote receiver rule 2: at most one vote per term (Election Safety).
func TestVoteGrantedOncePerTerm(t *testing.T) {
	n := New(cfgFor(1, []NodeID{1, 2, 3}))
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 1})
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1}) // retry from the same candidate is fine
	got := []bool{}
	for _, m := range n.msgs {
		got = append(got, m.Reject)
	}
	if !slices.Equal(got, []bool{false, true, false}) {
		t.Fatalf("rejects = %v, want [false true false]", got)
	}
	if rd := n.Ready(); !rd.MustSync || rd.HardState.Vote != 2 {
		t.Fatalf("vote not handed out for persistence: %+v", rd.HardState)
	}
}

// §5.4.1: deny a vote to a candidate whose log is less up-to-date.
func TestVoteRejectedForStaleLog(t *testing.T) {
	c := cfgFor(1, []NodeID{1, 2, 3})
	c.Entries = logOf(1, 2)
	c.HardState = HardState{Term: 2}
	cases := []struct {
		lastTerm, lastIndex uint64
		reject              bool
	}{
		{1, 5, true},  // older last term loses even if longer
		{2, 1, true},  // same term, shorter
		{2, 2, false}, // identical
		{3, 1, false}, // newer last term wins even if shorter
	}
	for _, tc := range cases {
		n := New(c)
		n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogTerm: tc.lastTerm, Index: tc.lastIndex})
		if got := n.msgs[0].Reject; got != tc.reject {
			t.Errorf("candidate last (t%d,i%d): reject=%v want %v", tc.lastTerm, tc.lastIndex, got, tc.reject)
		}
	}
	c.Mutations = mutation.SkipUpToDateCheck
	n := New(c)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, LogTerm: 1, Index: 5})
	if n.msgs[0].Reject {
		t.Error("mutation skip-up-to-date-check should grant the stale vote")
	}
}

// Both RPCs, rule 1: reply false if term < currentTerm, carrying our term.
func TestStaleTermRequestsRejected(t *testing.T) {
	c := cfgFor(1, []NodeID{1, 2, 3})
	c.HardState = HardState{Term: 5}
	n := New(c)
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4, LogTerm: 9, Index: 9})
	n.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 4, Entries: logOf(4)})
	for _, m := range n.msgs {
		if !m.Reject || m.Term != 5 {
			t.Fatalf("reply %v should reject with term 5", m)
		}
	}
	if n.log.lastIndex() != 0 {
		t.Fatal("stale AppendEntries modified the log")
	}
}

// All Servers rule 2: a higher term makes a leader step down.
func TestHigherTermMakesLeaderStepDown(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.drain()
	l := tn.nodes[1]
	l.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: l.term + 3, Reject: true})
	if l.state != Follower || l.term != 4 {
		t.Fatalf("leader saw term 4: state=%v term=%d", l.state, l.term)
	}
	if _, _, err := l.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose on deposed leader: %v", err)
	}
}

// ------------------------------------------------------------ replication

// AppendEntries receiver rules 2-5.
func TestAppendEntriesReceiverRules(t *testing.T) {
	base := func() *raftLog { return newLog(nil, logOf(1, 1, 2), 1) }

	l := base()
	if _, ok := l.maybeAppend(3, 9, 3, nil); ok {
		t.Error("rule 2: accepted a prevLogTerm mismatch")
	}
	if _, ok := l.maybeAppend(5, 2, 3, nil); ok {
		t.Error("rule 2: accepted a prevLogIndex past the end")
	}

	l = base()
	last, ok := l.maybeAppend(1, 1, 1, []Entry{{Term: 3, Index: 2}, {Term: 3, Index: 3}})
	if !ok || last != 3 || !slices.Equal(terms(l.entries), []uint64{1, 3, 3}) {
		t.Errorf("rule 3/4: got ok=%v last=%d terms=%v, want [1 3 3]", ok, last, terms(l.entries))
	}

	l = base()
	l.maybeAppend(3, 2, 2, []Entry{{Term: 2, Index: 4}})
	l.maybeAppend(1, 1, 2, []Entry{{Term: 1, Index: 2}}) // reordered old message
	if l.lastIndex() != 4 {
		t.Error("a delayed duplicate AppendEntries truncated a matching log")
	}

	l = base()
	l.maybeAppend(3, 2, 99, []Entry{{Term: 2, Index: 4}})
	if l.committed != 4 {
		t.Errorf("rule 5: commit = %d, want min(99, 4) = 4", l.committed)
	}
}

func TestCommittedEntryNeverTruncated(t *testing.T) {
	l := newLog(nil, logOf(1, 1, 2), 3)
	defer func() {
		e, ok := recover().(*InvariantError)
		if !ok || e.Name != "CommittedNeverTruncated" {
			t.Fatalf("recovered %v, want CommittedNeverTruncated", e)
		}
	}()
	l.maybeAppend(1, 1, 3, []Entry{{Term: 5, Index: 2}})
}

func TestReplicationCommitsAndApplies(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.drain()
	for i := range 5 {
		if _, _, err := tn.nodes[1].Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	tn.drain()
	// One more heartbeat round spreads the final commit index.
	tn.nodes[1].bcastHeartbeat()
	tn.drain()
	for _, id := range tn.ids {
		ents := tn.applied[id]
		if len(ents) != 6 { // no-op + 5 commands
			t.Fatalf("node %d applied %d entries, want 6", id, len(ents))
		}
	}
}

// Figure 7: a new leader brings followers with missing, extra or conflicting
// entries back in line with its own log.
func TestFigure7FollowersConverge(t *testing.T) {
	leaderLog := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	followers := map[string][]uint64{
		"a": {1, 1, 1, 4, 4, 5, 5, 6, 6},
		"b": {1, 1, 1, 4},
		"c": {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6},
		"d": {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7},
		"e": {1, 1, 1, 4, 4, 4, 4},
		"f": {1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3},
	}
	for name, flog := range followers {
		t.Run(name, func(t *testing.T) {
			tn := newTestNet(t, 3, func(c *Config) {
				c.HardState = HardState{Term: 7}
				switch c.ID {
				case 1, 3:
					c.Entries = logOf(leaderLog...)
				case 2:
					c.Entries = logOf(flog...)
				}
			})
			tn.nodes[1].Campaign() // term 8; node 3 votes for it
			tn.drain()
			if tn.leader() != 1 {
				t.Fatal("node 1 not elected")
			}
			want := append(slices.Clone(leaderLog), 8) // plus the term-8 no-op
			if got := terms(tn.nodes[2].Entries()); !slices.Equal(got, want) {
				t.Fatalf("follower %s log terms = %v, want %v", name, got, want)
			}
		})
	}
}

// Figure 8 / §5.4.2: a leader must not commit an entry from an earlier term
// just because a majority stores it.
func TestFigure8LeaderDoesNotCommitOldTermByCounting(t *testing.T) {
	for _, mut := range []mutation.Set{0, mutation.CommitPrevTerm} {
		c := cfgFor(1, []NodeID{1, 2, 3, 4, 5})
		c.Mutations = mut
		c.Entries = logOf(1, 2)
		c.HardState = HardState{Term: 4, Commit: 1}
		n := New(c)
		n.becomeLeader() // term 4, appends no-op at index 3
		n.prs[1].match = 2
		n.prs[2].match = 2
		n.prs[3].match = 2 // index 2 (term 2) is now on 3 of 5 servers
		committed := n.maybeCommit()
		if mut == 0 && (committed || n.log.committed != 1) {
			t.Fatalf("correct code committed the term-2 entry: commit=%d", n.log.committed)
		}
		if mut != 0 && n.log.committed != 2 {
			t.Fatalf("mutation should commit index 2 by counting, commit=%d", n.log.committed)
		}
		if mut == 0 {
			// Once the term-4 entry is on a majority, both commit.
			n.prs[1].match, n.prs[2].match, n.prs[3].match = 3, 3, 3
			if !n.maybeCommit() || n.log.committed != 3 {
				t.Fatalf("current-term entry on a majority did not commit: %d", n.log.committed)
			}
		}
	}
}

// A follower that lost its log suffix is backed up quickly using the hint.
func TestLeaderBacksUpWithHint(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.drain()
	tn.isolate(3)
	for range 50 {
		tn.nodes[1].Propose([]byte("x"))
		tn.drain()
	}
	tn.heal()
	tn.nodes[1].bcastHeartbeat()
	tn.drain()
	if got, want := tn.nodes[3].log.lastIndex(), tn.nodes[1].log.lastIndex(); got != want {
		t.Fatalf("follower last index %d, leader %d", got, want)
	}
}

// ------------------------------------------------------------ reads

// ReadIndex (thesis §6.4) needs a heartbeat round acknowledged by a majority.
func TestReadIndexNeedsQuorum(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.drain()
	l := tn.nodes[1]

	tn.isolate(1)
	l.ReadIndex(77)
	tn.drain()
	if len(l.readStates) != 0 || len(l.reads) != 1 {
		t.Fatal("isolated leader released a read without a quorum")
	}
	tn.heal()
	var got []ReadState
	l.needHeartbeat = true
	for l.HasReady() {
		rd := l.Ready()
		got = append(got, rd.ReadStates...)
		for _, m := range rd.Messages {
			tn.queue = append(tn.queue, m)
		}
		l.Advance(rd)
	}
	q := tn.queue
	tn.queue = nil
	for _, m := range q {
		tn.nodes[m.To].Step(m)
	}
	for _, id := range []NodeID{2, 3} {
		rd := tn.nodes[id].Ready()
		for _, m := range rd.Messages {
			if m.Type == MsgHeartbeatResp {
				l.Step(m)
			}
		}
		tn.nodes[id].Advance(rd)
	}
	got = append(got, l.Ready().ReadStates...)
	if len(got) != 1 || got[0].Context != 77 || got[0].Index != l.log.committed {
		t.Fatalf("read states = %+v, want ctx 77 at commit %d", got, l.log.committed)
	}
}

func TestReadIndexRejectedOnFollower(t *testing.T) {
	n := New(cfgFor(1, []NodeID{1, 2, 3}))
	if err := n.ReadIndex(1); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err = %v", err)
	}
}

// ------------------------------------------------------------ snapshots

func TestSnapshotSentToLaggingFollower(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.nodes[1].Campaign()
	tn.drain()
	tn.isolate(3)
	for range 10 {
		tn.nodes[1].Propose([]byte("x"))
		tn.drain()
	}
	l := tn.nodes[1]
	snap, rest := l.Compact(l.log.applied, []byte("state"))
	if snap.Index != l.log.applied || len(rest) != int(l.log.stable-snap.Index) {
		t.Fatalf("compact: snap=%+v rest=%d", snap, len(rest))
	}
	tn.heal()
	l.bcastHeartbeat()
	tn.drain()
	f := tn.nodes[3]
	if f.log.snapIndex != snap.Index || string(f.log.snapshot.Data) != "state" {
		t.Fatalf("follower snapIndex=%d, want %d", f.log.snapIndex, snap.Index)
	}
	if f.log.lastIndex() != l.log.lastIndex() {
		t.Fatalf("follower last=%d leader last=%d", f.log.lastIndex(), l.log.lastIndex())
	}
}

func TestRestartRestoresState(t *testing.T) {
	c := cfgFor(1, []NodeID{1, 2, 3})
	c.Snapshot = &Snapshot{Index: 2, Term: 1}
	c.Entries = logOf(1, 1, 2, 2)
	c.HardState = HardState{Term: 3, Vote: 2, Commit: 3}
	n := New(c)
	st := n.Status()
	if st.Term != 3 || st.Vote != 2 || st.Commit != 3 || st.SnapIndex != 2 || st.LastIndex != 4 {
		t.Fatalf("restored status %+v", st)
	}
	rd := n.Ready()
	if len(rd.CommittedEntries) != 1 || rd.CommittedEntries[0].Index != 3 {
		t.Fatalf("restart should re-apply entry 3, got %+v", rd.CommittedEntries)
	}
}

func TestSingleNodeCommitsAlone(t *testing.T) {
	n := New(cfgFor(1, []NodeID{1}))
	n.Campaign()
	n.Propose([]byte("a"))
	var applied []Entry
	for n.HasReady() {
		rd := n.Ready()
		applied = append(applied, rd.CommittedEntries...)
		n.Advance(rd)
	}
	if len(applied) != 2 {
		t.Fatalf("single node applied %d entries, want 2", len(applied))
	}
}
