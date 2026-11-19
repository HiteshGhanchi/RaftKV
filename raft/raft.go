package raft

import (
	"errors"
	"math/rand/v2"
	"slices"

	"raftlab/mutation"
)

// ErrNotLeader is returned by Propose and ReadIndex on a non-leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Config creates a Node. The restored fields come from storage after a restart.
type Config struct {
	ID    NodeID
	Peers []NodeID // every member, including ID

	// ElectionTick is the minimum election timeout in ticks; each timeout is
	// drawn uniformly from [ElectionTick, 2*ElectionTick) (paper §5.2).
	ElectionTick int
	// HeartbeatTick is how often a leader sends heartbeats, in ticks.
	HeartbeatTick int
	// MaxEntriesPerMsg caps how many entries one MsgApp carries.
	MaxEntriesPerMsg int

	// Rand drives the randomised election timeout. Required: it is how the
	// simulator keeps elections deterministic.
	Rand *rand.Rand

	HardState HardState
	Entries   []Entry
	Snapshot  *Snapshot

	Mutations mutation.Set
}

// progress is the leader's view of one follower (Figure 2: nextIndex and
// matchIndex), plus flow-control state borrowed from etcd's design.
type progress struct {
	match, next uint64
	// probing: we don't know where the follower's log matches, so send one
	// MsgApp at a time and wait. Otherwise (replicating) send optimistically.
	probing bool
	paused  bool // probing and waiting for a reply
	active  bool // heard from since the last CheckQuorum round
	readAck uint64
}

type pendingRead struct {
	seq   uint64
	ctx   uint64
	index uint64 // 0 until the leader has committed an entry in its term
}

// Node is one Raft server's protocol state.
type Node struct {
	id    NodeID
	peers []NodeID // sorted; iterate this, never a map, to stay deterministic
	cfg   Config
	mut   mutation.Set

	// Persistent state (Figure 2).
	term uint64
	vote NodeID
	log  *raftLog

	state StateType
	lead  NodeID
	prs   map[NodeID]*progress
	votes map[NodeID]bool

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int // randomised, in ticks

	msgs          []Message
	needBcast     bool
	readSeq       uint64
	reads         []pendingRead
	readStates    []ReadState
	needHeartbeat bool
	prevHardState HardState
}

// New creates a node from a configuration and any state restored from disk.
func New(cfg Config) *Node {
	invariant(cfg.Rand != nil, "Config", "Config.Rand is required")
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.MaxEntriesPerMsg == 0 {
		cfg.MaxEntriesPerMsg = 256
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	n := &Node{
		id:    cfg.ID,
		peers: peers,
		cfg:   cfg,
		mut:   cfg.Mutations,
		term:  cfg.HardState.Term,
		vote:  cfg.HardState.Vote,
		log:   newLog(cfg.Snapshot, cfg.Entries, cfg.HardState.Commit),
	}
	n.prevHardState = n.hardState()
	n.becomeFollower(n.term, None)
	return n
}

func (n *Node) quorum() int { return len(n.peers)/2 + 1 }

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, Vote: n.vote, Commit: n.log.committed}
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	return Status{
		ID: n.id, State: n.state, Term: n.term, Vote: n.vote, Lead: n.lead,
		Commit: n.log.committed, Applied: n.log.applied,
		LastIndex: n.log.lastIndex(), SnapIndex: n.log.snapIndex,
	}
}

// Entries returns a copy of the in-memory log after the snapshot (debugging and invariant checks).
func (n *Node) Entries() []Entry { return n.log.slice(n.log.snapIndex+1, n.log.lastIndex()+1) }

// Term returns the term of the entry at index i, if the log still has it.
func (n *Node) Term(i uint64) (uint64, bool) { return n.log.term(i) }

func (n *Node) send(m Message) {
	m.From = n.id
	if m.Term == 0 {
		m.Term = n.term
	}
	n.msgs = append(n.msgs, m)
}

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.electionTimeout = n.cfg.ElectionTick + n.cfg.Rand.IntN(n.cfg.ElectionTick)
}

// ---------------------------------------------------------------- roles

func (n *Node) becomeFollower(term uint64, lead NodeID) {
	if term > n.term {
		n.term = term
		n.vote = None
	}
	n.state = Follower
	n.lead = lead
	n.dropReads()
	n.resetElectionTimer()
}

// campaign starts an election. Figure 2, Candidates: increment currentTerm,
// vote for self, reset the election timer, send RequestVote to all others.
func (n *Node) campaign() {
	n.state = Candidate
	n.term++
	n.vote = n.id
	n.lead = None
	n.dropReads()
	n.votes = map[NodeID]bool{n.id: true}
	n.resetElectionTimer()
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.peers {
		if p != n.id {
			n.send(Message{Type: MsgVote, To: p, LogTerm: n.log.lastTerm(), Index: n.log.lastIndex()})
		}
	}
}

// becomeLeader: Figure 2, Leaders: reinitialise nextIndex to last log index
// + 1 and matchIndex to 0, then send heartbeats. We also append a no-op entry
// from the new term (thesis §6.4) so earlier entries can be committed.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.lead = n.id
	n.prs = map[NodeID]*progress{}
	for _, p := range n.peers {
		n.prs[p] = &progress{next: n.log.lastIndex() + 1, probing: true, active: true}
	}
	n.prs[n.id].match = n.log.stable
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.log.append(Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: EntryNoop})
	n.bcastAppend()
}

// ---------------------------------------------------------------- inputs

// Tick advances logical time by one tick.
func (n *Node) Tick() {
	if n.state == Leader {
		n.heartbeatElapsed++
		n.electionElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.bcastHeartbeat()
		}
		if n.electionElapsed >= n.cfg.ElectionTick {
			n.electionElapsed = 0
			n.checkQuorum()
		}
		return
	}
	// Figure 2, Followers: if the election timeout elapses without hearing
	// from a current leader or granting a vote, become a candidate.
	n.electionElapsed++
	if n.electionElapsed >= n.electionTimeout {
		n.campaign()
	}
}

// checkQuorum makes a leader step down if it has not heard from a majority
// for an election timeout (thesis §6.2). This is for liveness: clients find
// the real leader sooner. Safety never depends on it.
func (n *Node) checkQuorum() {
	active := 0
	for _, p := range n.peers {
		pr := n.prs[p]
		if p == n.id || pr.active {
			active++
		}
		pr.active = false
	}
	if active < n.quorum() {
		n.becomeFollower(n.term, None)
	}
}

// Campaign forces an election now (tests and leadership transfer).
func (n *Node) Campaign() {
	if n.state != Leader {
		n.campaign()
	}
}

// Propose appends a command to the leader's log. It returns the index and
// term the entry will have if it commits.
func (n *Node) Propose(data []byte) (index, term uint64, err error) {
	if n.state != Leader {
		return 0, 0, ErrNotLeader
	}
	e := Entry{Term: n.term, Index: n.log.lastIndex() + 1, Type: EntryNormal, Data: data}
	n.log.append(e)
	n.needBcast = true // batched: one MsgApp per follower per Ready
	return e.Index, e.Term, nil
}

// ReadIndex asks to serve a linearizable read without writing to the log
// (thesis §6.4). When a ReadState with the same ctx appears in a Ready, the
// read may be served once the state machine has applied up to its Index.
func (n *Node) ReadIndex(ctx uint64) error {
	if n.state != Leader {
		return ErrNotLeader
	}
	if n.mut.Has(mutation.StaleRead) {
		n.readStates = append(n.readStates, ReadState{Index: n.log.committed, Context: ctx})
		return nil
	}
	n.readSeq++
	r := pendingRead{seq: n.readSeq, ctx: ctx}
	// Step 1 of ReadIndex: the leader must have committed an entry from its
	// own term; until then its commit index may be behind.
	if n.committedInTerm() {
		r.index = n.log.committed
	}
	n.reads = append(n.reads, r)
	// Step 2: confirm leadership with a round of heartbeats.
	n.needHeartbeat = true
	n.processReads()
	return nil
}

func (n *Node) committedInTerm() bool {
	t, ok := n.log.term(n.log.committed)
	return ok && t == n.term
}

func (n *Node) dropReads() { n.reads = nil }

// processReads releases reads whose heartbeat round reached a majority.
func (n *Node) processReads() {
	for len(n.reads) > 0 {
		r := &n.reads[0]
		if r.index == 0 {
			if !n.committedInTerm() {
				return
			}
			r.index = n.log.committed
		}
		acks := 0
		for _, p := range n.peers {
			if p == n.id || n.prs[p].readAck >= r.seq {
				acks++
			}
		}
		if acks < n.quorum() {
			return
		}
		n.readStates = append(n.readStates, ReadState{Index: r.index, Context: r.ctx})
		n.reads = n.reads[1:]
	}
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		// Figure 2, All Servers rule 2: if an RPC request or response
		// contains term T > currentTerm, set currentTerm = T and convert to
		// follower.
		lead := None
		if m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap {
			lead = m.From
		}
		n.becomeFollower(m.Term, lead)
	case m.Term < n.term:
		if m.Type == MsgApp && n.mut.Has(mutation.AcceptStaleTerm) {
			n.handleAppend(m) // mutation: AppendEntries rule 1 ignored
			return
		}
		// Rule 1 of both RPCs: reply false if term < currentTerm. The reply
		// carries our term, which makes a stale leader or candidate step down.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
		case MsgApp, MsgHeartbeat, MsgSnap:
			n.send(Message{Type: MsgAppResp, To: m.From, Reject: true, Index: m.Index, Hint: n.log.lastIndex()})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgVoteResp:
		n.handleVoteResp(m)
	case MsgApp, MsgHeartbeat, MsgSnap:
		if n.state == Leader {
			// Two leaders in one term would break Election Safety; the
			// harness checks that globally, so here we just ignore it.
			return
		}
		// Figure 2, Candidates: if AppendEntries is received from a new
		// leader, convert to follower.
		n.state = Follower
		n.lead = m.From
		n.resetElectionTimer()
		switch m.Type {
		case MsgApp:
			n.handleAppend(m)
		case MsgHeartbeat:
			n.handleHeartbeat(m)
		case MsgSnap:
			n.handleSnapshot(m)
		}
	case MsgAppResp:
		n.handleAppResp(m)
	case MsgHeartbeatResp:
		n.handleHeartbeatResp(m)
	}
}

// ---------------------------------------------------------------- handlers

// handleVote is the RequestVote receiver, Figure 2 rule 2: grant the vote if
// votedFor is null or candidateId, and the candidate's log is at least as
// up-to-date as ours.
func (n *Node) handleVote(m Message) {
	canVote := n.vote == None || n.vote == m.From
	upToDate := n.log.isUpToDate(m.LogTerm, m.Index) || n.mut.Has(mutation.SkipUpToDateCheck)
	if canVote && upToDate {
		n.vote = m.From
		n.resetElectionTimer() // Figure 2, Followers: granting a vote resets the timer
		n.send(Message{Type: MsgVoteResp, To: m.From})
		return
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Reject: true})
}

// handleVoteResp: Figure 2, Candidates: become leader on votes from a majority.
func (n *Node) handleVoteResp(m Message) {
	if n.state != Candidate {
		return
	}
	n.votes[m.From] = !m.Reject
	granted, rejected := 0, 0
	for _, ok := range n.votes {
		if ok {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= n.quorum():
		n.becomeLeader()
	case rejected >= n.quorum():
		n.becomeFollower(n.term, None)
	}
}

func (n *Node) handleAppend(m Message) {
	if m.Index < n.log.committed {
		// Everything up to our commit index already matches the leader.
		n.send(Message{Type: MsgAppResp, To: m.From, Index: n.log.committed})
		return
	}
	if last, ok := n.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		n.send(Message{Type: MsgAppResp, To: m.From, Index: last})
		return
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Index: m.Index, Reject: true, Hint: n.log.lastIndex()})
}

func (n *Node) handleHeartbeat(m Message) {
	// The leader only sends a commit index this follower is known to have
	// matched, so it is safe to adopt (capped at our log length).
	n.log.commitTo(min(m.Commit, n.log.lastIndex()))
	n.send(Message{Type: MsgHeartbeatResp, To: m.From, Context: m.Context})
}

// handleSnapshot is the InstallSnapshot receiver (Figure 13).
func (n *Node) handleSnapshot(m Message) {
	s := m.Snapshot
	if s.Index <= n.log.committed {
		n.send(Message{Type: MsgAppResp, To: m.From, Index: n.log.committed})
		return
	}
	n.log.restore(s)
	n.send(Message{Type: MsgAppResp, To: m.From, Index: s.Index})
}

func (n *Node) handleAppResp(m Message) {
	if n.state != Leader {
		return
	}
	pr := n.prs[m.From]
	pr.active = true
	if m.Reject {
		// Figure 2, Leaders: if AppendEntries fails because of log
		// inconsistency, decrement nextIndex and retry. The follower's last
		// index lets us skip straight past a long missing suffix.
		if m.Index < pr.match || (pr.probing && m.Index != pr.next-1) {
			return // stale reply
		}
		pr.next = max(1, min(m.Index, m.Hint+1))
		if pr.next <= pr.match {
			pr.next = pr.match + 1
		}
		pr.probing, pr.paused = true, false
		n.sendAppend(m.From)
		return
	}
	// Figure 2, Leaders: on success update nextIndex and matchIndex.
	pr.paused = false
	if m.Index > pr.match {
		pr.match = m.Index
		pr.probing = false
	}
	pr.next = max(pr.next, m.Index+1)
	if n.maybeCommit() {
		n.bcastAppend() // tell followers the new commit index
	} else if pr.next <= n.log.lastIndex() {
		n.sendAppend(m.From)
	}
}

func (n *Node) handleHeartbeatResp(m Message) {
	if n.state != Leader {
		return
	}
	pr := n.prs[m.From]
	pr.active = true
	pr.paused = false
	pr.readAck = max(pr.readAck, m.Context)
	n.processReads()
	if pr.match < n.log.lastIndex() {
		n.sendAppend(m.From)
	}
}

// ---------------------------------------------------------------- leader

// maybeCommit is Figure 2, Leaders rule 4: if there exists an N > commitIndex
// such that a majority of matchIndex[i] >= N and log[N].term == currentTerm,
// set commitIndex = N. The term condition is the Figure 8 fix (§5.4.2).
func (n *Node) maybeCommit() bool {
	matches := make([]uint64, 0, len(n.peers))
	for _, p := range n.peers {
		matches = append(matches, n.prs[p].match)
	}
	slices.Sort(matches)
	N := matches[len(matches)-n.quorum()] // highest index held by a majority
	if N <= n.log.committed {
		return false
	}
	if t, _ := n.log.term(N); t != n.term && !n.mut.Has(mutation.CommitPrevTerm) {
		return false
	}
	n.log.commitTo(N)
	n.processReads()
	return true
}

func (n *Node) bcastAppend() {
	for _, p := range n.peers {
		if p != n.id {
			n.sendAppend(p)
		}
	}
}

func (n *Node) bcastHeartbeat() {
	for _, p := range n.peers {
		if p != n.id {
			pr := n.prs[p]
			n.send(Message{Type: MsgHeartbeat, To: p, Commit: min(pr.match, n.log.committed), Context: n.readSeq})
		}
	}
}

// sendAppend sends the follower the entries it is missing, or a snapshot if
// those entries were compacted away.
func (n *Node) sendAppend(to NodeID) {
	pr := n.prs[to]
	if pr.paused {
		return
	}
	prevIndex := pr.next - 1
	prevTerm, ok := n.log.term(prevIndex)
	if !ok {
		// The entries the follower needs are gone: InstallSnapshot.
		n.send(Message{Type: MsgSnap, To: to, Snapshot: n.log.snapshot})
		pr.probing, pr.paused = true, true
		return
	}
	hi := min(n.log.lastIndex(), prevIndex+uint64(n.cfg.MaxEntriesPerMsg))
	ents := n.log.slice(pr.next, hi+1)
	n.send(Message{Type: MsgApp, To: to, Index: prevIndex, LogTerm: prevTerm, Entries: ents, Commit: n.log.committed})
	if pr.probing {
		pr.paused = true
	} else if len(ents) > 0 {
		pr.next = ents[len(ents)-1].Index + 1 // pipeline: assume it will succeed
	}
}

// ---------------------------------------------------------------- Ready

// HasReady reports whether there is work for the caller.
func (n *Node) HasReady() bool {
	if n.needBcast || n.needHeartbeat || len(n.msgs) > 0 || len(n.readStates) > 0 {
		return true
	}
	if n.log.pendingSnapshot != nil || n.log.stable < n.log.lastIndex() {
		return true
	}
	if n.hardState() != n.prevHardState {
		return true
	}
	return n.log.applied < n.log.applyLimit(n.mut.Has(mutation.ApplyBeforeCommit))
}

// Ready returns the pending work. The caller must handle it and call Advance
// before calling any other method.
func (n *Node) Ready() Ready {
	if n.needBcast && n.state == Leader {
		n.bcastAppend()
	}
	if n.needHeartbeat && n.state == Leader {
		n.bcastHeartbeat()
	}
	n.needBcast, n.needHeartbeat = false, false

	hs := n.hardState()
	rd := Ready{
		State:      n.state,
		Lead:       n.lead,
		HardState:  hs,
		Entries:    n.log.unstableEntries(),
		Snapshot:   n.log.pendingSnapshot,
		Messages:   n.msgs,
		ReadStates: n.readStates,
	}
	if n.log.pendingSnapshot == nil {
		rd.CommittedEntries = n.log.nextCommitted(n.mut.Has(mutation.ApplyBeforeCommit))
	}
	rd.MustSync = len(rd.Entries) > 0 || hs.Term != n.prevHardState.Term || hs.Vote != n.prevHardState.Vote
	if n.mut.Has(mutation.ForgetVote) {
		rd.HardState.Vote = None // mutation: votedFor never reaches the disk
	}
	return rd
}

// Advance tells the node the caller finished handling rd.
func (n *Node) Advance(rd Ready) {
	n.prevHardState = rd.HardState
	if n.mut.Has(mutation.ForgetVote) {
		n.prevHardState.Vote = n.vote
	}
	if len(rd.Entries) > 0 {
		n.log.stable = max(n.log.stable, rd.Entries[len(rd.Entries)-1].Index)
	}
	if rd.Snapshot != nil {
		n.log.pendingSnapshot = nil
		n.log.applied = max(n.log.applied, rd.Snapshot.Index)
	}
	if k := len(rd.CommittedEntries); k > 0 {
		n.log.applied = max(n.log.applied, rd.CommittedEntries[k-1].Index)
	}
	n.msgs = nil
	n.readStates = nil
	if n.state == Leader {
		// The leader counts itself towards a majority only once its own
		// copy of the entries is durable.
		n.prs[n.id].match = n.log.stable
		if n.maybeCommit() {
			n.needBcast = true
		}
	}
}

// Compact replaces the log up to index (which must be applied) with a
// snapshot whose state machine contents are data. It returns the snapshot and
// the durable entries after it, which the caller rewrites into its log file.
func (n *Node) Compact(index uint64, data []byte) (*Snapshot, []Entry) {
	s := n.log.compact(index, data)
	return s, n.log.slice(index+1, n.log.stable+1)
}
