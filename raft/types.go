// Package raft is the Raft consensus protocol as a pure state machine.
//
// A Node never does I/O, never reads the clock and never starts goroutines.
// The caller feeds it inputs (Tick, Step, Propose, ReadIndex) and then drains
// a Ready: the state to persist, the messages to send, and the committed
// entries to apply, in that order. This is the same shape as etcd's raft
// library, and it is what lets the simulator drive several nodes
// deterministically from one goroutine.
//
// Comments of the form "Figure 2: ..." refer to the rule in Figure 2 of
// Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm
// (Extended Version)". "Thesis §x" refers to Ongaro's PhD thesis.
package raft

import "fmt"

// NodeID identifies a server. 0 means "nobody".
type NodeID = uint64

// None is the zero NodeID: no vote, no known leader.
const None NodeID = 0

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries a client command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by every new leader (thesis §6.4) so it can
	// commit an entry from its own term, which also commits everything
	// before it and makes ReadIndex safe.
	EntryNoop
)

// Entry is one log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// MsgType is the kind of a Message.
type MsgType uint8

const (
	MsgVote          MsgType = iota // RequestVote RPC
	MsgVoteResp                     // RequestVote reply
	MsgApp                          // AppendEntries RPC carrying entries
	MsgAppResp                      // AppendEntries reply
	MsgHeartbeat                    // empty AppendEntries used for liveness and ReadIndex
	MsgHeartbeatResp                // heartbeat reply
	MsgSnap                         // InstallSnapshot RPC (Figure 13)
)

var msgNames = [...]string{"MsgVote", "MsgVoteResp", "MsgApp", "MsgAppResp", "MsgHeartbeat", "MsgHeartbeatResp", "MsgSnap"}

func (t MsgType) String() string {
	if int(t) < len(msgNames) {
		return msgNames[t]
	}
	return fmt.Sprintf("MsgType(%d)", t)
}

// Message is every RPC request and reply. Which fields matter depends on Type:
//
//	MsgVote:        LogTerm/Index = candidate's last log term/index
//	MsgVoteResp:    Reject = vote not granted
//	MsgApp:         LogTerm/Index = prevLogTerm/prevLogIndex, Entries, Commit = leaderCommit
//	MsgAppResp:     Index = highest index known to match (success) or the rejected prevLogIndex,
//	                Reject, Hint = follower's last index (lets the leader back up quickly)
//	MsgHeartbeat:   Commit (never beyond what the follower is known to have), Context
//	MsgHeartbeatResp: Context (echoed, confirms leadership for ReadIndex)
//	MsgSnap:        Snapshot
type Message struct {
	Type     MsgType
	From, To NodeID
	Term     uint64
	LogTerm  uint64
	Index    uint64
	Entries  []Entry
	Commit   uint64
	Reject   bool
	Hint     uint64
	Context  uint64
	Snapshot *Snapshot
}

func (m Message) String() string {
	return fmt.Sprintf("%s t%d i%d n%d c%d r%v", m.Type, m.Term, m.Index, len(m.Entries), m.Commit, m.Reject)
}

// HardState is the state Figure 2 says must be on stable storage before
// responding to RPCs: currentTerm and votedFor. Commit is stored too so a
// restarted node can re-apply committed entries without waiting for a leader.
type HardState struct {
	Term   uint64
	Vote   NodeID
	Commit uint64
}

// Snapshot replaces every log entry up to and including Index (Figure 13).
type Snapshot struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// StateType is follower, candidate or leader.
type StateType uint8

const (
	Follower StateType = iota
	Candidate
	Leader
)

func (s StateType) String() string {
	return [...]string{"Follower", "Candidate", "Leader"}[s]
}

// ReadState says "a read that arrived with this Context is linearizable once
// the state machine has applied up to Index".
type ReadState struct {
	Index   uint64
	Context uint64
}

// Ready is the batch of work the caller must do, in this order:
//
//  1. Persist Snapshot (if any), Entries and HardState, and fsync.
//  2. Send Messages. (Several of them promise the data in step 1 is durable.)
//  3. Restore Snapshot into the state machine, then apply CommittedEntries.
//  4. Call Advance.
type Ready struct {
	State     StateType
	Lead      NodeID
	HardState HardState
	// MustSync is true when HardState or Entries changed and must be fsynced.
	MustSync bool
	// Entries to append. If the first entry's index is not past the end of
	// the stored log, the stored log must be truncated from that index first.
	Entries          []Entry
	Snapshot         *Snapshot
	Messages         []Message
	CommittedEntries []Entry
	ReadStates       []ReadState
}

// Status is a read-only view of a node, for tests, the harness and the CLI.
type Status struct {
	ID        NodeID
	State     StateType
	Term      uint64
	Vote      NodeID
	Lead      NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	SnapIndex uint64
}
