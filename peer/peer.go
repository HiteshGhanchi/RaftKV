// Package peer glues one Raft node, its storage and the KV state machine
// together. A Peer is single-threaded and event driven: the simulator calls it
// directly from its event loop, and the real server calls it from one
// goroutine. Either way, every input ends with process(), which carries out
// the Ready contract: persist, then send, then apply.
package peer

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"

	"raftlab/kv"
	"raftlab/mutation"
	"raftlab/raft"
	"raftlab/storage"
	"raftlab/vfs"
)

// Errors returned to clients through callbacks.
var (
	// ErrLost means the command was proposed but a different entry won its
	// log slot (leadership changed). The client should retry; dedup makes
	// that safe even if the command did commit elsewhere.
	ErrLost = errors.New("peer: proposal lost to a leadership change")
)

// NotLeaderError says which node this peer believes leads (0 if unknown).
type NotLeaderError struct{ Leader raft.NodeID }

func (e *NotLeaderError) Error() string { return fmt.Sprintf("not leader (leader hint %d)", e.Leader) }

// Result is the outcome of a client request.
type Result struct {
	Value string
	Err   error
}

// Callback receives a Result exactly once.
type Callback func(Result)

// Observer receives events the harness uses to check global invariants.
type Observer interface {
	// Applied is called for every entry handed to the state machine, with
	// the applying node's current term.
	Applied(id raft.NodeID, term uint64, e raft.Entry)
	// RoleChanged is called whenever this peer's state, term or leader changes.
	RoleChanged(id raft.NodeID, st raft.Status)
}

// Config creates a Peer.
type Config struct {
	ID            raft.NodeID
	Peers         []raft.NodeID
	ElectionTick  int
	HeartbeatTick int
	FS            vfs.FS
	Rand          *rand.Rand
	Mutations     mutation.Set
	// SnapshotEvery compacts the log once this many entries were applied
	// since the last snapshot (0 disables snapshots).
	SnapshotEvery uint64
	// MaxEntries caps entries per AppendEntries message (0 = raft default).
	MaxEntries  int
	CheckQuorum bool
	// Send delivers outgoing Raft messages. It must not call back into the peer.
	Send     func([]raft.Message)
	Observer Observer
}

type pendingWrite struct {
	term uint64
	cb   Callback
}

type pendingRead struct {
	key   string
	cb    Callback
	index uint64 // known once ReadIndex confirms
}

// Peer is one server.
type Peer struct {
	cfg   Config
	node  *raft.Node
	store *storage.Storage
	kv    *kv.Store

	writes   map[uint64]pendingWrite // by log index
	reads    map[uint64]*pendingRead // by read id
	nextRead uint64
	applied  uint64
	snapAt   uint64
	last     raft.Status

	stats Stats
}

// Stats counts peer activity.
type Stats struct {
	Applied, Snapshots, Restores uint64
}

// New opens storage and restores the node from it.
func New(cfg Config) (*Peer, error) {
	store, st, err := storage.Open(cfg.FS, storage.Options{NoSync: cfg.Mutations.Has(mutation.SkipFsync)})
	if err != nil {
		return nil, err
	}
	p := &Peer{
		cfg:    cfg,
		store:  store,
		kv:     kv.NewStore(!cfg.Mutations.Has(mutation.NoDedup)),
		writes: map[uint64]pendingWrite{},
		reads:  map[uint64]*pendingRead{},
	}
	if st.Snapshot != nil {
		if err := p.kv.Restore(st.Snapshot.Data); err != nil {
			return nil, err
		}
		p.applied, p.snapAt = st.Snapshot.Index, st.Snapshot.Index
	}
	p.node = raft.New(raft.Config{
		ID: cfg.ID, Peers: cfg.Peers,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		MaxEntriesPerMsg: cfg.MaxEntries,
		CheckQuorum:      cfg.CheckQuorum,
		Rand:             cfg.Rand,
		HardState:        st.HardState, Entries: st.Entries, Snapshot: st.Snapshot,
		Mutations: cfg.Mutations,
	})
	p.last = p.node.Status()
	p.process()
	return p, nil
}

// Status returns the node's protocol state.
func (p *Peer) Status() raft.Status { return p.node.Status() }

// Node exposes the protocol state machine to invariant checkers. Callers must
// only read from it.
func (p *Peer) Node() *raft.Node { return p.node }

// KV exposes the state machine (read-only use).
func (p *Peer) KV() *kv.Store { return p.kv }

// Stats returns counters.
func (p *Peer) Stats() Stats { return p.stats }

// Close releases the storage.
func (p *Peer) Close() error { return p.store.Close() }

// Tick advances logical time.
func (p *Peer) Tick() {
	p.node.Tick()
	p.process()
}

// Receive handles one incoming Raft message.
func (p *Peer) Receive(m raft.Message) {
	p.node.Step(m)
	p.process()
}

// Submit proposes a write (put/append). cb runs once the command is applied
// on this peer with the term it was proposed in, or when it fails.
func (p *Peer) Submit(c kv.Command, cb Callback) {
	p.SubmitBatch([]kv.Command{c}, []Callback{cb})
}

// SubmitBatch proposes several commands before one round of persistence, so
// they share an fsync and a message to each follower.
func (p *Peer) SubmitBatch(cmds []kv.Command, cbs []Callback) {
	for i, c := range cmds {
		idx, term, err := p.node.Propose(c.Encode())
		if err != nil {
			cbs[i](Result{Err: &NotLeaderError{Leader: p.node.Status().Lead}})
			continue
		}
		p.writes[idx] = pendingWrite{term: term, cb: cbs[i]}
	}
	p.process()
}

// Read serves a linearizable read with ReadIndex.
func (p *Peer) Read(key string, cb Callback) {
	p.nextRead++
	id := p.nextRead
	if err := p.node.ReadIndex(id); err != nil {
		cb(Result{Err: &NotLeaderError{Leader: p.node.Status().Lead}})
		return
	}
	p.reads[id] = &pendingRead{key: key, cb: cb}
	p.process()
}

// process drains every Ready, following the order the raft package requires.
func (p *Peer) process() {
	for p.node.HasReady() {
		rd := p.node.Ready()

		// 1. Persist. A failing disk is fatal: continuing could break the
		// promise that acknowledged state is durable.
		if rd.Snapshot != nil {
			if err := p.store.SaveSnapshot(rd.Snapshot, rd.HardState, rd.Entries); err != nil {
				panic(err)
			}
		} else if err := p.store.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
			panic(err)
		}

		// 2. Send.
		if len(rd.Messages) > 0 {
			p.cfg.Send(rd.Messages)
		}

		// 3. Apply.
		if rd.Snapshot != nil {
			if err := p.kv.Restore(rd.Snapshot.Data); err != nil {
				panic(err)
			}
			p.applied, p.snapAt = rd.Snapshot.Index, rd.Snapshot.Index
			p.stats.Restores++
			p.failWritesUpTo(rd.Snapshot.Index)
		}
		for _, e := range rd.CommittedEntries {
			p.apply(e)
		}
		for _, rs := range rd.ReadStates {
			if r := p.reads[rs.Context]; r != nil {
				r.index = rs.Index
			}
		}
		p.node.Advance(rd)

		p.noticeRoleChange()
		p.serveReads()
		p.maybeSnapshot()
	}
}

func (p *Peer) apply(e raft.Entry) {
	p.applied = e.Index
	p.stats.Applied++
	if p.cfg.Observer != nil {
		p.cfg.Observer.Applied(p.cfg.ID, p.node.Status().Term, e)
	}
	var result string
	if e.Type == raft.EntryNormal {
		c, err := kv.DecodeCommand(e.Data)
		if err != nil {
			panic(fmt.Sprintf("peer %d: undecodable entry %d: %v", p.cfg.ID, e.Index, err))
		}
		result = p.kv.Apply(c)
	}
	if w, ok := p.writes[e.Index]; ok {
		delete(p.writes, e.Index)
		if w.term == e.Term {
			w.cb(Result{Value: result})
		} else {
			w.cb(Result{Err: ErrLost})
		}
	}
}

func (p *Peer) failWritesUpTo(index uint64) {
	for _, idx := range sortedKeys(p.writes) {
		if idx <= index {
			p.writes[idx].cb(Result{Err: ErrLost})
			delete(p.writes, idx)
		}
	}
}

// noticeRoleChange reports role changes and fails reads that can no longer
// complete because this peer stopped being leader.
func (p *Peer) noticeRoleChange() {
	st := p.node.Status()
	if st.State == p.last.State && st.Term == p.last.Term && st.Lead == p.last.Lead {
		p.last = st
		return
	}
	wasLeader := p.last.State == raft.Leader
	p.last = st
	if p.cfg.Observer != nil {
		p.cfg.Observer.RoleChanged(p.cfg.ID, st)
	}
	if wasLeader && (st.State != raft.Leader) {
		for _, id := range sortedKeys(p.reads) {
			p.reads[id].cb(Result{Err: &NotLeaderError{Leader: st.Lead}})
			delete(p.reads, id)
		}
	}
}

// serveReads answers reads whose read index has been applied.
func (p *Peer) serveReads() {
	for _, id := range sortedKeys(p.reads) {
		r := p.reads[id]
		if r.index != 0 && r.index <= p.applied {
			r.cb(Result{Value: p.kv.Get(r.key)})
			delete(p.reads, id)
		}
	}
}

func (p *Peer) maybeSnapshot() {
	if p.cfg.SnapshotEvery == 0 || p.applied-p.snapAt < p.cfg.SnapshotEvery {
		return
	}
	snap, rest := p.node.Compact(p.applied, p.kv.Snapshot())
	if err := p.store.SaveSnapshot(snap, p.node.HardState(), rest); err != nil {
		panic(err)
	}
	p.snapAt = snap.Index
	p.stats.Snapshots++
}

func sortedKeys[V any](m map[uint64]V) []uint64 {
	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
