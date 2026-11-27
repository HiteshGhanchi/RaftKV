// Package harness runs a whole raftlab cluster inside the simulator: N peers,
// a lossy network, crashing disks, clients that retry, and a fault schedule,
// all derived from one seed. While it runs it checks the Raft safety
// properties after every event; afterwards it checks the client history for
// linearizability. Any failure can be replayed exactly from its seed.
package harness

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"runtime/debug"
	"slices"
	"time"

	"raftlab/check"
	"raftlab/mutation"
	"raftlab/peer"
	"raftlab/raft"
	"raftlab/sim"
)

// Faults selects which faults the schedule injects.
type Faults struct {
	Crash     bool          // crash a node (losing unsynced disk writes) and restart it later
	Partition bool          // split the cluster into two sides
	OneWay    bool          // block a single direction of a single link
	Pause     bool          // freeze a node (GC pause / VM stall); messages queue up
	Slow      bool          // delay spike: every message takes up to 250ms for a while
	Interval  time.Duration // mean time between fault events
	ClockSkew float64       // each node's tick runs up to this fraction fast or slow
	// LeaderBias is the probability that a crash, pause or partition picks
	// the current leader instead of a random node.
	LeaderBias float64
}

// AllFaults enables everything at a moderate rate.
func AllFaults() Faults {
	return Faults{Crash: true, Partition: true, OneWay: true, Pause: true, Slow: true,
		Interval: 150 * time.Millisecond, ClockSkew: 0.2, LeaderBias: 0.5}
}

// Options describe one run.
type Options struct {
	Seed      uint64
	Nodes     int
	Clients   int
	Keys      int
	Duration  time.Duration // faults are injected during this time
	Settle    time.Duration // then the network heals and clients finish
	Net       sim.NetConfig
	Faults    Faults
	Mutations mutation.Set

	Tick             time.Duration
	ElectionTick     int
	SnapshotEvery    uint64
	MaxEntriesPerMsg int
	CheckQuorum      bool
	ClientTimeout    time.Duration
	CheckTimeout     time.Duration
	TraceLog         bool

	// Sink, if set, receives every trace event as it happens. Observing a
	// run never changes it.
	Sink func(sim.Event) `json:"-"`
}

// DefaultOptions is the configuration used by the chaos runner and CI. The
// seed also picks the cluster shape: 3 or 5 nodes, how many entries one
// message may carry, how often faults happen and how often snapshots are
// taken, so that a seed sweep covers many configurations.
func DefaultOptions(seed uint64) Options {
	r := sim.NewRand(seed ^ 0xc0ffee)
	pick := func(xs ...int) int { return xs[r.IntN(len(xs))] }
	f := AllFaults()
	f.Interval = time.Duration(pick(50, 150, 300)) * time.Millisecond
	return Options{
		Seed: seed, Nodes: pick(3, 5), Clients: 4, Keys: 3,
		Duration: 4 * time.Second, Settle: 2 * time.Second,
		Net: sim.NetConfig{MinDelay: time.Millisecond, MaxDelay: 15 * time.Millisecond,
			DropRate: []float64{0.01, 0.05, 0.15}[r.IntN(3)], DupRate: 0.02},
		Faults:           f,
		Tick:             10 * time.Millisecond,
		ElectionTick:     10,
		SnapshotEvery:    uint64(pick(0, 10, 40)),
		MaxEntriesPerMsg: pick(1, 4, 64),
		CheckQuorum:      pick(0, 1) == 1,
		ClientTimeout:    time.Duration(pick(50, 250)) * time.Millisecond,
		CheckTimeout:     20 * time.Second,
	}
}

// Result summarises a run.
type Result struct {
	Seed      uint64
	Mutations string

	// Violation names the first safety property that broke, or "".
	Violation       string
	ViolationAt     time.Duration
	Linearizability check.Verdict

	Ops, Completed, Unknown      int
	Leaders, Crashes, Partitions int
	Pauses, OneWayBlocks         int
	Snapshots, Restores          uint64
	Steps                        uint64
	TraceHash                    uint64
	Net                          sim.NetStats

	History  *check.History `json:"-"`
	TraceLog []string       `json:"-"`
}

// Failed reports whether the run found a bug.
func (r Result) Failed() bool {
	return r.Violation != "" || r.Linearizability == check.NotLinearizable
}

func (r Result) String() string {
	status := "ok"
	if r.Failed() {
		status = "FAIL"
	}
	v := r.Violation
	if v == "" {
		v = "none"
	}
	return fmt.Sprintf("seed=%d %s mutations=%s violation=%s history=%s ops=%d/%d unknown=%d leaders=%d crashes=%d partitions=%d pauses=%d snapshots=%d trace=%x",
		r.Seed, status, r.Mutations, v, r.Linearizability, r.Completed, r.Ops, r.Unknown,
		r.Leaders, r.Crashes, r.Partitions, r.Pauses, r.Snapshots, r.TraceHash)
}

type entryKey struct {
	term uint64
	typ  raft.EntryType
	sum  uint64
}

type appliedEntry struct {
	entryKey
	commitBound uint64 // the entry was committed in a term <= this
}

type cluster struct {
	opt   Options
	w     *sim.World
	net   *sim.Network[raft.Message]
	frng  *rand.Rand // fault schedule
	crng  *rand.Rand // clients
	prng  *rand.Rand // seeds for restarted peers
	ids   []raft.NodeID
	disks map[raft.NodeID]*sim.Disk
	peers map[raft.NodeID]*peer.Peer
	pause map[raft.NodeID]time.Duration
	skew  map[raft.NodeID]float64

	hist   check.History
	res    Result
	failed bool

	leaders     map[uint64]raft.NodeID
	committed   map[uint64]appliedEntry
	leaderLast  map[raft.NodeID][2]uint64 // term, lastIndex while leader
	clients     []*client
	stopClients time.Duration
	end         time.Duration
}

// Run executes one simulated run.
func Run(opt Options) Result {
	c := &cluster{
		opt:        opt,
		w:          sim.New(opt.Seed),
		disks:      map[raft.NodeID]*sim.Disk{},
		peers:      map[raft.NodeID]*peer.Peer{},
		pause:      map[raft.NodeID]time.Duration{},
		skew:       map[raft.NodeID]float64{},
		leaders:    map[uint64]raft.NodeID{},
		committed:  map[uint64]appliedEntry{},
		leaderLast: map[raft.NodeID][2]uint64{},
	}
	c.res.Seed = opt.Seed
	c.res.Mutations = opt.Mutations.String()
	if opt.TraceLog {
		c.w.EnableTraceLog()
	}
	if opt.Sink != nil {
		c.w.SetSink(opt.Sink)
	}
	c.frng, c.crng, c.prng = c.w.Fork(), c.w.Fork(), c.w.Fork()
	c.net = sim.NewNetwork(c.w, opt.Net, func(m raft.Message) string { return m.String() })
	for i := 1; i <= opt.Nodes; i++ {
		c.ids = append(c.ids, raft.NodeID(i))
	}
	for _, id := range c.ids {
		c.disks[id] = sim.NewDisk(c.w.Fork())
		c.skew[id] = (c.frng.Float64()*2 - 1) * opt.Faults.ClockSkew
		c.net.Register(id, c.deliverFunc(id))
		c.start(id)
		c.scheduleTick(id)
	}
	c.end = opt.Duration + opt.Settle
	c.stopClients = opt.Duration + opt.Settle/2
	c.scheduleFaults()
	c.w.Schedule(opt.Duration, sim.Event{Kind: "heal-all"}, c.healAll)
	for i := range opt.Clients {
		c.startClient(i)
	}
	c.w.Schedule(opt.Tick*5, sim.Event{Kind: "check"}, c.periodicCheck)

	c.w.RunUntilOr(c.end, func() bool { return c.failed })

	c.res.Steps = c.w.Steps()
	c.res.TraceHash = c.w.TraceHash()
	c.res.Net = c.net.Stats
	c.res.Leaders = len(c.leaders)
	for _, id := range c.ids {
		if p := c.peers[id]; p != nil {
			c.res.Snapshots += p.Stats().Snapshots
			c.res.Restores += p.Stats().Restores
		}
	}
	c.finishClients()
	c.res.History = &c.hist
	if c.failed {
		c.res.Linearizability = "not checked (run stopped at violation)"
	} else {
		c.res.Linearizability = c.hist.Linearizable(opt.CheckTimeout)
	}
	if opt.TraceLog {
		c.res.TraceLog = c.w.TraceLog()
	}
	return c.res
}

// fail records the first safety violation and stops the run.
func (c *cluster) fail(name, format string, args ...any) {
	if c.failed {
		return
	}
	c.failed = true
	c.res.Violation = name + ": " + fmt.Sprintf(format, args...)
	c.res.ViolationAt = c.w.Now()
	c.w.Emit(sim.Event{Kind: "violation", Detail: c.res.Violation})
}

// guard runs peer code and turns a panic into a violation.
func (c *cluster) guard(id raft.NodeID, fn func()) {
	if c.failed {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if ie, ok := r.(*raft.InvariantError); ok {
				c.fail(ie.Name, "node %d: %s", id, ie.Detail)
				return
			}
			c.fail("Panic", "node %d: %v\n%s", id, r, debug.Stack())
		}
	}()
	fn()
}

// ------------------------------------------------------------- nodes

func (c *cluster) start(id raft.NodeID) {
	c.guard(id, func() {
		p, err := peer.New(peer.Config{
			ID: id, Peers: c.ids,
			ElectionTick: c.opt.ElectionTick, HeartbeatTick: 1,
			FS:            c.disks[id],
			Rand:          sim.NewRand(c.prng.Uint64()),
			Mutations:     c.opt.Mutations,
			SnapshotEvery: c.opt.SnapshotEvery,
			MaxEntries:    c.opt.MaxEntriesPerMsg,
			CheckQuorum:   c.opt.CheckQuorum,
			Observer:      c,
			Send: func(msgs []raft.Message) {
				for _, m := range msgs {
					c.net.Send(m.From, m.To, m)
				}
			},
		})
		if err != nil {
			c.fail("Recovery", "node %d could not restart: %v", id, err)
			return
		}
		c.peers[id] = p
		c.net.SetDown(id, false)
		st := p.Status()
		c.w.Emit(sim.Event{Kind: "start", Node: id, Attrs: []sim.Attr{sim.A("term", int64(st.Term)), sim.A("last", int64(st.LastIndex))}})
	})
}

func (c *cluster) crash(id raft.NodeID) {
	if c.peers[id] == nil {
		return
	}
	c.peers[id] = nil
	delete(c.leaderLast, id)
	loss := c.disks[id].Crash()
	c.net.SetDown(id, true)
	c.res.Crashes++
	detail := ""
	for _, l := range loss {
		state := "lost"
		switch {
		case l.Kept == l.Unsynced:
			state = "kept"
		case l.Kept > 0:
			state = "torn"
		}
		detail += fmt.Sprintf("%s %s %d/%d unsynced bytes kept; ", l.File, state, l.Kept, l.Unsynced)
	}
	c.w.Emit(sim.Event{Kind: "crash", Node: id, Detail: detail})
}

func (c *cluster) paused(id raft.NodeID) bool { return c.pause[id] > c.w.Now() }

func (c *cluster) jitter() time.Duration {
	return time.Duration(c.frng.Int64N(int64(10 * time.Millisecond)))
}

func (c *cluster) deliverFunc(id raft.NodeID) func(raft.NodeID, raft.Message) {
	var deliver func(raft.NodeID, raft.Message)
	deliver = func(from raft.NodeID, m raft.Message) {
		if c.peers[id] == nil {
			return
		}
		if c.paused(id) {
			// A process waking from a pause drains its sockets in no
			// particular order, so queued input gets a little jitter.
			c.w.Schedule(c.pause[id]-c.w.Now()+c.jitter(), sim.Event{Kind: "deliver-deferred", Node: id, Peer: from, Detail: m.String()}, func() { deliver(from, m) })
			return
		}
		c.guard(id, func() { c.peers[id].Receive(m) })
	}
	return deliver
}

func (c *cluster) scheduleTick(id raft.NodeID) {
	period := time.Duration(float64(c.opt.Tick) * (1 + c.skew[id]))
	var tick func()
	tick = func() {
		if p := c.peers[id]; p != nil && !c.paused(id) {
			c.guard(id, p.Tick)
		}
		c.w.Schedule(period, sim.Event{Kind: "tick", Node: id}, tick)
	}
	c.w.Schedule(time.Duration(c.frng.Int64N(int64(period))), sim.Event{Kind: "tick", Node: id}, tick)
}

// ------------------------------------------------------------- faults

func (c *cluster) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(c.frng.Int64N(int64(hi-lo)+1))
}

func (c *cluster) scheduleFaults() {
	f := c.opt.Faults
	var kinds []func()
	if f.Crash {
		kinds = append(kinds, c.faultCrash)
	}
	if f.Partition {
		kinds = append(kinds, c.faultPartition)
	}
	if f.OneWay {
		kinds = append(kinds, c.faultOneWay)
	}
	if f.Pause {
		kinds = append(kinds, c.faultPause)
	}
	if f.Slow {
		kinds = append(kinds, c.faultSlow)
	}
	if len(kinds) == 0 || f.Interval == 0 {
		return
	}
	var next func()
	next = func() {
		if c.w.Now() >= c.opt.Duration {
			return
		}
		kinds[c.frng.IntN(len(kinds))]()
		c.w.Schedule(c.between(f.Interval/2, f.Interval*3/2), sim.Event{Kind: "fault"}, next)
	}
	c.w.Schedule(c.between(f.Interval/2, f.Interval*3/2), sim.Event{Kind: "fault"}, next)
}

func (c *cluster) randomNode() raft.NodeID { return c.ids[c.frng.IntN(len(c.ids))] }

// target picks the node a fault hits: often the current leader, because
// that is where the interesting failures are (Jepsen does the same).
func (c *cluster) target() raft.NodeID {
	if c.frng.Float64() < c.opt.Faults.LeaderBias {
		var best raft.NodeID
		var bestTerm uint64
		for _, id := range c.ids {
			if p := c.peers[id]; p != nil {
				if st := p.Status(); st.State == raft.Leader && st.Term >= bestTerm {
					best, bestTerm = id, st.Term
				}
			}
		}
		if best != raft.None {
			return best
		}
	}
	return c.randomNode()
}

func (c *cluster) faultCrash() {
	id := c.target()
	if c.peers[id] == nil {
		return
	}
	c.crash(id)
	c.w.Schedule(c.between(5*time.Millisecond, 800*time.Millisecond), sim.Event{Kind: "restart", Node: id}, func() {
		if c.peers[id] == nil && !c.failed {
			c.start(id)
		}
	})
}

func (c *cluster) faultPartition() {
	ids := slices.Clone(c.ids)
	c.frng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if t := c.target(); true {
		// Put the target alone or in the minority side.
		i := slices.Index(ids, t)
		ids[0], ids[i] = ids[i], ids[0]
	}
	k := 1 + c.frng.IntN((len(ids)-1)/2+1)
	c.net.Heal()
	c.net.Partition(ids[:k], ids[k:])
	c.res.Partitions++
	c.w.Emit(sim.Event{Kind: "partition", Detail: fmt.Sprintf("%v | %v", ids[:k], ids[k:])})
	c.w.Schedule(c.between(100*time.Millisecond, time.Second), sim.Event{Kind: "heal-partition"}, c.net.Heal)
}

func (c *cluster) faultOneWay() {
	a, b := c.randomNode(), c.randomNode()
	if a == b {
		return
	}
	c.net.Block(a, b)
	c.res.OneWayBlocks++
	c.w.Emit(sim.Event{Kind: "block", Node: a, Peer: b})
	c.w.Schedule(c.between(100*time.Millisecond, time.Second), sim.Event{Kind: "unblock", Node: a, Peer: b}, func() { c.net.Unblock(a, b) })
}

func (c *cluster) faultSlow() {
	normal := c.opt.Net
	slow := normal
	slow.MaxDelay = 250 * time.Millisecond
	c.net.SetConfig(slow)
	c.w.Emit(sim.Event{Kind: "slow"})
	c.w.Schedule(c.between(100*time.Millisecond, 600*time.Millisecond), sim.Event{Kind: "net-normal"}, func() { c.net.SetConfig(normal) })
}

func (c *cluster) faultPause() {
	id := c.target()
	c.pause[id] = c.w.Now() + c.between(30*time.Millisecond, 400*time.Millisecond)
	c.res.Pauses++
	c.w.Emit(sim.Event{Kind: "pause", Node: id, Attrs: []sim.Attr{sim.A("until", int64(c.pause[id]))}})
}

func (c *cluster) healAll() {
	c.net.Heal()
	c.net.SetConfig(c.opt.Net)
	for _, id := range c.ids {
		c.pause[id] = 0
		if c.peers[id] == nil {
			c.start(id)
		}
	}
	c.w.Emit(sim.Event{Kind: "healed"})
}

// ------------------------------------------------------------- invariants

func hashData(e raft.Entry) entryKey {
	h := fnv.New64a()
	h.Write(e.Data)
	return entryKey{term: e.Term, typ: e.Type, sum: h.Sum64()}
}

// Applied checks State Machine Safety: if any server has applied a log entry
// at a given index, no other server ever applies a different entry there.
//
// It also records an upper bound on the term in which the entry was
// committed: a node only applies an entry after hearing about the commit from
// a leader whose term is at most the node's own current term.
func (c *cluster) Applied(id raft.NodeID, term uint64, e raft.Entry) {
	c.w.Emit(sim.Event{Kind: "apply", Node: id, Attrs: []sim.Attr{sim.A("index", int64(e.Index)), sim.A("term", int64(e.Term))}})
	k := hashData(e)
	if prev, ok := c.committed[e.Index]; ok {
		if prev.entryKey != k {
			c.fail("StateMachineSafety", "node %d applied index %d term %d, another node applied term %d there", id, e.Index, e.Term, prev.term)
			return
		}
		prev.commitBound = min(prev.commitBound, term)
		c.committed[e.Index] = prev
		return
	}
	c.committed[e.Index] = appliedEntry{k, term}
}

// RoleChanged checks Election Safety (at most one leader per term) and Leader
// Completeness (a leader has every entry committed in an earlier term).
func (c *cluster) RoleChanged(id raft.NodeID, st raft.Status) {
	c.w.Emit(sim.Event{Kind: "role", Node: id, Detail: st.State.String(), Attrs: []sim.Attr{
		sim.A("term", int64(st.Term)), sim.A("vote", int64(st.Vote)), sim.A("lead", int64(st.Lead)),
		sim.A("commit", int64(st.Commit)), sim.A("last", int64(st.LastIndex))}})
	if st.State != raft.Leader {
		delete(c.leaderLast, id)
		return
	}
	if other, ok := c.leaders[st.Term]; ok && other != id {
		c.fail("ElectionSafety", "nodes %d and %d both leader in term %d", other, id, st.Term)
		return
	}
	c.leaders[st.Term] = id
	if c.peers[id] == nil {
		return
	}
	n := c.peers[id].Node()
	idxs := make([]uint64, 0, len(c.committed))
	for idx := range c.committed {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)
	for _, idx := range idxs {
		k := c.committed[idx]
		// Leader Completeness only speaks about leaders of later terms than
		// the one the entry was committed in.
		if idx <= st.SnapIndex || st.Term <= k.commitBound {
			continue
		}
		if t, ok := n.Term(idx); !ok || t != k.term {
			c.fail("LeaderCompleteness", "new leader %d (term %d) lacks applied entry %d (term %d)", id, st.Term, idx, k.term)
			return
		}
	}
}

// periodicCheck verifies Log Matching across every pair of running nodes and
// that leaders only append (Leader Append-Only).
func (c *cluster) periodicCheck() {
	if c.failed {
		return
	}
	defer c.w.Schedule(c.opt.Tick*5, sim.Event{Kind: "check"}, c.periodicCheck)
	type view struct {
		id    raft.NodeID
		snap  uint64
		ents  []raft.Entry
		state raft.Status
	}
	var views []view
	for _, id := range c.ids {
		p := c.peers[id]
		if p == nil {
			continue
		}
		st := p.Status()
		views = append(views, view{id, st.SnapIndex, p.Node().Entries(), st})
		if st.State == raft.Leader {
			if prev, ok := c.leaderLast[id]; ok && prev[0] == st.Term && st.LastIndex < prev[1] {
				c.fail("LeaderAppendOnly", "leader %d in term %d shrank its log from %d to %d", id, st.Term, prev[1], st.LastIndex)
				return
			}
			c.leaderLast[id] = [2]uint64{st.Term, st.LastIndex}
		}
	}
	at := func(v view, i uint64) raft.Entry { return v.ents[i-v.snap-1] }
	for a := 0; a < len(views); a++ {
		for b := a + 1; b < len(views); b++ {
			va, vb := views[a], views[b]
			lo := max(va.snap, vb.snap) + 1
			hi := min(va.snap+uint64(len(va.ents)), vb.snap+uint64(len(vb.ents)))
			matched := false
			for i := hi; i >= lo && i > 0; i-- {
				ea, eb := at(va, i), at(vb, i)
				if ea.Term == eb.Term {
					if hashData(ea) != hashData(eb) {
						c.fail("LogMatching", "nodes %d and %d have different entries at index %d term %d", va.id, vb.id, i, ea.Term)
						return
					}
					matched = true
				} else if matched {
					c.fail("LogMatching", "nodes %d and %d agree on a later entry but differ at index %d", va.id, vb.id, i)
					return
				}
			}
		}
	}
}
