package harness

import (
	"fmt"
	"slices"
	"time"

	"raftlab/check"
	"raftlab/raft"
	"raftlab/sim"
)

// Fault kinds in a Schedule.
const (
	FaultCrash     = "crash"     // crash Node, restart it Dur later
	FaultPartition = "partition" // split into Side and Other, heal Dur later
	FaultOneWay    = "oneway"    // block Node -> Peer, unblock Dur later
	FaultPause     = "pause"     // freeze Node for Dur
	FaultSlow      = "slow"      // delay spike for Dur
	FaultNone      = "none"      // a drawn fault that had no effect (e.g. crash of a node already down)
	FaultStop      = "stop"      // the fault loop ended (time ran out)
)

// Fault is one injected fault, resolved to concrete nodes and durations so it
// can be replayed without the random generator.
type Fault struct {
	At    time.Duration `json:"at"`
	Kind  string        `json:"kind"`
	Node  raft.NodeID   `json:"node,omitempty"`
	Peer  raft.NodeID   `json:"peer,omitempty"`
	Side  []raft.NodeID `json:"side,omitempty"`
	Other []raft.NodeID `json:"other,omitempty"`
	Dur   time.Duration `json:"dur,omitempty"`
}

func (f Fault) String() string {
	switch f.Kind {
	case FaultCrash:
		return fmt.Sprintf("%v crash n%d, restart after %v", f.At, f.Node, f.Dur)
	case FaultPartition:
		return fmt.Sprintf("%v partition %v | %v for %v", f.At, f.Side, f.Other, f.Dur)
	case FaultOneWay:
		return fmt.Sprintf("%v block n%d->n%d for %v", f.At, f.Node, f.Peer, f.Dur)
	case FaultPause:
		return fmt.Sprintf("%v pause n%d for %v", f.At, f.Node, f.Dur)
	case FaultSlow:
		return fmt.Sprintf("%v slow network for %v", f.At, f.Dur)
	}
	return fmt.Sprintf("%v %s", f.At, f.Kind)
}

// Schedule is the list of faults a run injects, in time order.
type Schedule struct {
	Faults []Fault `json:"faults"`
}

// Effective returns the number of faults that actually do something.
func (s *Schedule) Effective() int {
	if s == nil {
		return 0
	}
	n := 0
	for _, f := range s.Faults {
		if f.Kind != FaultNone && f.Kind != FaultStop {
			n++
		}
	}
	return n
}

// Clone returns a deep copy.
func (s *Schedule) Clone() *Schedule {
	if s == nil {
		return nil
	}
	out := &Schedule{Faults: make([]Fault, len(s.Faults))}
	for i, f := range s.Faults {
		f.Side, f.Other = slices.Clone(f.Side), slices.Clone(f.Other)
		out.Faults[i] = f
	}
	return out
}

// scheduleFaults starts the fault loop. In record mode (no Options.Schedule)
// each fault is drawn from the fault generator when its timer fires and is
// appended to the run's schedule. In replay mode the timers fire at the
// recorded times and apply the recorded faults; the fault generator is never
// used. The two modes schedule exactly the same timers, which is what makes a
// recorded schedule replay to the same trace.
func (c *cluster) scheduleFaults() {
	if c.opt.Schedule != nil {
		c.replayFaults(c.opt.Schedule.Faults)
		return
	}
	f := c.opt.Faults
	var kinds []func() Fault
	if f.Crash {
		kinds = append(kinds, c.drawCrash)
	}
	if f.Partition {
		kinds = append(kinds, c.drawPartition)
	}
	if f.OneWay {
		kinds = append(kinds, c.drawOneWay)
	}
	if f.Pause {
		kinds = append(kinds, c.drawPause)
	}
	if f.Slow {
		kinds = append(kinds, c.drawSlow)
	}
	if len(kinds) == 0 || f.Interval == 0 {
		return
	}
	var next func()
	next = func() {
		if c.w.Now() >= c.opt.Duration {
			c.sched.Faults = append(c.sched.Faults, Fault{At: c.w.Now(), Kind: FaultStop})
			return
		}
		ft := kinds[c.frng.IntN(len(kinds))]()
		ft.At = c.w.Now()
		c.sched.Faults = append(c.sched.Faults, ft)
		c.apply(ft)
		c.w.Schedule(c.between(f.Interval/2, f.Interval*3/2), sim.Event{Kind: "fault"}, next)
	}
	c.w.Schedule(c.between(f.Interval/2, f.Interval*3/2), sim.Event{Kind: "fault"}, next)
}

func (c *cluster) replayFaults(faults []Fault) {
	if len(faults) == 0 {
		return
	}
	i := 0
	var next func()
	next = func() {
		ft := faults[i]
		i++
		c.sched.Faults = append(c.sched.Faults, ft)
		if ft.Kind == FaultStop {
			return
		}
		c.apply(ft)
		if i < len(faults) {
			c.w.Schedule(faults[i].At-c.w.Now(), sim.Event{Kind: "fault"}, next)
		}
	}
	c.w.Schedule(faults[0].At, sim.Event{Kind: "fault"}, next)
}

func (c *cluster) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(c.frng.Int64N(int64(hi-lo)+1))
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

func (c *cluster) drawCrash() Fault {
	id := c.target()
	if c.peers[id] == nil {
		return Fault{Kind: FaultNone}
	}
	return Fault{Kind: FaultCrash, Node: id, Dur: c.between(5*time.Millisecond, 800*time.Millisecond)}
}

func (c *cluster) drawPartition() Fault {
	ids := slices.Clone(c.ids)
	c.frng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	// Put the target alone or in the minority side.
	t := c.target()
	i := slices.Index(ids, t)
	ids[0], ids[i] = ids[i], ids[0]
	k := 1 + c.frng.IntN((len(ids)-1)/2+1)
	return Fault{Kind: FaultPartition, Side: slices.Clone(ids[:k]), Other: slices.Clone(ids[k:]),
		Dur: c.between(100*time.Millisecond, time.Second)}
}

func (c *cluster) drawOneWay() Fault {
	a, b := c.randomNode(), c.randomNode()
	if a == b {
		return Fault{Kind: FaultNone}
	}
	return Fault{Kind: FaultOneWay, Node: a, Peer: b, Dur: c.between(100*time.Millisecond, time.Second)}
}

func (c *cluster) drawSlow() Fault {
	return Fault{Kind: FaultSlow, Dur: c.between(100*time.Millisecond, 600*time.Millisecond)}
}

func (c *cluster) drawPause() Fault {
	return Fault{Kind: FaultPause, Node: c.target(), Dur: c.between(30*time.Millisecond, 400*time.Millisecond)}
}

// known reports whether id is a node of this cluster (a schedule shrunk to
// fewer nodes may still name the removed ones).
func (c *cluster) known(id raft.NodeID) bool { return slices.Contains(c.ids, id) }

func (c *cluster) only(ids []raft.NodeID) []raft.NodeID {
	var out []raft.NodeID
	for _, id := range ids {
		if c.known(id) {
			out = append(out, id)
		}
	}
	return out
}

// apply injects one resolved fault.
func (c *cluster) apply(f Fault) {
	c.annotate(f)
	switch f.Kind {
	case FaultCrash:
		id := f.Node
		if !c.known(id) || c.peers[id] == nil {
			return
		}
		c.crash(id)
		c.w.Schedule(f.Dur, sim.Event{Kind: "restart", Node: id}, func() {
			if c.peers[id] == nil && !c.failed {
				c.start(id)
			}
		})
	case FaultPartition:
		side, other := c.only(f.Side), c.only(f.Other)
		c.net.Heal()
		c.net.Partition(side, other)
		c.res.Partitions++
		c.w.Emit(sim.Event{Kind: "partition", Detail: fmt.Sprintf("%v | %v", side, other)})
		c.w.Schedule(f.Dur, sim.Event{Kind: "heal-partition"}, c.net.Heal)
	case FaultOneWay:
		a, b := f.Node, f.Peer
		if !c.known(a) || !c.known(b) {
			return
		}
		c.net.Block(a, b)
		c.res.OneWayBlocks++
		c.w.Emit(sim.Event{Kind: "block", Node: a, Peer: b})
		c.w.Schedule(f.Dur, sim.Event{Kind: "unblock", Node: a, Peer: b}, func() { c.net.Unblock(a, b) })
	case FaultSlow:
		normal := c.opt.Net
		slow := normal
		slow.MaxDelay = 250 * time.Millisecond
		c.net.SetConfig(slow)
		c.w.Emit(sim.Event{Kind: "slow"})
		c.w.Schedule(f.Dur, sim.Event{Kind: "net-normal"}, func() { c.net.SetConfig(normal) })
	case FaultPause:
		id := f.Node
		if !c.known(id) {
			return
		}
		c.pause[id] = c.w.Now() + f.Dur
		c.res.Pauses++
		c.w.Emit(sim.Event{Kind: "pause", Node: id, Attrs: []sim.Attr{sim.A("until", int64(c.pause[id]))}})
	}
}

// annotate puts the fault on the linearizability visualisation's timeline:
// node faults on that node's row, network faults on a "network" row.
func (c *cluster) annotate(f Fault) {
	row, desc := "network", f.Kind
	switch f.Kind {
	case FaultCrash:
		row, desc = fmt.Sprintf("n%d", f.Node), "crashed"
	case FaultPause:
		row, desc = fmt.Sprintf("n%d", f.Node), "paused"
	case FaultPartition:
		desc = fmt.Sprintf("partition %v | %v", f.Side, f.Other)
	case FaultOneWay:
		desc = fmt.Sprintf("block n%d->n%d", f.Node, f.Peer)
	case FaultSlow:
		desc = "slow network"
	default:
		return
	}
	c.hist.Annotate(check.Annotation{Row: row, Start: c.w.Now(), End: c.w.Now() + f.Dur, Description: desc, Details: f.String()})
}
