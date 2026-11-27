package sim

import (
	"math/rand/v2"
	"slices"
	"time"
)

// NodeID identifies a node on a simulated network.
type NodeID = uint64

// NetConfig describes how badly the network behaves.
type NetConfig struct {
	MinDelay time.Duration // every message takes at least this long
	MaxDelay time.Duration // and at most this long; random delays reorder messages
	DropRate float64       // probability a message is lost
	DupRate  float64       // probability a message is delivered twice
}

// NetStats counts what happened to messages.
type NetStats struct {
	Sent, Delivered, Dropped, Duplicated uint64
}

// Network delivers messages of type M between registered nodes with random
// delay, loss, duplication and reordering, and supports directional
// partitions. It never inspects message contents.
type Network[M any] struct {
	w        *World
	rng      *rand.Rand
	cfg      NetConfig
	handlers map[NodeID]func(from NodeID, m M)
	blocked  map[[2]NodeID]bool
	down     map[NodeID]bool
	describe func(M) string
	Stats    NetStats

	nextID   uint64
	inflight map[uint64]InFlight
}

// InFlight describes a message that has been sent and not yet delivered or
// dropped.
type InFlight struct {
	ID       uint64
	From, To NodeID
	SentAt   time.Duration
	ArriveAt time.Duration
	Desc     string
}

// NewNetwork creates a network driven by w. describe, if not nil, turns a
// message into a short string for the execution trace.
func NewNetwork[M any](w *World, cfg NetConfig, describe func(M) string) *Network[M] {
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	return &Network[M]{
		w:        w,
		rng:      w.Fork(),
		cfg:      cfg,
		handlers: map[NodeID]func(NodeID, M){},
		blocked:  map[[2]NodeID]bool{},
		down:     map[NodeID]bool{},
		describe: describe,
		inflight: map[uint64]InFlight{},
	}
}

// InFlight lists the messages currently in flight, ordered by arrival time
// (ties by send order).
func (n *Network[M]) InFlight() []InFlight {
	out := make([]InFlight, 0, len(n.inflight))
	for _, f := range n.inflight {
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b InFlight) int {
		if a.ArriveAt != b.ArriveAt {
			if a.ArriveAt < b.ArriveAt {
				return -1
			}
			return 1
		}
		if a.ID < b.ID {
			return -1
		}
		return 1
	})
	return out
}

// Register sets the function that receives messages addressed to id.
func (n *Network[M]) Register(id NodeID, h func(from NodeID, m M)) { n.handlers[id] = h }

// SetConfig changes delay, loss and duplication from now on.
func (n *Network[M]) SetConfig(cfg NetConfig) {
	if cfg.MaxDelay < cfg.MinDelay {
		cfg.MaxDelay = cfg.MinDelay
	}
	n.cfg = cfg
}

// Config returns the current network behaviour.
func (n *Network[M]) Config() NetConfig { return n.cfg }

// SetDown marks a node as crashed (true) or running (false). Messages that
// arrive at a crashed node are dropped, including ones already in flight.
func (n *Network[M]) SetDown(id NodeID, down bool) { n.down[id] = down }

// Block stops messages from one node to another (one direction only).
func (n *Network[M]) Block(from, to NodeID) { n.blocked[[2]NodeID{from, to}] = true }

// Unblock reverses Block.
func (n *Network[M]) Unblock(from, to NodeID) { delete(n.blocked, [2]NodeID{from, to}) }

// Partition splits the nodes into groups that can only talk inside their own
// group. Nodes not listed keep their existing links.
func (n *Network[M]) Partition(groups ...[]NodeID) {
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, x := range a {
				for _, y := range b {
					n.Block(x, y)
				}
			}
		}
	}
}

// Heal removes every partition and blocked link.
func (n *Network[M]) Heal() { n.blocked = map[[2]NodeID]bool{} }

// Blocked reports whether messages from one node to another are blocked.
func (n *Network[M]) Blocked(from, to NodeID) bool { return n.blocked[[2]NodeID{from, to}] }

// Send queues m for delivery. The partition is checked both when the message
// is sent and when it would arrive.
func (n *Network[M]) Send(from, to NodeID, m M) {
	n.Stats.Sent++
	desc := ""
	if n.describe != nil {
		desc = n.describe(m)
	}
	if n.Blocked(from, to) {
		n.Stats.Dropped++
		n.w.Emit(Event{Kind: "drop", Node: from, Peer: to, Detail: "blocked: " + desc})
		return
	}
	if chance(n.rng, n.cfg.DropRate) {
		n.Stats.Dropped++
		n.w.Emit(Event{Kind: "drop", Node: from, Peer: to, Detail: "lost: " + desc})
		return
	}
	copies := 1
	if chance(n.rng, n.cfg.DupRate) {
		copies = 2
		n.Stats.Duplicated++
	}
	for i := range copies {
		delay := n.cfg.MinDelay
		if span := n.cfg.MaxDelay - n.cfg.MinDelay; span > 0 {
			delay += time.Duration(n.rng.Int64N(int64(span) + 1))
		}
		n.nextID++
		id := n.nextID
		kind := "send"
		if i > 0 {
			kind = "dup"
		}
		n.w.Emit(Event{Kind: kind, Node: from, Peer: to, Attrs: []Attr{A("msg", int64(id)), A("delay", int64(delay))}, Detail: desc})
		n.inflight[id] = InFlight{ID: id, From: from, To: to, SentAt: n.w.Now(), ArriveAt: n.w.Now() + delay, Desc: desc}
		n.w.Schedule(delay, Event{Kind: "deliver", Node: to, Peer: from, Attrs: []Attr{A("msg", int64(id))}, Detail: desc}, func() {
			delete(n.inflight, id)
			h := n.handlers[to]
			if h == nil || n.down[to] || n.Blocked(from, to) {
				n.Stats.Dropped++
				n.w.Emit(Event{Kind: "drop", Node: from, Peer: to, Attrs: []Attr{A("msg", int64(id))}, Detail: "at arrival: " + desc})
				return
			}
			n.Stats.Delivered++
			h(from, m)
		})
	}
}

func chance(r *rand.Rand, p float64) bool { return p > 0 && r.Float64() < p }
