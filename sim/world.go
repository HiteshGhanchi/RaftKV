// Package sim is a deterministic discrete-event simulator.
//
// Everything that would normally be nondeterministic in a distributed system
// (time, message delivery, disk durability, random choices) is driven from one
// World, which owns a virtual clock, a priority queue of pending events and a
// single seeded random number generator. Events run one at a time on the
// caller's goroutine, so the same seed and the same binary always produce the
// same execution. The package knows nothing about Raft: it is reused by any
// code that wants reproducible concurrency.
package sim

import (
	"container/heap"
	"fmt"
	"hash"
	"hash/fnv"
	"math/rand/v2"
	"time"
)

// NewRand returns a seeded PCG generator. All randomness in a simulation must
// come from generators created here, never from the global math/rand functions.
func NewRand(seed uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
}

// World is the simulation: a virtual clock plus a queue of events.
type World struct {
	now   time.Duration
	seq   uint64
	queue eventQueue
	rng   *rand.Rand

	trace    hash.Hash64
	traceLog []string
	logTrace bool
	steps    uint64
}

// New creates a world whose every random choice derives from seed.
func New(seed uint64) *World {
	return &World{rng: NewRand(seed), trace: fnv.New64a()}
}

// Now returns the current virtual time (time since the simulation started).
func (w *World) Now() time.Duration { return w.now }

// Rand returns the world's generator. Components that need their own stream
// should call Fork so that adding a random call in one component does not
// shift the choices made by another.
func (w *World) Rand() *rand.Rand { return w.rng }

// Fork derives an independent generator from the world's generator.
func (w *World) Fork() *rand.Rand { return NewRand(w.rng.Uint64()) }

// Steps is the number of events executed so far.
func (w *World) Steps() uint64 { return w.steps }

// Timer is a handle to a scheduled event.
type Timer struct{ ev *event }

// Stop cancels the event. It reports whether the event had not yet run.
func (t *Timer) Stop() bool {
	if t == nil || t.ev == nil || t.ev.done {
		return false
	}
	t.ev.done = true
	return true
}

// After schedules fn to run once d of virtual time has passed. Events with the
// same deadline run in the order they were scheduled. The label is mixed into
// the execution trace, so it should describe the event (for example
// "deliver 1->2 MsgApp").
func (w *World) After(d time.Duration, label string, fn func()) *Timer {
	if d < 0 {
		d = 0
	}
	w.seq++
	ev := &event{at: w.now + d, seq: w.seq, label: label, fn: fn}
	heap.Push(&w.queue, ev)
	return &Timer{ev: ev}
}

// Step runs the next pending event. It returns false when the queue is empty.
func (w *World) Step() bool {
	for w.queue.Len() > 0 {
		ev := heap.Pop(&w.queue).(*event)
		if ev.done {
			continue
		}
		ev.done = true
		w.now = ev.at
		w.steps++
		w.record(ev.label)
		ev.fn()
		return true
	}
	return false
}

// RunUntil runs every event due at or before t, then sets the clock to t.
func (w *World) RunUntil(t time.Duration) {
	for w.queue.Len() > 0 {
		next := w.queue[0]
		if next.done {
			heap.Pop(&w.queue)
			continue
		}
		if next.at > t {
			break
		}
		w.Step()
	}
	if t > w.now {
		w.now = t
	}
}

// RunUntilOr is RunUntil, but it also stops as soon as stop returns true
// after an event (used to end a run at the first invariant violation).
func (w *World) RunUntilOr(t time.Duration, stop func() bool) {
	for w.queue.Len() > 0 && !stop() {
		next := w.queue[0]
		if next.done {
			heap.Pop(&w.queue)
			continue
		}
		if next.at > t {
			break
		}
		w.Step()
	}
	if t > w.now && !stop() {
		w.now = t
	}
}

// RunFor runs the simulation for d of virtual time.
func (w *World) RunFor(d time.Duration) { w.RunUntil(w.now + d) }

// Note mixes an extra observation into the trace without scheduling anything.
func (w *World) Note(format string, args ...any) { w.record(fmt.Sprintf(format, args...)) }

// EnableTraceLog keeps a human-readable copy of every trace line (memory heavy;
// meant for debugging a single failing seed).
func (w *World) EnableTraceLog() { w.logTrace = true }

// TraceLog returns the recorded trace lines when EnableTraceLog was called.
func (w *World) TraceLog() []string { return w.traceLog }

// TraceHash summarises the whole execution so far. Two runs with the same
// seed must produce the same hash; that is the determinism contract.
func (w *World) TraceHash() uint64 { return w.trace.Sum64() }

func (w *World) record(label string) {
	line := fmt.Sprintf("%d %s\n", w.now, label)
	w.trace.Write([]byte(line))
	if w.logTrace {
		w.traceLog = append(w.traceLog, line[:len(line)-1])
	}
}

type event struct {
	at    time.Duration
	seq   uint64
	label string
	fn    func()
	done  bool
	index int
}

// eventQueue is a min-heap ordered by (deadline, scheduling order).
type eventQueue []*event

func (q eventQueue) Len() int { return len(q) }
func (q eventQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q eventQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}
func (q *eventQueue) Push(x any) {
	ev := x.(*event)
	ev.index = len(*q)
	*q = append(*q, ev)
}
func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	ev := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]
	return ev
}
