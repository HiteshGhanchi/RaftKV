// Package debug is a time-travelling debugger for simulated raftlab runs.
//
// A Session owns one harness run and can step it forward, run to a
// breakpoint, and go back. Going back re-executes the run from the start up
// to the target step: the simulation is deterministic, so the replayed prefix
// is identical, and a whole run takes milliseconds (docs/DECISIONS.md V1).
// The terminal debugger (cmd/raftlab-debug) and, later, the browser build use
// this package, so they always agree.
package debug

import (
	"fmt"
	"time"

	"raftlab/harness"
	"raftlab/sim"
)

// Session is one debugged run.
type Session struct {
	opt    harness.Options
	sim    *harness.Sim
	events []sim.Event // every event so far
	last   int         // index in events where the most recent step's events start
	done   bool        // the run cannot advance any more
	result *harness.Result
	breaks []Breakpoint
}

// Breakpoint is a named predicate checked after every step by Continue.
type Breakpoint struct {
	Expr string
	Pred Pred
}

// Open starts a session for the given options. Options.Sink is replaced.
func Open(opt harness.Options) *Session {
	s := &Session{opt: opt}
	s.reset()
	return s
}

func (s *Session) reset() {
	s.events, s.last, s.done, s.result = s.events[:0], 0, false, nil
	o := s.opt
	o.TraceLog = false
	o.Sink = func(e sim.Event) { s.events = append(s.events, e) }
	s.sim = harness.Start(o)
}

// Options returns the options of the debugged run.
func (s *Session) Options() harness.Options { return s.opt }

// Sim gives read access to the running simulation.
func (s *Session) Sim() *harness.Sim { return s.sim }

// Step returns the number of events run so far.
func (s *Session) Step() uint64 { return s.sim.Steps() }

// Now returns the current virtual time.
func (s *Session) Now() time.Duration { return s.sim.Now() }

// Done reports whether the run has ended (failed or reached its end time).
func (s *Session) Done() bool { return s.done }

// Events returns every event recorded so far.
func (s *Session) Events() []sim.Event { return s.events }

// LastEvents returns the events recorded by the most recent step.
func (s *Session) LastEvents() []sim.Event { return s.events[s.last:] }

// Forward runs up to n more events and returns how many ran.
func (s *Session) Forward(n uint64) uint64 {
	var k uint64
	for ; k < n; k++ {
		if !s.stepOnce() {
			break
		}
	}
	return k
}

func (s *Session) stepOnce() bool {
	if s.done {
		return false
	}
	mark := len(s.events)
	if !s.sim.Step() {
		s.done = true
		return false
	}
	s.last = mark
	return true
}

// RunTo steps until p holds after a step, the run ends, or limit events have
// run (0 means no limit). It reports whether p was hit.
func (s *Session) RunTo(p Pred, limit uint64) bool {
	for n := uint64(0); limit == 0 || n < limit; n++ {
		if !s.stepOnce() {
			return false
		}
		if p(s) {
			return true
		}
	}
	return false
}

// Goto moves to exactly step k (or the last step, if the run ends sooner),
// re-executing from the start when k is behind the current step.
func (s *Session) Goto(k uint64) {
	if k < s.Step() {
		s.reset()
	}
	s.Forward(k - s.Step())
}

// Back moves n steps backwards.
func (s *Session) Back(n uint64) {
	cur := s.Step()
	if n > cur {
		n = cur
	}
	s.Goto(cur - n)
}

// AddBreak adds a breakpoint.
func (s *Session) AddBreak(expr string) error {
	p, err := Parse(expr)
	if err != nil {
		return err
	}
	s.breaks = append(s.breaks, Breakpoint{Expr: expr, Pred: p})
	return nil
}

// Breaks lists the breakpoints.
func (s *Session) Breaks() []Breakpoint { return s.breaks }

// ClearBreaks removes all breakpoints.
func (s *Session) ClearBreaks() { s.breaks = nil }

// Continue runs until any breakpoint holds or the run ends. It returns the
// breakpoint that stopped it, or "" if none did.
func (s *Session) Continue() string {
	for s.stepOnce() {
		for _, b := range s.breaks {
			if b.Pred(s) {
				return b.Expr
			}
		}
	}
	return ""
}

// Finish runs to the end and returns the result (computed once).
func (s *Session) Finish() harness.Result {
	if s.result == nil {
		for s.stepOnce() {
		}
		r := s.sim.Finish()
		s.result = &r
	}
	return *s.result
}

// Where describes the current position.
func (s *Session) Where() string {
	state := "running"
	if v := s.sim.Violation(); v != "" {
		state = "VIOLATION " + v
	} else if s.done {
		state = "finished"
	}
	return fmt.Sprintf("step %d, t=%v, %s", s.Step(), s.Now(), state)
}
