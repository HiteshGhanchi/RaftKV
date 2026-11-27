package debug

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Help lists the debugger commands.
const Help = `commands:
  where                    current step, time and status
  step [N]       (s)       run N events (default 1) and print what they did
  back [N]       (b)       go back N events (default 1) by re-running the prefix
  goto K                   go to step K
  run-to COND    (until)   run until COND holds (see "conds")
  break COND               add a breakpoint
  breaks / clear           list / remove breakpoints
  continue       (c)       run until a breakpoint holds or the run ends
  nodes                    one line per node: role, term, vote, leader, commit, applied, log
  node N                   node N in detail, including its log
  inflight                 messages sent and not yet delivered
  events [N]               the last N events (default 20)
  faults                   faults injected so far
  finish                   run to the end and print the result
  quit           (q)
`

// Conds documents breakpoint conditions.
const Conds = `conditions (combine with "and"; OP is > >= < <= ==):
  violation | elected | leader N | down N | up N
  term OP K | commit N OP K | applied N OP K
  kind NAME (crash, partition, pause, role, apply, violation, deliver, ...)
  time OP DURATION (e.g. time > 1.5s) | step OP K
`

// Exec runs one command line and writes its output to w. It returns true
// when the command asks to quit.
func (s *Session) Exec(line string, w io.Writer) (quit bool) {
	f := strings.Fields(line)
	if len(f) == 0 {
		return false
	}
	arg := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), f[0]))
	num := func(def uint64) (uint64, bool) {
		if len(f) < 2 {
			return def, true
		}
		n, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			fmt.Fprintf(w, "not a number: %s\n", f[1])
			return 0, false
		}
		return n, true
	}
	switch f[0] {
	case "help", "h", "?":
		fmt.Fprint(w, Help)
	case "conds":
		fmt.Fprint(w, Conds)
	case "where":
		fmt.Fprintln(w, s.Where())
	case "step", "s":
		n, ok := num(1)
		if !ok {
			return false
		}
		from := len(s.events)
		ran := s.Forward(n)
		evs := s.events[from:]
		if len(evs) > 40 {
			fmt.Fprintf(w, "... %d earlier events not shown\n", len(evs)-40)
			evs = evs[len(evs)-40:]
		}
		for _, e := range evs {
			fmt.Fprintf(w, "  #%d %s\n", e.Step, e)
		}
		if ran < n {
			fmt.Fprintf(w, "run ended after %d events\n", ran)
		}
		fmt.Fprintln(w, s.Where())
	case "back", "b":
		n, ok := num(1)
		if !ok {
			return false
		}
		s.Back(n)
		fmt.Fprintln(w, s.Where())
	case "goto":
		n, ok := num(0)
		if !ok {
			return false
		}
		s.Goto(n)
		fmt.Fprintln(w, s.Where())
	case "run-to", "until":
		p, err := Parse(arg)
		if err != nil {
			fmt.Fprintln(w, err)
			return false
		}
		if s.RunTo(p, 0) {
			fmt.Fprintf(w, "hit: %s\n", arg)
			s.printLast(w)
		} else {
			fmt.Fprintf(w, "not hit before the run ended\n")
		}
		fmt.Fprintln(w, s.Where())
	case "break":
		if err := s.AddBreak(arg); err != nil {
			fmt.Fprintln(w, err)
			return false
		}
		fmt.Fprintf(w, "breakpoint %d: %s\n", len(s.breaks), arg)
	case "breaks":
		for i, b := range s.breaks {
			fmt.Fprintf(w, "%d: %s\n", i+1, b.Expr)
		}
	case "clear":
		s.ClearBreaks()
		fmt.Fprintln(w, "breakpoints cleared")
	case "continue", "c":
		if hit := s.Continue(); hit != "" {
			fmt.Fprintf(w, "breakpoint: %s\n", hit)
			s.printLast(w)
		} else {
			fmt.Fprintln(w, "no breakpoint hit before the run ended")
		}
		fmt.Fprintln(w, s.Where())
	case "nodes":
		for _, n := range s.sim.Nodes() {
			fmt.Fprintln(w, NodeLine(n, s.Now()))
		}
	case "node":
		n, ok := num(0)
		if !ok {
			return false
		}
		for _, ns := range s.sim.Nodes() {
			if uint64(ns.ID) != n {
				continue
			}
			fmt.Fprintln(w, NodeLine(ns, s.Now()))
			for _, e := range ns.Log {
				kind := "entry"
				if e.Noop {
					kind = "no-op"
				}
				fmt.Fprintf(w, "  [%d] term %d %s (%d bytes)\n", e.Index, e.Term, kind, e.Size)
			}
		}
	case "inflight":
		fl := s.sim.InFlight()
		if len(fl) == 0 {
			fmt.Fprintln(w, "no messages in flight")
		}
		for _, m := range fl {
			fmt.Fprintf(w, "  #%d n%d -> n%d arrives %v: %s\n", m.ID, m.From, m.To, m.ArriveAt, m.Desc)
		}
	case "events":
		n, ok := num(20)
		if !ok {
			return false
		}
		evs := s.events
		if uint64(len(evs)) > n {
			evs = evs[uint64(len(evs))-n:]
		}
		for _, e := range evs {
			fmt.Fprintf(w, "  #%d %s\n", e.Step, e)
		}
	case "faults":
		fs := s.sim.Schedule()
		if len(fs) == 0 {
			fmt.Fprintln(w, "no faults yet")
		}
		for _, ft := range fs {
			fmt.Fprintf(w, "  %s\n", ft)
		}
	case "finish":
		r := s.Finish()
		fmt.Fprintln(w, r)
	case "quit", "q", "exit":
		return true
	default:
		fmt.Fprintf(w, "unknown command %q (try help)\n", f[0])
	}
	return false
}

func (s *Session) printLast(w io.Writer) {
	for _, e := range s.LastEvents() {
		fmt.Fprintf(w, "  #%d %s\n", e.Step, e)
	}
}
