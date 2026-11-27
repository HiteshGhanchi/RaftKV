package debug

import (
	"bytes"
	"fmt"
	"io"

	"raftlab/harness"
	"raftlab/sim"
)

// DiffResult describes where two runs first differ.
type DiffResult struct {
	Same      bool        // the traces are identical
	Index     int         // index of the first differing event
	Common    []sim.Event // events before the divergence (the last few, for context)
	A, B      []sim.Event // events from the divergence on (the first few of each run)
	ResA      harness.Result
	ResB      harness.Result
	LenA      int
	LenB      int
	FirstStep uint64 // step of the first differing event in run A
}

// Diff runs a and b and compares their traces event by event. context is
// how many events to keep on each side of the first difference.
func Diff(a, b harness.Options, context int) DiffResult {
	collect := func(o harness.Options) ([]sim.Event, harness.Result) {
		var evs []sim.Event
		o.TraceLog = false
		o.Sink = func(e sim.Event) { evs = append(evs, e) }
		r := harness.Run(o)
		return evs, r
	}
	ea, ra := collect(a)
	eb, rb := collect(b)
	d := DiffResult{ResA: ra, ResB: rb, LenA: len(ea), LenB: len(eb)}
	i := 0
	for i < len(ea) && i < len(eb) && bytes.Equal(ea[i].Canonical(), eb[i].Canonical()) {
		i++
	}
	if i == len(ea) && i == len(eb) {
		d.Same = true
		return d
	}
	d.Index = i
	d.Common = ea[max(0, i-context):i]
	d.A = ea[i:min(len(ea), i+context)]
	d.B = eb[i:min(len(eb), i+context)]
	if len(d.A) > 0 {
		d.FirstStep = d.A[0].Step
	}
	return d
}

// Print writes a readable report of the difference.
func (d DiffResult) Print(w io.Writer, nameA, nameB string) {
	if d.Same {
		fmt.Fprintf(w, "identical traces (%d events)\n", d.LenA)
		return
	}
	fmt.Fprintf(w, "first difference at event %d (step %d); %d and %d events in total\n", d.Index, d.FirstStep, d.LenA, d.LenB)
	fmt.Fprintln(w, "common prefix (last events):")
	for _, e := range d.Common {
		fmt.Fprintf(w, "    #%d %s\n", e.Step, e)
	}
	for _, side := range []struct {
		name string
		evs  []sim.Event
		res  harness.Result
	}{{nameA, d.A, d.ResA}, {nameB, d.B, d.ResB}} {
		fmt.Fprintf(w, "%s:\n", side.name)
		for _, e := range side.evs {
			fmt.Fprintf(w, "  > #%d %s\n", e.Step, e)
		}
		fmt.Fprintf(w, "  result: %s\n", side.res)
	}
}
