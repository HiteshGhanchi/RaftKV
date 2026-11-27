package debug

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"raftlab/harness"
	"raftlab/mutation"
)

// figure8Seed is the commit-prev-term regression seed (harness_test.go,
// results/bugzoo.json).
const figure8Seed = 151

// Going back n steps lands in exactly the state a fresh run reaches by going
// forward to the same step: same node states, same in-flight messages, same
// trace hash.
func TestDebug_BackMatchesFreshRun(t *testing.T) {
	for _, seed := range []uint64{1, 2, 3} {
		s := Open(harness.DefaultOptions(seed))
		s.Forward(3000)
		s.Back(1200)
		fresh := Open(harness.DefaultOptions(seed))
		fresh.Forward(1800)
		if s.Step() != 1800 || fresh.Step() != 1800 {
			t.Fatalf("seed %d: steps %d and %d, want 1800", seed, s.Step(), fresh.Step())
		}
		if s.Sim().TraceHash() != fresh.Sim().TraceHash() {
			t.Fatalf("seed %d: trace hash differs after back", seed)
		}
		if !reflect.DeepEqual(s.Sim().Nodes(), fresh.Sim().Nodes()) {
			t.Fatalf("seed %d: node states differ after back", seed)
		}
		if !reflect.DeepEqual(s.Sim().InFlight(), fresh.Sim().InFlight()) {
			t.Fatalf("seed %d: in-flight messages differ after back", seed)
		}
		if len(s.Events()) != len(fresh.Events()) {
			t.Fatalf("seed %d: %d events after back, fresh run has %d", seed, len(s.Events()), len(fresh.Events()))
		}
	}
}

// A debugged run is the same run the chaos runner sees.
func TestDebug_SessionMatchesRun(t *testing.T) {
	for _, seed := range []uint64{4, 5} {
		o := harness.DefaultOptions(seed)
		want := harness.Run(o)
		got := Open(o).Finish()
		if got.String() != want.String() {
			t.Fatalf("seed %d:\nsession %s\nrun     %s", seed, got, want)
		}
	}
}

// The violation breakpoint stops on the step where the harness reports the
// violation, and stepping further is impossible.
func TestDebug_ViolationBreakpointStopsAtTheViolation(t *testing.T) {
	o := harness.DefaultOptions(figure8Seed)
	o.Mutations = mutation.CommitPrevTerm
	want := harness.Run(o)
	if want.Violation == "" {
		t.Fatalf("seed %d no longer fails with commit-prev-term: %s", figure8Seed, want)
	}
	s := Open(o)
	if err := s.AddBreak("violation"); err != nil {
		t.Fatal(err)
	}
	if hit := s.Continue(); hit != "violation" {
		t.Fatalf("breakpoint not hit (got %q)", hit)
	}
	if s.Now() != want.ViolationAt || s.Sim().Violation() != want.Violation {
		t.Fatalf("stopped at %v with %q, run reports %v %q", s.Now(), s.Sim().Violation(), want.ViolationAt, want.Violation)
	}
	if s.Step() != want.Steps {
		t.Fatalf("stopped at step %d, run ended at step %d", s.Step(), want.Steps)
	}
	if s.Forward(1) != 0 {
		t.Fatal("stepped past a violation")
	}
	// Going back from the violation and running to it again is deterministic.
	s.Back(100)
	if !s.RunTo(mustParse(t, "violation"), 0) || s.Step() != want.Steps {
		t.Fatal("re-running to the violation did not reach the same step")
	}
}

func TestDebug_Predicates(t *testing.T) {
	s := Open(harness.DefaultOptions(1))
	if !s.RunTo(mustParse(t, "elected"), 0) {
		t.Fatal("no election")
	}
	var leader uint64
	for _, n := range s.Sim().Nodes() {
		if n.Up && n.Status.State.String() == "Leader" {
			leader = uint64(n.ID)
		}
	}
	if leader == 0 {
		t.Fatal("elected predicate hit but no leader")
	}
	if !mustParse(t, "leader "+itoa(leader)+" and term >= 1")(s) {
		t.Fatal("leader predicate false for the elected node")
	}
	if !s.RunTo(mustParse(t, "commit "+itoa(leader)+" > 3"), 0) {
		t.Fatal("commit never passed 3")
	}
	if !s.RunTo(mustParse(t, "time > 1s"), 0) || s.Now() <= 1e9 {
		t.Fatal("time predicate")
	}
	for _, bad := range []string{"", "leader", "term ~ 3", "time > soon", "frobnicate"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("Parse(%q) accepted", bad)
		}
	}
}

// A short scripted session, as a user would type it.
func TestDebug_ExecScript(t *testing.T) {
	o := harness.DefaultOptions(figure8Seed)
	o.Mutations = mutation.CommitPrevTerm
	s := Open(o)
	var out strings.Builder
	for _, cmd := range []string{"where", "step 3", "break violation", "continue", "nodes", "events 5", "faults", "back 10", "inflight", "goto 0", "where", "bogus"} {
		out.WriteString("> " + cmd + "\n")
		if s.Exec(cmd, &out) {
			t.Fatal("unexpected quit")
		}
	}
	got := out.String()
	for _, want := range []string{"step 0, t=0s, running", "breakpoint: violation", "VIOLATION LeaderCompleteness", "n1 ", "crash", "unknown command"} {
		if !strings.Contains(got, want) {
			t.Errorf("transcript lacks %q:\n%s", want, got)
		}
	}
	if !s.Exec("quit", &out) {
		t.Fatal("quit did not quit")
	}
}

func mustParse(t *testing.T, expr string) Pred {
	t.Helper()
	p, err := Parse(expr)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

// The same seed with and without the Figure 8 mutation runs identically up
// to some point, then diverges; the same options never diverge.
func TestDiff_FindsFirstDivergence(t *testing.T) {
	a := harness.DefaultOptions(figure8Seed)
	b := a
	b.Mutations = mutation.CommitPrevTerm
	if d := Diff(a, a, 5); !d.Same {
		t.Fatalf("a run differs from itself at event %d", d.Index)
	}
	d := Diff(a, b, 5)
	if d.Same || d.Index == 0 || len(d.Common) != 5 || len(d.A) == 0 || len(d.B) == 0 {
		t.Fatalf("unexpected diff: same=%v index=%d common=%d", d.Same, d.Index, len(d.Common))
	}
	if string(d.A[0].Canonical()) == string(d.B[0].Canonical()) {
		t.Fatal("reported divergence point has equal events")
	}
	if d.ResA.Failed() || !d.ResB.Failed() {
		t.Fatalf("want correct run to pass and mutated run to fail: %s / %s", d.ResA, d.ResB)
	}
	var out strings.Builder
	d.Print(&out, "correct", "mutated")
	if !strings.Contains(out.String(), "first difference at event") {
		t.Fatalf("report:\n%s", out.String())
	}
}
