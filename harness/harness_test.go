package harness

import (
	"testing"

	"raftlab/check"
	"raftlab/mutation"
)

// The determinism contract at cluster scale: the same seed replays exactly,
// including the fault schedule, every message, every client operation and
// the final verdict.
func TestSameSeedReplaysExactly(t *testing.T) {
	for _, m := range []mutation.Set{0, mutation.NoDedup} {
		for seed := uint64(1); seed <= 5; seed++ {
			o := DefaultOptions(seed)
			o.Mutations = m
			a, b := Run(o), Run(o)
			if a.TraceHash != b.TraceHash || a.String() != b.String() {
				t.Fatalf("seed %d (%s) did not replay:\n%s\n%s", seed, m, a, b)
			}
		}
	}
	if Run(DefaultOptions(1)).TraceHash == Run(DefaultOptions(2)).TraceHash {
		t.Fatal("different seeds gave the same trace")
	}
}

// Correct code must survive every fault on a sweep of seeds, with every
// history conclusively linearizable.
func TestCorrectCodeSurvivesChaos(t *testing.T) {
	n := uint64(300)
	if testing.Short() {
		n = 50
	}
	ops := 0
	for _, r := range Sweep(1, n, 0, DefaultOptions, false) {
		if r.Failed() || r.Linearizability != check.Linearizable {
			t.Errorf("%s", r)
		}
		if r.Crashes+r.Partitions+r.Pauses == 0 {
			t.Errorf("seed %d injected no faults", r.Seed)
		}
		ops += r.Completed
	}
	t.Logf("%d seeds, %d client operations, all linearizable", n, ops)
}

// The bug zoo regression suite: for every mutation, the first seed on which
// the harness caught it (from results/bugzoo.json) must keep failing, and the
// same seed must pass with correct code. If a change to the code moves these
// seeds, re-run `raftlab-chaos -bugzoo` and update the table.
func TestBugZooSeedsStillFail(t *testing.T) {
	cases := bugZooSeeds
	if len(cases) != len(mutation.All) {
		t.Fatal("every mutation needs a regression seed")
	}
	for _, tc := range cases {
		o := DefaultOptions(tc.seed)
		o.Mutations = tc.m
		if r := Run(o); !r.Failed() {
			t.Errorf("%s no longer caught at seed %d: %s", tc.m, tc.seed, r)
		}
		if r := Run(DefaultOptions(tc.seed)); r.Failed() {
			t.Errorf("seed %d fails even without %s: %s", tc.seed, tc.m, r)
		}
	}
}

// With no faults at all, every client operation completes.
func TestNoFaultsAllOpsComplete(t *testing.T) {
	o := DefaultOptions(9)
	o.Faults = Faults{}
	o.Net.DropRate, o.Net.DupRate = 0, 0
	r := Run(o)
	if r.Failed() || r.Unknown != 0 || r.Completed != r.Ops || r.Ops < 100 {
		t.Fatalf("fault-free run: %s", r)
	}
	if r.Leaders != 1 {
		t.Fatalf("fault-free run elected %d leaders, want 1", r.Leaders)
	}
}

// bugZooSeeds is the first seed on which each mutation was caught by
// `raftlab-chaos -bugzoo` (results/bugzoo.json).
var bugZooSeeds = []struct {
	m    mutation.Set
	seed uint64
}{
	{mutation.CommitPrevTerm, 151},
	{mutation.SkipUpToDateCheck, 1},
	{mutation.ForgetVote, 179},
	{mutation.ApplyBeforeCommit, 1},
	{mutation.StaleRead, 35},
	{mutation.AcceptStaleTerm, 28},
	{mutation.SkipFsync, 3},
	{mutation.NoDedup, 1},
}
