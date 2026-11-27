package harness

import (
	"encoding/json"
	"reflect"
	"testing"

	"raftlab/mutation"
)

// replayOf runs o in record mode, then again from only the recorded schedule
// (sent through JSON, as a schedule file would be).
func replayOf(t *testing.T, o Options) (rec, rep Result) {
	t.Helper()
	rec = Run(o)
	b, err := json.Marshal(rec.Schedule)
	if err != nil {
		t.Fatal(err)
	}
	var s Schedule
	if err := json.Unmarshal(b, &s); err != nil {
		t.Fatal(err)
	}
	o.Schedule = &s
	rep = Run(o)
	return rec, rep
}

// A recorded fault schedule replays to the identical execution: same trace
// hash, same steps, same verdict, same schedule. This is the property that
// makes shrinking and shareable links valid.
func TestSchedule_RecordThenReplayIsIdentical(t *testing.T) {
	n := uint64(200)
	if testing.Short() {
		n = 40
	}
	check := func(name string, o Options) {
		rec, rep := replayOf(t, o)
		if rec.TraceHash != rep.TraceHash || rec.Steps != rep.Steps || rec.String() != rep.String() {
			t.Fatalf("%s: replay differs:\nrecord %s\nreplay %s", name, rec, rep)
		}
		if !reflect.DeepEqual(rec.Schedule, rep.Schedule) {
			t.Fatalf("%s: replay recorded a different schedule", name)
		}
	}
	for seed := uint64(1); seed <= n; seed++ {
		check("default", DefaultOptions(seed))
		if seed <= n/4 {
			check("swarm", SwarmOptions(seed))
		}
	}
	for _, tc := range bugZooSeeds {
		o := DefaultOptions(tc.seed)
		o.Mutations = tc.m
		check(tc.m.String(), o)
	}
}

// A run with no faults at all records an empty schedule, and replaying an
// empty schedule injects nothing.
func TestSchedule_EmptyScheduleInjectsNothing(t *testing.T) {
	o := DefaultOptions(7)
	o.Schedule = &Schedule{}
	r := Run(o)
	if r.Crashes+r.Partitions+r.Pauses+r.OneWayBlocks != 0 || r.Schedule.Effective() != 0 {
		t.Fatalf("empty schedule injected faults: %s", r)
	}
	if r.Failed() {
		t.Fatalf("fault-free run failed: %s", r)
	}
}

// Swarm options always keep at least one fault kind and vary across seeds.
func TestSwarmOptionsVaryFaultKinds(t *testing.T) {
	seen := map[Faults]bool{}
	for seed := uint64(1); seed <= 200; seed++ {
		f := SwarmOptions(seed).Faults
		if !(f.Crash || f.Partition || f.OneWay || f.Pause || f.Slow) {
			t.Fatalf("seed %d: no fault kind enabled", seed)
		}
		seen[f] = true
	}
	if len(seen) < 20 {
		t.Fatalf("only %d distinct fault mixes in 200 seeds", len(seen))
	}
	_ = mutation.All
}
