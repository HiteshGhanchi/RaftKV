package harness

import (
	"path/filepath"
	"testing"

	"raftlab/check"
	"raftlab/mutation"
)

// Every shrunk bug-zoo failure in testdata/schedules must keep failing with
// the same property, replay bit for bit, and pass with correct code.
// Regenerate with: raftlab-chaos -shrink-zoo results/bugzoo.json -out results/shrink.json
func TestCorpusSchedulesStillFail(t *testing.T) {
	files, err := filepath.Glob("testdata/schedules/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) < len(mutation.All) {
		t.Fatalf("%d schedule files, want one per mutation (%d)", len(files), len(mutation.All))
	}
	for _, f := range files {
		rep, err := LoadRepro(f)
		if err != nil {
			t.Fatal(err)
		}
		o := rep.Options()
		a, b := Run(o), Run(o)
		if got := Property(a); got != rep.Expect {
			t.Errorf("%s: violates %q, want %q: %s", f, got, rep.Expect, a)
		}
		if a.TraceHash != b.TraceHash {
			t.Errorf("%s: does not replay deterministically", f)
		}
		o.Mutations = 0
		if r := Run(o); r.Failed() || r.Linearizability == check.Inconclusive {
			t.Errorf("%s: fails or is inconclusive with correct code: %s", f, r)
		}
	}
}
