package harness

import (
	"path/filepath"
	"slices"
	"testing"

	"raftlab/mutation"
)

func TestDdminFindsMinimalSubset(t *testing.T) {
	xs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	calls := 0
	got := ddmin(xs, func(s []int) bool {
		calls++
		return slices.Contains(s, 3) && slices.Contains(s, 9)
	})
	if !slices.Equal(got, []int{3, 9}) {
		t.Fatalf("ddmin = %v, want [3 9]", got)
	}
	if calls > 60 {
		t.Fatalf("ddmin used %d calls", calls)
	}
	if got := ddmin([]int{1, 2, 3}, func([]int) bool { return true }); len(got) != 0 {
		t.Fatalf("a failure that needs nothing should shrink to nothing, got %v", got)
	}
}

// Shrinking a failing run gives a smaller run that still violates the same
// property, replays bit for bit from its saved file, and passes with the
// mutation switched off.
func TestShrinkKeepsTheFailure(t *testing.T) {
	o := DefaultOptions(1)
	o.Mutations = mutation.SkipUpToDateCheck
	rep, st, err := Shrink(o, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Fewer faults and a simpler setup are the goal; the run itself may get
	// longer, because with fewer faults the bug can surface later.
	if st.After.Faults > st.Before.Faults || st.After.Nodes > st.Before.Nodes || st.After.Clients > st.Before.Clients {
		t.Fatalf("shrinking made the run bigger: %s -> %s", st.Before, st.After)
	}
	if st.After.Faults == st.Before.Faults && st.After.Clients == st.Before.Clients {
		t.Fatalf("shrinking removed nothing: %s", st.After)
	}
	path := filepath.Join(t.TempDir(), "r.json")
	if err := rep.Save(path); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadRepro(path)
	if err != nil {
		t.Fatal(err)
	}
	a, b := Run(loaded.Options()), Run(loaded.Options())
	if Property(a) != st.Property || a.TraceHash != b.TraceHash {
		t.Fatalf("saved repro: property %q (want %q), hashes %x %x", Property(a), st.Property, a.TraceHash, b.TraceHash)
	}
	correct := loaded.Options()
	correct.Mutations = 0
	if r := Run(correct); r.Failed() {
		t.Fatalf("shrunk schedule fails without the mutation: %s", r)
	}
	if _, _, err := Shrink(DefaultOptions(1), 0); err != ErrNotFailing {
		t.Fatalf("shrinking a passing run: err = %v", err)
	}
}
