package mutate

import (
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

const sample = "testdata/sample/sample.go"

func TestEnumerateFindsEveryOperatorOnce(t *testing.T) {
	ms, err := EnumerateFile(sample)
	if err != nil {
		t.Fatal(err)
	}
	ops := map[string]int{}
	ids := map[string]bool{}
	for _, m := range ms {
		ops[m.Op]++
		if ids[m.ID] {
			t.Fatalf("duplicate id %s", m.ID)
		}
		ids[m.ID] = true
		if m.Func == "T.String" {
			t.Fatalf("String methods must be skipped: %s", m.ID)
		}
		if strings.Contains(m.Desc, "mut.Has") {
			t.Fatalf("bug-zoo switches must be skipped: %s %s", m.ID, m.Desc)
		}
	}
	// Step has: if a<b&&b!=0 (if-true, if-false), < (boundary), && (logical),
	// != (negate), t.n++ (remove), t.n = a+1 (remove, off-by-one), a >= b
	// (boundary), t.n -= 2 (remove). Has has f&x != 0 (negate).
	want := map[string]int{OpIfTrue: 1, OpIfFalse: 1, OpBoundary: 2, OpLogical: 1, OpNegate: 2, OpRemove: 3, OpOffByOne: 1}
	for op, n := range want {
		if ops[op] != n {
			t.Errorf("%s: %d sites, want %d (all: %v)", op, ops[op], n, ops)
		}
	}
	if len(ms) != 11 {
		t.Errorf("%d mutants, want 11", len(ms))
	}
}

func TestEnumerationIsDeterministic(t *testing.T) {
	a, err := Enumerate("../raft")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Enumerate("../raft")
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("%d and %d mutants", len(a), len(b))
	}
	for i := range a {
		if a[i].ID != b[i].ID {
			t.Fatalf("order differs at %d: %s vs %s", i, a[i].ID, b[i].ID)
		}
	}
}

// Every mutant applies to source that still parses, differs from the
// original, and leaves the file on disk untouched.
func TestApplyProducesParsableChangedSource(t *testing.T) {
	orig, _ := os.ReadFile(sample)
	ms, _ := EnumerateFile(sample)
	seen := map[string]bool{}
	for _, m := range ms {
		out, err := Apply(m)
		if err != nil {
			t.Fatalf("%s: %v", m.ID, err)
		}
		if _, err := parser.ParseFile(token.NewFileSet(), "x.go", out, 0); err != nil {
			t.Fatalf("%s: mutated source does not parse: %v", m.ID, err)
		}
		if seen[string(out)] {
			t.Fatalf("%s: same output as another mutant", m.ID)
		}
		seen[string(out)] = true
	}
	now, _ := os.ReadFile(sample)
	if string(now) != string(orig) {
		t.Fatal("Apply modified the file on disk")
	}
}

func TestApplyBoundaryInIsUpToDate(t *testing.T) {
	ms, err := EnumerateFile("../raft/log.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range ms {
		if m.Func == "raftLog.isUpToDate" && m.Op == OpBoundary && strings.Contains(m.Desc, "lastIndex >= l.lastIndex()") {
			out, err := Apply(m)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(out), "lastIndex > l.lastIndex()") {
				t.Fatalf("mutant %s did not flip >= to >", m.ID)
			}
			return
		}
	}
	t.Fatal("isUpToDate boundary mutant not found")
}
