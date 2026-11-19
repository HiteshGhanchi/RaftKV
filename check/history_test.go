package check

import (
	"testing"
	"time"
)

const ms = time.Millisecond

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	var h History
	h.Add(Op{Client: 0, Input: Input{Put, "x", "1"}, Call: 0, Return: 1 * ms})
	h.Add(Op{Client: 1, Input: Input{Append, "x", "2"}, Call: 2 * ms, Return: 3 * ms})
	h.Add(Op{Client: 0, Input: Input{Get, "x", ""}, Output: Output{Value: "12"}, Call: 4 * ms, Return: 5 * ms})
	if v := h.Linearizable(time.Second); v != Linearizable {
		t.Fatal(v)
	}
}

// A read that starts after a write finished must see it.
func TestStaleReadIsNotLinearizable(t *testing.T) {
	var h History
	h.Add(Op{Client: 0, Input: Input{Put, "x", "1"}, Call: 0, Return: 1 * ms})
	h.Add(Op{Client: 1, Input: Input{Get, "x", ""}, Output: Output{Value: ""}, Call: 2 * ms, Return: 3 * ms})
	if v := h.Linearizable(time.Second); v != NotLinearizable {
		t.Fatal(v)
	}
}

// Overlapping operations may be ordered either way.
func TestConcurrentOpsMayReorder(t *testing.T) {
	var h History
	h.Add(Op{Client: 0, Input: Input{Put, "x", "1"}, Call: 0, Return: 10 * ms})
	h.Add(Op{Client: 1, Input: Input{Get, "x", ""}, Output: Output{Value: ""}, Call: 1 * ms, Return: 2 * ms})
	if v := h.Linearizable(time.Second); v != Linearizable {
		t.Fatal(v)
	}
}

// A write whose answer was lost may take effect at any later time, or never.
func TestUnknownWriteMayOrMayNotHappen(t *testing.T) {
	for _, seen := range []string{"", "1"} {
		var h History
		h.Add(Op{Client: 0, Input: Input{Put, "x", "1"}, Output: Output{Unknown: true}, Call: 0, Return: 1 * ms})
		h.Add(Op{Client: 1, Input: Input{Get, "x", ""}, Output: Output{Value: seen}, Call: 5 * ms, Return: 6 * ms})
		if v := h.Linearizable(time.Second); v != Linearizable {
			t.Fatalf("read %q after unknown write: %v", seen, v)
		}
	}
}

// An append applied twice (a retry without dedup) is caught.
func TestDuplicatedAppendIsNotLinearizable(t *testing.T) {
	var h History
	h.Add(Op{Client: 0, Input: Input{Append, "x", "a"}, Call: 0, Return: 10 * ms})
	h.Add(Op{Client: 1, Input: Input{Get, "x", ""}, Output: Output{Value: "aa"}, Call: 11 * ms, Return: 12 * ms})
	if v := h.Linearizable(time.Second); v != NotLinearizable {
		t.Fatal(v)
	}
}
