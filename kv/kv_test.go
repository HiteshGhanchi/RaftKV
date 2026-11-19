package kv

import (
	"bytes"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	c := Command{ClientID: 7, Seq: 42, Op: OpAppend, Key: "k", Value: "v\x00w"}
	got, err := DecodeCommand(c.Encode())
	if err != nil || got != c {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	if _, err := DecodeCommand([]byte{1}); err == nil {
		t.Fatal("decoded a truncated command")
	}
}

func TestPutAppendGet(t *testing.T) {
	s := NewStore(true)
	s.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "a", Value: "x"})
	s.Apply(Command{ClientID: 1, Seq: 2, Op: OpAppend, Key: "a", Value: "y"})
	if got := s.Apply(Command{ClientID: 1, Seq: 3, Op: OpGet, Key: "a"}); got != "xy" {
		t.Fatalf("get = %q", got)
	}
}

// Thesis §6.3: a retried command (same client, same seq) takes effect once.
func TestRetriedCommandAppliesOnce(t *testing.T) {
	s := NewStore(true)
	c := Command{ClientID: 1, Seq: 1, Op: OpAppend, Key: "a", Value: "x"}
	s.Apply(c)
	s.Apply(c) // the retry was also committed
	if got := s.Get("a"); got != "x" {
		t.Fatalf("after retry a = %q, want x", got)
	}
	bug := NewStore(false)
	bug.Apply(c)
	bug.Apply(c)
	if got := bug.Get("a"); got != "xx" {
		t.Fatalf("no-dedup mutation: a = %q, want xx", got)
	}
}

func TestDuplicateReturnsCachedResult(t *testing.T) {
	s := NewStore(true)
	s.Apply(Command{ClientID: 1, Seq: 1, Op: OpPut, Key: "a", Value: "1"})
	get := Command{ClientID: 2, Seq: 1, Op: OpGet, Key: "a"}
	first := s.Apply(get)
	s.Apply(Command{ClientID: 1, Seq: 2, Op: OpPut, Key: "a", Value: "2"})
	if again := s.Apply(get); again != first {
		t.Fatalf("duplicate get returned %q, first answer was %q", again, first)
	}
}

func TestSnapshotRestore(t *testing.T) {
	s := NewStore(true)
	for i, k := range []string{"b", "a", "c"} {
		s.Apply(Command{ClientID: uint64(i + 1), Seq: 1, Op: OpPut, Key: k, Value: k + k})
	}
	snap := s.Snapshot()
	r := NewStore(true)
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(r.Snapshot(), snap) || r.Get("a") != "aa" || r.Len() != 3 {
		t.Fatal("restored store differs")
	}
	// Sessions survive the snapshot: the retry is still deduplicated.
	r.Apply(Command{ClientID: 1, Seq: 1, Op: OpAppend, Key: "b", Value: "!"})
	if r.Get("b") != "bb" {
		t.Fatalf("dedup lost across snapshot: b = %q", r.Get("b"))
	}
	if err := r.Restore([]byte{5}); err == nil {
		t.Fatal("restored a corrupt snapshot")
	}
}
