package sim

import (
	"fmt"
	"os"
	"slices"
	"testing"
	"time"
)

// pingers builds the dummy workload used by the determinism test: every node
// fires randomised timers and sends a message to a random peer each time.
func pingers(seed uint64, d time.Duration) (*World, *Network[int]) {
	w := New(seed)
	net := NewNetwork(w, NetConfig{
		MinDelay: time.Millisecond, MaxDelay: 50 * time.Millisecond,
		DropRate: 0.10, DupRate: 0.05,
	}, func(m int) string { return fmt.Sprint(m) })
	rng := w.Fork()
	msgID := 0
	for id := NodeID(1); id <= 3; id++ {
		net.Register(id, func(from NodeID, m int) {})
		var tick func()
		tick = func() {
			to := NodeID(rng.IntN(3) + 1)
			msgID++
			net.Send(id, to, msgID)
			w.After(time.Duration(rng.IntN(20)+1)*time.Millisecond, fmt.Sprintf("tick %d", id), tick)
		}
		w.After(time.Duration(rng.IntN(10))*time.Millisecond, fmt.Sprintf("tick %d", id), tick)
	}
	w.RunFor(d)
	return w, net
}

// Test 1: same seed and same binary give the same execution.
func TestSim_SameSeedSameTrace(t *testing.T) {
	a, netA := pingers(42, 10*time.Second)
	b, _ := pingers(42, 10*time.Second)
	c, _ := pingers(43, 10*time.Second)
	if a.TraceHash() != b.TraceHash() {
		t.Fatalf("seed 42 produced two different traces: %x vs %x", a.TraceHash(), b.TraceHash())
	}
	if a.TraceHash() == c.TraceHash() {
		t.Fatalf("seeds 42 and 43 produced the same trace; the seed is not controlling the run")
	}
	s := netA.Stats
	if s.Dropped == 0 || s.Duplicated == 0 || s.Delivered == 0 {
		t.Fatalf("fault model not exercised: %+v", s)
	}
}

// Test 2: timers fire in deadline order, ties in scheduling order, cancelled
// timers never fire, and virtual time costs no wall time.
func TestClock_TimersFireInDeadlineOrder(t *testing.T) {
	w := New(1)
	var got []string
	at := func(d time.Duration, name string) *Timer {
		return w.After(d, name, func() {
			if w.Now() != d {
				t.Errorf("%s ran at %v, want %v", name, w.Now(), d)
			}
			got = append(got, name)
		})
	}
	at(30*time.Millisecond, "30")
	at(10*time.Millisecond, "10a")
	at(20*time.Millisecond, "20")
	at(10*time.Millisecond, "10b")
	cancelled := at(15*time.Millisecond, "15")
	if !cancelled.Stop() {
		t.Fatal("Stop on a pending timer returned false")
	}
	w.RunFor(time.Second)
	want := []string{"10a", "10b", "20", "30"}
	if !slices.Equal(got, want) {
		t.Fatalf("fire order = %v, want %v", got, want)
	}
	if cancelled.Stop() {
		t.Fatal("Stop on a cancelled timer returned true")
	}

	start := time.Now()
	var tick func()
	n := 0
	tick = func() { n++; w.After(time.Millisecond, "tick", tick) }
	w.After(0, "tick", tick)
	w.RunFor(time.Hour)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("one virtual hour took %v of wall time", elapsed)
	}
	if n < 3_600_000 {
		t.Fatalf("ticked %d times in a virtual hour, want 3.6M", n)
	}
}

// Test 3: partitions block delivery, are directional, and heal.
func TestNet_PartitionBlocksAndHeals(t *testing.T) {
	w := New(7)
	net := NewNetwork[string](w, NetConfig{MinDelay: time.Millisecond, MaxDelay: time.Millisecond}, nil)
	var got []string
	for id := NodeID(1); id <= 3; id++ {
		net.Register(id, func(from NodeID, m string) { got = append(got, m) })
	}
	sendAll := func() {
		got = nil
		net.Send(1, 2, "1->2")
		net.Send(2, 3, "2->3")
		net.Send(3, 1, "3->1")
		w.RunFor(10 * time.Millisecond)
		slices.Sort(got)
	}

	net.Partition([]NodeID{1}, []NodeID{2, 3})
	sendAll()
	if !slices.Equal(got, []string{"2->3"}) {
		t.Fatalf("during partition delivered %v, want only 2->3", got)
	}
	net.Heal()
	sendAll()
	if !slices.Equal(got, []string{"1->2", "2->3", "3->1"}) {
		t.Fatalf("after heal delivered %v", got)
	}

	net.Block(1, 2)
	got = nil
	net.Send(1, 2, "1->2")
	net.Send(2, 1, "2->1")
	w.RunFor(10 * time.Millisecond)
	if !slices.Equal(got, []string{"2->1"}) {
		t.Fatalf("asymmetric block delivered %v, want only 2->1", got)
	}
}

func TestNet_MessageToCrashedNodeIsDropped(t *testing.T) {
	w := New(3)
	net := NewNetwork[string](w, NetConfig{MinDelay: 5 * time.Millisecond, MaxDelay: 5 * time.Millisecond}, nil)
	delivered := 0
	net.Register(2, func(NodeID, string) { delivered++ })
	net.Send(1, 2, "in flight")
	net.SetDown(2, true) // crashes while the message is in flight
	w.RunFor(10 * time.Millisecond)
	if delivered != 0 {
		t.Fatal("message delivered to a crashed node")
	}
}

func TestDisk_CrashKeepsSyncedData(t *testing.T) {
	sawLoss, sawTear := false, false
	for seed := uint64(0); seed < 50; seed++ {
		d := NewDisk(NewRand(seed))
		f, _ := d.OpenAppend("wal")
		f.Write([]byte("synced|"))
		f.Sync()
		f.Write([]byte("unsynced"))
		d.Crash()
		got, _ := d.ReadFile("wal")
		if string(got[:7]) != "synced|" {
			t.Fatalf("seed %d: synced data lost: %q", seed, got)
		}
		switch tail := len(got) - 7; {
		case tail == 0:
			sawLoss = true
		case tail < len("unsynced"):
			sawTear = true
		}
		if _, err := f.Write([]byte("x")); err != ErrStaleHandle {
			t.Fatalf("write through a pre-crash handle: err = %v", err)
		}
	}
	if !sawLoss || !sawTear {
		t.Fatalf("crash model never lost (%v) or tore (%v) a write in 50 seeds", sawLoss, sawTear)
	}
}

func TestDisk_AtomicWriteAndTruncateAreDurable(t *testing.T) {
	d := NewDisk(NewRand(1))
	d.WriteFileAtomic("snap", []byte("abc"))
	f, _ := d.OpenAppend("wal")
	f.Write([]byte("0123456789"))
	if err := d.Truncate("wal", 4); err != nil {
		t.Fatal(err)
	}
	d.Crash()
	if got, _ := d.ReadFile("snap"); string(got) != "abc" {
		t.Fatalf("snap = %q", got)
	}
	if got, _ := d.ReadFile("wal"); string(got) != "0123" {
		t.Fatalf("wal = %q", got)
	}
	if _, err := d.ReadFile("missing"); err == nil {
		t.Fatal("reading a missing file succeeded")
	}
}

func TestOSFS_RoundTrip(t *testing.T) {
	fs, err := osfs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	f, _ := fs.OpenAppend("wal")
	f.Write([]byte("hello world"))
	f.Sync()
	f.Close()
	if err := fs.Truncate("wal", 5); err != nil {
		t.Fatal(err)
	}
	if got, _ := fs.ReadFile("wal"); string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
	if err := fs.WriteFileAtomic("snap", []byte("s")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fs.Dir + "/snap.tmp"); !os.IsNotExist(err) {
		t.Fatal("temporary file left behind")
	}
}
