package peer

import (
	"errors"
	"testing"

	"raftlab/kv"
	"raftlab/raft"
	"raftlab/sim"
)

func single(t *testing.T, disk *sim.Disk, snapEvery uint64) *Peer {
	t.Helper()
	p, err := New(Config{
		ID: 1, Peers: []raft.NodeID{1}, ElectionTick: 10, HeartbeatTick: 1,
		FS: disk, Rand: sim.NewRand(1), SnapshotEvery: snapEvery,
		Send: func([]raft.Message) { t.Fatal("single node sent a message") },
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 30 {
		p.Tick()
	}
	if p.Status().State != raft.Leader {
		t.Fatal("single node did not elect itself")
	}
	return p
}

func put(t *testing.T, p *Peer, seq uint64, op kv.Op, v string) {
	t.Helper()
	var got *Result
	p.Submit(kv.Command{ClientID: 1, Seq: seq, Op: op, Key: "k", Value: v}, func(r Result) { got = &r })
	if got == nil || got.Err != nil {
		t.Fatalf("submit seq %d: %+v", seq, got)
	}
}

func read(t *testing.T, p *Peer) string {
	t.Helper()
	var got *Result
	p.Read("k", func(r Result) { got = &r })
	if got == nil || got.Err != nil {
		t.Fatalf("read: %+v", got)
	}
	return got.Value
}

func TestWriteReadRestart(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	p := single(t, disk, 0)
	put(t, p, 1, kv.OpPut, "a")
	put(t, p, 2, kv.OpAppend, "b")
	if v := read(t, p); v != "ab" {
		t.Fatalf("read %q", v)
	}
	disk.Crash() // everything acknowledged was fsynced
	p2 := single(t, disk, 0)
	if v := read(t, p2); v != "ab" {
		t.Fatalf("after restart read %q", v)
	}
	put(t, p2, 2, kv.OpAppend, "b") // a retry of seq 2: deduplicated
	if v := read(t, p2); v != "ab" {
		t.Fatalf("retry applied twice: %q", v)
	}
}

func TestSnapshotAndRestartFromSnapshot(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	p := single(t, disk, 5)
	for i := uint64(1); i <= 12; i++ {
		put(t, p, i, kv.OpAppend, "x")
	}
	if p.Stats().Snapshots == 0 || p.Status().SnapIndex == 0 {
		t.Fatalf("no snapshot taken: %+v", p.Stats())
	}
	disk.Crash()
	p2 := single(t, disk, 5)
	if v := read(t, p2); v != "xxxxxxxxxxxx" {
		t.Fatalf("after restart from snapshot read %q", v)
	}
}

func TestFollowerRejectsClientRequests(t *testing.T) {
	p, err := New(Config{ID: 1, Peers: []raft.NodeID{1, 2, 3}, FS: sim.NewDisk(sim.NewRand(1)),
		Rand: sim.NewRand(1), Send: func([]raft.Message) {}})
	if err != nil {
		t.Fatal(err)
	}
	var nl *NotLeaderError
	p.Submit(kv.Command{ClientID: 1, Seq: 1, Op: kv.OpPut, Key: "k"}, func(r Result) {
		if !errors.As(r.Err, &nl) {
			t.Fatalf("submit on follower: %v", r.Err)
		}
	})
	p.Read("k", func(r Result) {
		if !errors.As(r.Err, &nl) {
			t.Fatalf("read on follower: %v", r.Err)
		}
	})
}
