package storage

import (
	"fmt"
	"slices"
	"testing"

	"raftlab/raft"
	"raftlab/sim"
	"raftlab/vfs"
)

func ents(from uint64, ts ...uint64) []raft.Entry {
	out := make([]raft.Entry, len(ts))
	for i, t := range ts {
		idx := from + uint64(i)
		out[i] = raft.Entry{Term: t, Index: idx, Data: []byte(fmt.Sprintf("e%d", idx))}
	}
	return out
}

func indexTerms(es []raft.Entry) []string {
	var out []string
	for _, e := range es {
		out = append(out, fmt.Sprintf("%d/%d", e.Index, e.Term))
	}
	return out
}

func mustOpen(t *testing.T, fs vfs.FS, opts Options) (*Storage, State) {
	t.Helper()
	s, st, err := Open(fs, opts)
	if err != nil {
		t.Fatal(err)
	}
	return s, st
}

func TestSaveAndReload(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	s, st := mustOpen(t, disk, Options{})
	if len(st.Entries) != 0 || st.Snapshot != nil {
		t.Fatalf("fresh disk state %+v", st)
	}
	s.Save(raft.HardState{Term: 1, Vote: 2}, ents(1, 1, 1), true)
	s.Save(raft.HardState{Term: 2, Vote: 3, Commit: 2}, ents(3, 2), true)
	disk.Crash()
	_, st = mustOpen(t, disk, Options{})
	if st.HardState != (raft.HardState{Term: 2, Vote: 3, Commit: 2}) {
		t.Fatalf("hard state %+v", st.HardState)
	}
	if got := indexTerms(st.Entries); !slices.Equal(got, []string{"1/1", "2/1", "3/2"}) {
		t.Fatalf("entries %v", got)
	}
}

// Figure 2, AppendEntries rule 3 must also hold on disk: a rewritten index
// drops the old suffix.
func TestOverwrittenSuffixIsDroppedOnReload(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	s, _ := mustOpen(t, disk, Options{})
	s.Save(raft.HardState{Term: 1}, ents(1, 1, 1, 1, 1), true)
	s.Save(raft.HardState{Term: 3}, ents(3, 3), true)
	_, st := mustOpen(t, disk, Options{})
	if got := indexTerms(st.Entries); !slices.Equal(got, []string{"1/1", "2/1", "3/3"}) {
		t.Fatalf("entries %v", got)
	}
}

// A crash during an unsynced write leaves a torn tail; recovery must cut it
// off and keep every record that was fsynced.
func TestTornTailIsTruncated(t *testing.T) {
	torn := 0
	for seed := uint64(0); seed < 100; seed++ {
		disk := sim.NewDisk(sim.NewRand(seed))
		s, _ := mustOpen(t, disk, Options{})
		s.Save(raft.HardState{Term: 1}, ents(1, 1, 1), true)
		s.Save(raft.HardState{Term: 1}, ents(3, 1, 1, 1), false) // not fsynced
		disk.Crash()
		s2, st := mustOpen(t, disk, Options{})
		if len(st.Entries) < 2 {
			t.Fatalf("seed %d: lost fsynced entries: %v", seed, indexTerms(st.Entries))
		}
		if st.TornBytes > 0 {
			torn++
		}
		// The log must be appendable after repair.
		next := uint64(len(st.Entries)) + 1
		s2.Save(raft.HardState{Term: 2}, ents(next, 2), true)
		_, st = mustOpen(t, disk, Options{})
		if last := st.Entries[len(st.Entries)-1]; last.Index != next || last.Term != 2 {
			t.Fatalf("seed %d: append after repair gave %v", seed, indexTerms(st.Entries))
		}
	}
	if torn == 0 {
		t.Fatal("no seed produced a torn tail; the test is not exercising recovery")
	}
}

func TestSnapshotRewritesLog(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	s, _ := mustOpen(t, disk, Options{})
	s.Save(raft.HardState{Term: 2, Commit: 4}, ents(1, 1, 1, 2, 2, 2), true)
	snap := &raft.Snapshot{Index: 4, Term: 2, Data: []byte("kv")}
	if err := s.SaveSnapshot(snap, raft.HardState{Term: 2, Commit: 4}, ents(5, 2)); err != nil {
		t.Fatal(err)
	}
	s.Save(raft.HardState{Term: 2, Commit: 5}, ents(6, 2), true)
	disk.Crash()
	_, st := mustOpen(t, disk, Options{})
	if st.Snapshot == nil || st.Snapshot.Index != 4 || string(st.Snapshot.Data) != "kv" {
		t.Fatalf("snapshot %+v", st.Snapshot)
	}
	if got := indexTerms(st.Entries); !slices.Equal(got, []string{"5/2", "6/2"}) {
		t.Fatalf("entries %v", got)
	}
}

// Crash after the snapshot file was replaced but before the log was
// rewritten: the old log disagrees with the snapshot and must be discarded.
func TestCrashBetweenSnapshotAndLogRewrite(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	s, _ := mustOpen(t, disk, Options{})
	s.Save(raft.HardState{Term: 3}, ents(1, 1, 1, 1, 1), true) // stale suffix, terms 1
	disk.WriteFileAtomic(snapFile, writeSnapshotFile(&raft.Snapshot{Index: 3, Term: 2}))
	disk.Crash()
	_, st := mustOpen(t, disk, Options{})
	if len(st.Entries) != 0 || st.HardState.Commit != 3 {
		t.Fatalf("entries %v commit %d; want none and commit 3", indexTerms(st.Entries), st.HardState.Commit)
	}
}

func TestCorruptSnapshotIsAnError(t *testing.T) {
	disk := sim.NewDisk(sim.NewRand(1))
	disk.WriteFileAtomic(snapFile, []byte("garbage!"))
	if _, _, err := Open(disk, Options{}); err == nil {
		t.Fatal("opened a corrupt snapshot")
	}
}

// NoSync is the skip-fsync bug: an acknowledged entry can vanish.
func TestNoSyncCanLoseAcknowledgedEntries(t *testing.T) {
	lost := false
	for seed := uint64(0); seed < 20 && !lost; seed++ {
		disk := sim.NewDisk(sim.NewRand(seed))
		s, _ := mustOpen(t, disk, Options{NoSync: true})
		s.Save(raft.HardState{Term: 1}, ents(1, 1, 1, 1), true)
		disk.Crash()
		_, st := mustOpen(t, disk, Options{})
		lost = len(st.Entries) < 3
	}
	if !lost {
		t.Fatal("NoSync never lost data in 20 crashes")
	}
}

func TestRealDiskRoundTrip(t *testing.T) {
	fs, err := vfs.NewOS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, _ := mustOpen(t, fs, Options{})
	s.Save(raft.HardState{Term: 1, Vote: 1, Commit: 1}, ents(1, 1), true)
	s.SaveSnapshot(&raft.Snapshot{Index: 1, Term: 1, Data: []byte("x")}, raft.HardState{Term: 1, Commit: 1}, nil)
	s.Save(raft.HardState{Term: 1, Commit: 2}, ents(2, 1), true)
	s.Close()
	_, st := mustOpen(t, fs, Options{})
	if st.Snapshot.Index != 1 || len(st.Entries) != 1 || st.HardState.Commit != 2 {
		t.Fatalf("state %+v", st)
	}
}
