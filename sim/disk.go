package sim

import (
	"errors"
	"fmt"
	"math/rand/v2"

	"raftlab/vfs"
)

// Disk is an in-memory vfs.FS that models what a real disk does in a crash:
// bytes that were written but not fsynced may be lost, and the last unsynced
// write may be torn (only a prefix survives).
type Disk struct {
	rng   *rand.Rand
	files map[string]*memFile
	gen   uint64 // bumped on every crash; old handles stop working
	Syncs uint64
}

type memFile struct {
	durable []byte // survived an fsync
	pending []byte // written, still only in the "page cache"
}

// NewDisk creates an empty disk. rng decides how much unsynced data a crash keeps.
func NewDisk(rng *rand.Rand) *Disk {
	return &Disk{rng: rng, files: map[string]*memFile{}}
}

// ErrStaleHandle is returned by a file handle opened before the last crash.
var ErrStaleHandle = errors.New("sim: file handle from before a crash")

// Crash throws away unsynced data. For every file a random prefix of its
// unsynced bytes survives (possibly none, possibly all), which models lost
// and torn writes. Handles opened before the crash become unusable.
func (d *Disk) Crash() []CrashLoss {
	d.gen++
	var lost []CrashLoss
	for _, name := range sortedKeys(d.files) {
		f := d.files[name]
		keep := 0
		if len(f.pending) > 0 {
			keep = d.rng.IntN(len(f.pending) + 1)
			lost = append(lost, CrashLoss{File: name, Unsynced: len(f.pending), Kept: keep})
		}
		f.durable = append(f.durable, f.pending[:keep]...)
		f.pending = nil
	}
	return lost
}

// CrashLoss reports what a crash did to one file's unsynced bytes: Kept of
// Unsynced survived. 0 < Kept < Unsynced is a torn write.
type CrashLoss struct {
	File     string
	Unsynced int
	Kept     int
}

// OpenAppend implements vfs.FS.
func (d *Disk) OpenAppend(name string) (vfs.File, error) {
	if _, ok := d.files[name]; !ok {
		d.files[name] = &memFile{}
	}
	return &diskFile{d: d, name: name, gen: d.gen}, nil
}

// ReadFile returns durable plus unsynced bytes, like reading through the OS
// page cache on a running machine.
func (d *Disk) ReadFile(name string) ([]byte, error) {
	f, ok := d.files[name]
	if !ok {
		return nil, fmt.Errorf("sim: %s: %w", name, vfs.ErrNotExist)
	}
	out := make([]byte, 0, len(f.durable)+len(f.pending))
	out = append(out, f.durable...)
	return append(out, f.pending...), nil
}

// WriteFileAtomic implements vfs.FS: the new contents are durable at once.
func (d *Disk) WriteFileAtomic(name string, data []byte) error {
	d.files[name] = &memFile{durable: append([]byte(nil), data...)}
	d.Syncs++
	return nil
}

// Truncate implements vfs.FS.
func (d *Disk) Truncate(name string, size int64) error {
	f, ok := d.files[name]
	if !ok {
		return fmt.Errorf("sim: %s: %w", name, vfs.ErrNotExist)
	}
	all := append(f.durable, f.pending...)
	if int(size) > len(all) {
		return fmt.Errorf("sim: truncate %s to %d beyond size %d", name, size, len(all))
	}
	f.durable = append([]byte(nil), all[:size]...)
	f.pending = nil
	d.Syncs++
	return nil
}

type diskFile struct {
	d    *Disk
	name string
	gen  uint64
}

func (f *diskFile) Write(p []byte) (int, error) {
	if f.gen != f.d.gen {
		return 0, ErrStaleHandle
	}
	mf := f.d.files[f.name]
	mf.pending = append(mf.pending, p...)
	return len(p), nil
}

func (f *diskFile) Sync() error {
	if f.gen != f.d.gen {
		return ErrStaleHandle
	}
	mf := f.d.files[f.name]
	mf.durable = append(mf.durable, mf.pending...)
	mf.pending = nil
	f.d.Syncs++
	return nil
}

func (f *diskFile) Close() error { return nil }
