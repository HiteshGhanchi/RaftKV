// Package storage keeps a Raft node's durable state: a write-ahead log of
// entries and hard state, plus the latest snapshot.
//
// Files (all inside one vfs.FS):
//
//	wal   append-only records: [len u32][crc32c u32][type u8][payload]
//	snap  the latest snapshot, replaced atomically, with its own checksum
//
// Recovery reads records until the first one that is short or fails its
// checksum, and truncates the file there. That tail can only be a write that
// was in progress during a crash and was never fsynced, so it was never
// acknowledged to anyone.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"raftlab/raft"
	"raftlab/vfs"
)

const (
	walFile  = "wal"
	snapFile = "snap"

	recEntry     = 1
	recHardState = 2

	headerSize = 9
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// State is everything a node needs to restart.
type State struct {
	HardState raft.HardState
	Entries   []raft.Entry
	Snapshot  *raft.Snapshot
	// TornBytes is how many bytes of a damaged tail recovery cut off.
	TornBytes int
}

// Options configure a Storage.
type Options struct {
	// NoSync skips fsync. Only for the skip-fsync bug zoo entry and for
	// benchmarks that measure what fsync costs.
	NoSync bool
}

// Storage is a node's durable state.
type Storage struct {
	fs   vfs.FS
	f    vfs.File
	opts Options
	buf  []byte

	Syncs uint64
	Bytes uint64
}

// Open loads the stored state (repairing a torn tail) and opens the log for appending.
func Open(fs vfs.FS, opts Options) (*Storage, State, error) {
	var st State
	if p, err := fs.ReadFile(snapFile); err == nil {
		s, err := readSnapshotFile(p)
		if err != nil {
			return nil, st, fmt.Errorf("storage: snapshot file: %w", err)
		}
		st.Snapshot = s
	} else if !errors.Is(err, vfs.ErrNotExist) {
		return nil, st, err
	}

	data, err := fs.ReadFile(walFile)
	if err != nil && !errors.Is(err, vfs.ErrNotExist) {
		return nil, st, err
	}
	good := replay(data, &st)
	if good < len(data) {
		st.TornBytes = len(data) - good
		if err := fs.Truncate(walFile, int64(good)); err != nil {
			return nil, st, err
		}
	}
	reconcileWithSnapshot(&st)

	f, err := fs.OpenAppend(walFile)
	if err != nil {
		return nil, st, err
	}
	return &Storage{fs: fs, f: f, opts: opts}, st, nil
}

// replay applies records to st and returns the length of the valid prefix.
func replay(data []byte, st *State) int {
	off := 0
	for len(data)-off >= headerSize {
		n := int(binary.LittleEndian.Uint32(data[off:]))
		sum := binary.LittleEndian.Uint32(data[off+4:])
		end := off + headerSize + n
		if n > len(data) || end > len(data) || crc32.Checksum(data[off+8:end], crcTable) != sum {
			break
		}
		typ, body := data[off+8], data[off+headerSize:end]
		d := &decoder{b: body}
		switch typ {
		case recEntry:
			e := decodeEntry(d)
			if d.err != nil {
				return off
			}
			// A rewritten index means the leader overwrote a conflicting
			// suffix (Figure 2, AppendEntries rule 3): drop it here too.
			for len(st.Entries) > 0 && st.Entries[len(st.Entries)-1].Index >= e.Index {
				st.Entries = st.Entries[:len(st.Entries)-1]
			}
			st.Entries = append(st.Entries, e)
		case recHardState:
			st.HardState = decodeHardState(d)
			if d.err != nil {
				return off
			}
		default:
			return off
		}
		off = end
	}
	return off
}

// reconcileWithSnapshot drops log entries the snapshot covers. If the log
// does not contain the snapshot's last entry with the same term, everything
// after it is from a different history and is discarded too (Figure 13,
// receiver rules 6-7). This matters when a crash lands between writing a
// received snapshot and rewriting the log.
func reconcileWithSnapshot(st *State) {
	s := st.Snapshot
	if s == nil {
		return
	}
	keepAfter := true
	for _, e := range st.Entries {
		if e.Index == s.Index {
			keepAfter = e.Term == s.Term
		}
	}
	var keep []raft.Entry
	if keepAfter {
		for _, e := range st.Entries {
			if e.Index > s.Index {
				keep = append(keep, e)
			}
		}
	}
	st.Entries = keep
	st.HardState.Commit = max(st.HardState.Commit, s.Index)
}

func (s *Storage) appendRecord(typ byte, body []byte) {
	start := len(s.buf)
	s.buf = append(s.buf, make([]byte, headerSize)...)
	s.buf[start+8] = typ
	s.buf = append(s.buf, body...)
	binary.LittleEndian.PutUint32(s.buf[start:], uint32(len(body)))
	binary.LittleEndian.PutUint32(s.buf[start+4:], crc32.Checksum(s.buf[start+8:], crcTable))
}

// Save durably appends entries and the hard state. It must return before
// any message from the same Ready is sent.
func (s *Storage) Save(hs raft.HardState, ents []raft.Entry, mustSync bool) error {
	s.buf = s.buf[:0]
	var tmp []byte
	for _, e := range ents {
		tmp = EncodeEntry(tmp[:0], e)
		s.appendRecord(recEntry, tmp)
	}
	s.appendRecord(recHardState, encodeHardState(tmp[:0], hs))
	if _, err := s.f.Write(s.buf); err != nil {
		return err
	}
	s.Bytes += uint64(len(s.buf))
	if mustSync && !s.opts.NoSync {
		s.Syncs++
		return s.f.Sync()
	}
	return nil
}

// SaveSnapshot stores a snapshot and rewrites the log to hold only the hard
// state and the entries after it. The snapshot is written first, so a crash
// in between leaves an old log that recovery reconciles.
func (s *Storage) SaveSnapshot(snap *raft.Snapshot, hs raft.HardState, rest []raft.Entry) error {
	if err := s.fs.WriteFileAtomic(snapFile, writeSnapshotFile(snap)); err != nil {
		return err
	}
	s.buf = s.buf[:0]
	var tmp []byte
	for _, e := range rest {
		tmp = EncodeEntry(tmp[:0], e)
		s.appendRecord(recEntry, tmp)
	}
	s.appendRecord(recHardState, encodeHardState(tmp[:0], hs))
	if err := s.fs.WriteFileAtomic(walFile, s.buf); err != nil {
		return err
	}
	s.f.Close()
	f, err := s.fs.OpenAppend(walFile)
	if err != nil {
		return err
	}
	s.f = f
	s.Syncs += 2
	return nil
}

// Close releases the log file.
func (s *Storage) Close() error { return s.f.Close() }

func writeSnapshotFile(snap *raft.Snapshot) []byte {
	body := encodeSnapshot(snap)
	out := binary.LittleEndian.AppendUint32(nil, crc32.Checksum(body, crcTable))
	return append(out, body...)
}

func readSnapshotFile(p []byte) (*raft.Snapshot, error) {
	if len(p) < 4 || crc32.Checksum(p[4:], crcTable) != binary.LittleEndian.Uint32(p) {
		return nil, errors.New("checksum mismatch")
	}
	return decodeSnapshot(p[4:])
}
