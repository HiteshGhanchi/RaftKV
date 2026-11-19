package storage

import (
	"encoding/binary"
	"errors"

	"raftlab/raft"
)

var errShort = errors.New("storage: truncated record")

// decoder reads uvarints and length-prefixed byte strings from a buffer.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = errShort
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *decoder) bytes() []byte {
	n := d.uvarint()
	if d.err != nil {
		return nil
	}
	if uint64(len(d.b)) < n {
		d.err = errShort
		return nil
	}
	out := append([]byte(nil), d.b[:n]...)
	d.b = d.b[n:]
	return out
}

func appendBytes(b, p []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

// EncodeEntry appends the binary form of e to b.
func EncodeEntry(b []byte, e raft.Entry) []byte {
	b = binary.AppendUvarint(b, e.Term)
	b = binary.AppendUvarint(b, e.Index)
	b = binary.AppendUvarint(b, uint64(e.Type))
	return appendBytes(b, e.Data)
}

func decodeEntry(d *decoder) raft.Entry {
	return raft.Entry{Term: d.uvarint(), Index: d.uvarint(), Type: raft.EntryType(d.uvarint()), Data: d.bytes()}
}

func encodeHardState(b []byte, hs raft.HardState) []byte {
	b = binary.AppendUvarint(b, hs.Term)
	b = binary.AppendUvarint(b, hs.Vote)
	return binary.AppendUvarint(b, hs.Commit)
}

func decodeHardState(d *decoder) raft.HardState {
	return raft.HardState{Term: d.uvarint(), Vote: d.uvarint(), Commit: d.uvarint()}
}

func encodeSnapshot(s *raft.Snapshot) []byte {
	b := binary.AppendUvarint(nil, s.Index)
	b = binary.AppendUvarint(b, s.Term)
	return appendBytes(b, s.Data)
}

func decodeSnapshot(p []byte) (*raft.Snapshot, error) {
	d := &decoder{b: p}
	s := &raft.Snapshot{Index: d.uvarint(), Term: d.uvarint(), Data: d.bytes()}
	return s, d.err
}
