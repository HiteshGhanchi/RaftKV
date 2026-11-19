// Package kv is the replicated state machine: a string map with client
// sessions so that a retried command takes effect exactly once.
package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

// Op is a command type.
type Op uint8

const (
	OpGet Op = iota
	OpPut
	OpAppend
)

func (o Op) String() string { return [...]string{"get", "put", "append"}[o] }

// Command is what clients submit. (ClientID, Seq) identifies it: a client
// numbers its commands 1, 2, 3, ... and only has one outstanding at a time,
// retrying the same Seq until it gets an answer (thesis §6.3).
type Command struct {
	ClientID uint64
	Seq      uint64
	Op       Op
	Key      string
	Value    string
}

// Encode serialises a command for the Raft log.
func (c Command) Encode() []byte {
	b := binary.AppendUvarint(nil, c.ClientID)
	b = binary.AppendUvarint(b, c.Seq)
	b = append(b, byte(c.Op))
	b = binary.AppendUvarint(b, uint64(len(c.Key)))
	b = append(b, c.Key...)
	b = binary.AppendUvarint(b, uint64(len(c.Value)))
	return append(b, c.Value...)
}

var errCorrupt = errors.New("kv: corrupt encoding")

type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errCorrupt
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if r.err != nil || len(r.b) == 0 {
		r.err = errCorrupt
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) str() string {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = errCorrupt
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

// DecodeCommand is the inverse of Encode.
func DecodeCommand(p []byte) (Command, error) {
	r := &reader{b: p}
	c := Command{ClientID: r.uvarint(), Seq: r.uvarint(), Op: Op(r.byte()), Key: r.str(), Value: r.str()}
	return c, r.err
}

type session struct {
	seq    uint64
	result string
}

// Store is the state machine. It is deterministic: every replica that applies
// the same commands in the same order ends in the same state.
type Store struct {
	data     map[string]string
	sessions map[uint64]session
	dedup    bool
}

// NewStore creates an empty store. dedup=false is the no-dedup bug zoo entry.
func NewStore(dedup bool) *Store {
	return &Store{data: map[string]string{}, sessions: map[uint64]session{}, dedup: dedup}
}

// Apply executes a committed command and returns its result. A command whose
// Seq the client's session has already seen returns the cached result
// without running again.
func (s *Store) Apply(c Command) string {
	if s.dedup {
		if sess, ok := s.sessions[c.ClientID]; ok && c.Seq <= sess.seq {
			if c.Seq == sess.seq {
				return sess.result
			}
			return "" // older than the last answer: the client stopped waiting for it
		}
	}
	var result string
	switch c.Op {
	case OpGet:
		result = s.data[c.Key]
	case OpPut:
		s.data[c.Key] = c.Value
	case OpAppend:
		s.data[c.Key] += c.Value
	}
	s.sessions[c.ClientID] = session{seq: c.Seq, result: result}
	return result
}

// Get reads a key directly (used by ReadIndex reads after the read barrier).
func (s *Store) Get(key string) string { return s.data[key] }

// Len is the number of keys.
func (s *Store) Len() int { return len(s.data) }

// Snapshot serialises the whole state, including sessions, in sorted order so
// identical states give identical bytes.
func (s *Store) Snapshot() []byte {
	var b []byte
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = appendStr(b, k)
		b = appendStr(b, s.data[k])
	}
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, s.sessions[id].seq)
		b = appendStr(b, s.sessions[id].result)
	}
	return b
}

// Restore replaces the state with a snapshot.
func (s *Store) Restore(p []byte) error {
	r := &reader{b: p}
	data := map[string]string{}
	for n := r.uvarint(); n > 0 && r.err == nil; n-- {
		k := r.str()
		data[k] = r.str()
	}
	sessions := map[uint64]session{}
	for n := r.uvarint(); n > 0 && r.err == nil; n-- {
		id := r.uvarint()
		sessions[id] = session{seq: r.uvarint(), result: r.str()}
	}
	if r.err != nil {
		return fmt.Errorf("kv: restore: %w", r.err)
	}
	s.data, s.sessions = data, sessions
	return nil
}

func appendStr(b []byte, v string) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}
