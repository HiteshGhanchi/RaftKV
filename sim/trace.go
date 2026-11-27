package sim

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// TraceVersion is the version of the JSON Lines trace format written by
// JSONL. Bump it when the meaning of a field changes.
const TraceVersion = 1

// Event is one entry of the execution trace: either a scheduled event that
// the world executed (a timer, a message delivery) or an observation emitted
// while one ran (a crash, a role change, an applied entry). Events emitted
// while a scheduled event runs share its Step number.
type Event struct {
	Step   uint64        // number of the scheduled event this belongs to (1-based)
	At     time.Duration // virtual time
	Kind   string        // short machine-readable kind, e.g. "deliver", "crash"
	Node   uint64        // the node the event happens on, 0 if none
	Peer   uint64        // the other node involved (message sender, blocked peer), 0 if none
	Detail string        // free text, e.g. the message description
	Attrs  []Attr        // numeric fields, in a fixed order chosen by the emitter
}

// Attr is a named integer field of an event.
type Attr struct {
	K string
	V int64
}

// A returns an attribute. It keeps emitters short: sim.A("term", int64(t)).
func A(k string, v int64) Attr { return Attr{k, v} }

// Get returns the value of attribute k and whether it is present.
func (e Event) Get(k string) (int64, bool) {
	for _, a := range e.Attrs {
		if a.K == k {
			return a.V, true
		}
	}
	return 0, false
}

// String renders the event as one human-readable line.
func (e Event) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %s", e.At, e.Kind)
	if e.Node != 0 {
		fmt.Fprintf(&b, " n%d", e.Node)
	}
	if e.Peer != 0 {
		fmt.Fprintf(&b, " peer=n%d", e.Peer)
	}
	for _, a := range e.Attrs {
		fmt.Fprintf(&b, " %s=%d", a.K, a.V)
	}
	if e.Detail != "" {
		b.WriteString(" ")
		b.WriteString(e.Detail)
	}
	return b.String()
}

// appendCanonical appends the canonical binary form of e, which is what the
// trace hash is computed from. Step is left out because it is implied by the
// order of events.
func (e Event) appendCanonical(buf []byte) []byte {
	buf = binary.AppendVarint(buf, int64(e.At))
	buf = appendString(buf, e.Kind)
	buf = binary.AppendUvarint(buf, e.Node)
	buf = binary.AppendUvarint(buf, e.Peer)
	buf = appendString(buf, e.Detail)
	buf = binary.AppendUvarint(buf, uint64(len(e.Attrs)))
	for _, a := range e.Attrs {
		buf = appendString(buf, a.K)
		buf = binary.AppendVarint(buf, a.V)
	}
	return buf
}

// Canonical returns the canonical binary form of e (used to compare traces).
func (e Event) Canonical() []byte { return e.appendCanonical(nil) }

func appendString(buf []byte, s string) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// MarshalJSON writes the event as a flat object: the fixed fields, then one
// key per attribute.
func (e Event) MarshalJSON() ([]byte, error) {
	b := []byte(`{"step":`)
	b = strconv.AppendUint(b, e.Step, 10)
	b = append(b, `,"t":`...)
	b = strconv.AppendInt(b, int64(e.At), 10)
	b = append(b, `,"kind":`...)
	b = strconv.AppendQuote(b, e.Kind)
	if e.Node != 0 {
		b = append(b, `,"node":`...)
		b = strconv.AppendUint(b, e.Node, 10)
	}
	if e.Peer != 0 {
		b = append(b, `,"peer":`...)
		b = strconv.AppendUint(b, e.Peer, 10)
	}
	for _, a := range e.Attrs {
		b = append(b, ',')
		b = strconv.AppendQuote(b, a.K)
		b = append(b, ':')
		b = strconv.AppendInt(b, a.V, 10)
	}
	if e.Detail != "" {
		b = append(b, `,"detail":`...)
		b = strconv.AppendQuote(b, e.Detail)
	}
	return append(b, '}'), nil
}

// JSONL writes a trace as JSON Lines: a header object, then one event per
// line. Call Close to flush.
type JSONL struct {
	w   *bufio.Writer
	err error
}

// NewJSONL starts a trace on w. The header gets "trace_version" added.
func NewJSONL(w io.Writer, header map[string]any) *JSONL {
	j := &JSONL{w: bufio.NewWriterSize(w, 1<<16)}
	h := map[string]any{"trace_version": TraceVersion}
	for k, v := range header {
		h[k] = v
	}
	line, err := json.Marshal(h) // map keys are sorted, so the header is deterministic
	if err != nil {
		j.err = err
		return j
	}
	j.write(line)
	return j
}

// Sink returns a function suitable for World.SetSink.
func (j *JSONL) Sink() func(Event) {
	return func(e Event) {
		line, _ := e.MarshalJSON()
		j.write(line)
	}
}

func (j *JSONL) write(line []byte) {
	if j.err != nil {
		return
	}
	if _, err := j.w.Write(line); err != nil {
		j.err = err
		return
	}
	j.err = j.w.WriteByte('\n')
}

// Close flushes buffered lines and reports the first write error.
func (j *JSONL) Close() error {
	if j.err != nil {
		return j.err
	}
	return j.w.Flush()
}
