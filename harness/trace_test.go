package harness

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"raftlab/mutation"
	"raftlab/sim"
)

// Observing a run must never change it: no sink, a counting sink and a JSONL
// sink give the same trace hash and step count, and two JSONL traces of the
// same seed are byte-identical.
func TestTrace_SinkDoesNotChangeExecution(t *testing.T) {
	for _, tc := range []struct {
		seed uint64
		m    mutation.Set
	}{{1, 0}, {2, 0}, {3, 0}, {262, mutation.CommitPrevTerm}} {
		o := DefaultOptions(tc.seed)
		o.Mutations = tc.m
		plain := Run(o)

		n := 0
		o.Sink = func(sim.Event) { n++ }
		counted := Run(o)

		jsonl := func() []byte {
			var buf bytes.Buffer
			j := sim.NewJSONL(&buf, map[string]any{"seed": tc.seed})
			o.Sink = j.Sink()
			r := Run(o)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if r.TraceHash != plain.TraceHash {
				t.Fatalf("seed %d: JSONL sink changed the trace hash", tc.seed)
			}
			return buf.Bytes()
		}
		a, b := jsonl(), jsonl()

		if counted.TraceHash != plain.TraceHash || counted.Steps != plain.Steps || counted.String() != plain.String() {
			t.Fatalf("seed %d: a sink changed the run:\n%s\n%s", tc.seed, plain, counted)
		}
		if !bytes.Equal(a, b) {
			t.Fatalf("seed %d: two JSONL traces of the same run differ", tc.seed)
		}
		lines := strings.Split(strings.TrimSpace(string(a)), "\n")
		if len(lines)-1 != n {
			t.Fatalf("seed %d: %d JSONL events, sink saw %d", tc.seed, len(lines)-1, n)
		}
		var head map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &head); err != nil || head["trace_version"] != float64(sim.TraceVersion) {
			t.Fatalf("bad header %q (%v)", lines[0], err)
		}
		var last map[string]any
		if err := json.Unmarshal([]byte(lines[len(lines)-1]), &last); err != nil {
			t.Fatalf("bad event line: %v", err)
		}
		if uint64(last["step"].(float64)) != plain.Steps {
			t.Fatalf("seed %d: last event step %v, run had %d steps", tc.seed, last["step"], plain.Steps)
		}
		if tc.m != 0 && !strings.Contains(lines[len(lines)-1], `"kind":"violation"`) {
			t.Fatalf("seed %d: failing run should end with a violation event, got %s", tc.seed, lines[len(lines)-1])
		}
	}
}
