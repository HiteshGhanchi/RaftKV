// Package check records client histories and checks them for
// linearizability with Porcupine. It has no dependency on Raft, so other
// projects can record histories against the same model.
package check

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
)

// Kind is a KV operation type.
type Kind uint8

const (
	Get Kind = iota
	Put
	Append
)

func (k Kind) String() string { return [...]string{"get", "put", "append"}[k] }

// Input is what a client asked for.
type Input struct {
	Kind  Kind
	Key   string
	Value string
}

// String renders the input as get(k), put(k, "v") or append(k, "v").
func (in Input) String() string {
	if in.Kind == Get {
		return fmt.Sprintf("get(%s)", in.Key)
	}
	return fmt.Sprintf("%s(%s, %q)", in.Kind, in.Key, in.Value)
}

// Output is what it got back. Unknown means the client gave up without an
// answer: the operation may or may not have taken effect.
type Output struct {
	Value   string
	Unknown bool
}

// Op is one completed (or abandoned) client operation.
type Op struct {
	Client int
	Input  Input
	Output Output
	Call   time.Duration
	Return time.Duration
}

// History collects operations in the order they finish.
type History struct{ ops []Op }

// Add records a finished operation.
func (h *History) Add(op Op) { h.ops = append(h.ops, op) }

// Ops returns the recorded operations.
func (h *History) Ops() []Op { return h.ops }

// Len is the number of operations.
func (h *History) Len() int { return len(h.ops) }

// toPorcupine converts the history. Abandoned writes stay in with an open
// interval (they might have happened at any later time); abandoned reads are
// dropped because they cannot change state.
func (h *History) toPorcupine() []porcupine.Operation {
	out := make([]porcupine.Operation, 0, len(h.ops))
	for _, op := range h.ops {
		ret := int64(op.Return)
		if op.Output.Unknown {
			if op.Input.Kind == Get {
				continue
			}
			ret = math.MaxInt64
		}
		out = append(out, porcupine.Operation{
			ClientId: op.Client, Input: op.Input, Output: op.Output,
			Call: int64(op.Call), Return: ret,
		})
	}
	return out
}

// KVModel is the sequential specification of a single-copy key-value map.
// Histories are partitioned by key, because operations on different keys are
// independent; that keeps checking fast.
var KVModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(Input).Key
			byKey[k] = append(byKey[k], op)
		}
		keys := make([]string, 0, len(byKey))
		for k := range byKey {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		s, in, out := state.(string), input.(Input), output.(Output)
		switch in.Kind {
		case Get:
			return out.Unknown || out.Value == s, s
		case Put:
			return true, in.Value
		default:
			return true, s + in.Value
		}
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(Input), output.(Output)
		res := out.Value
		if out.Unknown {
			res = "?"
		}
		if in.Kind == Get {
			return fmt.Sprintf("get(%s) -> %q", in.Key, res)
		}
		return fmt.Sprintf("%s(%s, %q)", in.Kind, in.Key, in.Value)
	},
}

// Verdict is the outcome of a linearizability check.
type Verdict string

const (
	Linearizable    Verdict = "linearizable"
	NotLinearizable Verdict = "NOT linearizable"
	Inconclusive    Verdict = "inconclusive (checker timed out)"
)

// Linearizable checks the history. A timeout is reported as Inconclusive,
// never as a pass.
func (h *History) Linearizable(timeout time.Duration) Verdict {
	res := porcupine.CheckOperationsTimeout(KVModel, h.toPorcupine(), timeout)
	switch res {
	case porcupine.Ok:
		return Linearizable
	case porcupine.Illegal:
		return NotLinearizable
	default:
		return Inconclusive
	}
}

// Visualize writes Porcupine's interactive HTML view of the history to path,
// which shows the longest linearizable prefix of a failing history.
func (h *History) Visualize(path string) error {
	_, info := porcupine.CheckOperationsVerbose(KVModel, h.toPorcupine(), 30*time.Second)
	return porcupine.VisualizePath(KVModel, info, path)
}
