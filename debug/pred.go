package debug

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"raftlab/raft"
)

// Pred is a condition on the session, checked after a step.
type Pred func(*Session) bool

// Parse reads a breakpoint expression. Forms (combine with "and"):
//
//	violation              a safety property has failed
//	elected                some node became leader in the last step
//	leader N               node N is currently leader
//	down N | up N          node N is crashed / running
//	term OP K              some running node's term OP K
//	commit N OP K          node N's commit index OP K
//	applied N OP K         node N's applied index OP K
//	kind NAME              the last step emitted an event of this kind (e.g. crash, partition)
//	time OP DURATION       virtual time OP DURATION (e.g. "time > 1.5s")
//	step OP K              step number OP K
//
// OP is one of > >= < <= ==.
func Parse(expr string) (Pred, error) {
	var preds []Pred
	for _, part := range strings.Split(expr, " and ") {
		p, err := parseOne(strings.Fields(part))
		if err != nil {
			return nil, fmt.Errorf("%q: %w", strings.TrimSpace(part), err)
		}
		preds = append(preds, p)
	}
	if len(preds) == 1 {
		return preds[0], nil
	}
	return func(s *Session) bool {
		for _, p := range preds {
			if !p(s) {
				return false
			}
		}
		return true
	}, nil
}

func parseOne(f []string) (Pred, error) {
	if len(f) == 0 {
		return nil, fmt.Errorf("empty condition")
	}
	switch f[0] {
	case "violation":
		return func(s *Session) bool { return s.sim.Violation() != "" }, nil
	case "elected":
		return func(s *Session) bool {
			for _, e := range s.LastEvents() {
				if e.Kind == "role" && e.Detail == raft.Leader.String() {
					return true
				}
			}
			return false
		}, nil
	case "leader", "down", "up":
		if len(f) != 2 {
			return nil, fmt.Errorf("want %q N", f[0])
		}
		id, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return nil, err
		}
		kind := f[0]
		return func(s *Session) bool {
			for _, n := range s.sim.Nodes() {
				if uint64(n.ID) != id {
					continue
				}
				switch kind {
				case "leader":
					return n.Up && n.Status.State == raft.Leader
				case "down":
					return !n.Up
				default:
					return n.Up
				}
			}
			return false
		}, nil
	case "term":
		if len(f) != 3 {
			return nil, fmt.Errorf("want term OP K")
		}
		cmp, k, err := opInt(f[1], f[2])
		if err != nil {
			return nil, err
		}
		return func(s *Session) bool {
			for _, n := range s.sim.Nodes() {
				if n.Up && cmp(int64(n.Status.Term), k) {
					return true
				}
			}
			return false
		}, nil
	case "commit", "applied":
		if len(f) != 4 {
			return nil, fmt.Errorf("want %s N OP K", f[0])
		}
		id, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			return nil, err
		}
		cmp, k, err := opInt(f[2], f[3])
		if err != nil {
			return nil, err
		}
		applied := f[0] == "applied"
		return func(s *Session) bool {
			for _, n := range s.sim.Nodes() {
				if uint64(n.ID) == id && n.Up {
					v := n.Status.Commit
					if applied {
						v = n.Status.Applied
					}
					return cmp(int64(v), k)
				}
			}
			return false
		}, nil
	case "kind":
		if len(f) != 2 {
			return nil, fmt.Errorf("want kind NAME")
		}
		name := f[1]
		return func(s *Session) bool {
			for _, e := range s.LastEvents() {
				if e.Kind == name {
					return true
				}
			}
			return false
		}, nil
	case "time":
		if len(f) != 3 {
			return nil, fmt.Errorf("want time OP DURATION")
		}
		d, err := time.ParseDuration(f[2])
		if err != nil {
			return nil, err
		}
		cmp, _, err := opInt(f[1], "0")
		if err != nil {
			return nil, err
		}
		return func(s *Session) bool { return cmp(int64(s.Now()), int64(d)) }, nil
	case "step":
		if len(f) != 3 {
			return nil, fmt.Errorf("want step OP K")
		}
		cmp, k, err := opInt(f[1], f[2])
		if err != nil {
			return nil, err
		}
		return func(s *Session) bool { return cmp(int64(s.Step()), k) }, nil
	}
	return nil, fmt.Errorf("unknown condition %q", f[0])
}

func opInt(op, val string) (func(a, b int64) bool, int64, error) {
	k, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil, 0, err
	}
	var cmp func(a, b int64) bool
	switch op {
	case ">":
		cmp = func(a, b int64) bool { return a > b }
	case ">=":
		cmp = func(a, b int64) bool { return a >= b }
	case "<":
		cmp = func(a, b int64) bool { return a < b }
	case "<=":
		cmp = func(a, b int64) bool { return a <= b }
	case "==":
		cmp = func(a, b int64) bool { return a == b }
	default:
		return nil, 0, fmt.Errorf("unknown operator %q", op)
	}
	return cmp, k, nil
}
