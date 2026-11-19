package raft

import "fmt"

// InvariantError is raised (as a panic) when the node detects that a safety
// property is about to be violated. Each has a stable Name that matches a
// property in spec/Raft.tla and docs/PROTOCOL.md, so a failing seed points
// straight at the rule that broke.
type InvariantError struct {
	Name   string
	Detail string
}

func (e *InvariantError) Error() string { return "invariant " + e.Name + " violated: " + e.Detail }

func invariant(ok bool, name, format string, args ...any) {
	if !ok {
		panic(&InvariantError{Name: name, Detail: fmt.Sprintf(format, args...)})
	}
}
