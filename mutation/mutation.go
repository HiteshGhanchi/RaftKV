// Package mutation lists the deliberately injectable bugs (the "bug zoo").
//
// Each mutation switches off one rule that Raft or the KV layer depends on.
// They exist only to prove that the simulation harness notices when a rule is
// broken: a harness that never fails is not evidence of anything.
package mutation

import (
	"fmt"
	"strings"
)

// Set is a bit set of enabled mutations. The zero value means "correct code".
type Set uint32

const (
	// CommitPrevTerm lets a leader commit an entry from an earlier term just
	// by counting replicas (the Figure 8 hazard, paper §5.4.2).
	CommitPrevTerm Set = 1 << iota
	// SkipUpToDateCheck grants votes without checking the candidate's log is
	// at least as up-to-date as the voter's (paper §5.4.1).
	SkipUpToDateCheck
	// ForgetVote never persists votedFor, so a restarted node can vote twice
	// in one term (Figure 2, "Persistent state on all servers").
	ForgetVote
	// ApplyBeforeCommit hands entries to the state machine before they are
	// committed (Figure 2, "All Servers" rule 1 applies only up to commitIndex).
	ApplyBeforeCommit
	// StaleRead serves reads from the leader's local state without the
	// ReadIndex heartbeat round, so a deposed leader can return stale data
	// (thesis §6.4).
	StaleRead
	// AcceptStaleTerm lets a follower accept AppendEntries from a leader with
	// an older term (Figure 2, AppendEntries receiver rule 1).
	AcceptStaleTerm
	// SkipFsync writes the log without fsync, so a crash can lose entries the
	// node already acknowledged (Figure 2: persist before responding).
	SkipFsync
	// NoDedup applies retried client commands again instead of returning the
	// cached result (thesis §6.3, client sessions).
	NoDedup
)

// Info describes one mutation for reports.
type Info struct {
	Flag Set
	Name string
	Rule string // which rule it breaks
}

// All is every mutation, in a stable order.
var All = []Info{
	{CommitPrevTerm, "commit-prev-term", "Figure 2 Leaders rule 4 / §5.4.2: only commit entries from the current term by counting"},
	{SkipUpToDateCheck, "skip-up-to-date-check", "RequestVote receiver rule 2 / §5.4.1: candidate's log must be at least as up-to-date"},
	{ForgetVote, "forget-vote", "Figure 2 persistent state: votedFor must survive restarts"},
	{ApplyBeforeCommit, "apply-before-commit", "All Servers rule 1: apply only entries up to commitIndex"},
	{StaleRead, "stale-read", "Thesis §6.4: a leader must confirm it is still leader before serving a read"},
	{AcceptStaleTerm, "accept-stale-term", "AppendEntries receiver rule 1: reply false if term < currentTerm"},
	{SkipFsync, "skip-fsync", "Figure 2: persist state before responding to RPCs"},
	{NoDedup, "no-dedup", "Thesis §6.3: retried commands must take effect exactly once"},
}

// Has reports whether m is enabled.
func (s Set) Has(m Set) bool { return s&m != 0 }

// String lists enabled mutation names, or "none".
func (s Set) String() string {
	var names []string
	for _, info := range All {
		if s.Has(info.Flag) {
			names = append(names, info.Name)
		}
	}
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ",")
}

// Parse turns a comma-separated list of names into a Set. "" and "none" mean
// no mutations.
func Parse(s string) (Set, error) {
	var out Set
	if s == "" || s == "none" {
		return 0, nil
	}
	for _, name := range strings.Split(s, ",") {
		found := false
		for _, info := range All {
			if info.Name == strings.TrimSpace(name) {
				out |= info.Flag
				found = true
			}
		}
		if !found {
			return 0, fmt.Errorf("unknown mutation %q", name)
		}
	}
	return out, nil
}
