package debug

import (
	"fmt"
	"time"

	"raftlab/harness"
)

// NodeLine renders one node as a single line.
func NodeLine(n harness.NodeState, now time.Duration) string {
	if !n.Up {
		return fmt.Sprintf("n%d DOWN", n.ID)
	}
	st := n.Status
	paused := ""
	if n.PausedUntil > now {
		paused = fmt.Sprintf(" PAUSED until %v", n.PausedUntil)
	}
	lastTerm := uint64(0)
	if len(n.Log) > 0 {
		lastTerm = n.Log[len(n.Log)-1].Term
	}
	return fmt.Sprintf("n%d %-9s term=%d vote=%d lead=%d commit=%d applied=%d log=[snap %d .. %d] lastTerm=%d%s",
		n.ID, st.State, st.Term, st.Vote, st.Lead, st.Commit, st.Applied, st.SnapIndex, st.LastIndex, lastTerm, paused)
}
