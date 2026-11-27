package debug

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"raftlab/sim"
)

// ShiVizRegex is the log-parsing expression to paste into ShiViz together
// with the output of ShiViz.
const ShiVizRegex = `(?<host>\S*) (?<clock>{.*}) (?<event>.*)`

// ShiViz writes a trace in the plain-text form the ShiViz visualiser reads:
// one line per event with the host, its vector clock as JSON, and a
// description. Vector clocks are computed from the trace itself: a send
// ticks the sender's clock and remembers it under the message id; a delivery
// that reaches its handler merges that clock into the receiver's and ticks
// it; role changes, crashes and restarts tick the node's own clock. Messages
// dropped on arrival are never merged. A delivery to a paused node is merged
// when it arrives, slightly before the node handles it, which keeps the
// order causally correct.
func ShiViz(events []sim.Event, w io.Writer) error {
	droppedOnArrival := map[int64]bool{}
	for _, e := range events {
		if e.Kind == "drop" {
			if id, ok := e.Get("msg"); ok {
				droppedOnArrival[id] = true
			}
		}
	}
	clocks := map[uint64]map[uint64]uint64{}
	clock := func(n uint64) map[uint64]uint64 {
		if clocks[n] == nil {
			clocks[n] = map[uint64]uint64{}
		}
		return clocks[n]
	}
	sent := map[int64]map[uint64]uint64{}
	write := func(n uint64, desc string) error {
		c := clock(n)
		keys := make([]uint64, 0, len(c))
		for k := range c {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			parts[i] = fmt.Sprintf(`"n%d":%d`, k, c[k])
		}
		_, err := fmt.Fprintf(w, "n%d {%s} %s\n", n, strings.Join(parts, ","), desc)
		return err
	}
	for _, e := range events {
		var desc string
		switch e.Kind {
		case "send", "dup":
			id, _ := e.Get("msg")
			c := clock(e.Node)
			c[e.Node]++
			sent[id] = maps(c)
			desc = fmt.Sprintf("t=%v send to n%d: %s", e.At, e.Peer, e.Detail)
		case "deliver":
			id, _ := e.Get("msg")
			if droppedOnArrival[id] || sent[id] == nil {
				continue
			}
			c := clock(e.Node)
			for k, v := range sent[id] {
				c[k] = max(c[k], v)
			}
			c[e.Node]++
			desc = fmt.Sprintf("t=%v receive from n%d: %s", e.At, e.Peer, e.Detail)
		case "role", "crash", "start":
			clock(e.Node)[e.Node]++
			desc = fmt.Sprintf("t=%v %s", e.At, strings.TrimSpace(strings.TrimPrefix(e.String(), e.At.String())))
		default:
			continue
		}
		if err := write(e.Node, desc); err != nil {
			return err
		}
	}
	return nil
}

func maps(m map[uint64]uint64) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
