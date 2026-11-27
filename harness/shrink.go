package harness

import (
	"errors"
	"fmt"
	"time"
)

// Size measures how big a failing run is. Shrinking tries to make every
// field smaller while the same safety property still fails.
type Size struct {
	Faults   int           `json:"faults"` // effective faults in the schedule
	Nodes    int           `json:"nodes"`
	Clients  int           `json:"clients"`
	Duration time.Duration `json:"duration"` // fault phase length
	Settle   time.Duration `json:"settle"`
	Steps    uint64        `json:"steps"` // events until the run stopped
	Ops      int           `json:"ops"`   // client operations started
	DropRate float64       `json:"dropRate"`
	DupRate  float64       `json:"dupRate"`
}

func sizeOf(o Options, r Result) Size {
	return Size{Faults: r.Schedule.Effective(), Nodes: o.Nodes, Clients: o.Clients, Duration: o.Duration,
		Settle: o.Settle, Steps: r.Steps, Ops: r.Ops, DropRate: o.Net.DropRate, DupRate: o.Net.DupRate}
}

func (s Size) String() string {
	return fmt.Sprintf("%d faults, %d nodes, %d clients, %v+%v, %d steps, %d ops, drop %.2f, dup %.2f",
		s.Faults, s.Nodes, s.Clients, s.Duration, s.Settle, s.Steps, s.Ops, s.DropRate, s.DupRate)
}

// ShrinkStats reports what shrinking did.
type ShrinkStats struct {
	Property string `json:"property"`
	Runs     int    `json:"runs"` // simulations executed while shrinking
	Before   Size   `json:"before"`
	After    Size   `json:"after"`
}

// ErrNotFailing is returned when the run to shrink does not fail.
var ErrNotFailing = errors.New("harness: the run does not fail, nothing to shrink")

// Shrink looks for a smaller run that violates the same safety property as
// o: fewer faults (delta debugging over the recorded schedule), then fewer
// nodes and clients, no message loss or duplication, and shorter phases.
// "Same property" means the same name (LeaderCompleteness, Linearizability,
// ...), not the same detail, because message delays still come from the seed
// and shift when faults are removed (docs/DECISIONS.md V5). budget caps the
// number of simulations (0 means 3000). The result replays deterministically.
func Shrink(o Options, budget int) (Repro, ShrinkStats, error) {
	if budget <= 0 {
		budget = 3000
	}
	o.Sink, o.TraceLog = nil, false
	first := Run(o)
	prop := Property(first)
	if prop == "" {
		return Repro{}, ShrinkStats{}, ErrNotFailing
	}
	st := ShrinkStats{Property: prop, Runs: 1, Before: sizeOf(o, first)}

	fails := func(c Options) bool {
		if st.Runs >= budget {
			return false
		}
		st.Runs++
		return Property(Run(c)) == prop
	}
	withFaults := func(c Options, fs []Fault) Options {
		c.Schedule = (&Schedule{Faults: fs}).Clone()
		return c
	}

	// Replay mode from here on, starting with only the faults that did
	// something.
	var faults []Fault
	for _, f := range first.Schedule.Faults {
		if f.Kind != FaultNone && f.Kind != FaultStop {
			faults = append(faults, f)
		}
	}
	cur := withFaults(o, faults)
	if !fails(cur) {
		// Dropping the no-op entries changed the timer pattern enough to
		// lose the failure; keep the schedule exactly as recorded.
		faults = first.Schedule.Clone().Faults
		cur = withFaults(o, faults)
	}

	minimiseFaults := func() {
		faults = ddmin(faults, func(fs []Fault) bool { return fails(withFaults(cur, fs)) })
		cur = withFaults(cur, faults)
	}
	try := func(change func(*Options)) {
		c := cur
		change(&c)
		c = withFaults(c, faults)
		if fails(c) {
			cur = c
		}
	}

	minimiseFaults()
	if cur.Nodes > 3 {
		try(func(c *Options) { c.Nodes = 3 })
	}
	for n := 1; n < cur.Clients; n++ {
		before := cur.Clients
		try(func(c *Options) { c.Clients = n })
		if cur.Clients != before {
			break
		}
	}
	try(func(c *Options) { c.Net.DupRate = 0 })
	try(func(c *Options) { c.Net.DropRate = 0 })
	try(func(c *Options) { c.Faults.ClockSkew = 0 })
	if len(faults) > 0 {
		last := faults[len(faults)-1].At
		if d := (last/(100*time.Millisecond) + 1) * 100 * time.Millisecond; d < cur.Duration {
			try(func(c *Options) { c.Duration = d })
		}
	}
	// The settle phase must leave time for an election after the network
	// heals, or correct code would fail the Progress check.
	for cur.Settle/2 >= time.Second {
		before := cur.Settle
		try(func(c *Options) { c.Settle /= 2 })
		if cur.Settle == before {
			break
		}
	}
	minimiseFaults() // smaller configurations sometimes need fewer faults

	final := Run(cur)
	st.Runs++
	if Property(final) != prop {
		return Repro{}, st, fmt.Errorf("harness: shrunk run no longer violates %s", prop)
	}
	st.After = sizeOf(cur, final)
	cur.Schedule = final.Schedule.Clone()
	return Repro{Version: ReproVersion, Expect: prop, Opts: cur}, st, nil
}

// ddmin is Zeller's delta-debugging minimisation: it returns a subset of xs
// that still satisfies fails and from which no single chunk at the final
// granularity can be removed.
func ddmin[T any](xs []T, fails func([]T) bool) []T {
	n := 2
	for len(xs) >= 2 {
		chunk := (len(xs) + n - 1) / n
		reduced := false
		// Try each complement (xs without one chunk).
		for start := 0; start < len(xs); start += chunk {
			end := min(start+chunk, len(xs))
			comp := append(append([]T{}, xs[:start]...), xs[end:]...)
			if fails(comp) {
				xs = comp
				n = max(n-1, 2)
				reduced = true
				break
			}
		}
		if reduced {
			continue
		}
		if n >= len(xs) {
			break
		}
		n = min(n*2, len(xs))
	}
	if len(xs) == 1 && fails(nil) {
		return nil
	}
	return xs
}
