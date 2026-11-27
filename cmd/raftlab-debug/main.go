// Command raftlab-debug is a time-travelling debugger for one simulated run.
//
//	raftlab-debug -seed 151 -mutation commit-prev-term          # interactive
//	raftlab-debug -seed 151 -mutation commit-prev-term -c "break violation; continue; nodes"
//	raftlab-debug -repro testdata/schedules/commit-prev-term.json
//	raftlab-debug -measure-back -seeds 200 -out results/debug-back.json
//
// Type "help" at the prompt for the commands.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"raftlab/debug"
	"raftlab/harness"
	"raftlab/mutation"
)

func main() {
	var (
		seed    = flag.Uint64("seed", 1, "seed to debug")
		mut     = flag.String("mutation", "none", "comma-separated bug zoo mutations to enable")
		swarm   = flag.Bool("swarm", false, "use swarm-testing options for the seed")
		repro   = flag.String("repro", "", "debug a saved repro file (seed, options and fault schedule) instead of -seed")
		script  = flag.String("c", "", "run these commands (separated by ;) and exit, echoing each one")
		measure = flag.Bool("measure-back", false, "measure how long 'back 1' takes at 10%, 50% and 90% of runs")
		seeds   = flag.Uint64("seeds", 200, "with -measure-back: number of seeds")
		out     = flag.String("out", "", "with -measure-back: write results as JSON here")
	)
	flag.Parse()

	if *measure {
		measureBack(*seeds, *out)
		return
	}

	var o harness.Options
	if *repro != "" {
		r, err := harness.LoadRepro(*repro)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		o = r.Options()
	} else {
		m, err := mutation.Parse(*mut)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if *swarm {
			o = harness.SwarmOptions(*seed)
		} else {
			o = harness.DefaultOptions(*seed)
		}
		o.Mutations = m
	}
	s := debug.Open(o)
	fmt.Printf("raftlab-debug: seed %d, %d nodes, mutations=%s, %d scheduled faults. Type help.\n",
		o.Seed, o.Nodes, o.Mutations, len(scheduled(o)))

	if *script != "" {
		for _, cmd := range strings.Split(*script, ";") {
			cmd = strings.TrimSpace(cmd)
			if cmd == "" {
				continue
			}
			fmt.Printf("(debug) %s\n", cmd)
			if s.Exec(cmd, os.Stdout) {
				return
			}
		}
		return
	}
	in := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("(debug) ")
		if !in.Scan() {
			fmt.Println()
			return
		}
		if s.Exec(in.Text(), os.Stdout) {
			return
		}
	}
}

func scheduled(o harness.Options) []harness.Fault {
	if o.Schedule == nil {
		return nil
	}
	return o.Schedule.Faults
}

// measureBack records the wall time of "back 1" at three points of each run.
// Going back re-executes the run from the start, so the cost grows with the
// position; this measures how much (docs/DECISIONS.md V1).
func measureBack(n uint64, out string) {
	type point struct {
		Position string
		P50ms    float64
		P99ms    float64
		Maxms    float64
	}
	positions := []float64{0.1, 0.5, 0.9}
	samples := make([][]float64, len(positions))
	var steps []uint64
	for seed := uint64(1); seed <= n; seed++ {
		o := harness.DefaultOptions(seed)
		total := harness.Run(o).Steps
		steps = append(steps, total)
		for i, p := range positions {
			s := debug.Open(o)
			s.Goto(uint64(float64(total) * p))
			t0 := time.Now()
			s.Back(1)
			samples[i] = append(samples[i], float64(time.Since(t0).Microseconds())/1000)
		}
	}
	pct := func(xs []float64, q float64) float64 {
		ys := slices.Clone(xs)
		slices.Sort(ys)
		return ys[min(len(ys)-1, int(q*float64(len(ys))))]
	}
	var pts []point
	for i, p := range positions {
		pt := point{Position: fmt.Sprintf("%.0f%% of the run", p*100), P50ms: pct(samples[i], 0.5), P99ms: pct(samples[i], 0.99), Maxms: pct(samples[i], 1)}
		pts = append(pts, pt)
		fmt.Printf("back 1 at %-16s p50 %6.2f ms  p99 %6.2f ms  max %6.2f ms\n", pt.Position, pt.P50ms, pt.P99ms, pt.Maxms)
	}
	slices.Sort(steps)
	fmt.Printf("%d seeds, steps per run: median %d, max %d\n", n, steps[len(steps)/2], steps[len(steps)-1])
	if out == "" {
		return
	}
	b, _ := json.MarshalIndent(map[string]any{"seeds": n, "goos": runtime.GOOS, "goarch": runtime.GOARCH, "go": runtime.Version(),
		"medianSteps": steps[len(steps)/2], "maxSteps": steps[len(steps)-1], "points": pts}, "", "  ")
	if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
