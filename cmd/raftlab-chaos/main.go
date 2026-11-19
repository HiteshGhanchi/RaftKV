// Command raftlab-chaos runs the deterministic simulation harness.
//
//	raftlab-chaos -seeds 1000                 # sweep seeds 1..1000 with correct code
//	raftlab-chaos -seed 42 -v                 # replay one seed and print its trace
//	raftlab-chaos -seeds 2000 -mutation stale-read
//	raftlab-chaos -bugzoo -seeds 2000 -out results/bugzoo.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"time"

	"raftlab/check"
	"raftlab/harness"
	"raftlab/mutation"
)

func main() {
	var (
		seed     = flag.Uint64("seed", 0, "run a single seed")
		start    = flag.Uint64("start", 1, "first seed of a sweep")
		seeds    = flag.Uint64("seeds", 100, "number of seeds in a sweep")
		mut      = flag.String("mutation", "none", "comma-separated bug zoo mutations to enable")
		workers  = flag.Int("workers", 0, "parallel runs (default: number of CPUs)")
		verbose  = flag.Bool("v", false, "single seed: print the full event trace")
		viz      = flag.String("viz", "", "single seed: write Porcupine's HTML history view here")
		bugzoo   = flag.Bool("bugzoo", false, "run every mutation over the sweep and report detection")
		out      = flag.String("out", "", "write results as JSON to this file")
		stopFail = flag.Bool("stop", false, "sweep: stop at the first failing seed")
	)
	flag.Parse()

	muts, err := mutation.Parse(*mut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	optsFor := func(m mutation.Set) func(uint64) harness.Options {
		return func(s uint64) harness.Options {
			o := harness.DefaultOptions(s)
			o.Mutations = m
			return o
		}
	}

	switch {
	case *seed != 0:
		o := optsFor(muts)(*seed)
		o.TraceLog = *verbose
		r := harness.Run(o)
		if *verbose {
			for _, line := range r.TraceLog {
				fmt.Println(line)
			}
		}
		fmt.Printf("config: nodes=%d maxEntries=%d snapshotEvery=%d checkQuorum=%v drop=%.2f faultInterval=%v\n",
			o.Nodes, o.MaxEntriesPerMsg, o.SnapshotEvery, o.CheckQuorum, o.Net.DropRate, o.Faults.Interval)
		fmt.Println(r)
		if *viz != "" {
			if err := r.History.Visualize(*viz); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
		writeJSON(*out, r)
		if r.Failed() {
			os.Exit(1)
		}

	case *bugzoo:
		runBugZoo(*start, *seeds, *workers, *out, optsFor)

	default:
		t0 := time.Now()
		rs := harness.Sweep(*start, *seeds, *workers, optsFor(muts), *stopFail)
		fails, inconclusive, ops := 0, 0, 0
		for _, r := range rs {
			ops += r.Completed
			if r.Linearizability == check.Inconclusive {
				inconclusive++
			}
			if r.Failed() {
				fails++
				fmt.Println(r)
			}
		}
		fmt.Printf("%d runs, %d failed, %d inconclusive, %d client ops checked, mutations=%s, %.1fs\n",
			len(rs), fails, inconclusive, ops, muts, time.Since(t0).Seconds())
		var failed []harness.Result
		for _, r := range rs {
			if r.Failed() {
				failed = append(failed, r)
			}
		}
		writeJSON(*out, summary{Runs: len(rs), Failed: fails, Inconclusive: inconclusive, Ops: ops, Mutations: muts.String(), Start: *start, Failures: failed})
		if fails > 0 {
			os.Exit(1)
		}
	}
}

type summary struct {
	Runs, Failed, Inconclusive, Ops int
	Mutations                       string
	Start                           uint64
	Failures                        []harness.Result `json:",omitempty"`
}

type zooRow struct {
	Mutation      string
	Rule          string
	Seeds         int
	Caught        int
	FirstSeed     uint64 // smallest failing seed (kept as a regression test)
	SeedsToDetect int    // how many seeds, counted from the start, until the first failure
	FirstFailure  string
}

func runBugZoo(start, n uint64, workers int, out string, optsFor func(mutation.Set) func(uint64) harness.Options) {
	var rows []zooRow
	for _, m := range mutation.All {
		t0 := time.Now()
		rs := harness.Sweep(start, n, workers, optsFor(m.Flag), false)
		row := zooRow{Mutation: m.Name, Rule: m.Rule, Seeds: len(rs)}
		for i, r := range rs {
			if !r.Failed() {
				continue
			}
			row.Caught++
			if row.FirstSeed == 0 {
				row.FirstSeed = r.Seed
				row.SeedsToDetect = i + 1
				row.FirstFailure = r.Violation
				if row.FirstFailure == "" {
					row.FirstFailure = "history " + string(r.Linearizability)
				}
			}
		}
		rows = append(rows, row)
		fmt.Printf("%-22s caught %5d/%d  first seed %-6d %.1fs  %s\n", m.Name, row.Caught, row.Seeds, row.FirstSeed, time.Since(t0).Seconds(), trim(row.FirstFailure, 70))
	}
	caught, detect := 0, []int{}
	for _, r := range rows {
		if r.Caught > 0 {
			caught++
			detect = append(detect, r.SeedsToDetect)
		}
	}
	slices.Sort(detect)
	median := 0.0
	if k := len(detect); k > 0 {
		median = float64(detect[k/2])
		if k%2 == 0 {
			median = float64(detect[k/2-1]+detect[k/2]) / 2
		}
	}
	fmt.Printf("%d/%d injected bugs caught; median seeds to first detection = %.1f\n", caught, len(rows), median)
	writeJSON(out, map[string]any{"start": start, "seeds": n, "caught": caught, "total": len(rows), "medianSeedsToDetect": median, "rows": rows})
}

func trim(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func writeJSON(path string, v any) {
	if path == "" {
		return
	}
	b, _ := json.MarshalIndent(v, "", "  ")
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}
