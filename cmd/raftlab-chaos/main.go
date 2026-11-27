// Command raftlab-chaos runs the deterministic simulation harness.
//
//	raftlab-chaos -seeds 1000                 # sweep seeds 1..1000 with correct code
//	raftlab-chaos -seed 42 -v                 # replay one seed and print its trace
//	raftlab-chaos -seeds 2000 -mutation stale-read
//	raftlab-chaos -bugzoo -seeds 2000 -out results/bugzoo.json
//	raftlab-chaos -seed 42 -trace run.jsonl   # structured trace, one JSON event per line
//	raftlab-chaos -overhead -seeds 500 -out results/trace-overhead.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"time"

	"raftlab/check"
	"raftlab/harness"
	"raftlab/mutation"
	"raftlab/sim"
)

func main() {
	var (
		seed      = flag.Uint64("seed", 0, "run a single seed")
		start     = flag.Uint64("start", 1, "first seed of a sweep")
		seeds     = flag.Uint64("seeds", 100, "number of seeds in a sweep")
		mut       = flag.String("mutation", "none", "comma-separated bug zoo mutations to enable")
		workers   = flag.Int("workers", 0, "parallel runs (default: number of CPUs)")
		verbose   = flag.Bool("v", false, "single seed: print the full event trace")
		viz       = flag.String("viz", "", "single seed: write Porcupine's HTML history view here")
		bugzoo    = flag.Bool("bugzoo", false, "run every mutation over the sweep and report detection")
		out       = flag.String("out", "", "write results as JSON to this file")
		stopFail  = flag.Bool("stop", false, "sweep: stop at the first failing seed")
		traceOut  = flag.String("trace", "", "single seed: write the structured trace (JSON Lines) to this file")
		overhead  = flag.Bool("overhead", false, "measure tracing overhead over the sweep's seeds (one worker)")
		swarm     = flag.Bool("swarm", false, "swarm testing: each seed also disables a random subset of fault kinds")
		shrink    = flag.Bool("shrink", false, "single seed: shrink the failing run and print its minimal schedule")
		save      = flag.String("save", "", "with -shrink: write the shrunk run as a repro file")
		shrinkZoo = flag.String("shrink-zoo", "", "shrink every bug-zoo regression seed from this bugzoo.json into harness/testdata/schedules")
	)
	flag.Parse()

	muts, err := mutation.Parse(*mut)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	base := harness.DefaultOptions
	if *swarm {
		base = harness.SwarmOptions
	}
	optsFor := func(m mutation.Set) func(uint64) harness.Options {
		return func(s uint64) harness.Options {
			o := base(s)
			o.Mutations = m
			return o
		}
	}

	switch {
	case *shrinkZoo != "":
		shrinkAll(*shrinkZoo, *out)

	case *shrink:
		o := optsFor(muts)(*seed)
		rep, st, err := harness.Shrink(o, 0)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("property: %s (%d simulations)\nbefore: %s\nafter:  %s\n", st.Property, st.Runs, st.Before, st.After)
		for _, f := range rep.Opts.Schedule.Faults {
			fmt.Printf("  %s\n", f)
		}
		if *save != "" {
			rep.Note = fmt.Sprintf("shrunk from seed %d, mutations %s", *seed, muts)
			if err := rep.Save(*save); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}

	case *seed != 0:
		o := optsFor(muts)(*seed)
		o.TraceLog = *verbose
		var j *sim.JSONL
		if *traceOut != "" {
			f, err := os.Create(*traceOut)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(2)
			}
			defer f.Close()
			j = sim.NewJSONL(f, map[string]any{"seed": *seed, "mutations": muts.String(), "nodes": o.Nodes})
			o.Sink = j.Sink()
		}
		r := harness.Run(o)
		if j != nil {
			if err := j.Close(); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}
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

	case *overhead:
		measureOverhead(*start, *seeds, *out)

	case *bugzoo:
		name := "default"
		if *swarm {
			name = "swarm"
		}
		runBugZoo(*start, *seeds, *workers, *out, name, optsFor)

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

func runBugZoo(start, n uint64, workers int, out, optionsName string, optsFor func(mutation.Set) func(uint64) harness.Options) {
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
	writeJSON(out, map[string]any{"start": start, "seeds": n, "options": optionsName, "caught": caught, "total": len(rows), "medianSeedsToDetect": median, "rows": rows})
}

// measureOverhead runs the same seeds three times on one goroutine: with no
// trace sink, with a sink that only counts events, and with a JSON Lines sink
// writing to io.Discard. All three must give identical trace hashes.
func measureOverhead(start, n uint64, out string) {
	type mode struct {
		Name        string
		Seconds     float64
		SeedsPerSec float64
		Events      uint64
		Slowdown    float64
	}
	var modes []mode
	var hashes [3][]uint64
	for i, name := range []string{"no sink", "counting sink", "JSON Lines sink (io.Discard)"} {
		var events uint64
		t0 := time.Now()
		for s := start; s < start+n; s++ {
			o := harness.DefaultOptions(s)
			var j *sim.JSONL
			switch i {
			case 1:
				o.Sink = func(sim.Event) { events++ }
			case 2:
				j = sim.NewJSONL(io.Discard, nil)
				sink := j.Sink()
				o.Sink = func(e sim.Event) { events++; sink(e) }
			}
			hashes[i] = append(hashes[i], harness.Run(o).TraceHash)
			if j != nil {
				j.Close()
			}
		}
		el := time.Since(t0).Seconds()
		modes = append(modes, mode{Name: name, Seconds: el, SeedsPerSec: float64(n) / el, Events: events})
	}
	for i := range modes {
		modes[i].Slowdown = modes[i].Seconds / modes[0].Seconds
		fmt.Printf("%-30s %7.2fs  %6.1f seeds/s  %10d events  x%.2f\n", modes[i].Name, modes[i].Seconds, modes[i].SeedsPerSec, modes[i].Events, modes[i].Slowdown)
	}
	same := slices.Equal(hashes[0], hashes[1]) && slices.Equal(hashes[0], hashes[2])
	fmt.Printf("trace hashes identical across modes: %v\n", same)
	writeJSON(out, map[string]any{"start": start, "seeds": n, "workers": 1, "goos": runtime.GOOS, "goarch": runtime.GOARCH,
		"go": runtime.Version(), "cpus": runtime.NumCPU(), "hashesIdentical": same, "modes": modes})
	if !same {
		os.Exit(1)
	}
}

// shrinkAll shrinks the first failing seed of every mutation in a bug-zoo
// results file and saves each as a repro in harness/testdata/schedules.
func shrinkAll(zooPath, out string) {
	b, err := os.ReadFile(zooPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	var zoo struct{ Rows []zooRow }
	if err := json.Unmarshal(b, &zoo); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	type row struct {
		Mutation string
		Seed     uint64
		File     string
		harness.ShrinkStats
	}
	var rows []row
	for _, z := range zoo.Rows {
		m, err := mutation.Parse(z.Mutation)
		if err != nil || z.FirstSeed == 0 {
			fmt.Fprintf(os.Stderr, "%s: skipped (%v)\n", z.Mutation, err)
			continue
		}
		o := harness.DefaultOptions(z.FirstSeed)
		o.Mutations = m
		t0 := time.Now()
		rep, st, err := harness.Shrink(o, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s seed %d: %v\n", z.Mutation, z.FirstSeed, err)
			os.Exit(1)
		}
		rep.Note = fmt.Sprintf("shrunk from bug-zoo seed %d with %s", z.FirstSeed, z.Mutation)
		file := fmt.Sprintf("harness/testdata/schedules/%s.json", z.Mutation)
		if err := os.MkdirAll("harness/testdata/schedules", 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := rep.Save(file); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		rows = append(rows, row{z.Mutation, z.FirstSeed, file, st})
		fmt.Printf("%-22s seed %-5d %-20s faults %2d -> %-2d nodes %d -> %d clients %d -> %d steps %5d -> %-5d (%d sims, %.1fs)\n",
			z.Mutation, z.FirstSeed, st.Property, st.Before.Faults, st.After.Faults, st.Before.Nodes, st.After.Nodes,
			st.Before.Clients, st.After.Clients, st.Before.Steps, st.After.Steps, st.Runs, time.Since(t0).Seconds())
	}
	writeJSON(out, map[string]any{"source": zooPath, "rows": rows})
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
