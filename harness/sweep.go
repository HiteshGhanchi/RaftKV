package harness

import (
	"runtime"
	"sort"
	"sync"
)

// Sweep runs seeds [start, start+n) in parallel and returns results in seed
// order. Each run is single-threaded and independent, so running many at once
// does not affect determinism. If stopAtFirst is set, workers stop picking up
// new seeds after the first failure (results then hold every finished run).
func Sweep(start, n uint64, workers int, opts func(seed uint64) Options, stopAtFirst bool) []Result {
	if workers <= 0 {
		workers = runtime.NumCPU()
	}
	var (
		mu      sync.Mutex
		results []Result
		next    = start
		stop    bool
		wg      sync.WaitGroup
	)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if stop || next >= start+n {
					mu.Unlock()
					return
				}
				seed := next
				next++
				mu.Unlock()

				r := Run(opts(seed))
				r.History, r.TraceLog = nil, nil

				mu.Lock()
				results = append(results, r)
				if r.Failed() && stopAtFirst {
					stop = true
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	sort.Slice(results, func(i, j int) bool { return results[i].Seed < results[j].Seed })
	return results
}
