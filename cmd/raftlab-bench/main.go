// Command raftlab-bench measures a real raftlab cluster: N servers in this
// process on loopback, each with its own data directory on the real disk,
// talking over real HTTP. Clients run in the same process.
//
//	raftlab-bench -nodes 3 -clients 32 -duration 10s
//	raftlab-bench -failover 20
//
// Every run prints the machine description and writes JSON when -out is set.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"raftlab/client"
	"raftlab/raft"
	"raftlab/server"
)

type result struct {
	Machine   map[string]string
	Nodes     int
	Clients   int
	Payload   int
	Duration  string
	NoSync    bool
	MaxBatch  int
	Ops       int64
	Errors    int64
	OpsPerSec float64
	P50ms     float64
	P99ms     float64
	MaxMs     float64

	FailoverRuns []float64 `json:",omitempty"` // ms from leader kill to first acknowledged write
	FailoverP50  float64   `json:",omitempty"`
	FailoverMax  float64   `json:",omitempty"`
}

func main() {
	nodes := flag.Int("nodes", 3, "cluster size")
	clients := flag.Int("clients", 32, "concurrent clients (one outstanding write each)")
	dur := flag.Duration("duration", 10*time.Second, "measurement time after warm-up")
	payload := flag.Int("payload", 100, "value size in bytes")
	nosync := flag.Bool("nosync", false, "skip fsync (unsafe; shows what durability costs)")
	batch := flag.Int("batch", 256, "max writes per proposal batch (1 = no batching)")
	failover := flag.Int("failover", 0, "instead of throughput, kill the leader this many times and time recovery")
	dir := flag.String("dir", "", "data directory (default: a fresh temp dir)")
	out := flag.String("out", "", "write JSON results here")
	flag.Parse()

	if *dir == "" {
		d, err := os.MkdirTemp("", "raftlab-bench-")
		check(err)
		defer os.RemoveAll(d)
		*dir = d
	}
	cl := newCluster(*nodes, *dir, *nosync, *batch)
	defer cl.stopAll()
	cl.waitLeader()

	res := result{Machine: machine(*dir), Nodes: *nodes, Clients: *clients, Payload: *payload,
		Duration: dur.String(), NoSync: *nosync, MaxBatch: *batch}
	if *failover > 0 {
		res.FailoverRuns = cl.failover(*failover)
		s := slices.Clone(res.FailoverRuns)
		slices.Sort(s)
		res.FailoverP50, res.FailoverMax = s[len(s)/2], s[len(s)-1]
		fmt.Printf("failover over %d leader kills: p50 %.0f ms, max %.0f ms (all: %v)\n", len(s), res.FailoverP50, res.FailoverMax, round(res.FailoverRuns))
	} else {
		cl.throughput(&res, *clients, *payload, *dur)
		fmt.Printf("nodes=%d clients=%d payload=%dB batch=%d nosync=%v: %.0f ops/s, p50 %.2f ms, p99 %.2f ms, max %.1f ms, errors %d\n",
			res.Nodes, res.Clients, res.Payload, res.MaxBatch, res.NoSync, res.OpsPerSec, res.P50ms, res.P99ms, res.MaxMs, res.Errors)
	}
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		check(os.WriteFile(*out, append(b, '\n'), 0o644))
	}
}

type cluster struct {
	addrs   map[raft.NodeID]string
	dir     string
	nosync  bool
	batch   int
	mu      sync.Mutex
	servers map[raft.NodeID]*server.Server
}

func newCluster(n int, dir string, nosync bool, batch int) *cluster {
	c := &cluster{addrs: map[raft.NodeID]string{}, dir: dir, nosync: nosync, batch: batch, servers: map[raft.NodeID]*server.Server{}}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		check(err)
		c.addrs[raft.NodeID(i)] = ln.Addr().String()
		ln.Close()
	}
	for id := range c.addrs {
		c.start(id)
	}
	return c
}

func (c *cluster) start(id raft.NodeID) {
	s, err := server.Start(server.Config{ID: id, Addrs: c.addrs, DataDir: filepath.Join(c.dir, fmt.Sprint(id)),
		NoSync: c.nosync, MaxBatch: c.batch})
	check(err)
	c.mu.Lock()
	c.servers[id] = s
	c.mu.Unlock()
}

func (c *cluster) stopAll() {
	for _, s := range c.servers {
		s.Stop()
	}
}

func (c *cluster) waitLeader() raft.NodeID {
	for {
		c.mu.Lock()
		for id, s := range c.servers {
			if st := s.Status(); st.State == raft.Leader {
				c.mu.Unlock()
				return id
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *cluster) client() *client.Client {
	var addrs []string
	for i := 1; i <= len(c.addrs); i++ {
		addrs = append(addrs, c.addrs[raft.NodeID(i)])
	}
	cl := client.New(addrs)
	cl.Timeout = 30 * time.Second
	return cl
}

func (c *cluster) throughput(res *result, clients, payload int, dur time.Duration) {
	value := strings.Repeat("x", payload)
	var ops, errs atomic.Int64
	var mu sync.Mutex
	var lat []time.Duration
	var measuring atomic.Bool
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl := c.client()
			var mine []time.Duration
			for j := 0; ; j++ {
				select {
				case <-stop:
					mu.Lock()
					lat = append(lat, mine...)
					mu.Unlock()
					return
				default:
				}
				t0 := time.Now()
				err := cl.Put(fmt.Sprintf("key-%d-%d", i, j%100), value)
				if !measuring.Load() {
					continue
				}
				if err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
				mine = append(mine, time.Since(t0))
			}
		}()
	}
	time.Sleep(time.Second) // warm-up: connections, leader known
	measuring.Store(true)
	t0 := time.Now()
	time.Sleep(dur)
	measuring.Store(false)
	elapsed := time.Since(t0)
	close(stop)
	wg.Wait()
	slices.Sort(lat)
	res.Ops, res.Errors = ops.Load(), errs.Load()
	res.OpsPerSec = float64(res.Ops) / elapsed.Seconds()
	if len(lat) > 0 {
		res.P50ms = ms(lat[len(lat)/2])
		res.P99ms = ms(lat[len(lat)*99/100])
		res.MaxMs = ms(lat[len(lat)-1])
	}
}

// failover kills the leader, then measures until a write is acknowledged.
func (c *cluster) failover(runs int) []float64 {
	cl := c.client()
	check(cl.Put("warm", "up"))
	var out []float64
	for i := range runs {
		leader := c.waitLeader()
		c.mu.Lock()
		s := c.servers[leader]
		delete(c.servers, leader)
		c.mu.Unlock()
		t0 := time.Now()
		s.Stop()
		check(cl.Put("failover", fmt.Sprint(i)))
		out = append(out, ms(time.Since(t0)))
		c.start(leader) // bring it back so the cluster has N nodes again
		time.Sleep(500 * time.Millisecond)
	}
	return out
}

func machine(dir string) map[string]string {
	m := map[string]string{
		"go": runtime.Version(), "os": runtime.GOOS + "/" + runtime.GOARCH,
		"cpus": fmt.Sprint(runtime.NumCPU()), "dataDir": dir,
	}
	if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "model name") {
				m["cpu"] = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
				break
			}
		}
	}
	if out, err := exec.Command("uname", "-r").Output(); err == nil {
		m["kernel"] = strings.TrimSpace(string(out))
	}
	if out, err := exec.Command("df", "-T", dir).Output(); err == nil {
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		if f := strings.Fields(lines[len(lines)-1]); len(f) > 1 {
			m["filesystem"] = f[0] + " (" + f[1] + ")"
		}
	}
	return m
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func round(xs []float64) []int {
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x + 0.5)
	}
	return out
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
