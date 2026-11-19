package server

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"raftlab/client"
	"raftlab/raft"
)

func freeAddrs(t *testing.T, n int) map[raft.NodeID]string {
	t.Helper()
	out := map[raft.NodeID]string{}
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		out[raft.NodeID(i)] = ln.Addr().String()
		ln.Close()
	}
	return out
}

type testCluster struct {
	t       *testing.T
	addrs   map[raft.NodeID]string
	dir     string
	servers map[raft.NodeID]*Server
}

func startCluster(t *testing.T, n int) *testCluster {
	tc := &testCluster{t: t, addrs: freeAddrs(t, n), dir: t.TempDir(), servers: map[raft.NodeID]*Server{}}
	for id := range tc.addrs {
		tc.start(id)
	}
	t.Cleanup(func() {
		for _, s := range tc.servers {
			s.Stop()
		}
	})
	return tc
}

func (tc *testCluster) start(id raft.NodeID) {
	s, err := Start(Config{ID: id, Addrs: tc.addrs, DataDir: filepath.Join(tc.dir, fmt.Sprint(id))})
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.servers[id] = s
}

func (tc *testCluster) stop(id raft.NodeID) {
	tc.servers[id].Stop()
	delete(tc.servers, id)
}

func (tc *testCluster) leader() raft.NodeID {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, s := range tc.servers {
			if s.Status().State == raft.Leader {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatal("no leader elected")
	return 0
}

func (tc *testCluster) client() *client.Client {
	var addrs []string
	for i := 1; i <= len(tc.addrs); i++ {
		addrs = append(addrs, tc.addrs[raft.NodeID(i)])
	}
	return client.New(addrs)
}

// End to end over real sockets and real disks: writes survive killing the
// leader, and a restarted node catches up.
func TestClusterSurvivesLeaderKill(t *testing.T) {
	tc := startCluster(t, 3)
	c := tc.client()
	if err := c.Put("k", "a"); err != nil {
		t.Fatal(err)
	}
	if err := c.Append("k", "b"); err != nil {
		t.Fatal(err)
	}
	old := tc.leader()
	tc.stop(old)
	if err := c.Append("k", "c"); err != nil {
		t.Fatalf("write after leader kill: %v", err)
	}
	if v, err := c.Get("k"); err != nil || v != "abc" {
		t.Fatalf("get = %q, %v", v, err)
	}
	tc.start(old)
	deadline := time.Now().Add(5 * time.Second)
	for tc.servers[old].Status().Applied < tc.servers[tc.leader()].Status().Commit {
		if time.Now().After(deadline) {
			t.Fatal("restarted node did not catch up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConcurrentClients(t *testing.T) {
	tc := startCluster(t, 3)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := tc.client()
			for j := range 20 {
				if err := c.Append("log", fmt.Sprintf("%d.%d;", i, j)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, err := tc.client().Get("log")
	if err != nil {
		t.Fatal(err)
	}
	if n := countSemicolons(v); n != 160 {
		t.Fatalf("log has %d appends, want 160", n)
	}
}

func countSemicolons(s string) int {
	n := 0
	for _, r := range s {
		if r == ';' {
			n++
		}
	}
	return n
}

func TestParseAddrs(t *testing.T) {
	m, err := ParseAddrs("1=127.0.0.1:7001, 2=127.0.0.1:7002")
	if err != nil || m[2] != "127.0.0.1:7002" || len(m) != 2 {
		t.Fatalf("%v %v", m, err)
	}
	if _, err := ParseAddrs("x"); err == nil {
		t.Fatal("accepted garbage")
	}
}
