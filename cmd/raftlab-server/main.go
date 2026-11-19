// Command raftlab-server runs one node of a raftlab cluster.
//
//	raftlab-server -id 1 -peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 -data data/1
package main

import (
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"raftlab/server"
)

func main() {
	id := flag.Uint64("id", 0, "this node's ID (must appear in -peers)")
	peers := flag.String("peers", "", "every member as id=host:port, comma separated")
	data := flag.String("data", "", "data directory (default data/<id>)")
	snap := flag.Uint64("snapshot-every", 10000, "compact the log after this many applied entries")
	flag.Parse()

	addrs, err := server.ParseAddrs(*peers)
	if err != nil || *id == 0 || addrs[*id] == "" {
		slog.Error("need -id and a -peers list that contains it", "err", err)
		os.Exit(2)
	}
	if *data == "" {
		*data = "data/" + flagValue("id")
	}
	s, err := server.Start(server.Config{ID: *id, Addrs: addrs, DataDir: *data, SnapshotEvery: *snap})
	if err != nil {
		slog.Error("start failed", "err", err)
		os.Exit(1)
	}
	slog.Info("raftlab node started", "id", *id, "addr", addrs[*id], "data", *data)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	s.Stop()
	slog.Info("stopped", "id", *id)
}

func flagValue(name string) string { return flag.Lookup(name).Value.String() }
