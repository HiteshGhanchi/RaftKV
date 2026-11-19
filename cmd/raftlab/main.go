// Command raftlab is the command-line client.
//
//	raftlab put  <key> <value>
//	raftlab append <key> <value>
//	raftlab get  <key>
//	raftlab status
//
// The cluster comes from -cluster or $RAFTLAB_CLUSTER (comma-separated host:port).
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"raftlab/client"
)

func main() {
	cluster := flag.String("cluster", envOr("RAFTLAB_CLUSTER", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"), "comma-separated server addresses")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: raftlab [-cluster a,b,c] put|append <key> <value> | get <key> | status")
	}
	flag.Parse()
	addrs := strings.Split(*cluster, ",")
	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		os.Exit(2)
	}
	c := client.New(addrs)
	var err error
	switch {
	case args[0] == "get" && len(args) == 2:
		var v string
		if v, err = c.Get(args[1]); err == nil {
			fmt.Println(v)
		}
	case args[0] == "put" && len(args) == 3:
		err = c.Put(args[1], args[2])
	case args[0] == "append" && len(args) == 3:
		err = c.Append(args[1], args[2])
	case args[0] == "status":
		for _, a := range addrs {
			resp, e := http.Get("http://" + a + "/status")
			if e != nil {
				fmt.Printf("%s  unreachable\n", a)
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			fmt.Printf("%s  %s", a, body)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
