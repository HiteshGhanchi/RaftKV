#!/usr/bin/env bash
# Run the benchmark matrix described in docs/BENCHMARKS.md. Each run gets a
# fresh directory under data/bench on the project's disk (not /tmp, which may
# be RAM-backed). Delete data/bench yourself when you no longer need it.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
mkdir -p results data/bench
stamp=$(date +%Y%m%d-%H%M%S)
run() {
  name=$1; shift
  ./bin/raftlab-bench -dir "data/bench/$stamp-$name" -out "results/bench-$name.json" "$@"
}
run 3n-32c         -nodes 3 -clients 32 -duration 10s
run 5n-32c         -nodes 5 -clients 32 -duration 10s
run 3n-1c          -nodes 3 -clients 1  -duration 10s
run 3n-32c-nobatch -nodes 3 -clients 32 -duration 10s -batch 1
run 3n-32c-nosync  -nodes 3 -clients 32 -duration 10s -nosync
run 3n-failover    -nodes 3 -failover 20
