#!/usr/bin/env bash
# Start a local 3-node raftlab cluster (Ctrl+C stops it).
#   ./scripts/cluster.sh
#   ./bin/raftlab put hello world && ./bin/raftlab get hello && ./bin/raftlab status
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
PEERS=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
mkdir -p data/cluster
pids=()
for i in 1 2 3; do
  ./bin/raftlab-server -id "$i" -peers "$PEERS" -data "data/cluster/$i" >"data/cluster/$i.log" 2>&1 &
  pids+=($!)
done
trap 'kill "${pids[@]}" 2>/dev/null; wait' INT TERM EXIT
echo "cluster running; logs in data/cluster/*.log; node pids: ${pids[*]}"
echo "kill one with 'kill <pid>' and keep using the CLI to watch failover"
wait
