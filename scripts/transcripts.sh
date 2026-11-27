#!/usr/bin/env bash
# Write a debugger session and a correct-vs-mutated diff for every shrunk
# bug-zoo failure in harness/testdata/schedules into results/transcripts/.
set -euo pipefail
cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/...
mkdir -p results/transcripts
for f in harness/testdata/schedules/*.json; do
  name=$(basename "$f" .json)
  out=results/transcripts/$name.txt
  {
    echo "# raftlab-debug session on $f"
    echo
    ./bin/raftlab-debug -repro "$f" \
      -c "faults; break violation; continue; nodes; back 20; events 12; inflight; run-to violation; finish"
    echo
    echo "# first divergence: same options with correct code (A) vs with $name (B)"
    echo
    ./bin/raftlab-debug -repro "$f" -mutation none -diff "$name" -context 6 2>&1 || true
  } > "$out"
  echo "wrote $out"
done
