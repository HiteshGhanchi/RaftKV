#!/usr/bin/env bash
# Model-check spec/Raft.tla with TLC.
#
#   ./scripts/tlc.sh                       # exhaustive check, bounds from spec/MCRaft.cfg
#   ./scripts/tlc.sh check 3 1 1           # exhaustive with MaxTerm MaxLogLen MaxMsgs
#   ./scripts/tlc.sh simulate 5 3 3 600    # random behaviours for 600 s at larger bounds
#   ./scripts/tlc.sh bug 4 1 1             # exhaustive with the Figure 8 bug switched on
#   ./scripts/tlc.sh bugsim 5 1 2 600      # simulate with the Figure 8 bug switched on
#
# Needs Java 11+ and tools/tla2tools.jar (download it from the TLA+ GitHub
# releases page). Working files go to spec/states and are removed afterwards;
# each file TLC writes is capped (TLC_FILE_LIMIT_KB, default 40 MB) so a large model cannot fill the disk.
set -euo pipefail
cd "$(dirname "$0")/../spec"
JAR=../tools/tla2tools.jar
[ -f "$JAR" ] || { echo "missing $JAR (see the comment at the top of this script)"; exit 1; }

mode=${1:-default}
cfg=MCRaft.cfg
if [ "$mode" != default ]; then
  term=${2:-2}; len=${3:-1}; msgs=${4:-1}
  bug=FALSE; { [ "$mode" = bug ] || [ "$mode" = bugsim ]; } && bug=TRUE
  cfg=run.cfg
  sed -e "s/MaxTerm = .*/MaxTerm = $term/" -e "s/MaxLogLen = .*/MaxLogLen = $len/" \
      -e "s/MaxMsgs = .*/MaxMsgs = $msgs/" -e "s/CommitPrevTermBug = .*/CommitPrevTermBug = $bug/" \
      MCRaft.cfg > "$cfg"
  echo "MaxTerm=$term MaxLogLen=$len MaxMsgs=$msgs CommitPrevTermBug=$bug"
fi

extra=()
if [ "$mode" = simulate ] || [ "$mode" = bugsim ]; then
  secs=${5:-300}
  extra=(-simulate -depth 60)
  timeout_cmd=(timeout "$secs")
else
  timeout_cmd=()
fi

trap 'rm -rf states "$PWD/run.cfg"' EXIT
ulimit -f "${TLC_FILE_LIMIT_KB:-40960}"
"${timeout_cmd[@]}" java -Xmx1700m -XX:+UseParallelGC -Djava.io.tmpdir="$PWD/states" \
  -jar "$JAR" -workers auto -checkpoint 0 -metadir "$PWD/states" "${extra[@]}" \
  -config "$cfg" MCRaft.tla || true
