# The replay debugger

Every simulated run is a pure function of its seed (and, optionally, a saved fault schedule), so a
failure can be replayed, stepped through, rewound, shrunk and compared with a correct run. This
page lists the tools and then walks through one real bug from the bug zoo: Figure 8.

## Tools

| What | Command |
|---|---|
| Structured trace, one JSON event per line (`trace_version: 1`) | `./bin/raftlab-chaos -seed N -trace run.jsonl` |
| Interactive debugger | `./bin/raftlab-debug -seed N [-mutation M]` (type `help`) |
| Scripted debugger session | `./bin/raftlab-debug -seed N -c "break violation; continue; nodes"` |
| Shrink a failing run to a minimal repro file | `./bin/raftlab-chaos -seed N -mutation M -shrink -save r.json` |
| Debug a repro file | `./bin/raftlab-debug -repro r.json` |
| First divergence between two runs of the same seed | `./bin/raftlab-debug -seed N -diff M` (correct vs mutated) |
| ShiViz log (vector clocks) | `./bin/raftlab-debug -seed N -shiviz run.log` |
| Porcupine history view with faults marked | `./bin/raftlab-debug -seed N -viz history.html` |

Debugger commands: `step [N]`, `back [N]`, `goto K`, `run-to COND`, `break COND`, `continue`,
`nodes`, `node N` (with its log), `inflight`, `events [N]`, `faults`, `finish`, `shiviz FILE`,
`viz FILE`. Conditions: `violation`, `elected`, `leader N`, `down N`, `up N`, `term OP K`,
`commit N OP K`, `applied N OP K`, `kind NAME`, `time OP DURATION`, `step OP K`, joined with `and`.

How it works, briefly:

- **Trace.** The simulator, network, harness and clients emit typed events (`sim.Event`). The
  trace hash is computed from a canonical binary form, so observing a run never changes it
  (`TestTrace_SinkDoesNotChangeExecution`). Overhead over 500 seeds: 2% with a counting sink, 1.95x
  with a JSON Lines sink (`results/trace-overhead.json`).
- **Going back** re-runs the deterministic prefix instead of keeping checkpoints. Over 200 seeds,
  `back 1` at 90% of a run takes 9.2 ms at the median and 24.8 ms at p99
  (`results/debug-back.json`). `TestDebug_BackMatchesFreshRun` checks that going back lands on
  exactly the state a fresh run reaches.
- **Fault schedule as data.** Every run records the faults it injected, resolved to nodes and
  durations (`Result.Schedule`). Replaying a schedule gives the identical trace
  (`TestSchedule_RecordThenReplayIsIdentical`), which is what makes shrinking possible.
- **Shrinking** is delta debugging over the schedule, then fewer nodes and clients, no message
  loss or duplication, and shorter phases, while the *same property* still fails
  (`results/shrink.json`). Message delays still come from the seed, so a shrunk failure is a seed
  plus a shorter fault list, not a script free of randomness.
- **ShiViz** vector clocks are computed from the trace's send and deliver events; paste the log
  and the regex `(?<host>\S*) (?<clock>{.*}) (?<event>.*)` into ShiViz. The format follows the
  ShiViz paper and examples; it has not yet been tried in the live ShiViz web app.

Session transcripts for all eight bug-zoo mutations, each with a correct-vs-mutated diff, are in
[`results/transcripts/`](../results/transcripts/) (regenerate with `./scripts/transcripts.sh`).

## Walkthrough: Figure 8 (`commit-prev-term`)

The mutation removes the term check from the leader's commit rule (Figure 2, Leaders rule 4): a
leader may commit an entry from an earlier term just because a majority stores it. The bug zoo
first catches it at seed 151 (7 of 3000 seeds). Shrinking cut the schedule from 28 faults to 18;
the file is `harness/testdata/schedules/commit-prev-term.json`. All output below is copied from
`results/transcripts/commit-prev-term.txt` and from the commands shown.

### 1. Run to the violation

```
$ ./bin/raftlab-debug -repro harness/testdata/schedules/commit-prev-term.json
(debug) break violation
(debug) continue
breakpoint: violation
  #3556 3.945764369s role n2 term=13 vote=2 lead=2 commit=25 last=30 Leader
  #3556 3.945764369s violation LeaderCompleteness: new leader 2 (term 13) lacks applied entry 29 (term 2)
(debug) nodes
n1 Follower  term=13 vote=0 lead=0 commit=31 applied=31 log=[snap 30 .. 68] lastTerm=12
n2 Leader    term=13 vote=2 lead=2 commit=25 applied=25 log=[snap 20 .. 30] lastTerm=13
n3 Follower  term=13 vote=2 lead=0 commit=29 applied=29 log=[snap 20 .. 33] lastTerm=2
```

n2 became leader of term 13 with n3's vote, but entry 29 (from term 2) was already applied by
other nodes, and n2 does not have it. A committed entry is missing from a new leader: Leader
Completeness is broken.

### 2. Find where entry 29 was committed

```
(debug) goto 0
(debug) run-to applied 1 >= 29 and step > 2000
  #3515 3.915783184s deliver n1 peer=n3 msg=860 MsgAppResp t12 i29 n0 c0 rfalse
  #3515 3.915783184s send n1 peer=n3 msg=862 delay=5592814 MsgApp t12 i29 n1 c29 rfalse
  #3515 3.915783184s apply n1 index=28 term=2
  #3515 3.915783184s apply n1 index=29 term=2
(debug) nodes
n1 Leader    term=12 vote=1 lead=1 commit=29 applied=29 log=[snap 20 .. 67] lastTerm=12
n2 Follower  term=12 vote=1 lead=0 commit=25 applied=25 log=[snap 20 .. 29] lastTerm=8
n3 Follower  term=12 vote=1 lead=1 commit=27 applied=27 log=[snap 20 .. 29] lastTerm=2
```

At step 3515, n1 is leader of **term 12**. n3's acknowledgement for index 29 arrives, so n1 and
n3 (two of three) store entry 29, and the buggy rule commits it, even though entry 29 is from
**term 2** and none of n1's term-12 entries are on a majority. That is the unsafe step.

The danger is visible in the same `nodes` output: n2 has a *different* entry at index 29, from
term 8, and term 8 is newer than term 2.

### 3. Watch the election that loses it

Thirty milliseconds later (step 3556), n2 asks for votes in term 13. n3's log ends with the term-2
entry, n2's with the term-8 entry, so by the up-to-date rule (§5.4.1) n3 must vote for n2. n2 wins
without entry 29, and would overwrite it. With the correct rule, n1 could not have committed
entry 29 until an entry of its own term 12 was stored on a majority, and that entry would have
made n3's log at least as up-to-date as n2's, so n2 could not have won.

### 4. Compare with correct code

```
$ ./bin/raftlab-debug -repro harness/testdata/schedules/commit-prev-term.json -mutation none -diff commit-prev-term
first difference at event 1095 (step 704); 7816 and 4983 events in total
A (mutations=none):
  > #704 726.248589ms send n1 peer=n2 msg=226 delay=6221609 MsgApp t2 i10 n1 c9 rfalse
B (mutations=commit-prev-term):
  > #704 726.248589ms send n1 peer=n2 msg=226 delay=6221609 MsgApp t2 i10 n1 c10 rfalse
  > #704 726.248589ms apply n1 index=10 term=1
```

The two runs are identical for 703 steps. The first time the buggy rule makes a difference, a
term-2 leader commits a **term-1** entry (index 10) by counting replicas: the correct leader still
advertises commit index 9 (`c9`), the mutated one advertises 10 and applies it. That early commit
happens to be harmless; the one at step 3515 is not. The diff shows where a bug first changes
behaviour, which is often well before it breaks a property.

### 5. Look at it another way

`-viz history.html` writes Porcupine's view of the client history with the crashes, pauses and
partitions drawn on one row per node, and `-shiviz run.log` writes the message flow with vector
clocks for ShiViz.
