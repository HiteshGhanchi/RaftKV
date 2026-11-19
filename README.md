# raftlab

A replicated, linearizable key-value store in Go, built on a from-scratch implementation of the
Raft consensus protocol, plus a **deterministic simulation harness** that tries hard to break it.

The harness is the main point. Every run is driven by one seed, so any failure replays exactly.
Every run checks the Raft safety properties while it runs (on every applied entry and election,
and log comparisons every 50 ms of virtual time) and checks the client history for linearizability with [Porcupine](https://github.com/anishathalye/porcupine). A **bug zoo** of eight
deliberately injectable Raft bugs shows the harness really catches mistakes, and a small TLA+
spec states the same properties formally.

```
8/8 injected bugs caught over 3000 seeds each (median 9.5 seeds to first detection)
10,000 correct-code chaos runs, 1,593,004 client operations, all linearizable, 0 failures
3 nodes on one laptop, real fsync, 32 clients: 5,454 writes/s, p50 5.6 ms, p99 12.1 ms
```

Every number on this page comes from a file in [`results/`](results/) produced by a command in
this repository. The caveats are listed next to the numbers.

---

## Who this is for, and why it is useful

| Reader | What they get from it |
|---|---|
| **Students learning distributed systems** | A Raft you can read in one sitting (`raft/` is about 1,000 lines including comments), with every rule linked to Figure 2 of the paper, a [build guide](docs/BUILD_GUIDE.md) that explains the order an engineer would write the files in and why, and [teaching notes](lessons/teaching.md) with self-check questions. |
| **Engineers who build or review consensus code** | A worked example of *how to test* a consensus implementation: a seeded simulator for time, network and disk, named invariants, Porcupine histories, mutation testing and a formal spec. The `sim/` and `check/` packages know nothing about Raft and can be reused. |
| **Interviewers and reviewers** | Claims that can be checked. Every result has a reproduction command, and the "bugs the harness found" section is a real log, including the bugs in the harness itself. |
| **People building deterministic-simulation tools** | A small, complete case study of the approach used by FoundationDB and TigerBeetle, applied to a different system. |

**Why it matters.** Consensus protocols run underneath etcd, Consul, CockroachDB and many other
systems. Most Raft implementations are tested by running them and hoping that real timing hits the
rare interleavings. Here the timing *is* the input: a seed picks the cluster shape, every message
delay, every crash point and every torn write. Thousands of runs take about a minute, and any
failure comes with a one-line replay command.

**What it is not.** It is not a production database and it doesn't compete with `etcd-io/raft`,
`hashicorp/raft` or `dragonboat`. There is no TLS, no membership change and no multi-group
sharding (see [Limitations](#limitations)).

---

## Architecture

```
 client lib ──HTTP──► server (one goroutine owns the peer) ──HTTP transport──► other servers
                         │
                         ▼
                      peer.Peer        Ready loop: persist → send → apply → advance
                     ┌───┼──────────────┐
                     ▼   ▼              ▼
              raft.Node  storage (WAL)  kv.Store
          (pure state     + snapshots   (sessions: retries take
           machine, no    on a vfs.FS    effect exactly once)
           I/O, no clock)

 In the simulator, the same peer.Peer runs with:
   sim.World (virtual clock, event queue, one seeded RNG)
   sim.Network (delay, loss, duplication, reordering, partitions)
   sim.Disk (loses or tears unsynced writes on crash)
   harness (faults, retrying clients, invariant checks)  →  check (Porcupine)
```

| Package | Role |
|---|---|
| [`raft/`](raft/) | The protocol as a pure state machine: `Tick`, `Step`, `Propose`, `ReadIndex` in; a `Ready` (state to persist, messages, committed entries) out. Leader election, replication with pipelining, the Figure 8 commit rule, ReadIndex reads, CheckQuorum, snapshots. |
| [`storage/`](storage/) | Write-ahead log with CRC32C per record and torn-tail repair, atomically replaced snapshots. |
| [`kv/`](kv/) | The state machine: put / append / get, plus client sessions for deduplication. |
| [`peer/`](peer/) | Glues node + storage + store; runs the Ready loop in the order Raft requires. |
| [`sim/`](sim/) | Deterministic discrete-event simulator: clock, network and disk models. Knows nothing about Raft. |
| [`check/`](check/) | History recorder and Porcupine model for a KV store. Knows nothing about Raft. |
| [`harness/`](harness/) | A whole cluster in the simulator: fault schedule, retrying clients, invariant checks, sweeps. |
| [`mutation/`](mutation/) | The bug zoo switches. |
| [`server/`](server/), [`transport/`](transport/), [`client/`](client/) | The real networked system over HTTP. |
| [`spec/`](spec/) | TLA+ spec of election and replication, with the same property names as the Go invariants. |
| [`cmd/`](cmd/) | `raftlab-server`, `raftlab` (CLI), `raftlab-chaos`, `raftlab-bench`. |

---

## Quick start

Requires Go 1.22 or newer. Java 11+ and `tools/tla2tools.jar` are only needed for the TLA+ checks.

```bash
make test                     # all unit, scenario, chaos and end-to-end tests
```

```bash
make chaos SEEDS=1000         # 1000 seeds of the fault-injection harness, in parallel
```

```bash
./bin/raftlab-chaos -seed 18 -mutation stale-read -v   # replay one failing seed with its full trace
```

```bash
./scripts/cluster.sh          # start 3 real nodes on 127.0.0.1:7001-7003
```

Then, in another terminal:

```bash
./bin/raftlab put city Pune && ./bin/raftlab append city ,India && ./bin/raftlab get city
```

---

## How correctness is checked

1. **Unit tests per Figure 2 rule** (`raft/raft_test.go`): one-vote-per-term, the up-to-date check
   (§5.4.1), AppendEntries receiver rules 1–5, stale terms, step-down on higher terms, ReadIndex
   needing a quorum, snapshots to lagging followers.
2. **Paper scenarios**: all six follower logs from Figure 7 converge to the leader's log. The
   Figure 8 hazard: the leader does not commit an old-term entry just by counting replicas (and
   does with the mutation enabled).
3. **Deterministic chaos** (`harness/`): each seed picks 3 or 5 nodes, the message batch size,
   the snapshot interval, the loss rate, whether CheckQuorum is on and the fault rate. Then it
   injects crashes (unsynced disk writes are lost or torn), symmetric and one-way partitions,
   process pauses, delay spikes and per-node clock skew. Faults aim at the leader half the time.
   Clients time out and retry with the same sequence number.
4. **Invariants checked while running**: Election Safety, Leader Append-Only, Log Matching,
   Leader Completeness, State Machine Safety, and a node-local "a committed entry is never
   truncated" assertion. The first violation stops the run and is reported with its name.
5. **Linearizability**: every completed client operation goes into a history that Porcupine checks
   (partitioned by key). A checker timeout counts as *inconclusive*, never as a pass. Across the
   10,000-seed sweep there were 0 inconclusive results.
6. **Bug zoo**: 8 mutations, each switching off one rule. For each, the first failing seed is kept
   as a regression test (`harness/harness_test.go`).
7. **TLA+**: `spec/Raft.tla` checked with TLC (details and limits below).
8. **Race detector** on the real server, transport and client (`make race`).

Mapping from paper rules to code and from spec properties to Go assertions:
[`docs/PROTOCOL.md`](docs/PROTOCOL.md).

---

## Results

### Bug zoo (`./bin/raftlab-chaos -bugzoo -seeds 3000`, [`results/bugzoo.json`](results/bugzoo.json))

| Mutation | Rule it breaks | Caught in | First failing seed | First failure |
|---|---|---|---|---|
| `skip-up-to-date-check` | RequestVote: candidate log must be up-to-date (§5.4.1) | 2600/3000 | 1 | LeaderCompleteness |
| `apply-before-commit` | Apply only up to commitIndex | 2344/3000 | 1 | StateMachineSafety |
| `no-dedup` | Retries must take effect once (thesis §6.3) | 2394/3000 | 1 | history not linearizable |
| `skip-fsync` | Persist before responding | 883/3000 | 1 | LeaderCompleteness |
| `stale-read` | Leader must confirm leadership before a read (thesis §6.4) | 52/3000 | 18 | history not linearizable |
| `accept-stale-term` | AppendEntries rule 1: reject older terms | 47/3000 | 27 | StateMachineSafety |
| `commit-prev-term` | Leaders rule 4: commit only current-term entries by counting (Figure 8) | 5/3000 | 262 | LeaderCompleteness |
| `forget-vote` | votedFor is persistent state | 2/3000 | 1915 | ElectionSafety |

**8/8 caught; median seeds to first detection = 9.5.** The last two are rare on purpose: they need
a specific schedule (Figure 8 needs about five elections in a particular order, and a double vote
needs a crash between granting a vote and a second candidate asking). The detection rate is the
honest measure, and it says that a few hundred seeds are not enough for these bugs.

### Chaos sweep (`./bin/raftlab-chaos -seeds 10000`, [`results/chaos-10000.json`](results/chaos-10000.json))

10,000 runs (4 s of faults plus 2 s of recovery each, virtual time), 0 failures, 0 inconclusive,
1,593,004 client operations checked. It took 53 s of wall time on 16 threads.

### Benchmarks (`./scripts/bench.sh`, `results/bench-*.json`)

All nodes run in one process on one laptop (Intel i5-12500H, ext4 on NVMe, Linux 6.5, Go 1.22.2),
talking over loopback HTTP, each with its own WAL with fsync. 100-byte values, every operation a
write. This is a single-machine measurement: it shows relative costs (batching, fsync, cluster
size), not what a real multi-machine cluster would do. Method: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md).

| Setup (10 s each) | Writes/s | p50 | p99 |
|---|---|---|---|
| 3 nodes, 32 clients | 5,454 | 5.55 ms | 12.06 ms |
| 5 nodes, 32 clients | 4,163 | 5.57 ms | 24.39 ms |
| 3 nodes, 1 client | 473 | 2.08 ms | 2.90 ms |
| 3 nodes, 32 clients, **no request batching** | 667 | 34.06 ms | 163.54 ms |
| 3 nodes, 32 clients, **no fsync** (unsafe, for comparison) | 27,998 | 0.98 ms | 2.60 ms |

**Failover** (leader killed 20 times, time until the next write is acknowledged): p50 163 ms,
max 733 ms. The election timeout is 100–200 ms.

### TLA+ ([`results/tlc.md`](results/tlc.md))

- **TLC finds Figure 8.** With the commit rule's term check switched off, exhaustive TLC reports a
  LeaderCompleteness violation at 3 servers, terms ≤ 4, logs ≤ 1, 1 message in flight: a 39-state
  trace found after 39,347,865 distinct states (10 min). Trace in
  [`results/tlc-figure8-trace.txt`](results/tlc-figure8-trace.txt).
- With the correct rule, exhaustive TLC **completed with no errors** at terms ≤ 3 / logs ≤ 1
  (6,518,959 distinct states) and terms ≤ 2 / logs ≤ 2 (1,650,315).
- At the bounds where the bug was found (terms ≤ 4), the correct spec showed no violation in
  87,461,124 distinct states before a 45-minute limit. That run did not finish, so it is not a
  complete check.
- TLC simulation at terms ≤ 4, logs ≤ 3, 3 messages: no violation in 1,077,780 random behaviours.

---

## Bugs the harness found in my own code

A log of real findings, including ones in the test tooling itself.

1. **The Leader Completeness check was wrong (harness bug).** The first 500-seed sweep of
   *correct* code reported 7 failures, all "new leader lacks an applied entry". Every one was a
   leader of an *older* term elected late, which Raft allows: Leader Completeness only covers
   leaders of terms after the one the entry was committed in. The fix records an upper bound on
   each entry's commit term (a node only applies an entry after hearing of the commit from a leader
   whose term is at most its own). After that, 10,000 seeds pass. (The faulty check was fixed
   before the harness was first committed; commit `d757ad9` describes it.)
2. **The first fault model was too gentle (harness gap).** Four of the eight mutations were caught
   0–4 times in 300 seeds. The causes were CheckQuorum making deposed leaders step down quickly,
   messages never long-delayed, and paused nodes replaying their input in a fixed order. The fixes
   were faults aimed at the leader, delay spikes, CheckQuorum turned off on some seeds, jitter when
   a paused node wakes, per-seed message batch sizes, and vote-request retries in the candidate
   (Figure 2 assumes lost RPCs are retried). After them every mutation was caught at least twice in
   3000 seeds.
3. **Snapshot install could drop log entries from disk (found in review, before the harness ran).**
   When a follower installed a snapshot and kept the entries after it, those entries were not
   handed back for the log-file rewrite, so a crash would have lost them. Now every retained entry
   is marked unstable after a restore.
4. **A snapshot path called `Ready()` for its side-effect-free hard state (found in review).**
   `Ready()` also flushes pending messages, so calling it from the snapshot code would have
   dropped them. It now uses a dedicated `HardState()` accessor.
5. **The TLA+ spec had two evaluation errors (found by TLC).** Inside an action, TLC treats each
   `\/` branch as a separate step and evaluates it even when an earlier branch is true, so two
   branches indexed an empty sequence. Each branch now guards itself.

---

## Limitations

- **No membership change.** The cluster is fixed at start (see `docs/DECISIONS.md`).
- **Session table never expires** (thesis §6.3 describes expiry); fine for tests and benchmarks.
- **Slow disks are not modelled** in the simulator (crashes, lost and torn writes are).
- **Benchmarks are single-machine, loopback** numbers.
- **The TLA+ model is small** and covers election and replication only (no snapshots, no
  sessions). TLC checks the spec, not the Go code: the link between them is the shared property
  names and the mapping table.
- **The real server uses HTTP + gob**, chosen for zero dependencies over performance.

## Prior art

These are the projects this one learns from, described here without comparing against their
current state (to be verified before any comparison is published): `etcd-io/raft` (whose
`Step`/`Ready` design `raft/` follows), `hashicorp/raft`, `dragonboat`, the MIT 6.5840 labs,
Jepsen, Porcupine, FoundationDB's simulation testing, TigerBeetle's VOPR, MadRaft, and Diego
Ongaro's `raft.tla`.

## Documentation

| Document | What it is |
|---|---|
| [`docs/BUILD_GUIDE.md`](docs/BUILD_GUIDE.md) | How an engineer would build this from an empty folder: what to think first, which file to write next and why, with a checkpoint and interview questions at each step |
| [`docs/DECISIONS.md`](docs/DECISIONS.md) | Every planning question (deadline, environment, scope, design trade-offs) with the answer or decision and who made it |
| [`docs/PROTOCOL.md`](docs/PROTOCOL.md) | Figure 2 rule → code, and spec property → Go assertion |
| [`docs/BUGZOO.md`](docs/BUGZOO.md) | Each mutation: what it breaks, why it is caught, how to replay it |
| [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) | Benchmark method, raw results and caveats |
| [`docs/WRITEUP.md`](docs/WRITEUP.md) | One-page summary / blog draft |
| [`lessons/teaching.md`](lessons/teaching.md) | Distributed-systems notes for someone new to the topic |

## Licence

MIT, see [`LICENSE`](LICENSE).
