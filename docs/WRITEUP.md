# raftlab: testing Raft by trying to break it

*One-page summary and blog-post draft. Every number links to a file in `results/`.*

## The problem

Raft is famous for being understandable, and many people implement it. Far fewer can show their
implementation is *correct*: consensus bugs hide in rare timings, such as a vote that arrives just
after a crash, or an AppendEntries from a leader that no longer exists. Running a cluster and
waiting for those timings rarely finds them, and when it does, you can't reproduce what happened.

## The approach

raftlab makes timing an input. The Raft core is a pure state machine with no clock, no goroutines
and no I/O, driven by a discrete-event simulator. One seed decides everything: the cluster size,
every message delay and loss, every crash point, how much of an unsynced write survives, every
partition and pause, every client retry. A run covering 6 virtual seconds costs under 0.1 s of
CPU (10,000 runs took 53 s on 16 threads), and a failing seed replays bit for bit.

While a run is going, the harness checks Raft's five safety properties continuously: on every
applied entry and every election, and Log Matching every 50 ms of virtual time. When it ends, it checks the client history for linearizability with Porcupine.

To show the harness isn't just passing everything, there is a **bug zoo**: eight switches that each
break one Raft rule (skip the up-to-date check, forget the vote, commit old-term entries by
counting, serve reads without confirming leadership, skip fsync, and so on).

## Results

- **8/8 injected bugs caught** over 3000 seeds each; median 9.5 seeds to first detection
  (`results/bugzoo.json`). Two are genuinely rare: Figure 8 (5/3000 seeds) and the double vote
  after forgetting `votedFor` (2/3000). A few hundred seeds would have missed them.
- **10,000 chaos runs of correct code**: 0 failures and 1,593,004 client operations, all
  linearizable (`results/chaos-10000.json`).
- **Real cluster** (3 nodes in one process on a laptop, real fsync, loopback HTTP): 5,454
  writes/s at p50 5.6 ms / p99 12.1 ms with 32 clients. Batching is worth 8.2x and fsync costs
  5.1x. Failover takes 163 ms at the median (`results/bench-*.json`).
- **TLA+**: with the commit rule's term check removed, TLC finds the Figure 8 counterexample
  (a 39-state trace, after 39 million distinct states). With the correct rule, exhaustive checks
  pass at small bounds, including the Figure 8 bounds themselves (114.5 million distinct states,
  55 minutes on a laptop; `results/tlc.md`).

## What I learned

1. **The first bug the harness found was in the harness.** My Leader Completeness check flagged
   leaders of *older* terms, which Raft allows. Seven of 500 correct seeds "failed". Writing an
   invariant checker means re-reading the paper's exact wording.
2. **A harness that catches the easy bugs can still miss the important ones.** At first, four of
   the eight mutations were nearly invisible. Aiming faults at the leader, adding delay spikes,
   letting some seeds run without CheckQuorum and adding jitter when a paused process wakes made
   all eight detectable. This kind of tuning is the real work of fault injection.
3. **Determinism is a design constraint, not a test feature.** It decided the shape of the Raft
   core (`Step`/`Ready`), the storage interface and the client model, long before the first test ran.

## Resume bullets (real numbers only)

- Built a replicated, linearizable key-value store on a from-scratch Raft in Go (election,
  pipelined replication, ReadIndex reads, snapshots, client sessions, checksummed WAL with
  torn-write recovery).
- Built a deterministic simulation harness (seeded clock, network, disk); verified linearizability
  across 10,000 fault-injection runs (1.59M operations), with every failure reproducible from one seed.
- Injected 8 known Raft bugs; the harness caught 8/8 (median 9.5 seeds to detection). Kept each
  failing seed as a regression test.
- Specified election and replication in TLA+ and model-checked it with TLC; measured 5.4k writes/s
  and 163 ms median failover on a 3-node cluster.
