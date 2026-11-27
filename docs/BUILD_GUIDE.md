# Build guide: how an engineer builds raftlab from an empty folder

This is the order an experienced engineer would write the files in, and what they would be
thinking at each step. Read it top to bottom once, then use each step as a study unit: read the
files it names, run its checkpoint, and answer its interview questions aloud.

Every step has the same parts:

- **Think first**: the question the engineer asks before typing.
- **Files**: what gets created, and why each exists.
- **Key ideas**: the functions you must be able to explain.
- **Pitfalls**: the mistakes that step invites (several were made and fixed in this repository).
- **Checkpoint**: the command that must pass before moving on.
- **In five lines**, and **interview questions**.

---

## Step 0. Before any code: decide what "correct" means and how you will know

**Think first.** "I am going to write a consensus protocol. Bugs in consensus show up only under
rare timing: a message delayed past an election, a crash between two writes. If I can't *produce*
those timings on demand, and *replay* them when they fail, I will never know whether the code
works."

That one thought sets the whole architecture:

1. **Properties first.** Write down what must always hold, from Figure 3 of the Raft paper:
   Election Safety, Leader Append-Only, Log Matching, Leader Completeness, State Machine Safety.
   For the KV store, add linearizability and exactly-once effect for retried writes.
2. **Determinism first.** Time, network and disk become inputs controlled by one seed. That forces
   the protocol code to be a pure state machine (no goroutines, no clock, no sockets), because
   real goroutines and real time cannot be replayed.
3. **Test the tests.** A harness that never fails proves nothing, so plan the bug zoo from day one.

**Files**: `go.mod` (module `raftlab`, Go 1.22), `Makefile` (vet / test / race / chaos / bugzoo /
bench / tla), `.gitignore`.

**Interview questions**
1. Why can't you test a consensus protocol well by running it on real machines and waiting?
2. List Raft's five safety properties and say which one a client would notice first if it broke.
3. What does "deterministic simulation testing" buy you that randomised testing alone does not?

---

## Step 1. The virtual clock and event queue: `sim/world.go`

**Think first.** "Everything that happens is an event at a time. If one loop pops events in a
fixed order, and every random choice comes from one seeded generator, the run is a pure function
of the seed."

**Files**
- `sim/world.go`: `World` holds `now`, a min-heap of events ordered by `(deadline, sequence
  number)`, and one PCG random generator. `After(d, label, fn)` schedules, `Step` runs the next
  event, `RunUntil`/`RunUntilOr` run up to a time or until a stop condition.
- `sim/util.go`: `sortedKeys`, because Go randomises map iteration order and any loop over a map
  that affects behaviour must be sorted.

**Key ideas**
- *Tie-break by scheduling order*: two events at the same instant must run in a fixed order, so
  every event gets a sequence number.
- *Trace hash*: every executed event's `(time, label)` is fed into an FNV hash. Two runs with the
  same seed must give the same hash. That turns "is it deterministic?" into a one-line assertion.
- *Fork*: components get their own generator (`w.Fork()`), so adding one random call in the
  network does not shift every random choice in the fault schedule.

**Pitfalls**: calling `time.Now()`, the global `math/rand`, `go func()`, or ranging over a map
anywhere in simulated code. Any of them breaks replay silently.

**Checkpoint**: `go test ./sim -run 'SameSeed|Clock'`. The tests are `TestSim_SameSeedSameTrace`
(same seed → same hash, different seed → different hash) and
`TestClock_TimersFireInDeadlineOrder` (order, tie-breaks, cancel, and one virtual hour in well
under a second of wall time).

**In five lines**: The simulator is a priority queue of closures keyed by virtual time. One
goroutine pops them in order, so there is no real concurrency to make runs differ. All randomness
comes from one seeded PCG generator, forked per component. A hash of every executed event proves
two runs were identical. Virtual time is free: an hour of cluster time runs in about a second.

**Interview questions**
1. Why must events with equal deadlines carry a sequence number?
2. Name three sources of nondeterminism in a Go program and how this design removes each one.
3. How would you prove to a reviewer that your simulator is deterministic?

---

## Step 2. The network model: `sim/network.go`

**Think first.** "Raft must survive loss, duplication, reordering and partitions, including
one-way ones. The network should cause all of these, and it should not know what a Raft message
is, so the replay-debugger project can reuse it."

**Files**: `sim/network.go`, a generic `Network[M]`. `Send(from, to, m)` drops the message (by
partition or by loss rate), maybe duplicates it, and schedules delivery after a random delay.
Random delays are what reorder messages. Partitions are a set of blocked *directed* pairs, checked
both when a message is sent and when it arrives. `SetDown` drops messages to a crashed node,
including ones already in flight.

**Pitfalls**: checking the partition only at send time. A message sent just before a partition
would then still arrive, which is less realistic and hides bugs.

**Checkpoint**: `go test ./sim -run 'Net'`. The tests cover partitions blocking and healing,
one-way blocks, and messages to a crashed node being dropped.

**In five lines**: The network is generic over the message type and never looks inside. Delay is
uniform between a minimum and a maximum, which reorders messages for free. Loss and duplication
are coin flips from the network's own generator. Partitions are directional blocked pairs, so
asymmetric failures are one call. Crashed receivers drop everything, including messages in flight.

**Interview questions**
1. How does this network produce reordering without any special code for it?
2. Why model one-way partitions? Give a Raft failure only they can cause.
3. Which faults can't this model produce (hint: corrupted bytes, slow links), and does Raft need them?

---

## Step 3. The disk model: `vfs/vfs.go`, `sim/disk.go`

**Think first.** "Figure 2 says 'persist before responding'. The bugs live in what happens when
you don't, or when a crash lands mid-write. My fake disk must lose and tear writes that were never
fsynced, and keep everything that was."

**Files**
- `vfs/vfs.go`: the four operations the log needs (`OpenAppend`, `ReadFile`, `WriteFileAtomic`,
  `Truncate`) plus the real implementation `OS`. Its atomic write is temp file → fsync → rename →
  fsync of the directory.
- `sim/disk.go`: every file has `durable` and `pending` bytes. `Sync` moves pending to durable.
  `Crash` keeps a random prefix of pending (none = lost write, part = torn write) and invalidates
  old file handles.

**Pitfalls**: forgetting the directory fsync after a rename (on a real disk the rename itself can
be lost); letting a pre-crash file handle keep writing after the crash.

**Checkpoint**: `go test ./sim -run Disk`. Over 50 seeds the crash model must produce both a lost
write and a torn write, and must never lose synced bytes.

**In five lines**: The storage layer sees one small interface with a real and a simulated
implementation. The simulated disk separates bytes written from bytes fsynced. A crash keeps all
synced bytes and a random prefix of the rest, which models lost and torn writes. Atomic replace is
durable on return in both implementations. Old handles fail after a crash, as they would after a reboot.

**Interview questions**
1. What exactly does `fsync` guarantee, and what does `rename` guarantee without a directory fsync?
2. What is a torn write, and how does a log detect one on restart?
3. Why is the disk interface defined in its own package instead of inside `storage/`?

---

## Step 4. Protocol types and the Ready contract: `raft/types.go`

**Think first.** "Before any logic: what goes in and out of a node? Inputs are ticks, messages and
proposals. Outputs are state to persist, messages to send and entries to apply. The *order* of
those outputs is the contract."

**Files**: `raft/types.go`, with `Entry`, `Message` (one struct for every RPC and its reply; the
comment table says which fields matter for which type), `HardState` (term, vote, commit),
`Snapshot`, `Ready`, `Status`.

**Key idea, the Ready contract**: (1) persist snapshot, entries and hard state, then fsync;
(2) send messages; (3) apply the snapshot and committed entries; (4) call `Advance`. Step 2
after step 1 is "persist before responding": a vote or an append acknowledgement is a promise
about data already on disk.

**Interview questions**
1. Why must the hard state be persisted *before* the vote reply is sent? Describe the bug otherwise.
2. Why is `commitIndex` saved at all when Figure 2 lists it as volatile?
3. What does a single `Message` struct for all RPCs gain and cost?

---

## Step 5. The log: `raft/log.go`, `raft/invariant.go`

**Think first.** "The log is an array with a hole at the front (the snapshot). Most Raft bugs are
off-by-one errors here, so every index calculation goes through `term(i)` and `slice(lo, hi)`."

**Files**
- `raft/log.go`: `raftLog` with `snapIndex`/`snapTerm`, `entries`, `committed`, `applied`,
  `stable` (the last index known to be on disk). `maybeAppend` *is* the AppendEntries receiver,
  rules 2–5, with the rule numbers in the comments. Also `isUpToDate` (§5.4.1), `compact`, and
  `restore` (Figure 13 rules 6–7).
- `raft/invariant.go`: `invariant(cond, name, …)` panics with a named `InvariantError`. The
  harness turns these into reported violations.

**Pitfalls**
- Truncating a *committed* entry: impossible in correct Raft, so it is an invariant
  (`CommittedNeverTruncated`). It is how `accept-stale-term` gets caught.
- A delayed duplicate AppendEntries truncating a log that already matches. Rule 3 says truncate
  only on a *conflict* (same index, different term), not whenever the follower's log is longer.
  Test: `TestAppendEntriesReceiverRules`.
- After installing a snapshot, entries kept after it must be written to disk again (this was a
  real bug, found in review: `restore` now marks them unstable).

**Checkpoint**: `go test ./raft -run 'AppendEntries|Committed'`.

**Interview questions**
1. Walk through AppendEntries rules 2–5 on a follower whose log is longer than the leader's.
2. Why is "the follower has more entries" not a reason to truncate?
3. What is `stable` for, and what would go wrong if the leader counted unstable entries?

---

## Step 6. Leader election: `raft/raft.go` (first half)

**Think first.** "Election is the simplest complete feature, so it comes first: timeouts, terms,
votes, majorities. Replication can wait."

**Key functions** (each comment names its Figure 2 rule)
- `Tick`: a follower or candidate times out after a *randomised* 10–19 ticks (§5.2) and calls
  `campaign`. A candidate also re-asks silent peers for votes every 3 ticks (Figure 2 assumes
  lost RPCs are retried).
- `campaign`: term+1, vote for self, ask everyone, with our last log term and index.
- `handleVote`: grant if we haven't voted for someone else this term *and* the candidate's log is
  at least as up-to-date (§5.4.1). Granting resets our election timer.
- `handleVoteResp` → `becomeLeader`: on a majority, reset `next`/`match`, append a **no-op from
  the new term** (thesis §6.4), then broadcast.
- `Step`: first the term rules. A higher term makes anyone a follower. A lower term gets a
  rejection carrying our term, which makes the stale sender step down.
- `checkQuorum` (optional): a leader that hasn't heard from a majority in an election timeout
  steps down. Liveness only.

**Pitfalls**: not persisting the vote (bug zoo `forget-vote`: a restarted node votes twice in one
term → two leaders); skipping the up-to-date check (`skip-up-to-date-check`: a node missing
committed entries becomes leader).

**Checkpoint**: `go test ./raft -run 'Vote|Campaign|Candidate|HigherTerm|CheckQuorum'`.

**In five lines**: A follower that hears nothing for a random timeout becomes a candidate for the
next term. Each server grants at most one vote per term, and only to candidates whose log is at
least as up-to-date as its own. A majority makes a leader, and two majorities always overlap, so
there is at most one leader per term. The new leader appends a no-op so it can commit something in
its own term. Any message with a higher term demotes the receiver to follower.

**Interview questions**
1. Prove Election Safety in two sentences.
2. Why are election timeouts randomised, and what happens if they are not?
3. Why does the up-to-date check compare the *last term* before the length?

**Exercise** (owner): delete the bodies of `handleVote` and `handleVoteResp`, rewrite them from
Figure 2, and get `go test ./raft` and `make chaos SEEDS=500` green again.

---

## Step 7. Replication and commitment: `raft/raft.go` (second half)

**Key functions**
- `sendAppend`: send entries from `next` onwards, or a snapshot if they were compacted away.
  *Probing* followers get one message at a time; *replicating* followers get pipelined messages
  (`next` advances optimistically). This is etcd's flow-control design.
- `handleAppend` → `maybeAppend`; replies with the last matching index, or a rejection with a
  hint (the follower's last index) so the leader can skip back quickly.
- `handleAppResp`: on a reject, move `next` back and probe. On success, raise `match`, then
  `maybeCommit`.
- `maybeCommit`: **Figure 2 Leaders rule 4**. The highest index stored on a majority commits
  *only if its entry is from the current term*. That is the Figure 8 fix (§5.4.2).
- `bcastHeartbeat`: heartbeats carry `min(match, commit)` (never more than the follower is known
  to have) and the ReadIndex context.

**Pitfalls**: committing an old-term entry by counting replicas (bug zoo `commit-prev-term`);
treating a stale rejection as fresh and moving `next` backwards forever (guarded in `handleAppResp`).

**Checkpoint**: `go test ./raft -run 'Figure|Replication|BacksUp'`. The Figure 7 test runs all six
follower logs (a–f) from the paper; the Figure 8 test shows the rule with and without the mutation.

**In five lines**: The leader keeps, per follower, the next index to send and the highest index
known to match. AppendEntries carries the previous index and term, so a follower accepts only if
its log matches up to there, and by induction logs then match entirely (Log Matching). A
rejection moves the leader back until the logs agree, then it pipelines. An entry commits when a
majority stores it *and* it is from the leader's current term. Followers learn the commit index
from later messages and apply in order.

**Interview questions**
1. Draw Figure 8 and explain why counting replicas of an old-term entry is unsafe.
2. Why does committing the current-term no-op also commit every earlier entry?
3. What is the difference between probing and replicating a follower, and why have both?

**Exercise** (owner): rewrite `maybeCommit`, then enable `commit-prev-term` and replay seed 262
(`./bin/raftlab-chaos -seed 262 -mutation commit-prev-term -v | tail -50`).

**Exercise** (owner): the AppendEntries receiver. Delete the bodies of `handleAppend`
(`raft/raft.go`) and `raftLog.maybeAppend` (`raft/log.go`) and rewrite them from Figure 2,
"AppendEntries RPC, Receiver implementation", rules 2–5. Rule 1 (reject a stale term) is already
handled for every message type in `Step`, so leave that alone. Things to get right: what the reply
says on a rejection (the hint the leader uses to
skip back), deleting a conflicting entry *and everything after it* but never an entry that
already matches, and moving the commit index to `min(leaderCommit, index of last new entry)`.
Done when `go test ./raft -run 'AppendEntries|CommittedEntryNeverTruncated|Figure7|BacksUp|Replication'`,
then `go test ./...` and `make chaos SEEDS=500` pass, and the `accept-stale-term` regression seed
still fails when that mutation is enabled.

---

## Step 8. Reads, Ready and snapshots: `raft/raft.go` (last part)

- `ReadIndex(ctx)` (thesis §6.4): the leader (1) waits until it has committed an entry in its own
  term, (2) records the commit index, (3) confirms it is still leader with a heartbeat round that a
  majority acknowledges (`processReads`). The caller then waits until applied ≥ that index. No log
  write is needed, and a deposed leader can never serve a stale value (bug zoo `stale-read` skips
  step 3).
- `HasReady` / `Ready` / `Advance`: batching lives here. `Propose` only appends and sets a flag;
  the broadcast happens once per `Ready`, so many proposals share one message per follower and one
  fsync. In `Advance` the leader counts itself only up to its *stable* index (D3 in
  `docs/DECISIONS.md`).
- `Compact(index, data)`: replace the log up to an applied index with a snapshot.
  `handleSnapshot` installs one from the leader.

**Checkpoint**: `go test ./raft`: all 20 Raft tests (26 counting the Figure 7 subtests).

**Exercise** (owner): the ReadIndex path. Delete the bodies of `ReadIndex`, `processReads` and
`handleHeartbeatResp` (`raft/raft.go`) and rewrite them from thesis §6.4 (steps 1–4 of the
ReadIndex algorithm). Things to get right: refusing on a follower, refusing until the leader has
committed an entry of its own term, recording the commit index *before* the heartbeat round,
counting acknowledgements per read context (the leader counts itself), and releasing reads in
order. Keep the two-line `mutation.StaleRead` block at the top of `ReadIndex`: the bug zoo needs
it. Done when `go test ./raft -run ReadIndex`, then `go test ./...` and `make chaos SEEDS=500`
pass, and the `stale-read` regression seed still fails with that mutation enabled.

**Interview questions**
1. Why does ReadIndex need the "committed an entry in its term" step?
2. Compare ReadIndex, reading through the log, and leader leases.
3. Where does batching come from in this design, and why does it cost nothing in correctness?

---

## Step 9. The write-ahead log: `storage/codec.go`, `storage/wal.go`

**Think first.** "Recovery must decide which bytes to trust. A checksum per record makes the
answer exact: trust everything up to the first record that fails."

**Files**
- `storage/codec.go`: uvarint encoding of entries, hard state and snapshots.
- `storage/wal.go`: `Open` loads the snapshot, replays records until the first short or
  bad-checksum one, and truncates there. A rewritten index drops the old suffix (AppendEntries
  rule 3 again, now on disk). `reconcileWithSnapshot` handles a crash between writing a snapshot
  and rewriting the log. `Save` writes entries plus hard state and fsyncs. `SaveSnapshot` replaces
  the snapshot atomically, then the log.

**Pitfalls**: ordering the snapshot and log rewrite the other way round; trusting a torn tail;
`NoSync` (bug zoo `skip-fsync`: acknowledged entries vanish after a crash).

**Checkpoint**: `go test ./storage`. `TestTornTailIsTruncated` crashes the simulated disk at 100
random points.

**Interview questions**
1. Why is truncating at the first bad record safe? Which promise makes it safe?
2. Walk through a crash between the snapshot write and the log rewrite.
3. What would you change for a log of many gigabytes?

---

## Step 10. The state machine with sessions: `kv/kv.go`

**Think first.** "Clients retry. A retried append that was already applied must not apply again,
or the client sees its write twice. That is not linearizable."

`Command{ClientID, Seq, Op, Key, Value}`. The store keeps `sessions[ClientID] = (last Seq, last
result)`. A command whose Seq is not newer returns the cached result without running. Sessions are
part of the snapshot. Bug zoo `no-dedup` turns this off, and Porcupine catches the duplicate append.

**Checkpoint**: `go test ./kv`.

**Interview questions**
1. Why must sessions be replicated state and not a cache on the leader?
2. What breaks if a client has two writes outstanding with this scheme?
3. How would you expire sessions safely (thesis §6.3)?

---

## Step 11. The driver: `peer/peer.go`

**Think first.** "Exactly one place should implement the Ready contract, and both the simulator
and the real server should use it, so what I test is what I run."

`Peer` owns a `raft.Node`, a `storage.Storage` and a `kv.Store`. `process()` loops while
`HasReady`: save (or save the snapshot) → `Send` → restore snapshot → apply entries (completing a
write callback only if the applied entry has the term it was proposed in; otherwise `ErrLost`) →
record read indexes → `Advance` → serve reads whose index is applied → maybe snapshot. Role changes
are reported to an `Observer` (the harness) and fail pending reads.

**Checkpoint**: `go test ./peer`: a single node writes, reads, restarts from disk, deduplicates a
retry, and restarts from a snapshot.

**Interview questions**
1. Why compare the applied entry's term with the proposal's term before acknowledging a write?
2. What happens to pending reads when the leader steps down, and why?
3. Why is the peer single-threaded, and who enforces that in the real server?

---

## Step 12. Recording and checking histories: `check/history.go`

Each client operation records `(input, output, call time, return time)`. `KVModel` is the
sequential spec: get returns the current value, put replaces it, append concatenates. Partitioning
by key makes checking fast. An abandoned write gets an open-ended interval (it may take effect
later or never); an abandoned read is dropped. A Porcupine timeout returns `Inconclusive`, never a pass.

**Checkpoint**: `go test ./check`: a stale read and a duplicated append are rejected; concurrent
reorderings and unknown writes are accepted.

**Interview questions**
1. Define linearizability in one sentence, then explain why checking it is expensive.
2. Why can an abandoned write not just be dropped from the history?
3. Why is partitioning by key valid for this model?

---

## Step 13. The whole cluster in the simulator: `harness/`

**Think first.** "Now combine everything: N peers, crashing disks, a bad network, retrying
clients, and a fault schedule, all from one seed, with the safety properties checked continuously."

**Files**
- `harness/harness.go`: `Run(Options)` builds the cluster. Per-node ticks are skewed for clock
  drift. Faults: crash/restart (with `Disk.Crash`), partitions, one-way blocks, pauses (input is
  queued and replayed with jitter on wake-up), delay spikes. Half of the faults target the
  leader. Invariant checks: `Applied` (State Machine Safety, plus a commit-term bound),
  `RoleChanged` (Election Safety, Leader Completeness) and `periodicCheck` (Log Matching, Leader
  Append-Only). `guard` turns a panic into a named violation and stops the run.
- `harness/clients.go`: clients with a leader guess, per-attempt timeouts and same-Seq retries.
- `harness/sweep.go`: many seeds in parallel; each run stays single-threaded and deterministic.
- `DefaultOptions(seed)`: the seed also picks the cluster shape (D12 in `docs/DECISIONS.md`).

**Pitfalls (both happened)**
1. *A wrong checker.* The first Leader Completeness check flagged leaders of *older* terms; 7 of
   500 correct seeds "failed". Ask whether the property you check is the property in the paper.
2. *A gentle fault model.* Four mutations were caught 0–4 times in 300 seeds until the faults
   targeted leaders, delays spiked, CheckQuorum varied and paused nodes woke up with jitter.

**Checkpoint**: `go test ./harness` (replay, 300 clean seeds, bug-zoo regression seeds,
fault-free liveness). Then `make chaos SEEDS=10000`.

**Interview questions**
1. Which invariants are checked during the run, and why stop at the first violation?
2. How can a harness have false positives, and how did you find one?
3. Why aim faults at the leader?

---

## Step 14. The chaos runner and the bug zoo: `cmd/raftlab-chaos`, `mutation/`

`mutation.Set` is a bit set with eight switches. Each lives exactly where the rule lives: the
commit rule, the vote check, hard-state persistence, the apply limit, ReadIndex, the term check,
fsync, dedup. `raftlab-chaos -bugzoo` sweeps every mutation and reports detection counts, the first
failing seed and the median seeds-to-detection. Those first seeds become
`TestBugZooSeedsStillFail`.

**Checkpoint**: `make bugzoo` → `8/8 injected bugs caught`.

**Interview questions**
1. What is mutation testing, and why is "the tests still pass" bad news for a mutant?
2. Two mutations are caught in under 0.2% of seeds. Is the harness weak, or are the bugs rare? How would you tell?
3. If you change the code and a regression seed stops failing, what do you do?

---

## Step 15. The real system: `transport/`, `server/`, `client/`, `cmd/raftlab-server`, `cmd/raftlab`

One goroutine per server owns the `Peer`. It selects on a ticker, an inbox of Raft messages and a
queue of client requests, and groups the writes already queued into one `SubmitBatch` (one fsync
for all of them). Transport: one sender goroutine per peer, batches of messages per HTTP POST,
drop on overload (Raft retries). Client library: random client ID, increasing Seq per write, the
same Seq on every retry, and it follows the 421 leader hints.

**Checkpoint**: `make race` (the end-to-end test kills the leader over real sockets and real
disks). Then `./scripts/cluster.sh` and the CLI.

**Interview questions**
1. Why does dropping messages on a full queue not break anything?
2. How does request batching happen, and what does it cost in latency?
3. What does the race detector check here, and why is it irrelevant for the simulated code?

---

## Step 16. Benchmarks: `cmd/raftlab-bench`, `scripts/bench.sh`

Write the method down first (`docs/BENCHMARKS.md`): hardware, value size, concurrency, warm-up,
duration, what one "op" is. Record the machine next to every result. Measure the knobs that
explain the design: batching on/off, fsync on/off, 3 vs 5 nodes, 1 vs 32 clients, failover.

**Interview questions**
1. Why did turning off batching cut throughput by about 8x on this machine?
2. Why is p99 so much worse than p50 without batching?
3. What would change on three real machines?

---

## Step 17. The formal spec: `spec/Raft.tla`, `spec/MCRaft.tla`, `scripts/tlc.sh`

Model the same rules with messages as a set (reorder and loss), crashes that keep persistent state,
and history variables (`elections`, `committed`) so the properties can be stated directly with the
**same names as the Go invariants**. Check exhaustively on small bounds and by simulation on larger
ones. Switch the Figure 8 bug on with a constant.

**Pitfall (happened)**: in an action, TLC evaluates *every* `\/` branch as a possible step, even
when an earlier one is true, so each branch must guard its own indexing.

**Interview questions**
1. What does TLC check that the chaos harness cannot, and the other way round?
2. Why do bounded models still find most protocol bugs ("small scope hypothesis")?
3. How are the spec and the Go code linked, and how weak is that link?

---

## Step 18. Write it up

README with real numbers and caveats, a decisions log, this guide, the protocol mapping, and the
bug-zoo write-ups. Every number has a command next to it.
