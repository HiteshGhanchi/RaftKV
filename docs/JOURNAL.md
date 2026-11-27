# Developer journal

My working notes on building RaftKV, part by part: what I built, what went wrong, how I found it
and what I changed. Dates are when the work happened. Numbers come from files in `results/`;
anything I only estimated is marked as an estimate.

---

## 1. Planning (2026-11-19)

Before writing code I listed what "correct" would mean and how I would know. I wrote down the
Figure 2 safety properties (Election Safety, Leader Append-Only, Log Matching, Leader
Completeness, State Machine Safety) and decided every one of them would be a named check in code,
plus linearizability of the client history. The plan, the open questions and every trade-off went
into `docs/DECISIONS.md`; each design choice there has two options and the reason I picked one.

The order I chose was simulator first, Raft second. The reasoning: if I cannot control time, the
network and the disk, I cannot reproduce a failure, and a consensus bug I cannot reproduce is a bug
I cannot fix.

Things I checked about my own machine first: Go 1.22.2 (I set `GOTOOLCHAIN=local` because Go kept
trying to download a newer toolchain), OpenJDK 17 for TLA+, an i5-12500H with 16 threads and 7.4 GB
of RAM, ext4 on NVMe.

## 2. The deterministic simulator (`sim/`)

I built a discrete-event simulator: a virtual clock, a heap of pending events ordered by
(deadline, sequence number), and one seeded PCG generator from `math/rand/v2`. The sequence-number
tie-break matters: without it, two events due at the same instant would run in heap order, which is
not stable, and the same seed would not replay.

On top of that: a generic `Network[M]` (delay, loss, duplication, reordering, one-way blocks,
partitions, crashed receivers) and a `Disk` that loses unsynced bytes on a crash, or keeps a random
prefix of them, which models a torn write. Old file handles fail after a crash, so code cannot keep
writing through a handle from before it.

The first tests I wrote were the ones that prove the simulator is trustworthy: the same seed gives
the same trace hash; timers fire in deadline order; a crash loses exactly the unsynced data.

A small trap I hit: iterating over a Go map is randomised, so any behaviour that depended on map
order broke determinism. I added a `sortedKeys` helper and used it everywhere order affects what
happens.

## 3. The Raft core (`raft/`)

I wrote Raft as a pure state machine in the etcd style: `Tick`, `Step`, `Propose`, `ReadIndex` go
in, and a `Ready` comes out (state to persist, messages to send, entries to apply). It has no I/O,
no clock and no goroutines, which is what lets the simulator drive it. The caller's contract is
persist, then send, then apply, then `Advance`. A leader only counts itself toward a majority after
its own fsync.

Features: elections with randomised timeouts, pipelined replication with probe and replicate
states, rejection hints so a leader can skip back quickly, the Figure 8 commit rule, a no-op entry
on election (thesis §6.4), ReadIndex reads, CheckQuorum and snapshots. Every rule has a comment
citing Figure 2 or the thesis section.

Tests: 20 unit tests, including all six follower logs (a–f) from Figure 7 and a direct Figure 8
scenario that fails when the commit rule's term check is removed.

Later I added **vote retries**: a candidate re-sends vote requests every 3 ticks. Figure 2 assumes
lost RPCs are retried, and without this a single dropped vote request could stall an election for a
whole timeout. I also made CheckQuorum optional per seed, for reasons in section 6.

## 4. The write-ahead log (`storage/`)

Records are `[length u32][crc32c u32][type u8][payload]`. On recovery I read until the first record
whose length or checksum is wrong and truncate there, which repairs a torn tail. Snapshots are
written to a temporary file, fsynced, renamed, and then the directory is fsynced, so a crash
leaves either the old snapshot or the new one, never half of one.

I tested torn tails explicitly and a crash between writing a snapshot and rewriting the log.

**Bug I found in review:** when a follower installed a snapshot and kept the log entries after it,
those entries were not handed back for the log-file rewrite, so a crash right after would have lost
them. I fixed it by marking every retained entry as not yet stable after a restore.

## 5. KV store and the peer driver (`kv/`, `peer/`)

The state machine supports put, append and get, plus **client sessions**: every request carries a
client id and sequence number, and a retried request takes effect only once (thesis §6.3). The peer
driver glues node, storage and store together and runs the Ready loop in the order Raft requires.

**Second bug found in review:** the snapshot code called `Ready()` just to read the hard state.
`Ready()` also hands over pending messages, so those messages would have been silently dropped. I
added a separate `HardState()` accessor.

## 6. The chaos harness and the bug zoo (`harness/`, `check/`, `mutation/`)

The harness runs a whole cluster in the simulator: 3 or 5 nodes, retrying clients, and a fault
schedule with crashes, partitions, one-way blocks, pauses and network slowdowns, all from one seed.
It checks the safety properties live (on every applied entry, every election, and log comparisons
every 50 ms of virtual time) and checks the history with Porcupine at the end. A checker timeout
counts as *inconclusive*, never as a pass.

The bug zoo is eight switches that each break one rule (commit-prev-term, skip-up-to-date-check,
forget-vote, apply-before-commit, stale-read, accept-stale-term, skip-fsync, no-dedup). If the
harness cannot catch all eight, the harness is too weak.

Two real problems here, both in my own tooling:

- **False positives in my Leader Completeness check.** The first 500-seed sweep of *correct* code
  reported 7 failures, all "new leader lacks an applied entry". Every one was a leader of an older
  term elected late, which Raft allows: the property only covers leaders of later terms than the
  one the entry was committed in. I fixed the check by recording an upper bound on each entry's
  commit term. After that, 10,000 seeds passed.
- **My first fault model was too gentle.** Four of the eight bugs were caught 0–4 times in 300
  seeds. CheckQuorum made deposed leaders step down too fast, messages were never delayed for long,
  and paused nodes replayed their input in a fixed order. I added faults aimed at the leader,
  delay spikes, CheckQuorum switched off on some seeds, jitter when a paused node wakes, varied batch
  sizes, and the vote retries. After that every bug was caught at least twice in 3000 seeds.

First recorded results: 8/8 bugs caught over 3000 seeds each, and 10,000 runs of correct code with
0 failures.

## 7. The real networked system (`server/`, `transport/`, `client/`)

Same peer, real processes: one goroutine per server owns the peer, peers talk HTTP with gob-encoded
batches, and clients use Go 1.22's pattern-based `ServeMux` routes (`GET/PUT /kv/{key}`). A
non-leader answers 421 with a leader hint, and the client library retries with the same sequence
number. Requests are batched into one proposal and one fsync.

## 8. Benchmarks

Three nodes on one laptop over loopback with real fsync and 32 clients: 5,454 writes/s, p50
5.55 ms, p99 12.06 ms. Turning off batching dropped it to 667 writes/s (batching is worth about
8.2x); turning off fsync raised it to 27,998 (fsync costs about 5.1x). Failover (killing the
leader 20 times): p50 163 ms, max 733 ms. These are single-machine loopback numbers and I label
them that way.

The benchmark script first deleted old data directories with a glob, which made me nervous, so I
changed it to write each run into a fresh timestamped directory and never delete anything.

## 9. TLA+ and the disk incident (2026-11-19 to 2026-11-20)

I wrote a small TLA+ spec of election and replication with the same property names as the Go
checks, and a switch that removes the commit rule's term check.

**Spec bug found by TLC:** inside an action TLC evaluates every `\/` branch, even after an earlier
one is true, so two of my branches indexed an empty sequence. Each branch now guards its own
indexing.

**The disk filled up.** TLC's state files are large, and the disk was already nearly full: GNOME's
file indexer (`tracker-miner-fs-3`) was stuck in a loop on a temp file in another project and had
grown `/var/log/syslog` to 18.5 GB. I stopped the indexer, cleared TLC's state, bench data and the
Go build cache, and added a per-file size cap to `scripts/tlc.sh` (`ulimit -f`) so a big model fails
instead of filling the disk. Lesson: check `df -h` before any model-checking run.

With space back:

- With the bug switched on, exhaustive TLC found the **Figure 8 counterexample**: a 39-state trace
  at terms ≤ 4 after 39,347,865 distinct states (10 minutes). Breadth-first search means it is a
  shortest one within those bounds. Earlier attempts with simulation and depth-first search had
  missed it, which makes sense now: it is 38 steps deep and rare among random behaviours.
- With the correct rule, exhaustive checks passed at (2,1,1), (2,2,1), (3,1,1) and, after a 55-minute
  run, at the Figure 8 bounds themselves: 114,534,684 distinct states, no error. TLC estimates
  about a 0.6% chance that a fingerprint collision skipped a state, and I wrote that down.

My first long run was cut off at a 45-minute limit, and I almost recorded it as a pass. It was not
one: it was unfinished, and the docs say so. I re-ran it without the limit to get a real result.

Small shell lesson: `zsh` does not split words in unquoted variables the way `bash` does, so a loop
over several TLC configurations passed them as one argument. I ran those loops with `bash -c`.

## 10. Repository hygiene

- When I reviewed the history I found two files used by earlier commits that were never committed
  (`sim/world.go`'s `RunUntilOr` and `mutation_test.go`), so those commits did not build on their
  own. I folded the missing pieces into the right commits with `git commit --fixup` and an
  autosquash rebase, and checked that every commit vets and tests on its own.
- Commit style: a plain subject line and a short description, no `feat:`-style prefixes.
- Before publishing, I searched every file and the whole history for references to the tools I had
  used, removed the one I found by folding the fix into its original commit (the repo was not yet
  public, so rewriting history was safe), and added a local `pre-push` hook that blocks a push if
  any outgoing commit matches. I tested the hook both ways.

## 11. Publishing on GitHub (2026-11-22)

- The GitHub CLI was not installed, so I created the repository on the website and pushed over SSH
  (my key was already set up); fetching uses the HTTPS URL. I checked that the remote was empty
  before the first push.
- The first CI run (Go 1.22 and stable: vet, tests, race detector, 2000-seed chaos sweep) passed.
- **A test that failed only under the race detector:** a simulator test asserted that one virtual
  hour runs in under 5 seconds of wall time. Under `-race` it took 14.9 s. The logic was fine; the
  race detector is just slow. I added a build-tagged `raceEnabled` constant and skip only the
  wall-clock bound under `-race`.
- I checked with myself whether publishing a Raft implementation could break a course's
  academic-integrity rules (some courses use Raft labs). Mine was a blockchain course, so it was fine.

## 12. Planning v2: debugger, browser, hardening (2026-11-27)

I wrote a plan for the next phases (`docs/PLAN_V2.md`) and first checked the facts it depends on.

- **WebAssembly spike.** I compiled the whole simulated path, Porcupine included, with
  `GOOS=js GOARCH=wasm`: it built unchanged, 3.0 MB (0.85 MB gzipped), and native and wasm runs gave
  byte-identical trace hashes on 50 seeds and 3 bug-zoo seeds.
- **I got a speed number wrong at first.** The spike measured 19.9 seeds/s native and 11.4 in wasm.
  Later the same binary ran at 107 seeds/s: the machine had been busy during the first measurement.
  On an idle machine wasm under Node runs about 21 seeds/s, so it is about 5x slower than native,
  not 1.75x. I corrected the plan and the decisions log. Lesson: re-measure on an idle machine
  before writing a number down.

## 13. P1: the replay debugger (2026-11-27)

I shipped this in six milestones, pushing each one separately.

1. **Typed trace.** I replaced the text trace with typed events (`sim.Event`: time, step, kind,
   node, peer, numeric fields, detail). The trace hash is computed from a canonical binary encoding,
   so observing a run cannot change it, and a test proves that. A JSON Lines sink costs 1.95x; a
   counting sink costs 2% (500 seeds).
2. **Fault schedule as data.** Faults are now recorded as resolved values (time, kind, nodes,
   duration) and can be replayed without the random generator. To make replay identical I had to
   make both modes schedule exactly the same timers, and move clock skew, tick phase and wake-up
   jitter onto their own random stream. That second change moved every seed's behaviour, so I
   re-ran the bug zoo (still 8/8, median 15.5 seeds to first detection) and the 10,000-seed sweep
   (still 0 failures) and updated every number and regression seed.
   - **Swarm testing did not help.** I tried randomly disabling fault kinds per seed. It caught the
     two rarest bugs *less* often, so I kept it as an option (`-swarm`) instead of the default.
   - **I forgot the README in that commit**, so GitHub briefly showed the old bug-zoo table. I
     pushed a follow-up commit with the README a minute later. Lesson: stage by explicit list and
     check `git status` before committing.
3. **Time-travel debugger.** I split `harness.Run` into start, step and finish, so the debugger runs
   exactly what the chaos runner runs. Going back re-executes the deterministic prefix instead of
   keeping checkpoints (Go closures in the event queue cannot be copied). Measured: `back 1` at 90%
   of a run takes 24.8 ms at p99 over 200 seeds, so checkpoints were never needed.
4. **Shrinking and diffs.** Delta debugging over the recorded schedule, then fewer nodes and
   clients. Six of the eight bug-zoo failures shrank to 0–3 faults; `no-dedup` needs no faults at
   all. The two rarest shrink least. My first shrink test wrongly required the shrunk run to have
   fewer steps; with fewer faults a bug can surface later, so I changed the test to check faults,
   nodes and clients instead. The diff tool shows the first event where a correct and a mutated
   run of the same seed differ.
5. **Exports.** A ShiViz log with vector clocks computed from the trace, and fault annotations on
   Porcupine's history view.
6. **Transcripts and walkthrough.** A debugger session and a diff for all eight bugs, and a
   walkthrough (`docs/DEBUGGER.md`) that finds the exact step where the Figure 8 bug commits a
   term-2 entry in term 12, and the election 30 ms later that loses it.

## 14. P8.1 design: automatic mutation testing (2026-11-27)

To check whether my hand-picked bug zoo was biased, I designed automatic mutation testing. A spike
counted about 440 candidate mutation sites in `raft/` and measured about 2–3 s per mutant using Go's
`-overlay` flag (no files touched).

**The first mutant I tried exposed a real blind spot in my harness.** Changing `>=` to `>` in the
vote up-to-date check is caught by the unit tests, but the harness passed it on 2000 seeds: with
that change no election can be won, nothing happens, and the harness only checked safety. Doing
nothing is safe. So the design adds a progress check: after faults stop, the cluster must complete
at least one client operation.
