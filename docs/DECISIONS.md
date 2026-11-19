# Questions and decisions

Every question raised while planning raftlab, with its answer, who decided it and the evidence.
Items marked **[OPEN]** are the owner's to answer. Nothing else in the repository depends on
them yet, but the next steps do.

Status legend: **owner** = the owner answered · **default** = decided while building, because the
owner asked for the whole project in one pass (2026-11-19); each can be revisited · **measured**
= settled by an experiment whose output is in `results/`.

---

## 1. Planning questions (environment, time, goals)

| ID | Question | Why it matters | Answer | Status |
|---|---|---|---|---|
| Q1 | **What is the deadline?** When do placement / internship applications open? | Decides how much polish (blog post, larger TLA+ runs, membership change) fits before the resume needs it. | **[OPEN]** The code is complete; see §4 for what to do with more or less time. | owner |
| Q2 | Time budget per week? | The original plan assumed ~6 h/week (about 40 weeks). | Superseded: the owner asked for everything to be built in one session on 2026-11-19. Learning the code is now the time sink; `docs/BUILD_GUIDE.md` is ordered for that. | owner |
| Q3 | Which Go version? | 1.22 is out of upstream support; newer versions add nothing this code needs. | Stay on the installed **Go 1.22.2** (`go.mod` says `go 1.22`). It already has `math/rand/v2`, range-over-int and `ServeMux` method patterns, all used here. CI also tests the latest stable Go. Upgrading locally is recommended but not required. | default |
| Q4 | **Is the owner taking (or about to take) a course that uses the MIT 6.5840 Raft labs?** | Publishing a Raft implementation can break that course's academic-integrity rules. | **[OPEN]** Check before making the repository public. | owner |
| Q5 | Public repo, CI? | Free GitHub Actions minutes for public repos; thousands of seeds per CI run. | **Not pushed** (owner, 2026-11-19: keep it local). `.github/workflows/ci.yml` is ready for when it is pushed: vet, tests, race detector, 2000-seed chaos sweep. | owner |
| Q6 | Benchmark hardware? | Single-laptop numbers can't speak for a real cluster. | Only the development laptop (i5-12500H, 16 threads, 7.4 GB RAM, NVMe ext4). Every benchmark says "single machine, loopback" next to its number. | default |
| Q7 | How strict is the step-by-step process? | The original rule was "the owner writes the spec and the intent of each test, and tests are approved before the code that makes them pass". | The owner asked for the complete project in one go, so the approval gate was skipped for this pass. To make the code interview-defensible, re-implement `handleVote` and `maybeCommit` from memory against the existing tests (exercises in `docs/BUILD_GUIDE.md`, steps 6 and 7). | owner |
| Q8 | **Where will the blog post live?** | The resume link needs a stable URL. | **[OPEN]** Draft is `docs/WRITEUP.md`. | owner |
| Q9 | What claims about prior art (etcd, hashicorp/raft, Jepsen, …) may the README make? | The owner asked to confirm their current state before any claim. | The README only names them as influences and makes **no comparisons**. Confirm current facts before adding any. | default |

---

## 2. Scope decisions

| ID | Question | Options | Decision | Status |
|---|---|---|---|---|
| S1 | Membership change in v1? | (a) single-server changes now; (b) leave as a stretch goal | **(b)**. It is the most bug-prone feature (the known single-server bug fixed in Ongaro's 2015 raft-dev post), and the harness, bug zoo and spec were the priority. The cluster is fixed at start. | default |
| S2 | Chaos testing before or after snapshots? | (a) snapshots first, as in the original plan; (b) chaos first | Moot: built in one pass. Snapshots were in the chaos harness from its first run, at three intervals (never, every 10, every 40 entries) chosen per seed. | default |
| S3 | Client API: gRPC or HTTP? | (a) gRPC: typed, conventional, needs `protoc` and several dependencies; (b) HTTP + JSON for clients, HTTP + gob between servers: standard library only | **(b)**. The project is about consensus and testing, not RPC frameworks, and zero dependencies keeps the build trivial. Only Porcupine is a dependency. The cost is speed: gob re-sends type information per batch. | default |
| S4 | Lease-based reads? | (a) ReadIndex only; (b) also leases (faster, depend on bounded clock drift) | **(a)**. Consequence: clock skew in the simulator tests liveness only, because no safety property depends on clocks. Leases would be a good later bug-zoo entry (skew → stale read). | default |
| S5 | Pre-vote, leadership transfer, joint consensus? | include or not | Not included; listed under Limitations. | default |

---

## 3. Design decisions (each had two options; the reasoning is kept)

**D1. Shape of the Raft core.**
(a) Goroutine per node with channels and timers, as in most course implementations.
(b) A pure state machine: `Tick` / `Step` / `Propose` in, a `Ready` out, no I/O, like etcd/raft.
**Chosen: (b).** It is the only shape a deterministic simulator can drive, and it makes unit tests
plain function calls. The cost is the `Ready` contract (persist → send → apply → `Advance`), which
callers must follow exactly; `peer.process` is the one place that does.

**D2. How the simulator schedules work.**
(a) Discrete events on one goroutine.
(b) Real goroutines under a controlled runtime (the madsim approach).
**Chosen: (a).** Go can't intercept its own scheduler without patching the runtime. One goroutine
plus sorted map iteration gives bit-for-bit replay, checked by a trace hash
(`TestSim_SameSeedSameTrace`, `TestSameSeedReplaysExactly`).

**D3. When the leader counts itself towards a majority.**
(a) As soon as an entry is appended (faster; thesis §10.2.1 allows writing in parallel with sending).
(b) Only after its own fsync.
**Chosen: (b).** It is simpler to argue about: `Advance` sets the leader's own match index to its
stable index. A leader that counted unsynced entries and then crashed could lose a committed entry.

**D4. No-op entry on election.**
(a) None, as in the bare Figure 2.
(b) Append a no-op from the new term (thesis §6.4).
**Chosen: (b).** ReadIndex needs a committed entry from the current term, and the no-op commits old
entries promptly. Likely side effect (reasoned, not measured): the no-op usually travels in the same
message as the old-term entry, so the buggy commit rule rarely gets a chance to act alone. That
probably explains why `commit-prev-term` is caught in only 5/3000 seeds.

**D5. Replication flow control.**
(a) One AppendEntries at a time per follower.
(b) Probe/replicate states (etcd): probe one at a time until the match point is found, then
pipeline optimistically.
**Chosen: (b),** with rejection hints (the follower's last index) for fast backup. Tested by
`TestLeaderBacksUpWithHint` and the Figure 7 cases.

**D6. Retrying vote requests.**
(a) Send RequestVote once per election.
(b) Re-ask silent peers every 3 ticks.
**Chosen: (b).** Figure 2 assumes RPCs are retried, and a lost vote request otherwise costs a whole
election timeout. (It went in together with the other fault-model changes in D7 and D12, so its
individual effect on bug detection was not measured.)

**D7. CheckQuorum.**
On by default in the real server (a partitioned leader steps down so clients find the real one).
In the simulator it is chosen **per seed**, because it is liveness-only. With it always on, a
deposed leader disappears too fast for `stale-read` and `accept-stale-term` to be visible.

**D8. WAL format.**
(a) One record per entry with a CRC, one file, rewritten at snapshot time.
(b) Segmented files.
**Chosen: (a):** `[len u32][crc32c u32][type u8][payload]`. Recovery truncates at the first bad
record. A snapshot is written first, then the log is rewritten; recovery throws away log entries
that disagree with the snapshot (Figure 13 rules 6–7). Segments would matter for very large logs.

**D9. Linearizability checker.**
(a) Write our own.
(b) Porcupine.
**Chosen: (b), v1.3.1.** The original plan says to use it rather than rewrite it. Histories are
partitioned by key. A checker timeout is reported as *inconclusive* and never counted as a pass
(0 inconclusive in 10,000 seeds).

**D10. Client model in the harness.**
One outstanding operation per client and a per-attempt timeout. Writes are retried with the same
sequence number (this exercises dedup). An operation still pending at the end is recorded with an
open interval, so it may or may not have happened. Abandoned reads are dropped from the history.

**D11. Leader Completeness check in the harness** (fixed after it produced false positives).
A leader of term T must hold every entry *committed in a term below T*. The harness can't see
commit terms directly, so it records an upper bound: the smallest current term of any node when
it applied the entry. It checks only when T is above that bound. This is sound and still catches
4 of the 8 mutations first.

**D12. Per-seed configuration.**
Each seed chooses: 3 or 5 nodes, 1/4/64 entries per message, snapshots never/10/40, loss
1%/5%/15%, CheckQuorum on/off, client timeout 50/250 ms, fault interval 50/150/300 ms. One sweep
covers many configurations, and the seed alone still reproduces the run.

**D13. TLA+ model.**
(a) Use Ongaro's `raft.tla`.
(b) Write a smaller spec whose property names match the Go assertions.
**Chosen: (b),** to keep the spec-to-code mapping one-to-one. Messages are a set (reorder and loss,
no duplication); a leader sends at most one entry per message.

**D14. TLC bounds.**
Limited by the machine (7.4 GB RAM, a few GB of free disk). At first the disk had under 100 MB free,
because a GNOME indexer was looping and had filled `/var/log/syslog` with 18.5 GB, so only
terms ≤ 2 / log length ≤ 1 could be checked exhaustively. After space was freed: the bug config at
terms ≤ 4 produced the Figure 8 counterexample (breadth-first, 39 states); the correct spec passed
exhaustively at (3, 1, 1) and (2, 2, 1); (4, 1, 1) and (3, 2, 1) ran 45 minutes each without a
violation but did not finish. `scripts/tlc.sh` caps each file TLC writes (`TLC_FILE_LIMIT_KB`) so
a large model fails instead of filling the disk. See `results/tlc.md`.

**D15. Commit style.**
Same as the qsim project: plain descriptive subjects without `phaseN:` prefixes, and private
planning files are kept out of git through `.git/info/exclude`.

---

## 4. If there is more (or less) time before the deadline (Q1)

| Time available | Do this |
|---|---|
| 1 week | Read `docs/BUILD_GUIDE.md` steps 1–10 and re-implement `handleVote`, `maybeCommit` and `maybeAppend` from memory against the tests. Read the TLC Figure 8 trace (`results/tlc-figure8-trace.txt`) step by step against the paper's figure. |
| 1 month | All of the above, plus: publish the blog post (`docs/WRITEUP.md`), push the repo once Q4 is cleared, and run the benchmarks on two or three real machines. |
| 3 months | Add single-server membership change with its own bug-zoo entry (the 2015 single-server bug), lease reads with a clock-skew bug-zoo entry, and Porcupine visualisations of each bug-zoo failure in the docs. Then start the replay-debugger project on top of `sim/` and `check/`. |
