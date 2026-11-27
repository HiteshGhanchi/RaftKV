# Plan v2: replay debugger, browser product, hardening

Status: **approved by the owner on 2026-11-27.** The questions and decisions are recorded in
`docs/DECISIONS.md` §5. No production code is written until the P1 ownership gate is done
(`docs/OWNERSHIP.md`), and each later phase still starts with its own design note and approval.

Effort figures are estimates of focused build hours, not measurements. Calendar time depends
mostly on the owner's review and ownership work.

---

## 1. Section 0 of the brief, checked against the code (2026-11-27, commit 7b950c9)

| Claim in the brief | Verdict |
|---|---|
| Raft core as a pure state machine with election, pipelining, probe/replicate, reject hints, Figure 8 rule, no-op, ReadIndex, CheckQuorum, snapshots | **Correct.** |
| WAL with CRC32C and torn-tail repair, atomic snapshots, sessions, Ready loop, HTTP + gob server/transport/client/CLI | **Correct.** |
| Simulator, harness with live invariants, Porcupine | **Correct.** The harness already keeps **separate RNG streams** for faults, clients and restarted peers (`harness/harness.go`, `frng`/`crng`/`prng`). That helps P1 shrinking. The fault stream also drives tick and wake-up jitter, so it has to be split (see P1.3). |
| Bug zoo 8/8 over 3000 seeds; 10,000-seed sweep, 0 failures, 1,593,004 ops | **Correct** (`results/bugzoo.json`, `results/chaos-10000.json`). |
| TLC finds Figure 8 with the bug and passes without it | **Correct**, and stronger than stated: the correct spec passed a *complete* exhaustive check at the Figure 8 bounds (3 servers, terms ≤ 4: 114,534,684 distinct states, `results/tlc.md`). |
| Benchmarks 5,454 writes/s, p99 12.1 ms | **Correct** (`results/bench-3n-32c.json`). |
| Replay today is `raftlab-chaos -seed N -v`: text trace plus hash, nothing structured | **Correct**, with one addition: `raftlab-chaos -seed N -viz file.html` already writes Porcupine's HTML history view (`check.History.Visualize` uses `porcupine.VisualizePath`). Part of P1.5 exists already. |
| Known limits | **Correct** as listed in the README. |
| Q5 says "not pushed" | **Correct, and stale.** It was the only stale "not pushed" line in tracked files; now fixed. |
| Q7: owner must re-implement key functions | **Correct.** Q7 and `docs/BUILD_GUIDE.md` have exercises only for `handleVote`/`handleVoteResp` (step 6) and `maybeCommit` (step 7). **There is no exercise yet** for the AppendEntries receiver (`handleAppend` plus `raftLog.maybeAppend`, steps 5 and 7) or the ReadIndex path (`ReadIndex`, `processReads`, `handleHeartbeatResp`, step 8). Both exercises have now been added to `docs/BUILD_GUIDE.md` (instructions only). |
| Repo URL `github.com/HiteshGhanchi/raftkv` | The repo is `HiteshGhanchi/RaftKV`. GitHub URLs ignore case, so both work. |
| P4.7 suspicion "gob re-sends type info" | **Confirmed by reading the code**: `transport/http.go` creates a new `gob.Encoder` for every batch, so type descriptors go out with every request. Not yet measured. |

---

## 2. Facts that P1 and P2 depend on (checked 2026-11-27)

### 2.1 The simulated path compiles to WebAssembly and replays identically (spike, measured)

Method: a throwaway program in a copy of the repo (outside the repo, not committed) that runs
`harness.Run(harness.DefaultOptions(seed))` for seeds 1–50, plus three bug-zoo seeds
(commit-prev-term 262, skip-fsync 1, stale-read 18). It was built natively and with
`GOOS=js GOARCH=wasm` (Go 1.22.2), and the wasm build was run under Node v22.17.1 with Go's
`misc/wasm/wasm_exec_node.js`.

| Result | Value |
|---|---|
| Compiles to wasm (raft, peer, kv, storage, sim, harness, check, mutation, **Porcupine v1.3.1**) | **Yes**, no code changes |
| wasm size, standard Go toolchain | 3,000,530 bytes raw, 849,825 bytes gzip -9 |
| Native vs wasm output (trace hash, step count, violation, linearizability verdict) for 50 seeds + 3 mutation seeds | **Byte-identical** |
| Speed, native, one goroutine, i5-12500H | First run: 50 seeds in 2.51 s (19.9 seeds/s). **Re-measured on an idle machine the same day: 50 seeds in about 0.47 s (106–108 seeds/s).** The first figure was taken while the machine was slowed by something else and should be ignored. |
| Speed, wasm under Node | First run: 4.38 s (11.4 seeds/s). Re-measured: about 2.40 s (20.8 seeds/s), so wasm is **about 5x slower** than native, not 1.75x as the first run suggested |
| Speed in a browser | **Not measured** (UNVERIFIED). Will be measured in P2 with Playwright. |
| TinyGo | **Not tested**: not installed here. Standard Go already fits the budget proposed below, so TinyGo is only an optimisation for later. |

These are spike numbers. In P2 they get a reproducible command in the repo and a file in
`results/` before they appear in the README.

### 2.2 Porcupine visualisation (v1.3.1, read from the module source)

`porcupine.CheckOperationsVerbose`, `Visualize(model, info, io.Writer)`, `VisualizePath`, and
`LinearizationInfo.AddAnnotations([]Annotation)` all exist in v1.3.1 (`visualization.go`). The
annotations API means the fault schedule (crashes, partitions, pauses) can be drawn on the same
timeline as the client operations, which is a cheap, high-value addition in P1.5.

### 2.3 ShiViz input format

ShiViz reads a plain-text log and a regular expression with named groups. The minimum fields are
`host`, `event` and `clock`, and the clock is a JSON vector clock. There is no separate notion of
messages: vector clocks alone order the events. Sources: the ShiViz TOSEM 2020 paper
([UBC PDF](https://www.cs.ubc.ca/~bestchai/papers/tosem20-shiviz.pdf)) and a worked example
([gist](https://gist.github.com/dariusf/7e4eae9736efedc958f4386cb53ce647)). The exact regex will be
tested against the real ShiViz web app when P1.5 is built.

### 2.4 Prior art for the website (only what sources say)

- **RaftScope** by Diego Ongaro: its repository describes it as a "super hacky visualization of
  raft" inspired by The Secret Lives of Data; a replay version is linked from Ongaro's 2015 talk
  slides at raft.github.io/raftscope-replay ([repo](https://github.com/ongardie/raftscope),
  [slides](https://Raft.github.io/slides/buildstuff2015.pdf)). Whether the live page works today
  was not checked.
- **The Secret Lives of Data** (thesecretlivesofdata.com/raft, repo benbjohnson/thesecretlivesofdata):
  described as a guided, tutorial-style walkthrough of each phase of Raft, "more guided and less
  interactive" than RaftScope (summarised from search results, not checked on the site itself).
- **Intended difference** (still a goal, nothing is claimed yet): the site runs the actual tested
  Go implementation with fault injection, invariant checks, linearizability checks, replay and
  shrinking, rather than an animation or a separate model of the algorithm. Before saying that
  publicly, the owner looks at both sites and confirms the wording.

### 2.5 Hosting and analytics

- **GitHub Pages** (official limits page): published site at most 1 GB, soft bandwidth limit
  100 GB/month, deployments time out after 10 minutes, and the 10 builds/hour soft limit does not
  apply to custom Actions workflows ([docs](https://docs.github.com/en/pages/getting-started-with-github-pages/github-pages-limits)).
  Arithmetic, not a measurement: at about 0.85 MB of compressed wasm per visit, 100 GB is roughly
  100,000 wasm loads a month. That assumes Pages serves the wasm compressed, which is `[TO VERIFY]`
  at build time. Otherwise it is about 3 MB per load and about 33,000 loads.
- **GoatCounter**: cookieless, open source, hosted for free for "reasonable public usage", and can
  be self-hosted ([site](https://goatcounter.com/),
  [README](https://raw.githubusercontent.com/arp242/goatcounter/main/README.md)). It is the
  proposed analytics option; custom-event support for "challenge solved" is `[TO VERIFY]` at build
  time.

---

## 3. P1 Replay debugger: design, with the places I differ from the brief

**Architecture (the main change from the brief):** one engine package, two frontends. A new
`debug/` package owns a simulated run and exposes `Open(opts)`, `Step(n)`, `RunTo(pred)`,
`Back(n)`, `State(node)`, `InFlight()` and `Events()`. `cmd/raftlab-debug` is a terminal frontend
over it, and the P2 website is a second frontend calling the same API through wasm. That way
everything P1 builds is reused in P2 for free, and the CLI and the site can never disagree.

### P1.1 Structured trace
- Typed `trace.Event` values: virtual time, step, node, kind, and the fields from the brief.
  Events are emitted by `sim` (network, disk, timers) and `harness` (faults, clients, invariants)
  and through the existing `peer.Observer` (role, term, commit, apply). **`raft/` stays
  I/O-free**: it emits nothing itself.
- The hash comes from a canonical **binary** encoding of each event, not from JSON: it is faster,
  and it avoids float and field-order formatting issues. JSON Lines (`trace_version: 1`) is only
  produced when a sink is attached.
- Changing what gets hashed changes every trace hash. No test stores literal hashes (the replay
  tests compare two runs), so nothing breaks, but this is called out in the commit.
- Overhead measured as seeds/s for no sink, a hash-only sink and a JSONL sink, over the same 500
  seeds, saved in `results/trace-overhead.json`.

### P1.2 Time travel: re-execution, not checkpoints (I differ from the brief)
- In-memory checkpoints would need a deep copy of the `World`. Its event heap is made of Go
  closures that capture peers, disks and RNGs, and Go cannot copy closures. Checkpointing would
  mean rewriting the simulator so all state is plain data, a large refactor that touches the
  determinism core.
- Re-execution is cheap: a full default run takes about 10 ms native and about 50 ms in wasm (spike
  figures above, re-measured). `Back(n)` re-runs to step k − n. I will still measure it (`results/debug-back.json`:
  latency of `Back` at the start, middle and end of runs) as the brief asks. Checkpoints only get
  reconsidered if p99 goes above 200 ms.
- Breakpoints: **Option A (recommended)** is a fixed set of predicates (`leader <node>`,
  `term > k`, `commit <node> > k`, `kind <event>`, `violation`, `time > T`) combined with `and`.
  **Option B** embeds an expression library (a new dependency and a bigger wasm). A covers every
  example in the brief.

### P1.3 Shrinking
- **Fault schedule as data.** Today faults are drawn from the fault stream when they happen, and
  leader-biased faults pick their target from live state. New "record" mode logs every *resolved*
  fault (time, kind, concrete node ids, duration) into a `Schedule`. "Replay" mode executes a
  `Schedule` and never draws fault decisions from the RNG.
- **Required split:** the fault stream also drives tick jitter and wake-up jitter
  (`harness.go` around lines 303–338). These move to their own `jitter` stream, so replaying a
  schedule consumes exactly the same jitter draws. **Cost:** this changes the behaviour of every
  seed, so the bug-zoo regression seeds will move. `make bugzoo` gets re-run, and
  `harness_test.go`, `results/bugzoo.json`, the README and `docs/BUGZOO.md` are updated in the
  same commit. Swarm testing (P8.2a) changes per-seed configuration in the same
  place, so I propose doing both in this one seed-moving commit.
- **Delta debugging** over the schedule (ddmin on the fault list), then client operation budget,
  client count, network drop/dup rates set to 0 where possible, and 5 → 3 nodes. "Still fails" means
  the **same property name** fails (the exact detail text may differ). Time shortens on its own,
  because the run stops at the violation.
- Honest caveat: the seed still drives message delays, so a minimised failure is a seed plus a
  shorter fault list, not a randomness-free script. Removing a fault changes message counts and
  therefore which delay goes with which message. ddmin tolerates that; it just tries more cases.
- Output `testdata/schedules/<mutation>.json` (seed, options, faults), which replays exactly.
  Before/after sizes for all 8 mutations are saved in `results/shrink.json`.

### P1.4 Diff
Run two event streams in lockstep and report the first step whose canonical event differs, with
N events of context and the node states at that point.

### P1.5 Causality
The harness keeps a vector clock per node, ticks it on send and merges it on deliver, and stores
it on message events. Export the ShiViz format (2.3). Add fault annotations to the Porcupine view
through `AddAnnotations` (2.2).

### P1.6 Regression corpus
A table test in `harness/` replays every `testdata/schedules/*.json` file and asserts the
expected violation. All of them are replayed with mutations off and must pass.

### P1 tests, risks, effort
- **Files touched:** `sim/world.go`, `sim/network.go`, `sim/disk.go` (event emission), new
  `trace/`, new `debug/`, `harness/` (schedule, streams, vector clocks, corpus test),
  `cmd/raftlab-debug`, `cmd/raftlab-chaos` (`-shrink`, `-schedule`, `-diff`), `docs/DEBUGGER.md`,
  README, BUGZOO, and the results files above.
- **Risks:** moving the bug-zoo seeds (planned, see P1.3); memory use for long traces (events
  stream to a writer and are never all kept by default); shrinking cost (each attempt is about
  10 ms native, so a few hundred attempts take seconds).
- **Effort:** trace 8–12 h, engine and CLI 10–15 h, schedule and shrinker 12–20 h (including the
  bug-zoo re-run), diff 3–4 h, causality 4–6 h, corpus 2 h, docs 4 h. **About 45–65 h.**

### The first three P1 tests
1. **`TestTrace_SinkDoesNotChangeExecution`**: the same seed run with no sink, a hash sink and a
   JSONL sink gives the same trace hash and step count, and two JSONL runs are byte-identical. It
   checks that observing a run never changes it.
2. **`TestSchedule_RecordThenReplayIsIdentical`**: for 200 seeds plus every bug-zoo regression
   seed, run in record mode, then run again from only the recorded schedule (fault decisions not
   drawn from the RNG). Trace hash and verdict must match. This is the property that makes
   shrinking and shareable links valid.
3. **`TestDebug_BackMatchesFreshRun`**: step to k, go back n, and compare node states and the trace
   hash prefix with a fresh run stepped to k − n. Also, a `violation` breakpoint on
   the commit-prev-term regression seed stops at the same virtual time as `Result.ViolationAt`.

---

## 4. P2 Raftlab in the browser: design differences

- **Toolchain:** standard Go wasm (measured 0.85 MB gzip). **Budget:** HTML, CSS and JS ≤ 200 KB
  compressed, wasm ≤ 1.2 MB compressed and loaded lazily on the first run. TinyGo stays an untested
  optimisation.
- **The engine runs in a Web Worker**, so the UI never blocks; it talks to the P1 `debug/` API
  through `syscall/js` with JSON messages.
- **Frontend: Option A (recommended)** is plain TypeScript and SVG built with esbuild, with no
  framework: small, few dependencies, and easy to read in an interview. **Option B** is Svelte or
  Preact, faster to build richer UI but more dependencies and churn. A cluster of at most 5 nodes
  does not need a framework.
- **Clicked faults use the P1 schedule.** Each click becomes a schedule entry at the current
  virtual time. A session is seed + options + schedule, compressed into the URL fragment, so share
  links need no server. This makes P1.3 (schedule as data) a hard prerequisite for P2.
- **Hash parity in CI:** build wasm, run N seeds under Node with `wasm_exec_node.js`, and compare
  with native (the spike already passes this). A Playwright test repeats it in a real browser and
  measures browser seeds/s.
- **Learn pages:** the owner writes the text (good ownership work) and I build the preset
  scenarios.
- **"Real people use it" is a launch task, not a build task:** blog post, a Show HN/Reddit post,
  and mailing course instructors. It goes in the plan as its own step with its own numbers.
- **Files:** `web/` (TS, CSS, HTML), `cmd/raftlab-wasm`, `.github/workflows/pages.yml`, Playwright
  tests, `docs/SITE.md`.
- **Risks:** frontend scope creep (mitigation: ship an MVP with seed mode, cluster view, bug-zoo
  challenge and share links first); whether Pages compresses wasm; memory and GC on phones; CI time
  for Playwright.
- **Effort:** wasm bridge and worker 6–8 h, cluster view and time controls 15–20 h, fault clicks
  and share links 8–10 h, bug-zoo challenge 6–8 h, replay viewer 3–4 h, linearizability view 3–4 h,
  learn pages 10–15 h, engineering page from `results/` 4–6 h, Playwright and CI 5–8 h,
  accessibility and mobile 5 h. **About 65–90 h.**

---

## 5. P3–P8, one paragraph each

**P3.1 Pre-vote** (thesis §9.6). 8–12 h, low risk, depends on nothing. Mutation: skip the
pre-vote check, caught by a new liveness-disruption check after healed partitions. **Recommend.**

**P3.2 Leadership transfer** (thesis §3.10, TimeoutNow). 6–10 h, low risk. Mutation: transfer to
a follower that is not caught up. It also gives the real server a graceful shutdown.
**Recommend.**

**P3.3 Membership change.** 25–40 h, **high risk**, the highest interview value in P3. I
recommend single-server changes with the known fix (the leader commits an entry of its own term
before changing configuration) over joint consensus: smaller, and it matches the thesis.
Reference: Ongaro's raft-dev post about the single-server bug `[TO VERIFY]`. TLA+ extension only at
tiny bounds. **Recommend, after pre-vote and transfer.**

**P3.4 Lease reads** (thesis §6.4.1). 10–15 h, medium risk. The first feature where clock skew
becomes a *safety* fault, so the skew model the harness already has starts to matter. The
mutation (lease too long for the drift bound) should produce stale reads. **Recommend.**

**P3.5 Session expiry** driven by the log (thesis §6.3). 6–8 h, low risk. Mutation: expire on
local time. **Recommend.**

**P3.6 Slow disk plus liveness bound.** 6–10 h, medium risk: the liveness bound must be stated
carefully or the test becomes flaky. **Recommend.**

**P4.1 Containers** (Dockerfile, compose for 3 and 5 nodes, graceful shutdown). 6–8 h, low risk.
**Recommend.**

**P4.2 Real fault testing.** 25–40 h, medium-high risk. netem/iptables need `NET_ADMIN` inside
containers; toxiproxy is the alternative (tool choice `[TO VERIFY]` when it is built). It reuses
`check/`. **Recommend**, labelled as non-deterministic as the brief says.

**P4.3 Observability** (metrics, structured logs; `/status` already exists). 6–10 h.
**Recommend.**

**P4.4 Transport.** TLS 6–8 h, recommend. **gRPC: recommend not building** unless P4.7 profiling
shows the transport dominates; fixing gob encoder reuse is probably most of the gain.

**P4.5 Client library.** Leader redirect and session retries already exist; what is left is
timeouts and documented semantics. 3–5 h.

**P4.6 Multi-machine benchmark.** 6–10 h plus setup. Depends on hardware (owner question 2).
Free-tier cloud options `[TO VERIFY]` when this phase starts.

**P4.7 Profiling.** 6–10 h. The gob suspect is confirmed in the code, not measured.
**Recommend.**

**P5.1 TLA+ snapshots** (and membership/pre-vote if built). 10–20 h, medium risk (state
explosion). Recommend at small bounds.

**P5.2 Trace validation.** 25–40 h, high risk, very strong result. Needs the P1 structured
trace. Prior work on validating implementation traces against TLA+ specs `[TO VERIFY and cite
before designing]`. **Recommend**, starting with election and replication only.

**P6 etcd raft adapter.** 15–25 h, medium risk. Module path and API `[TO VERIFY]`. It shows the
harness is reusable. Responsible disclosure if anything looks wrong. **Recommend, late.**

**P7 Multi-Raft sharding.** 80 h or more, high risk, and little extra signal compared with
P3/P4/P5. **Recommend not doing it.**

**P8.1 Automatic mutation testing.** 15–25 h, low-medium risk, independent of everything else.
Recommend a small `go/ast` mutator of our own over an existing tool, because existing Go mutation
tools' maintenance state is `[TO VERIFY]` and the mutator must keep every mutant deterministic. A
very strong headline (unit vs harness mutation score). **Strongly recommend; can run alongside P1.**

**P8.2a Swarm testing.** 4–6 h, cheap, done in the P1.3 seed-moving commit. **Recommend.**
**P8.2b Coverage-guided search.** 25–40 h, research-level risk; read Mallory first `[TO VERIFY]`.
Recommend after P8.1, so survivors can be fed to it.

**P8.3 Open-loop benchmark.** 10–15 h, low risk. Fixes coordinated omission in today's
closed-loop numbers. **Recommend**, and it is needed before P8.8.

**P8.4 Simulate more production code.** 30–50 h, high risk: `net/http` and goroutines in
`server/` make deterministic simulation hard. **Recommend a scoped version only** (transport and
client logic behind interfaces), or skip.

**P8.5 TLC traces replayed in Go.** 10–15 h, medium risk. Needs P1 schedule replay plus a
scripted delivery mode. Strong spec ↔ code link. **Recommend.**

**P8.6 Apalache.** 10–20 h, medium risk, low marginal value now that TLC finished the Figure 8
bounds exhaustively. **Optional, late.**

**P8.7 Own linearizability checker.** 20–30 h. Low signal next to Porcupine, and a risk of subtle
wrongness. **Recommend skipping**, or doing it last.

**P8.8 Parallel leader fsync** (thesis §10.2.1). 8–12 h, medium risk. **Recommend after P8.3.**

**P8.9 Cross-shard transactions.** Depends on P7. **Recommend not doing it.**

---

## 6. Proposed order

0. **Ownership gate** (owner): handleVote, maybeCommit, AppendEntries receiver, ReadIndex.
   `docs/OWNERSHIP.md` and `docs/INTERVIEW.md` start here.
1. **P1.1 trace + P1.3 schedule-as-data + RNG split + P8.2a swarm**: one seed-moving commit, with
   the bug zoo re-run.
2. **P1 rest:** debugger engine and CLI, shrinker, diff, causality, corpus, `docs/DEBUGGER.md`.
3. **P8.1 automatic mutation testing** (can run alongside step 2).
4. **P2 MVP:** seed mode, cluster view, fault clicks, share links, bug-zoo challenge, hash-parity
   CI. Then the replay viewer, linearizability view, learn pages and engineering page. Then launch.
5. **P8.3 open-loop benchmark.**
6. **P3:** pre-vote → session expiry → slow disk and liveness → leadership transfer → lease reads →
   membership change. A new 10,000-seed sweep at the end.
7. **P8.5 TLC → Go**, then **P8.2b guided search**.
8. **P4:** containers → observability → profiling (gob) → real fault testing → TLS → multi-machine
   benchmark.
9. **P8.8 parallel fsync.**
10. **P5:** TLA+ snapshots, then trace validation. Then P8.6 Apalache if still useful.
11. **P6 etcd adapter.**

Not recommended: P7, P8.9, P8.7, gRPC, and P8.4 beyond a scoped version.

Rough total for steps 1–4 (the "debugger as a product" heart): **about 120–170 build hours**.

---

## 7. Questions for the owner

1. **Weekly hours:** still about 6 h a week? It mostly sets the pace of the ownership gate and
   reviews.
2. **Hardware for multi-machine tests:** other laptops or desktops on one LAN, or cloud only?
3. **Priorities:** which matter most? (The proposed order assumes debugger, website, P8.1.)
4. **Blog location** (Q8 is still open).
5. **Ownership gate:** started yet? If not, I will write the two missing exercises
   (AppendEntries receiver, ReadIndex) as instructions only.
6. **Website address:** `hiteshghanchi.github.io/RaftKV` is free; a custom domain costs money.
7. **Analytics:** GoatCounter (cookieless) or none?
