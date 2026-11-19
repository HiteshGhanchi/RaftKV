# TLC runs of spec/Raft.tla

Machine: 12th Gen Intel i5-12500H (16 threads), 7.4 GB RAM, TLC 1.8.0 (tla2tools.jar from the
v1.8.0 GitHub release), OpenJDK 17. Run on 2026-11-19. The disk had under 100 MB free during these
runs (a separate system problem filled it), which capped how large an exhaustive model could be.

| Mode | Bounds (3 servers, 1 value) | Result |
|---|---|---|
| Exhaustive (`./scripts/tlc.sh`) | MaxTerm=2, MaxLogLen=1, MaxMsgs=1 | **Model checking completed. No error has been found.** 1,262,527 states generated, 183,800 distinct, depth 45, 5 s |
| Exhaustive | MaxTerm=2, MaxLogLen=1, MaxMsgs=2 | Not finished: ran out of disk after about 6.4 million distinct states (no violation reported before that) |
| Exhaustive | MaxTerm=3, MaxLogLen=1, MaxMsgs=1 | Not finished: ran out of disk |
| Simulation (`./scripts/tlc.sh simulate 4 3 3 240`) | MaxTerm=4, MaxLogLen=3, MaxMsgs=3 | No violation in at least 81,274,707 states over 1,077,780 random behaviours (240 s; the last progress line before the time limit) |
| Bug config, simulation (`./scripts/tlc.sh bug 5 1 2 240`) | MaxTerm=5, MaxLogLen=1, MaxMsgs=2, CommitPrevTermBug=TRUE | **Not found** in 1,099,023 random behaviours (240 s) |
| Bug config, depth-first (`-dfid 26`) | MaxTerm=4, MaxLogLen=1, MaxMsgs=1, CommitPrevTermBug=TRUE | **Not found** up to depth 26 (2.3 million distinct states, 7 min time limit) |

Properties checked in every run: TypeOK, ElectionSafety, LogMatching, LeaderCompleteness,
StateMachineSafety (invariants) and LeaderAppendOnly (temporal property).

What this does and does not show:
- The correct spec has no violation in the small exhaustive model, and none in a large number of
  random behaviours at bigger bounds. That is evidence, not a proof for all cluster sizes.
- TLC has **not yet** produced the Figure 8 counterexample for the buggy commit rule. By hand, the
  shortest counterexample in this spec is about 36 steps (it needs a restart and two timeouts to
  skip terms), beyond what fit on this machine. The Go side does catch the same bug: the unit test
  `TestFigure8LeaderDoesNotCommitOldTermByCounting` and the chaos harness (5 of 3000 seeds).
- Next step once disk space is available: `./scripts/tlc.sh check 4 1 1` with the bug enabled
  (exhaustive BFS finds the shortest counterexample).
