# TLC runs of spec/Raft.tla

Machine: 12th Gen Intel i5-12500H (16 threads), 7.4 GB RAM, TLC 1.8.0 (tla2tools.jar from the
v1.8.0 GitHub release), OpenJDK 17, `-Xmx1700m`, `-workers auto`.

All runs use 3 servers and 1 value. Properties checked in every run: TypeOK, ElectionSafety,
LogMatching, LeaderCompleteness, StateMachineSafety (invariants) and LeaderAppendOnly (temporal
property). Bounds are written as (MaxTerm, MaxLogLen, MaxMsgs).

## Figure 8 counterexample (bug switched on)

`TLC_FILE_LIMIT_KB=1500000 ./scripts/tlc.sh bug 4 1 1`, run 2026-11-19:

> **Invariant LeaderCompleteness is violated.** 271,922,292 states generated, 39,347,865 distinct,
> search depth 41, 9 min 59 s.

The full trace (39 states) is in [`tlc-figure8-trace.txt`](tlc-figure8-trace.txt). Because the
search is breadth-first, this is a shortest counterexample within these bounds. In outline:

1. s1 wins term 1 and appends an entry, then restarts before replicating it.
2. s2 wins term 2 with s3's vote, appends its own entry, and restarts too.
3. s1 times out again, wins term 3 with s3's vote, and replicates its **term-1** entry to s3.
4. Under the buggy rule s1 commits that entry because a majority (s1, s3) stores it, even though it
   is not from s1's current term.
5. s2, whose log ends in a higher-term entry, wins term 4 with s1's vote. Its log does not contain
   the committed entry, so LeaderCompleteness fails (and s2 would overwrite the entry).

The same bug is also caught on the Go side by the unit test
`TestFigure8LeaderDoesNotCommitOldTermByCounting` and by the chaos harness (5 of 3000 seeds,
`results/bugzoo.json`).

## Correct spec (bug off)

| Mode | Bounds | Result | Date |
|---|---|---|---|
| Exhaustive (`./scripts/tlc.sh`) | (2, 1, 1) | **No error has been found.** 1,262,527 generated, 183,800 distinct, depth 45, 5 s | 2026-11-19 |
| Exhaustive (`check 2 2 1`) | (2, 2, 1) | **No error has been found.** 1,650,315 distinct, depth 57, 30 s | 2026-11-19 |
| Exhaustive (`check 3 1 1`) | (3, 1, 1) | **No error has been found.** 6,518,959 distinct, depth 64, 1 min 41 s | 2026-11-19 |
| Exhaustive (`check 4 1 1`) | (4, 1, 1) | **Not finished** (45 min limit). No violation in the 636,786,345 states generated / 87,461,124 distinct explored, up to depth 52; 4.6 million states still queued | 2026-11-19 |
| Exhaustive (`check 3 2 1`) | (3, 2, 1) | **Not finished** (45 min limit). No violation in the 484,625,011 states generated / 71,495,811 distinct explored, up to depth 54; 4.0 million states still queued | 2026-11-20 |
| Simulation (`simulate 4 3 3 240`) | (4, 3, 3) | No violation in at least 81,274,707 states over 1,077,780 random behaviours (240 s) | 2026-11-19 |

Numbers for the unfinished runs come from TLC's last progress line before the time limit.

## What this does and does not show

- TLC finds the Figure 8 bug when the commit rule ignores the term check, and the same bounds with
  the correct rule show no violation for at least 87 million distinct states, more than twice the
  number that was enough to find the bug (39 million, depth 41). The (4, 1, 1) run did not
  finish, so it is strong evidence, not a complete check.
- The correct spec passes exhaustively at (2, 1, 1), (2, 2, 1) and (3, 1, 1). This is evidence for
  3 servers within these bounds, not a proof for all cluster sizes or log lengths.
- TLC checks the spec, not the Go code. The link is the shared property names (see the mapping in
  the README).

## Earlier runs while the disk was nearly full (2026-11-19, superseded)

Before disk space was freed, (2, 1, 2) and (3, 1, 1) ran out of disk; a simulation at (5, 1, 2)
with the bug on (1,099,023 behaviours, 240 s) and a depth-first search to depth 26 at (4, 1, 1)
with the bug on (2.3 million distinct states) did **not** find the counterexample. The breadth-first
run above shows why: the shortest counterexample is 38 steps, deeper than the depth-first limit
and rare among random behaviours.
