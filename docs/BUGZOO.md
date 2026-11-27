# Bug zoo

Each entry switches off exactly one rule. The harness must catch every one of them; if it cannot,
the harness is too weak, not the code too good. Numbers come from
`./bin/raftlab-chaos -bugzoo -seeds 3000` (`results/bugzoo.json`, run 2026-11-19); the first failing
seed of each is a regression test in `harness/harness_test.go`.

Replay any of them with its full event trace:

```bash
./bin/raftlab-chaos -seed <first seed> -mutation <name> -v | less
```

Add `-viz out.html` to get Porcupine's visualisation of a non-linearizable history.

| Mutation | Caught | First seed | Detected by |
|---|---|---|---|
| skip-up-to-date-check | 2601/3000 | 1 | LeaderCompleteness |
| no-dedup | 2413/3000 | 1 | Porcupine |
| apply-before-commit | 2361/3000 | 1 | StateMachineSafety |
| skip-fsync | 860/3000 | 3 | LeaderCompleteness |
| stale-read | 55/3000 | 35 | Porcupine |
| accept-stale-term | 40/3000 | 28 | StateMachineSafety |
| commit-prev-term | 7/3000 | 151 | LeaderCompleteness |
| forget-vote | 3/3000 | 179 | ElectionSafety |

**8/8 caught, median seeds to first detection 15.5.**

These numbers changed on 2026-11-27, first when the fault schedule became replayable data and timing
jitter got its own random stream (`docs/DECISIONS.md` V4). That moves every seed's behaviour, so
the first failing seeds are new; the earlier run (2026-11-19) had the same 8/8 with a median of 9.5. They changed slightly again when the harness gained its Progress check (P8.1): a few more runs are caught, the first failing seeds did not move.

### Swarm testing (`-swarm`, `results/bugzoo-swarm.json`)

With swarm testing, each seed also switches off a random subset of fault kinds and picks how
strongly faults aim at the leader. One 3000-seed run of each:

| Mutation | Default: caught | Swarm: caught | Default: first seed | Swarm: first seed |
|---|---|---|---|---|
| skip-up-to-date-check | 2601 | 2338 | 1 | 2 |
| no-dedup | 2413 | 2508 | 1 | 1 |
| apply-before-commit | 2361 | 2152 | 1 | 2 |
| skip-fsync | 860 | 615 | 3 | 3 |
| stale-read | 55 | 51 | 35 | 2 |
| accept-stale-term | 40 | 74 | 28 | 54 |
| commit-prev-term | 7 | 4 | 151 | 1302 |
| forget-vote | 3 | 2 | 179 | 450 |

Swarm reaches its first detection sooner (median 2.5 seeds against 15.5) and catches
`accept-stale-term` more often, but it catches the rarest bugs (`commit-prev-term`, `forget-vote`)
and `skip-fsync` *less* often. This is a single run of each, not repeated, so small differences
are noise. The default stays as it is; swarm is available with `-swarm`.


### Shrunk failures (`raftlab-chaos -shrink-zoo results/bugzoo.json`, `results/shrink.json`)

Each regression seed above was shrunk automatically: delta debugging removes faults from the
recorded schedule while the same property still fails, then it tries fewer nodes and clients, no
message loss or duplication, and shorter phases. The result is saved in
`harness/testdata/schedules/<mutation>.json` and replayed by `TestCorpusSchedulesStillFail`
(which also checks that each schedule passes with correct code).

| Mutation | Seed | Property | Faults | Nodes | Clients | Simulations |
|---|---|---|---|---|---|---|
| commit-prev-term | 151 | LeaderCompleteness | 28 → 18 | 3 → 3 | 4 → 4 | 187 |
| skip-up-to-date-check | 1 | LeaderCompleteness | 16 → 1 | 5 → 5 | 4 → 1 | 16 |
| forget-vote | 179 | ElectionSafety | 13 → 6 | 3 → 3 | 4 → 1 | 64 |
| apply-before-commit | 1 | StateMachineSafety | 19 → 3 | 5 → 5 | 4 → 4 | 35 |
| stale-read | 35 | Linearizability | 22 → 1 | 3 → 3 | 4 → 3 | 29 |
| accept-stale-term | 28 | StateMachineSafety | 24 → 19 | 5 → 5 | 4 → 4 | 125 |
| skip-fsync | 3 | LeaderCompleteness | 63 → 3 | 3 → 3 | 4 → 2 | 42 |
| no-dedup | 1 | Linearizability | 25 → 0 | 5 → 5 | 4 → 1 | 14 |

`no-dedup` needs no faults at all: message loss and duplication alone make a retried append take
effect twice. The two rarest bugs (`commit-prev-term`, `accept-stale-term`) shrink least: removing
almost any fault changes the timing they depend on. The shrunk runs can be longer than the
originals, because with fewer faults the bug surfaces later. Open one in the debugger with
`raftlab-debug -repro harness/testdata/schedules/<mutation>.json`.

---

### skip-up-to-date-check
- **Breaks**: RequestVote receiver rule 2 / §5.4.1: vote only for candidates whose log is at least
  as up-to-date as yours.
- **What goes wrong**: a node that missed committed entries can win an election and then overwrite
  them.
- **How it is caught**: when the stale node becomes leader, `LeaderCompleteness` finds an applied
  entry missing from its log. Common, because elections happen all the time under faults.

### apply-before-commit
- **Breaks**: All Servers rule 1 (apply only up to commitIndex).
- **What goes wrong**: an entry that later gets overwritten has already changed the state machine,
  and clients may have been told it succeeded.
- **How it is caught**: two nodes apply different entries at the same index → `StateMachineSafety`.

### no-dedup
- **Breaks**: client sessions (thesis §6.3).
- **What goes wrong**: a write whose reply was lost is retried and applied twice. With append, the
  value visibly contains the same suffix twice.
- **How it is caught**: Porcupine finds no order of operations that explains a read showing a
  doubled append. It needs a lost reply or a timeout, which the lossy client links and pauses supply.

### skip-fsync
- **Breaks**: "persist before responding".
- **What goes wrong**: a node acknowledges entries or votes, crashes, and comes back without them;
  the cluster had counted it towards a majority.
- **How it is caught**: mostly `LeaderCompleteness` (a leader elected with the help of a node that
  forgot entries). It needs a crash that loses unsynced data, which `sim.Disk.Crash` does.

### stale-read
- **Breaks**: ReadIndex leadership confirmation (thesis §6.4).
- **What goes wrong**: a partitioned or paused old leader serves a read from its own state after a
  new leader has committed newer writes.
- **How it is caught**: Porcupine (a read returning an old value after a newer write finished).
  Visible only while a deposed leader still believes it leads, so it needs seeds where CheckQuorum
  is off or a pause hides the new leader; that's why it is only 55/3000.

### accept-stale-term
- **Breaks**: AppendEntries receiver rule 1.
- **What goes wrong**: a deposed leader's late AppendEntries overwrite entries the new leader
  already committed.
- **How it is caught**: the follower would truncate a committed entry
  (`CommittedNeverTruncated`), or two nodes end up applying different entries
  (`StateMachineSafety`). Needs late messages from an old leader: delay spikes and pauses.

### commit-prev-term (the Figure 8 hazard)
- **Breaks**: Figure 2 Leaders rule 4's `log[N].term == currentTerm`.
- **What goes wrong**: exactly Figure 8. A leader commits an old-term entry because a majority
  stores it, crashes, and a server with a newer conflicting entry is elected and overwrites it.
- **How it is caught**: `LeaderCompleteness` when the overwriting leader is elected. Rare (7/3000).
  The schedule needs about five elections in a specific order, and the leader's no-op usually rides
  in the same message as the old entry, so the buggy rule rarely acts alone. Seeds with one entry
  per message make it possible. The unit test
  `TestFigure8LeaderDoesNotCommitOldTermByCounting` shows it directly, and TLC finds the same bug
  in the TLA+ spec (`CommitPrevTermBug = TRUE`): a shortest 39-state counterexample in
  `results/tlc-figure8-trace.txt`.

### forget-vote
- **Breaks**: votedFor is persistent state.
- **What goes wrong**: a node votes, crashes, forgets, and votes again in the same term for a
  different candidate. Two leaders in one term.
- **How it is caught**: `ElectionSafety`. Rarest (3/3000): it needs a crash between granting a vote
  and a second candidate of the *same* term asking.

---

## Bugs found in raftlab's own code

See the README section of the same name. The short version: a false positive in the harness's
Leader Completeness check (7 of 500 correct seeds), a fault model that initially caught only half
the zoo reliably, two persistence bugs found in review before they could ship, and two evaluation
errors in the TLA+ spec found by TLC.
