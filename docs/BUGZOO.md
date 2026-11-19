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
| skip-up-to-date-check | 2600/3000 | 1 | LeaderCompleteness |
| apply-before-commit | 2344/3000 | 1 | StateMachineSafety |
| no-dedup | 2394/3000 | 1 | Porcupine |
| skip-fsync | 883/3000 | 1 | LeaderCompleteness |
| stale-read | 52/3000 | 18 | Porcupine |
| accept-stale-term | 47/3000 | 27 | StateMachineSafety |
| commit-prev-term | 5/3000 | 262 | LeaderCompleteness |
| forget-vote | 2/3000 | 1915 | ElectionSafety |

**8/8 caught, median seeds to first detection 9.5.**

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
  is off or a pause hides the new leader; that's why it is only 52/3000.

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
- **How it is caught**: `LeaderCompleteness` when the overwriting leader is elected. Rare (5/3000).
  The schedule needs about five elections in a specific order, and the leader's no-op usually rides
  in the same message as the old entry, so the buggy rule rarely acts alone. Seeds with one entry
  per message make it possible. The unit test
  `TestFigure8LeaderDoesNotCommitOldTermByCounting` shows it directly.

### forget-vote
- **Breaks**: votedFor is persistent state.
- **What goes wrong**: a node votes, crashes, forgets, and votes again in the same term for a
  different candidate. Two leaders in one term.
- **How it is caught**: `ElectionSafety`. Rarest (2/3000): it needs a crash between granting a vote
  and a second candidate of the *same* term asking.

---

## Bugs found in raftlab's own code

See the README section of the same name. The short version: a false positive in the harness's
Leader Completeness check (7 of 500 correct seeds), a fault model that initially caught only half
the zoo reliably, two persistence bugs found in review before they could ship, and two evaluation
errors in the TLA+ spec found by TLC.
