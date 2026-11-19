# Distributed systems for raftlab: teaching notes

Notes for someone who knows programming and Go well but is new to distributed systems. Read with
the code open; each section ends with what to look at in this repository.

**Contents**
1. [The problem: many machines, one answer](#1-the-problem-many-machines-one-answer)
2. [Failures you must assume](#2-failures-you-must-assume)
3. [Majorities (quorums)](#3-majorities-quorums)
4. [Terms: a logical clock for leadership](#4-terms-a-logical-clock-for-leadership)
5. [The replicated log](#5-the-replicated-log)
6. [Commitment and the Figure 8 trap](#6-commitment-and-the-figure-8-trap)
7. [Persistence: what must survive a crash](#7-persistence-what-must-survive-a-crash)
8. [Linearizability: what "correct" means to a client](#8-linearizability-what-correct-means-to-a-client)
9. [Exactly-once effect for retries](#9-exactly-once-effect-for-retries)
10. [Reads are not free](#10-reads-are-not-free)
11. [Snapshots](#11-snapshots)
12. [Deterministic simulation](#12-deterministic-simulation)
13. [Mutation testing](#13-mutation-testing)
14. [TLA+ in one page](#14-tla-in-one-page)
15. [Glossary](#15-glossary)
16. [Check yourself](#16-check-yourself)

---

## 1. The problem: many machines, one answer

One server holding a key-value map is easy, until it dies. Copy the data to three servers and a
new problem appears: if two clients write different values at the same time, and the copies receive
them in different orders, the servers disagree. **Consensus** means getting several machines to
agree on *one sequence of operations* even though some of them crash and messages get lost.

Raft solves it by electing one **leader** that decides the order, and copying that order (the
**log**) to the others. Every server applies the same log in the same order to the same
deterministic state machine, so every server ends in the same state. That idea is called
**state machine replication**.

*In the code*: `kv.Store.Apply` is the deterministic state machine; `raft/` produces the order.

## 2. Failures you must assume

Raft assumes **crash-stop with restart**: a machine may stop at any instant and later come back
with whatever it had saved to disk. Messages may be **lost, delayed, duplicated or reordered**, and
the network may **partition**, sometimes in only one direction. Clocks drift, and a process can
**pause** for hundreds of milliseconds (garbage collection, a VM being moved). Raft does *not*
handle machines that lie (Byzantine faults).

*In the code*: `sim/network.go`, `sim/disk.go` and the fault schedule in `harness/harness.go`
produce every one of these failures.

## 3. Majorities (quorums)

With 5 servers, any two groups of 3 share at least one server. Raft uses that overlap everywhere:

- A leader needs votes from a majority, and each server votes once per term, so two leaders in the
  same term would need two disjoint majorities, which can't exist.
- An entry is committed once a majority stores it. Any future leader also needs a majority's votes,
  so at least one voter has the entry, and the voting rule (section 6) makes sure the winner has it too.

A cluster of 2f+1 servers survives f failures: 3 servers survive 1, 5 survive 2.

## 4. Terms: a logical clock for leadership

Time is divided into numbered **terms**. Each term starts with an election and has at most one
leader. Every message carries the sender's term. Seeing a higher term makes a server adopt it and
become a follower; a message with a lower term is from the past and gets rejected. Terms let a
deposed leader find out, the moment it talks to anyone, that it has been replaced.

*In the code*: the first `switch` in `Node.Step`.

## 5. The replicated log

Each log entry has an index (its position) and the term in which a leader created it. The leader
sends AppendEntries with "the entry before these is at index i with term t". A follower accepts only
if it has that exact entry. This check, applied by induction, gives the **Log Matching** property:
if two logs have an entry with the same index and term, they are identical up to that point. When a
follower's log has diverged, the leader walks back until they agree and overwrites the rest.

*In the code*: `raftLog.maybeAppend` (receiver rules 2–5), `sendAppend`/`handleAppResp` (walking back).

## 6. Commitment and the Figure 8 trap

An entry is **committed** when it is safe to apply: no future leader will ever remove it. Two rules
make that true:

1. **Voting rule (§5.4.1)**: vote only for a candidate whose log is at least as up-to-date as
   yours (a later last term, or the same last term and at least as long). A candidate missing a
   committed entry can't collect a majority, because every majority includes someone who has it.
2. **Commit rule (Figure 8)**: a leader marks an entry committed by counting replicas *only if the
   entry is from its current term*. Older entries become committed indirectly, when a current-term
   entry after them commits.

Why rule 2? In Figure 8 an old entry sits on a majority, yet a server with a *newer-term* entry at
the same index can still win an election (its last term is higher) and overwrite it. Counting
replicas of an old-term entry is not enough. Each new leader writes a no-op entry, so it has a
current-term entry to commit straight away.

*In the code*: `Node.maybeCommit`; `TestFigure8LeaderDoesNotCommitOldTermByCounting`; bug zoo
`commit-prev-term`.

## 7. Persistence: what must survive a crash

Before a server *says* anything that depends on its state, that state must be on disk:
`currentTerm`, `votedFor` and the log. If a server voted, crashed and forgot, it could vote again in
the same term → two leaders (bug zoo `forget-vote`). If it acknowledged entries it never fsynced, a
"committed" entry could vanish (`skip-fsync`).

Disks have their own failure modes: a write that was not fsynced may be lost or only half written
(**torn**). The WAL puts a checksum on every record and, on restart, throws away everything from
the first bad record.

*In the code*: the order in `peer.process` (persist → send → apply); `storage/wal.go`.

## 8. Linearizability: what "correct" means to a client

A system is **linearizable** if every operation appears to take effect at one instant between its
call and its return, and the result matches some sequential order of those instants. In practice:
once a write has returned, every read that *starts* afterwards sees it (or something newer).

Checking a history is NP-complete in general, because you search over orders of overlapping
operations. Porcupine does it quickly for realistic histories, especially when they split into
independent parts (one per key). Operations whose outcome is unknown (the client timed out) may
take effect at any later time or never; the checker must allow both.

*In the code*: `check/history.go`.

## 9. Exactly-once effect for retries

Clients retry when a reply is lost. Without care, an `append("x")` that already committed runs
again. The fix: each client numbers its writes; the replicated state machine remembers each
client's last number and result and returns the saved result for a repeat. Because this table is
part of the replicated state (and of snapshots), any leader can deduplicate.

*In the code*: `kv.Store.Apply`; bug zoo `no-dedup`.

## 10. Reads are not free

A leader can't just answer reads from memory: it might have been replaced a moment ago without
knowing, and serve stale data (bug zoo `stale-read`). Options:

- **Log reads**: put the read through the log. Correct but slow.
- **ReadIndex** (used here): note the commit index, confirm leadership with one heartbeat round
  acknowledged by a majority, wait until that index is applied, then read locally.
- **Leases**: assume no other leader can exist for a while after the last heartbeat. Fastest, but
  safety depends on bounded clock drift.

## 11. Snapshots

The log can't grow forever. Each server periodically saves its state machine (here: the map and
the session table) together with the index and term of the last entry it covers, and deletes the
log up to there. A follower that is too far behind gets the snapshot instead of the entries
(InstallSnapshot). Crash ordering matters: write the snapshot first, then shorten the log.

## 12. Deterministic simulation

The idea, from FoundationDB: run the whole distributed system inside one process, on one thread,
with simulated time, network and disk, all driven by one random seed. Then:

- thousands of runs per minute, because virtual time costs nothing;
- every failure replays exactly from its seed;
- you can inject faults that are hard to cause on real hardware (torn writes, pauses, one-way links).

The price: the system under test can't use real time, real goroutines or real I/O in its core. That
is why `raft.Node` is a pure state machine.

## 13. Mutation testing

If you break the code on purpose and the tests still pass, the tests are weak. A bug zoo is
mutation testing aimed at the rules that matter. The measurements to report are how many mutants
are caught, and how many runs each takes.

## 14. TLA+ in one page

TLA+ describes a system as a set of variables, an initial state and a next-state relation (all the
steps that may happen). TLC explores every reachable state of a small instance (for example 3
servers, terms ≤ 2) and checks that the invariants hold in each one. Small instances find most
design bugs (the "small scope hypothesis"). TLC checks the *spec*, not the Go code; matching
property names and the mapping in `docs/PROTOCOL.md` are what connect them.

## 15. Glossary

| Term | Meaning |
|---|---|
| Quorum | A majority of servers |
| Term | Election epoch; at most one leader per term |
| Commit index | Highest log index known to be committed |
| Applied index | Highest index applied to the state machine (≤ commit) |
| matchIndex / nextIndex | Leader's view of how much of each follower's log matches / what to send next |
| Heartbeat | Empty AppendEntries that keeps followers from starting elections |
| Split vote | An election where no candidate gets a majority; a new term follows |
| CheckQuorum | A leader steps down if it can't reach a majority |
| ReadIndex | Linearizable read via a leadership-confirming heartbeat round |
| Snapshot | Saved state machine replacing a log prefix |
| Linearizable | Operations appear atomic at one point within their call/return interval |
| fsync | Ask the OS to make written data durable on the device |
| Torn write | A write that was interrupted, leaving only part of it on disk |
| Seed | The single number that determines a whole simulated run |

## 16. Check yourself

1. Why do 4 servers tolerate no more failures than 3?
2. A follower has entries the leader does not. When must it delete them, and when must it keep them?
3. Draw Figure 8 from memory and say which rule prevents the bad outcome.
4. Which three values must be on disk before a server replies to RequestVote? Why is commitIndex optional?
5. A write timed out on the client. Can it still take effect? What does the checker do with it?
6. Why is ReadIndex safe without writing to the log, and why does it first need a committed entry from the leader's term?
7. What does a trace hash prove, and what doesn't it prove?
8. `commit-prev-term` was caught in only 5 of 3000 seeds. Name two changes to the harness that would make it more frequent.
9. Why does the leader in this implementation count itself towards a majority only after its own fsync?
10. What can TLC find that the chaos harness cannot, and the other way round?
