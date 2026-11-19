# Protocol map

Where each rule of the Raft paper lives in the code, and how each safety property is checked in Go
and in TLA+. "Figure 2" means Figure 2 of Ongaro & Ousterhout, *In Search of an Understandable
Consensus Algorithm (Extended Version)*; "thesis" means Ongaro's PhD thesis, *Consensus: Bridging
Theory and Practice*.

## Figure 2 → code

| Rule | Code | Test |
|---|---|---|
| Persistent state: currentTerm, votedFor, log on stable storage before responding | `raft.Ready` (HardState, Entries, `MustSync`); `peer.process` persists before `Send`; `storage.Save` fsyncs | `TestVoteGrantedOncePerTerm`, `TestRestartRestoresState`, bug zoo `forget-vote`, `skip-fsync` |
| All Servers 1: apply committed entries in order | `raftLog.nextCommitted` (up to `committed`); `peer.apply` | bug zoo `apply-before-commit` |
| All Servers 2: higher term → update term, become follower | `Node.Step` (first `switch`) → `becomeFollower` | `TestHigherTermMakesLeaderStepDown` |
| Followers: respond to RPCs; time out → candidate | `Node.Tick`, `Node.Step` | `TestFollowerCampaignsAfterElectionTimeout` |
| Candidates: increment term, vote self, reset timer, send RequestVote | `Node.campaign` | same |
| Candidates: majority → leader | `handleVoteResp` → `becomeLeader` | `TestCandidateWithMajorityBecomesLeader`, `TestCandidateWithoutMajorityStaysCandidate` |
| Candidates: AppendEntries from new leader → follower | `Node.Step` (case `MsgApp`/`MsgHeartbeat`/`MsgSnap`) | Figure 7 tests |
| (Implicit) retry RPCs that get no response | candidate vote retry in `Node.Tick`; leader heartbeats re-trigger `sendAppend` | `TestCandidateRetriesVoteRequests` |
| Leaders: heartbeats | `Node.bcastHeartbeat` from `Tick` | chaos runs |
| Leaders: client command → append, respond after applied | `Node.Propose`; `peer.apply` completes the callback if the term matches | `TestReplicationCommitsAndApplies`, `TestWriteReadRestart` |
| Leaders: nextIndex/matchIndex; decrement on failure | `progress`, `sendAppend`, `handleAppResp` (with hint and probe/replicate) | `TestLeaderBacksUpWithHint`, Figure 7 |
| Leaders 4: commit N if a majority has it **and log[N].term == currentTerm** | `Node.maybeCommit` | `TestFigure8LeaderDoesNotCommitOldTermByCounting`, bug zoo `commit-prev-term` |
| RequestVote 1: reply false if term < currentTerm | `Node.Step` (case `m.Term < n.term`) | `TestStaleTermRequestsRejected` |
| RequestVote 2: votedFor null/candidate and log up-to-date (§5.4.1) | `Node.handleVote`, `raftLog.isUpToDate` | `TestVoteRejectedForStaleLog`, bug zoo `skip-up-to-date-check` |
| AppendEntries 1: reply false if term < currentTerm | `Node.Step` | `TestStaleTermRequestsRejected`, bug zoo `accept-stale-term` |
| AppendEntries 2: reply false if prevLogIndex/prevLogTerm don't match | `raftLog.maybeAppend` | `TestAppendEntriesReceiverRules` |
| AppendEntries 3: conflict → delete it and all that follow | `raftLog.maybeAppend`; on disk `storage.replay` | `TestAppendEntriesReceiverRules`, `TestOverwrittenSuffixIsDroppedOnReload` |
| AppendEntries 4: append new entries | `raftLog.maybeAppend` | same |
| AppendEntries 5: commitIndex = min(leaderCommit, last new index) | `raftLog.maybeAppend` → `commitTo` | same |

## Beyond Figure 2

| Feature | Source | Code | Test |
|---|---|---|---|
| No-op on election | thesis §6.4 | `becomeLeader` | `TestCandidateWithMajorityBecomesLeader` |
| ReadIndex reads | thesis §6.4 | `Node.ReadIndex`, `processReads`, `peer.Read`/`serveReads` | `TestReadIndexNeedsQuorum`, bug zoo `stale-read` |
| Client sessions (exactly-once effect) | thesis §6.3 | `kv.Store.Apply` | `TestRetriedCommandAppliesOnce`, bug zoo `no-dedup` |
| CheckQuorum | thesis §6.2 | `Node.checkQuorum` | `TestCheckQuorumDeposesIsolatedLeader` |
| Snapshots, InstallSnapshot | paper §7, Figure 13 | `raftLog.compact`/`restore`, `Node.Compact`, `handleSnapshot`, `storage.SaveSnapshot`, `reconcileWithSnapshot` | `TestSnapshotSentToLaggingFollower`, `TestSnapshotRewritesLog`, `TestCrashBetweenSnapshotAndLogRewrite`, `TestSnapshotAndRestartFromSnapshot` |

## Safety properties: Go ↔ TLA+

| Property (paper Figure 3) | Go check | Where it runs | TLA+ (`spec/Raft.tla`) |
|---|---|---|---|
| **Election Safety**: at most one leader per term | `harness.RoleChanged` keeps `term → leader` | every role change, every chaos run | `ElectionSafety` over the `elections` history |
| **Leader Append-Only** | `harness.periodicCheck`: a leader's last index never shrinks within its term | every 50 ms of virtual time | `LeaderAppendOnly` (temporal property) |
| **Log Matching** | `harness.periodicCheck`: for every pair of nodes, once a common index matches by term, every earlier index matches too (term and data) | every 50 ms | `LogMatching` |
| **Leader Completeness** | `harness.RoleChanged`: a new leader of term T holds every applied entry whose commit-term bound is below T | every election | `LeaderCompleteness` over the `committed` history |
| **State Machine Safety** | `harness.Applied`: no two nodes apply different entries at one index | every applied entry | `StateMachineSafety` |
| Committed entries are never truncated (node-local) | `invariant("CommittedNeverTruncated")` in `raftLog.maybeAppend` | always, also in production | implied by `LeaderCompleteness` + `StateMachineSafety` |
| **Linearizability** of client operations | `check.History.Linearizable` (Porcupine) | end of every chaos run | not modelled (the spec has no clients) |

Invariant names in failure reports (for example `LeaderCompleteness: new leader 3 …`) are exactly
the names in this table.
