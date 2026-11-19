-------------------------------- MODULE Raft --------------------------------
(***************************************************************************)
(* Leader election and log replication from Figure 2 of "In Search of an   *)
(* Understandable Consensus Algorithm (Extended Version)", written for     *)
(* raftlab. It is smaller than Diego Ongaro's raft.tla: no snapshots, no   *)
(* membership changes, no client sessions. Messages live in a set, so the  *)
(* network may reorder or drop them (duplication is not modelled).         *)
(*                                                                         *)
(* Property names match the Go assertions (see docs/PROTOCOL.md):          *)
(*   ElectionSafety, LogMatching, LeaderCompleteness, StateMachineSafety,  *)
(*   LeaderAppendOnly.                                                     *)
(*                                                                         *)
(* CommitPrevTermBug = TRUE removes the "log[N].term = currentTerm" check  *)
(* from the commit rule, which is the raftlab bug zoo's commit-prev-term.  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Server, Value, Nil, MaxTerm, MaxLogLen, MaxMsgs, CommitPrevTermBug

VARIABLES currentTerm, state, votedFor, log, commitIndex,
          votesGranted, nextIndex, matchIndex, messages,
          elections,  \* history: every (term, leader) pair that ever existed
          committed   \* history: every entry a leader ever marked committed

vars == <<currentTerm, state, votedFor, log, commitIndex,
          votesGranted, nextIndex, matchIndex, messages, elections, committed>>

Quorum == {Q \in SUBSET Server : Cardinality(Q) * 2 > Cardinality(Server)}
LastTerm(l) == IF Len(l) = 0 THEN 0 ELSE l[Len(l)].term
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b
MaxOf(S) == CHOOSE x \in S : \A y \in S : x >= y

Send(m) == messages' = messages \cup {m}
Discard(m) == messages' = messages \ {m}
Reply(resp, req) == messages' = (messages \ {req}) \cup {resp}

-----------------------------------------------------------------------------

Init ==
    /\ currentTerm = [s \in Server |-> 0]
    /\ state       = [s \in Server |-> "Follower"]
    /\ votedFor    = [s \in Server |-> Nil]
    /\ log         = [s \in Server |-> << >>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ votesGranted = [s \in Server |-> {}]
    /\ nextIndex   = [s \in Server |-> [t \in Server |-> 1]]
    /\ matchIndex  = [s \in Server |-> [t \in Server |-> 0]]
    /\ messages    = {}
    /\ elections   = {}
    /\ committed   = {}

\* A crash loses volatile state; currentTerm, votedFor and log are persistent.
Restart(i) ==
    /\ state' = [state EXCEPT ![i] = "Follower"]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {}]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [t \in Server |-> 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [t \in Server |-> 0]]
    /\ commitIndex' = [commitIndex EXCEPT ![i] = 0]
    /\ UNCHANGED <<currentTerm, votedFor, log, messages, elections, committed>>

\* Figure 2, Followers/Candidates: on election timeout start an election.
Timeout(i) ==
    /\ state[i] \in {"Follower", "Candidate"}
    /\ currentTerm[i] < MaxTerm
    /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
    /\ state' = [state EXCEPT ![i] = "Candidate"]
    /\ votedFor' = [votedFor EXCEPT ![i] = i]
    /\ votesGranted' = [votesGranted EXCEPT ![i] = {i}]
    /\ UNCHANGED <<log, commitIndex, nextIndex, matchIndex, messages, elections, committed>>

RequestVote(i, j) ==
    /\ state[i] = "Candidate"
    /\ j # i
    /\ Send([mtype |-> "RVReq", mterm |-> currentTerm[i], mlastTerm |-> LastTerm(log[i]),
             mlastIndex |-> Len(log[i]), msrc |-> i, mdst |-> j])
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted,
                   nextIndex, matchIndex, elections, committed>>

\* Figure 2, Candidates: votes from a majority -> leader.
BecomeLeader(i) ==
    /\ state[i] = "Candidate"
    /\ votesGranted[i] \in Quorum
    /\ state' = [state EXCEPT ![i] = "Leader"]
    /\ nextIndex' = [nextIndex EXCEPT ![i] = [t \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] = [t \in Server |-> 0]]
    /\ elections' = elections \cup {[eterm |-> currentTerm[i], eleader |-> i]}
    /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, votesGranted, messages, committed>>

ClientRequest(i, v) ==
    /\ state[i] = "Leader"
    /\ Len(log[i]) < MaxLogLen
    /\ log' = [log EXCEPT ![i] = Append(log[i], [term |-> currentTerm[i], value |-> v])]
    /\ UNCHANGED <<currentTerm, state, votedFor, commitIndex, votesGranted,
                   nextIndex, matchIndex, messages, elections, committed>>

\* Figure 2, Leaders rule 4 (and the Figure 8 fix).
AdvanceCommitIndex(i) ==
    /\ state[i] = "Leader"
    /\ LET Agree(n) == {i} \cup {k \in Server : matchIndex[i][k] >= n}
           ok == {n \in 1..Len(log[i]) : Agree(n) \in Quorum}
           N == IF ok = {} THEN 0 ELSE MaxOf(ok)
       IN /\ N > commitIndex[i]
          /\ CommitPrevTermBug \/ log[i][N].term = currentTerm[i]
          /\ commitIndex' = [commitIndex EXCEPT ![i] = N]
          /\ committed' = committed \cup
                {[idx |-> k, entry |-> log[i][k], cterm |-> currentTerm[i]] : k \in commitIndex[i]+1..N}
    /\ UNCHANGED <<currentTerm, state, votedFor, log, votesGranted, nextIndex, matchIndex, messages, elections>>

\* Leaders send at most one entry per message (raftlab can send many; one
\* keeps the state space small and still covers every interleaving).
AppendEntries(i, j) ==
    /\ state[i] = "Leader"
    /\ j # i
    /\ LET prev == nextIndex[i][j] - 1
           prevTerm == IF prev > 0 THEN log[i][prev].term ELSE 0
           last == Min(Len(log[i]), nextIndex[i][j])
           ents == SubSeq(log[i], nextIndex[i][j], last)
       IN Send([mtype |-> "AEReq", mterm |-> currentTerm[i], mprevIndex |-> prev,
                mprevTerm |-> prevTerm, mentries |-> ents,
                mcommit |-> Min(commitIndex[i], last), msrc |-> i, mdst |-> j])
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted,
                   nextIndex, matchIndex, elections, committed>>

-----------------------------------------------------------------------------
\* Message handlers.

\* All Servers rule 2: a newer term turns any server into a follower. The
\* message stays in the network and is handled again in the new term.
UpdateTerm(m) ==
    LET i == m.mdst IN
    /\ m.mterm > currentTerm[i]
    /\ currentTerm' = [currentTerm EXCEPT ![i] = m.mterm]
    /\ state' = [state EXCEPT ![i] = "Follower"]
    /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
    /\ UNCHANGED <<log, commitIndex, votesGranted, nextIndex, matchIndex, messages, elections, committed>>

\* RequestVote receiver rules 1-2 and the up-to-date check of §5.4.1.
HandleRVReq(m) ==
    LET i == m.mdst
        j == m.msrc
        logOk == \/ m.mlastTerm > LastTerm(log[i])
                 \/ /\ m.mlastTerm = LastTerm(log[i])
                    /\ m.mlastIndex >= Len(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mtype = "RVReq"
       /\ m.mterm <= currentTerm[i]
       /\ IF grant THEN votedFor' = [votedFor EXCEPT ![i] = j] ELSE UNCHANGED votedFor
       /\ Reply([mtype |-> "RVResp", mterm |-> currentTerm[i], mgranted |-> grant,
                 msrc |-> i, mdst |-> j], m)
       /\ UNCHANGED <<currentTerm, state, log, commitIndex, votesGranted, nextIndex, matchIndex, elections, committed>>

HandleRVResp(m) ==
    LET i == m.mdst IN
    /\ m.mtype = "RVResp"
    /\ m.mterm = currentTerm[i]
    /\ IF m.mgranted
          THEN votesGranted' = [votesGranted EXCEPT ![i] = votesGranted[i] \cup {m.msrc}]
          ELSE UNCHANGED votesGranted
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, nextIndex, matchIndex, elections, committed>>

\* AppendEntries receiver rules 1-5.
HandleAEReq(m) ==
    LET i == m.mdst
        j == m.msrc
        logOk == IF m.mprevIndex = 0 THEN TRUE
                 ELSE IF m.mprevIndex > Len(log[i]) THEN FALSE
                 ELSE log[i][m.mprevIndex].term = m.mprevTerm
        index == m.mprevIndex + 1
    IN /\ m.mtype = "AEReq"
       /\ m.mterm <= currentTerm[i]
       /\ \/ \* Rule 1 (stale term) or rule 2 (log mismatch): reject.
             /\ \/ m.mterm < currentTerm[i]
                \/ /\ m.mterm = currentTerm[i]
                   /\ state[i] = "Follower"
                   /\ ~logOk
             /\ Reply([mtype |-> "AEResp", mterm |-> currentTerm[i], msuccess |-> FALSE,
                       mmatch |-> 0, msrc |-> i, mdst |-> j], m)
             /\ UNCHANGED <<state, log, commitIndex>>
          \/ \* A candidate hearing from this term's leader steps down.
             /\ m.mterm = currentTerm[i]
             /\ state[i] = "Candidate"
             /\ state' = [state EXCEPT ![i] = "Follower"]
             /\ UNCHANGED <<log, commitIndex, messages>>
          \/ /\ m.mterm = currentTerm[i]
             /\ state[i] = "Follower"
             /\ logOk
             /\ \/ \* Already have it (or nothing to add): rule 5, then ack.
                   /\ \/ m.mentries = << >>
                      \/ /\ m.mentries # << >>
                         /\ Len(log[i]) >= index
                         /\ log[i][index].term = m.mentries[1].term
                   /\ commitIndex' = [commitIndex EXCEPT ![i] = Max(commitIndex[i], m.mcommit)]
                   /\ Reply([mtype |-> "AEResp", mterm |-> currentTerm[i], msuccess |-> TRUE,
                             mmatch |-> m.mprevIndex + Len(m.mentries), msrc |-> i, mdst |-> j], m)
                   /\ UNCHANGED <<state, log>>
                \/ \* Rule 3: conflicting entry -> delete it and all that follow.
                   /\ m.mentries # << >>
                   /\ Len(log[i]) >= index
                   /\ log[i][index].term # m.mentries[1].term
                   /\ log' = [log EXCEPT ![i] = SubSeq(log[i], 1, index - 1)]
                   /\ UNCHANGED <<state, commitIndex, messages>>
                \/ \* Rule 4: append the new entry.
                   /\ m.mentries # << >>
                   /\ Len(log[i]) = m.mprevIndex
                   /\ log' = [log EXCEPT ![i] = Append(log[i], m.mentries[1])]
                   /\ UNCHANGED <<state, commitIndex, messages>>
       /\ UNCHANGED <<currentTerm, votedFor, votesGranted, nextIndex, matchIndex, elections, committed>>

HandleAEResp(m) ==
    LET i == m.mdst
        j == m.msrc
    IN /\ m.mtype = "AEResp"
       /\ m.mterm = currentTerm[i]
       /\ IF m.msuccess
             THEN /\ nextIndex' = [nextIndex EXCEPT ![i][j] = m.mmatch + 1]
                  /\ matchIndex' = [matchIndex EXCEPT ![i][j] = Max(matchIndex[i][j], m.mmatch)]
             ELSE /\ nextIndex' = [nextIndex EXCEPT ![i][j] = Max(nextIndex[i][j] - 1, 1)]
                  /\ UNCHANGED matchIndex
       /\ Discard(m)
       /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted, elections, committed>>

\* Responses from an older term are ignored.
DropStale(m) ==
    /\ m.mtype \in {"RVResp", "AEResp"}
    /\ m.mterm < currentTerm[m.mdst]
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted,
                   nextIndex, matchIndex, elections, committed>>

\* The network may lose any message.
Drop(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex, votesGranted,
                   nextIndex, matchIndex, elections, committed>>

Receive(m) ==
    \/ UpdateTerm(m)
    \/ HandleRVReq(m)
    \/ HandleRVResp(m)
    \/ HandleAEReq(m)
    \/ HandleAEResp(m)
    \/ DropStale(m)

Next ==
    \/ \E i \in Server : Restart(i)
    \/ \E i \in Server : Timeout(i)
    \/ \E i, j \in Server : RequestVote(i, j)
    \/ \E i \in Server : BecomeLeader(i)
    \/ \E i \in Server, v \in Value : ClientRequest(i, v)
    \/ \E i \in Server : AdvanceCommitIndex(i)
    \/ \E i, j \in Server : AppendEntries(i, j)
    \/ \E m \in messages : Receive(m)
    \/ \E m \in messages : Drop(m)

Spec == Init /\ [][Next]_vars

\* Keeps the model finite for TLC.
StateConstraint ==
    /\ \A i \in Server : currentTerm[i] <= MaxTerm /\ Len(log[i]) <= MaxLogLen
    /\ Cardinality(messages) <= MaxMsgs

-----------------------------------------------------------------------------
\* Properties (paper Figure 3).

\* At most one leader can be elected in a given term.
ElectionSafety ==
    \A e1, e2 \in elections : e1.eterm = e2.eterm => e1.eleader = e2.eleader

\* If two logs contain an entry with the same index and term, the logs are
\* identical in all entries up through that index.
LogMatching ==
    \A i, j \in Server : \A n \in 1..Min(Len(log[i]), Len(log[j])) :
        log[i][n].term = log[j][n].term => SubSeq(log[i], 1, n) = SubSeq(log[j], 1, n)

\* If an entry is committed in a term, it is in the log of every leader of
\* every later term.
LeaderCompleteness ==
    \A i \in Server : state[i] = "Leader" =>
        \A c \in committed : c.cterm < currentTerm[i] =>
            /\ Len(log[i]) >= c.idx
            /\ log[i][c.idx] = c.entry

\* No two different entries are ever committed at the same index.
StateMachineSafety ==
    \A c1, c2 \in committed : c1.idx = c2.idx => c1.entry = c2.entry

\* A leader never overwrites or deletes entries in its log; it only appends.
LeaderAppendOnly ==
    [][\A i \in Server :
        (state[i] = "Leader" /\ state'[i] = "Leader" /\ currentTerm[i] = currentTerm'[i]) =>
            /\ Len(log'[i]) >= Len(log[i])
            /\ SubSeq(log'[i], 1, Len(log[i])) = log[i]]_vars

TypeOK ==
    /\ state \in [Server -> {"Follower", "Candidate", "Leader"}]
    /\ votedFor \in [Server -> Server \cup {Nil}]
    /\ commitIndex \in [Server -> Nat]
=============================================================================
