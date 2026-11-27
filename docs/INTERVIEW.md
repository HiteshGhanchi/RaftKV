# Likely interview questions

Thirty questions an interviewer is likely to ask about this repository. **The answers are the
owner's, written in their own words** after the ownership exercises; a short answer you can say
out loud beats a long one you have to read. Pointers say where in the repo the answer can be
checked. The list is updated at the end of every phase.

## Raft protocol

1. Walk me through a write, from the client's request to the reply. Where can it be lost, and how does the client find out? *(peer/, kv/, client/)*
   - Answer:
2. Prove Election Safety in two sentences. *(BUILD_GUIDE step 6)*
   - Answer:
3. Why does the vote up-to-date check compare the last term before the log length? *(raft/log.go `isUpToDate`)*
   - Answer:
4. Draw Figure 8. Why can't a leader commit an old-term entry by counting replicas? *(results/tlc-figure8-trace.txt)*
   - Answer:
5. Why does a new leader append a no-op? *(thesis §6.4)*
   - Answer:
6. What must be on disk before a node replies to a vote or an AppendEntries, and why? *(peer/peer.go Ready loop)*
   - Answer:
7. How does ReadIndex avoid stale reads without writing to the log? What does `stale-read` break? *(BUILD_GUIDE step 8)*
   - Answer:
8. Compare ReadIndex, log reads and lease reads. When is each unsafe? *(BUILD_GUIDE step 8)*
   - Answer:
9. What is CheckQuorum for, and what would happen without it? *(raft/raft.go `checkQuorum`)*
   - Answer:
10. How do snapshots interact with followers that are far behind? *(raft/raft.go `sendAppend`, `handleSnapshot`)*
    - Answer:
11. How do client sessions make retries take effect exactly once? What happens without them (`no-dedup`)? *(kv/kv.go)*
    - Answer:
12. What is the difference between probing and replicating a follower? *(BUILD_GUIDE step 7)*
    - Answer:

## Testing and verification

13. What does "deterministic simulation" mean here, and what had to be true of the code for it to work? *(sim/, docs/DECISIONS.md)*
    - Answer:
14. Which invariants does the harness check, and when? *(harness/harness.go)*
    - Answer:
15. What is linearizability, and how does Porcupine check it? Why partition by key? *(check/history.go)*
    - Answer:
16. Why does a checker timeout count as inconclusive and not as a pass? *(check/)*
    - Answer:
17. What is the bug zoo, and why do some mutations take thousands of seeds to catch? *(docs/BUGZOO.md)*
    - Answer:
18. Tell me about a bug the harness found in your own code. *(README "Bugs the harness found")*
    - Answer:
19. What does the TLA+ spec check, what doesn't it check, and how does it relate to the Go code? *(spec/, results/tlc.md)*
    - Answer:
20. TLC passed at terms ≤ 4. Does that prove Raft correct? *(results/tlc.md caveats)*
    - Answer:
21. How do you simulate a disk crash, and what is a torn write? *(sim/disk.go, storage/wal.go)*
    - Answer:

## Engineering and performance

22. Why is the Raft core a pure state machine with `Ready`/`Advance`? What does it buy you? *(raft/types.go)*
    - Answer:
23. Where does batching come from, and why is it safe? *(BUILD_GUIDE step 8, results/bench-3n-32c-nobatch.json)*
    - Answer:
24. Your benchmark shows fsync costs about 5x. Why, and how would you reduce it safely? *(docs/BENCHMARKS.md, thesis §10.2.1)*
    - Answer:
25. What are the limits of your benchmark numbers? *(docs/BENCHMARKS.md)*
    - Answer:
26. How does the WAL recover from a crash in the middle of a write? *(storage/wal.go)*
    - Answer:
27. Why HTTP + gob, and what would you change for production? *(docs/DECISIONS.md, transport/http.go)*
    - Answer:

## Judgment

28. What is missing before anyone should run this in production? *(README Limitations)*
    - Answer:
29. If you had another month, what would you build next and why? *(docs/PLAN_V2.md)*
    - Answer:
30. Which part of this project are you least sure is correct, and how would you find out? *(open)*
    - Answer:
