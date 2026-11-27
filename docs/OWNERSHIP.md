# Ownership log

The first version of this code was written in one pass. To be able to explain and rebuild it, the
owner re-implements the core functions from memory against the existing tests, with review and
hints only (no code given). Each row is filled in by the owner when an exercise is finished.

Exercises: `docs/BUILD_GUIDE.md` steps 6, 7 and 8.

How to do one exercise without risking the working code:

```bash
git switch -c ownership/<function>
```

Delete the function body, rewrite it, and run the checks listed in the exercise. Compare with
`git diff main` only after the tests pass, then write down what was different. Merge only if the
rewrite is at least as clear as the original, or just delete the branch.

## Gate for P1 (all four needed before P1 code starts)

| Function(s) | Date | Checks passed | What I got wrong or had to look up | Hints used |
|---|---|---|---|---|
| `handleVote`, `handleVoteResp` (step 6) | | | | |
| `maybeCommit` (step 7) | | | | |
| `handleAppend`, `raftLog.maybeAppend` (step 7) | | | | |
| `ReadIndex`, `processReads`, `handleHeartbeatResp` (step 8) | | | | |

## Design notes written by the owner before later phases

| Phase | Note (link) | Date | Review outcome |
|---|---|---|---|
| P2 | | | |
| P3 | | | |
| P4 | | | |
| P5 | | | |
