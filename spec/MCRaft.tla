---- MODULE MCRaft ----
\* Model-checking wrapper: adds the symmetry set so TLC treats servers as interchangeable.
EXTENDS Raft, TLC
Symmetry == Permutations(Server)
====
