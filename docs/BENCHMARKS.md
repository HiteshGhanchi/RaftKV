# Benchmarks

## Method (written before the runs)

- **What runs**: `raftlab-bench` starts N `server.Server`s in one process. Each listens on a
  loopback port, has its own data directory on the real disk (`data/bench/…`, ext4 on NVMe, not
  `/tmp`), and fsyncs its WAL before acknowledging, unless `-nosync` is given.
- **Clients**: C goroutines in the same process, each using the real client library over HTTP,
  one outstanding write at a time (the dedup protocol allows only one). Keys rotate over 100 per
  client; values are 100 bytes. Every operation is a `Put`, so it goes through the log, a majority
  fsync and apply.
- **Timing**: 1 s warm-up (not measured), then 10 s measured. Latency is measured per operation
  around `client.Put`, including retries. p50/p99 come from the sorted list of every completed
  operation's latency.
- **Failover**: kill the current leader (`Server.Stop`, which closes its listener without flushing
  anything extra), then time until the next `Put` from a client is acknowledged. The killed node is
  restarted between runs. 20 runs.
- **Machine** (recorded inside every JSON file): 12th Gen Intel i5-12500H (16 threads), Linux
  6.5.0-44, ext4 on `/dev/nvme0n1p5`, Go 1.22.2. The laptop was otherwise idle but not isolated.

Reproduce: `./scripts/bench.sh` (writes `results/bench-*.json`).

## Results (2026-11-19)

| File | Nodes | Clients | Batch | fsync | Ops/s | p50 ms | p99 ms | max ms |
|---|---|---|---|---|---|---|---|---|
| `bench-3n-32c.json` | 3 | 32 | 256 | yes | 5,454 | 5.55 | 12.06 | 26.5 |
| `bench-5n-32c.json` | 5 | 32 | 256 | yes | 4,163 | 5.57 | 24.39 | 35.3 |
| `bench-3n-1c.json` | 3 | 1 | 256 | yes | 473 | 2.08 | 2.90 | 7.1 |
| `bench-3n-32c-nobatch.json` | 3 | 32 | 1 | yes | 667 | 34.06 | 163.54 | 221.9 |
| `bench-3n-32c-nosync.json` | 3 | 32 | 256 | **no** | 27,998 | 0.98 | 2.60 | 27.5 |

Failover (`bench-3n-failover.json`), 20 leader kills, in ms:
164 162 733 162 163 162 163 331 162 164 331 162 162 164 325 326 322 163 162 162,
so **p50 163 ms, max 733 ms**. Most runs land just above the 100–200 ms election timeout plus
client back-off. The runs near 330 ms and the one at 733 ms are consistent with split votes needing
another election round. That reading comes from the timing pattern only; it was not traced.

## What the numbers say

- **Batching is the biggest lever**: 5,454 vs 667 ops/s (8.2x). Without it every write pays its
  own fsync on the leader and on the followers, and the queue backs up, hence a p99 of 164 ms.
- **fsync is the main cost of durability on this disk**: 27,998 vs 5,454 ops/s (5.1x). The
  `-nosync` mode reuses the bug-zoo `skip-fsync` code path and is unsafe; it is a measurement only.
- **5 nodes vs 3**: lower throughput and double the p99. The leader waits for 3 of 5 instead of 2
  of 3 and sends more messages, all on one CPU and one disk here.
- **One client** sees 2.1 ms per write. That is roughly one leader fsync, one follower fsync, two
  loopback round trips and the HTTP overhead.

## Caveats

- One machine, one disk, loopback networking: all nodes compete for the same CPU and the same
  fsync queue. A real cluster has network latency but independent disks. Use these numbers to
  compare configurations, not to predict a deployment.
- The transport is HTTP + gob, picked for zero dependencies, not speed.
- Each configuration ran once for 10 s; no confidence intervals.
