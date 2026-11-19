# Environment

Recorded 2026-11-19 on the development machine.

| Item | Value |
|---|---|
| CPU | 12th Gen Intel Core i5-12500H, 16 threads |
| RAM | 7.4 GB |
| Disk | NVMe, ext4 (`/dev/nvme0n1p5`) |
| OS | Ubuntu, Linux 6.5.0-44-generic |
| Go | 1.22.2 (`go.mod`: `go 1.22`) |
| Java | OpenJDK 17.0.21 (only for TLC) |
| TLA+ tools | `tla2tools.jar` from the TLA+ v1.8.0 GitHub release, placed at `tools/tla2tools.jar` (not committed) |
| Go dependencies | `github.com/anishathalye/porcupine v1.3.1` (only dependency) |

## Commands

| Task | Command |
|---|---|
| All tests | `make test` (or `go test ./...`) |
| Quick tests | `make test-short` |
| Race detector (real server) | `make race` |
| Vet + gofmt check | `make vet` |
| Chaos sweep | `make chaos SEEDS=10000` |
| Replay one seed | `./bin/raftlab-chaos -seed N [-mutation name] -v` |
| Bug zoo | `make bugzoo` |
| Benchmarks | `make bench` |
| TLA+ | `make tla`, or `./scripts/tlc.sh check|simulate|bug MaxTerm MaxLogLen MaxMsgs [seconds]` |
| Local cluster | `./scripts/cluster.sh`, then `./bin/raftlab put|get|append|status` |

## Notes

- Delete `data/` whenever you like: benchmarks and the local cluster write there, and it is
  ignored by git.
- During this session the disk filled up because GNOME's `tracker-miner-fs-3` was stuck logging
  the same warning to `/var/log/syslog` (18.5 GB). The indexer was stopped for the user session.
  The log needs `sudo truncate -s 0 /var/log/syslog` to reclaim the space. TLC needs free disk for
  anything beyond the smallest models.
