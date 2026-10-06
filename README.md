English | [简体中文](README_CN.md)

# Raft-KV

[![Portable checks](https://github.com/Die4U23/Raft-KV/actions/workflows/portable.yml/badge.svg)](https://github.com/Die4U23/Raft-KV/actions/workflows/portable.yml)
[![Linux cluster](https://github.com/Die4U23/Raft-KV/actions/workflows/linux-cluster.yml/badge.svg)](https://github.com/Die4U23/Raft-KV/actions/workflows/linux-cluster.yml)

A three-node Raft KV store written in C++17. One request path covers the Muduo network layer, Raft consensus, RocksDB persistence, bounded concurrency, and real fault tests.

Longer project write-up: [From a three-node store to fault tests and performance work](docs/raft-kv-project-practice.md).

## What it does

- **Raft write path**: leader election, log replication, majority commit, and ordered state-machine apply.
- **ReadIndex linearizable reads**: a heartbeat majority confirms the read barrier, then the read waits until the local apply index catches up. The read itself is not written to the log.
- **Persistence**: the Raft log and hard state are synced to disk. KV data and `lastApplied` commit in the same RocksDB WriteBatch.
- **Bounded service**: connection count, input and output buffers, the write queue, and proposal count and bytes are capped. Overload is rejected instead of queued without limit.
- **Batching and async apply**: commands are grouped inside one event-loop turn. A committed KV batch goes to a serial worker, and the completion callback returns to the owner thread.
- **RESP access**: TCP splits and coalesced packets, pipelined commands, binary values, and hostile length fields.
- **Traceable checks**: partition, leader crash and restart, overload, and comparison runs keep reports, node logs, source and binary fingerprints, and separate verification scripts.

## Architecture

```text
Client (RESP)
      │
      ▼
Muduo client server ── GET ──► KVStateMachine ──► RocksDB
      │                          ▲
      └─ SET / DEL ─► RaftNode ──┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
        RaftLog / RocksDB      PeerManager / Muduo
                                    │
                              other Raft nodes
```

A write is appended on the leader, replicated through PeerManager to a majority, advances `commit_index`, and is then applied to the KV state machine in order. Timing, thread ownership, and callback lifetime are in the [concurrency notes](docs/concurrency.md).

## Recorded results

The table below is the archive from 2026-09-08 through 09-13, when the portable CTest suite had 5 or 7 targets. The current suite and the ReadIndex / Pre-Vote scope are in the [project status](docs/review-status.md). Those archives were not rerun on later binaries.

| Scenario | Result | Evidence |
| --- | --- | --- |
| Ubuntu 26.04 three-process smoke | Build, CTest 5/5, smoke 10/10 | [Full build record](docs/benchmarks/linux-fresh-validation.md) |
| Short TCP partition | Isolated old leader, majority keeps writing, replicas converge after heal | [Partition record](docs/benchmarks/partition-validation.md) |
| Crash and restart during writes | 1,704 acknowledged keys remain on all three replicas after two recoveries | [Recovery record](docs/benchmarks/write-restart-validation.md) |
| Overload and a 60-second window | Admission control, `BUSY`, recovery to zero, and resource thresholds passed | [Overload record](docs/benchmarks/overload-validation.md) |

An archived two-core VM baseline, with three nodes and the client on one machine, 32 connections, `pipeline=1`, 128-byte values, and 50% GET / 50% SET:

| Mode | Combined throughput | Two-round p50 | Two-round p99 |
| --- | ---: | ---: | ---: |
| Synchronous apply | 3,656.53 ops/s | 7.88 / 8.85 ms | 20.61 / 25.67 ms |
| Serial async apply | 3,824.56 ops/s | 8.35 / 8.05 ms | 18.39 / 26.89 ms |

These numbers belong to that VM and that load. They are for regression and mechanism checks, not a general capacity claim. Parameters, CPU cost, and raw evidence are in the [performance baseline](docs/benchmarks/ubuntu-2cpu-abba.md).

## Quick start

### 1. Build

On Ubuntu, the reproducible flow records source, dependency, and binary identity:

```bash
python3 scripts/build_linux.py --jobs 2 --smoke
```

The server binary is then at `build-linux-repro/server/raft_kv_server`. System packages, the Muduo prepare step, and the report layout are in the [Ubuntu build notes](docs/linux-build.md).

Portable regression, without the real Muduo and RocksDB libraries:

```bash
cmake -S . -B build-portable -G Ninja \
  -DRAFTKV_BUILD_SERVER=OFF -DCMAKE_BUILD_TYPE=Debug
cmake --build build-portable --parallel 2
ctest --test-dir build-portable --output-on-failure
python3 -m unittest discover -s tests -p '*_tests.py'
```

### 2. One-command demo

Start three processes from a Linux build. They elect a leader, write a key, and read it on every node. A `SET` to a follower returns `MOVED`. After the leader is killed, the new leader still has the old key and can write another. The old process restarts from its original directory, and the three nodes agree again. All three processes are on one machine.

```bash
python3 scripts/demo.py \
  --binary build-linux-repro/server/raft_kv_server
```

`--hold` leaves the processes up after the story so you can try `redis-cli -p <client port>`. Press Enter to stop them.

### 3. Start three nodes by hand

Give each node its own KV directory and Raft log directory:

```bash
$BIN --node_id=0 --client_port=8080 --raft_port=9080 \
  --db_path=/tmp/kv_db_0 --raft_log_path=/tmp/raft_log_0 \
  --linearizable_reads=true
$BIN --node_id=1 --client_port=8081 --raft_port=9081 \
  --db_path=/tmp/kv_db_1 --raft_log_path=/tmp/raft_log_1 \
  --linearizable_reads=true
$BIN --node_id=2 --client_port=8082 --raft_port=9082 \
  --db_path=/tmp/kv_db_2 --raft_log_path=/tmp/raft_log_2 \
  --linearizable_reads=true
```

Run the three commands in three terminals. After startup, `redis-cli -p 8080 INFO` shows the role and the leader.

The same machine can also run three containers. Each container has its own volume, and both the KV data and the Raft log live under `/data` on that volume. `GET /health` means this process is serving. It does not mean the process is the leader, and it does not mean a majority is still available.

```bash
docker compose -f docker/compose.yaml up --build
curl -fsS localhost:9090/health
```

**Optional flags**:
- `--linearizable_reads=true`: ReadIndex linearizable reads (default false)
- `--lease_reads=true`: with linearizable reads, a leader whose lease is valid confirms a GET locally (default false)
- `--leader_only_reads=true`: only the leader answers reads (default false)
- `--metrics_port=9090`: serve `GET /metrics` (Prometheus text) and `GET /health` on that port. The default is 0, which does not listen. The port must differ from `client_port` and `raft_port`

### 4. Read and write

```bash
redis-cli -p 8080 SET user:1 alice
redis-cli -p 8080 GET user:1
redis-cli -p 8080 DEL user:1
```

A write to a follower returns this project's `-ERR MOVED <leader_id>`. That is not the Redis Cluster redirect protocol.

## Client commands

| Command | Meaning |
| --- | --- |
| `PING` | Returns `PONG` |
| `SET key value` | Returns `OK` after the Raft commit and the state-machine apply |
| `GET key` | By default, reads the local state machine. With `--linearizable_reads=true`, the leader uses ReadIndex. Adding `--lease_reads=true` confirms locally while the lease holds. A follower returns `MOVED` |
| `DEL key` | Deletes through Raft and returns `0` or `1` |
| `SELECT namespace` | Selects a logical namespace for this TCP connection |
| `INFO` | Role, term, leader, commit and apply indexes, and overload counters. When `--metrics_port` is not 0, the same numbers are also on `GET /metrics` |

`SELECT` applies only to the current connection. Separate `redis-cli` processes do not share namespace state.

## How the checks are layered

- **Portable C++ regression**: protocol, Raft edges, storage batches, the async executor, batching, and reconnect policy.
- **Python checks**: the build flow, client behavior, performance comparisons, and the testable parts of fault orchestration.
- **Linux server**: Muduo TCP, Protobuf, RocksDB, a three-process smoke test, and fault injection.
- **Evidence checks**: `docs/benchmarks/verify_*.py` cross-checks archives, hashes, reports, and node logs.

Commands, and what each layer can and cannot show, are in [tests/README.md](tests/README.md).

## Current limits

- The cluster is one Raft group with a fixed membership. There is no dynamic membership and no sharding.
- A snapshot replaces the applied prefix (`--snapshot_threshold`, default 1024, `0` disables it). Applied entries still kept in the log stay below that threshold. Entries written after the snapshot accumulate until the next threshold. On restart the saved tail marks the end, and every entry after the snapshot is read. A damaged entry in the middle fails the open.
- Plain `SET` / `DEL` are not deduplicated. `IDEMP <client-id> <request-id> SET|DEL ...` keeps only the latest request for that client: the same id returns the first reply, and a smaller id returns stale.
- A write that was accepted but not committed within 1000 ms gets `-ERR proposal timeout; outcome unknown`. The log entry stays and may still commit later.
- The checks here do not cover whole-machine power loss, damaged storage media, long soaks, or a full Raft correctness proof.

Scope and check limits are in the [project status](docs/review-status.md).

## Layout

```text
src/server/        RESP access, sessions, batching, and overload protection
src/raft/          RaftNode, PeerManager, the KV state machine, and the serial applier
src/raftcore/      Raft log and hard-state persistence
src/storage/       RocksDB wrapper and storage error boundary
src/common/        RESP, batching, metrics, and reconnect policy
src/namespace/     Per-connection logical namespaces
proto/             Raft RPC messages
tests/             Portable regression, real cluster fault tests, and load tools
docs/benchmarks/   Raw evidence, verification scripts, and result limits
scripts/           Linux build and the pinned Muduo prepare step
```

Further reading: [tests](tests/README.md) · [build](docs/linux-build.md) · [benchmark method](docs/benchmark.md) · [read consistency](docs/read-consistency.md) · [changelog](CHANGELOG.md)

## Roadmap

1. ✅ ~~ReadIndex linearizable reads~~: a majority confirms the read barrier, then the read waits until the local apply index catches up. Off by default. The election deadline and the probe lease share one `steady_clock`. An isolated old leader steps down when CheckQuorum expires, so a linearizable GET fails instead of returning a stale value.
2. ✅ ~~Pre-Vote~~: an election timeout enters pre-candidate first. It does not raise the term and does not record `votedFor`. A real election starts only after a majority of pre-votes. A lost election returns to pre-vote instead of raising the term again.
3. ✅ ~~Snapshots and log reclamation~~: the applied prefix becomes a KV snapshot, and a lagging replica catches up with chunked `InstallSnapshot`. A replica whose log is longer and whose term conflicts sends `nextIndex` to the start of that conflicting term in one step. If that index is inside the snapshot, the next round installs the snapshot. Applied entries still kept in the log stay below `--snapshot_threshold`.
4. `IDEMP` deduplicates writes that carry a client id and a request id. A retry of plain `SET` / `DEL` can still run twice. Each client id keeps only its latest record. The record stays with the snapshot and does not expire.

## Dependencies and sources

The main dependencies are Muduo, RocksDB, Protobuf, gflags, glog, and Boost.

The implementation and design draw on these papers, notes, and projects:

| Reference | URL | Role |
|---|---|---|
| **Raft paper** | https://raft.github.io/raft.pdf | Specification of the Raft algorithm |
| **Raft visualization** | https://raft.github.io/ | Election and log replication |
| **muduo** | https://github.com/chenshuo/muduo | Network layer |
| **RocksDB** | https://github.com/facebook/rocksdb | Storage engine |
| **etcd** | https://github.com/etcd-io/etcd | A mature Raft system used as a design reference |
| **TiKV** | https://github.com/tikv/tikv | An industrial Raft and RocksDB system |

Thanks to those projects and their maintainers. The later changes that can be traced are listed in the [project write-up](docs/raft-kv-project-practice.md#项目来源许可证与改造范围).

## License

This project is released under the [MIT License](LICENSE).
