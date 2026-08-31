# Orris

> A distributed, fault-tolerant key-value store with Raft consensus and durable Write-Ahead Logging — implemented entirely using the Go standard library with zero third-party dependencies.

- **Interactive Web Simulation:** [https://orrisraft.vercel.app](https://orrisraft.vercel.app)
- **Local Terminal Launch:** `go run ./cmd/orrisctl`

---

## Overview

Orris (`orris`) is a distributed, fault-tolerant key-value storage engine implementing the [Raft consensus algorithm](https://raft.github.io/) from first principles.

Most distributed systems rely on heavy third-party frameworks (`hashicorp/raft`, `etcd`, `grpc`, `boltdb`) to manage cluster state and disk persistence. Orris demonstrates that a complete distributed consensus engine, linearizable state machine replication, physical disk durability with `fsync`, and real-time observability can be implemented using only Go standard library primitives.

### Core Capabilities

- **Linearizable Consistency:** Reads and writes are strictly ordered and synchronized across the cluster.
- **Automated Leader Election:** Randomized election timeouts (300ms–600ms) prevent split-vote deadlocks and ensure rapid failover.
- **Quorum-Based Log Replication:** State transitions require explicit acknowledgement from a majority `(N/2 + 1)` of nodes before commit.
- **Physical Crash Durability:** Write-Ahead Log (WAL) persistence with IEEE CRC32 checksums, binary Gob encoding, and synchronous `file.Sync()` (`fsync`) calls.
- **Fault Tolerance:** A 3-node cluster tolerates 1 node failure; a 5-node cluster tolerates 2 concurrent failures without service interruption.
- **Interactive Visual Control Plane:** Built-in terminal dashboard and web visualizer for real-time monitoring and chaos testing.
- **Zero Third-Party Dependencies:** Empty `require` block in `go.mod`. Verified via `go list -m all`.

---

## Architecture

```
                     ┌─────────────────────────────────────┐
                     │           orrisctl (CLI/TUI)         │
                     │  set/get/del/wals/kill/spawn/bench   │
                     └──────────────┬──────────────────────┘
                                    │ Plaintext TCP Protocol
                     ┌──────────────▼──────────────────────┐
                     │          Client Wire Server         │
                     │      (internal/client/server.go)    │
                     └──────────────┬──────────────────────┘
                                    │ Non-blocking Proposals
                     ┌──────────────▼──────────────────────┐
                     │               Raft Core             │
                     │  ┌──────────────────────────────┐   │
                     │  │  Election   │  Replication   │   │
                     │  │  (election.go) (replication.go)  │
                     │  └──────────────────────────────┘   │
                     │  ┌──────────────────────────────┐   │
                     │  │   Apply Loop (apply.go)      │   │
                     │  │   Event-driven notification  │   │
                     │  └──────────────┬───────────────┘   │
                     └─────────────────┼───────────────────┘
                          ┌────────────┼────────────┐
                          │            │            │
                   ┌──────▼──┐  ┌──────▼──┐  ┌─────▼────┐
                   │  WAL    │  │ KV Store│  │ TCP RPC  │
                   │ wal.go  │  │ store.go│  │ tcp.go   │
                   │ CRC32   │  │ RWMutex │  │ Gob enc  │
                   │ fsync   │  │ map[]   │  │ Timeouts │
                   └─────────┘  └─────────┘  └──────────┘
```

### Component Breakdown

1. **Consensus Engine (`internal/raft/`)**:
   - `state.go`: State definitions (`Follower`, `Candidate`, `Leader`), term tracking, and concurrency primitives.
   - `election.go`: Randomized election timers, candidate vote collection, and term promotion.
   - `replication.go`: `AppendEntries` RPC dispatch, `NextIndex` backtracking, and quorum commit index resolution.
   - `apply.go`: Event-driven state machine applier using dedicated notify channels to eliminate polling latency.
   - `client.go`: Proposal coordination, write routing to active leader, and request timeout handling.

2. **Storage Layer (`internal/storage/`)**:
   - `wal.go`: Length-prefixed binary records, IEEE CRC32 checksum verification, Gob serialization, and `fsync` durability.
   - Handles four record types: `STATE`, `ENTRY`, `COMMIT`, and `TRUNCATE`.
   - Automatic log replay and corrupted tail truncation on crash recovery.

3. **Transport Layer (`internal/transport/`)**:
   - `tcp.go` & `rpc.go`: Point-to-point TCP RPC layer using streaming Gob encoders with connection timeouts and socket deadline enforcement.

4. **Client Interface (`internal/client/` & `cmd/`)**:
   - `server.go`: Line-delimited plaintext TCP server supporting `SET`, `GET`, `DEL`, `PING`, `STATUS`, `KEYS`, `LOGS`, and `LEADER`.
   - `orrisd`: Node daemon process with structured `log/slog` logging and clean signal termination.
   - `orrisctl`: Control plane client, cluster orchestrator, concurrent benchmarking tool, and interactive visualizer.

---

## Consensus & Durability Mechanics

### 1. Leader Election

Nodes initialize in the `Follower` state. If a follower receives no heartbeat within its randomized election timeout (300ms–600ms), it transitions to `Candidate`, increments its `CurrentTerm`, votes for itself, and broadcasts `RequestVote` RPCs to all peers.

```
FOLLOWER ──[ election timeout ]──> CANDIDATE ──[ quorum votes ]──> LEADER
   ^                                                                  │
   │                                  [ higher term / valid leader ]  │
   └──────────────────────────────────────────────────────────────────┘
```

A candidate becomes `Leader` upon receiving votes from a strict majority `(N/2 + 1)`. Raft guarantees that at most one leader can be elected in any given term.

### 2. Log Replication & Commit Protocol

All state mutations flow through the leader:

1. **Client Proposal:** Client submits `SET key value` to the cluster.
2. **Local Append:** The leader appends the entry to its local log and writes it to disk.
3. **Parallel Replication:** The leader sends `AppendEntries` RPCs to all followers.
4. **Follower Persistence:** Followers append the entry to their on-disk WAL and acknowledge the RPC.
5. **Quorum Commit:** Once a majority of nodes acknowledge, the leader advances its `CommitIndex`.
6. **State Machine Execution:** The entry is applied to the in-memory KV store and the client receives `OK COMMITTED`.

Followers apply committed entries during subsequent heartbeat rounds when the leader communicates its updated `LeaderCommit`.

### 3. Write-Ahead Log (WAL) Format

Every state mutation and term change is written to `data/nodeX/wal.log` before consensus acknowledgement.

```
+-------------------+--------------------+------------------------+
| Length (4 Bytes)  | CRC32 (4 Bytes)    | Payload (N Bytes, Gob) |
| Big-Endian uint32 | IEEE 802.3 Checksum| Binary Encoded Struct  |
+-------------------+--------------------+------------------------+
```

On node restart, `ReadAll()` sequentially reads each frame, computes the CRC32 checksum, and validates payload integrity. If an uncompleted write is detected at the tail (e.g. from power loss during disk write), the corrupt segment is truncated, allowing the node to recover to the last valid state and synchronize missing entries from the leader.

---

## Wire Protocol

The client server listens on ports `9001` through `9003` (and onward for larger clusters) using a plaintext line-delimited protocol:

| Command | Syntax | Description | Example Response |
| :--- | :--- | :--- | :--- |
| `SET` | `SET <key> <value>` | Writes or updates a key via Raft consensus | `OK COMMITTED` |
| `GET` | `GET <key>` | Reads a key from local state machine | `VALUE Alice` or `ERR NOT_FOUND` |
| `DEL` | `DEL <key>` | Deletes a key via Raft consensus | `OK COMMITTED` |
| `KEYS` | `KEYS` | Dumps all keys stored in state machine | `KEYS user=Alice \| role=Admin` |
| `PING` | `PING` | Health check and round-trip latency test | `PONG` |
| `LEADER` | `LEADER` | Queries current term and active leader | `ROLE LEADER TERM 3` |
| `STATUS` | `STATUS` | Dumps Raft term, commit, and log lengths | `NODE node1 ROLE LEADER TERM 3 COMMIT 14 ...` |
| `LOGS` | `LOGS` | Dumps replicated log timeline | `LOGS [0]T1:SET:user=Alice \| [1]T1:SET:...` |

---

## Quick Start

### Prerequisites

- Go 1.21 or later
- No external packages or package managers required

### 1. Launch with Interactive Visualizer

```bash
git clone https://github.com/imshreyaskn/orris.git
cd orris
go run ./cmd/orrisctl
```

If no cluster is running, `orrisctl` automatically compiles the daemon, starts a 3-node cluster in the background, coordinates leader election, and opens the live interactive dashboard.

### 2. Interactive Browser Simulation

An in-browser replica of the visualizer is available at:
**[https://orrisraft.vercel.app](https://orrisraft.vercel.app)**

---

## Operations & Command Reference

### Cluster Management

```bash
# Start a 3-node cluster
go run ./cmd/orrisctl start

# Start an N-node cluster (e.g. 5 nodes)
go run ./cmd/orrisctl start 5

# Stop all running cluster nodes
go run ./cmd/orrisctl stop

# Run automated end-to-end demo (start -> write -> failover -> verify)
go run ./cmd/orrisctl demo
```

### Interactive REPL Commands

Inside the `orris>` prompt:

```text
orris> set user Alice
[COMMITTED] Set 'user' = 'Alice' via 127.0.0.1:9001 (latency: 11ms)

orris> get user
VALUE Alice

orris> kill leader
[CHAOS] Killed active leader node1 (127.0.0.1:9001)

orris> set user Bob
[COMMITTED] Set 'user' = 'Bob' via 127.0.0.1:9002 (latency: 13ms)

orris> spawn node1
[OK] Started node1 (Consensus: 127.0.0.1:8001, Client: 127.0.0.1:9001)

orris> wals
=== ALL NODE WRITE-AHEAD LOG (WAL) COMPARISON ===
   NODE       | DISK SIZE    | RECORDS    | LAST TERM    | LOG ENTRIES     
   ------------------------------------------------------------------------
   node1      | 1420 B       | 18         | 2            | 8               
   node2      | 1420 B       | 18         | 2            | 8               
   node3      | 1420 B       | 18         | 2            | 8               
```

---

## Benchmarking

Run concurrent write performance benchmarks directly from the CLI:

```bash
# Execute 100 writes across 10 concurrent workers
go run ./cmd/orrisctl bench 100 10
```

Inside the interactive console:

```text
orris> bench 200 20
Running concurrent benchmark: 200 writes with concurrency 20...
[OK] Benchmark Finished: 200/200 committed in 4.2s (~47.6 ops/sec, 0 failed)
```

Throughput is governed by network consensus round-trips and physical disk synchronization (`file.Sync()`) on write operations.

---

## Testing & Verification

Run the test suite including persistence, state machine concurrency, corruption recovery, and multi-node failover:

```bash
go test -v -count=1 ./...
```

### Test Coverage Summary

| Test Suite | Source File | Verification Scope |
| :--- | :--- | :--- |
| `TestWALPersistence` | `tests/wal_test.go` | Verifies full state and log record recovery across file reopen cycles |
| `TestWALCorruptionResilience` | `tests/wal_test.go` | Injects bit-level corruptions and verifies safe recovery at corruption boundary |
| `TestKVStoreConcurrency` | `tests/raft_test.go` | Exercises concurrent state machine reads and writes across 20 goroutines |
| `TestClusterReplicationAndFailover` | `tests/integration_test.go` | In-process 3-node cluster: write replication, leader termination, term reelection, and state verification |

---

## Dependency Verification

Verify zero third-party dependencies:

```bash
go list -m all
```

Output:

```text
orris
```

### Standard Library Substitution Mapping

| Domain | Standard Library Implementation | Common Third-Party Alternative |
| :--- | :--- | :--- |
| Distributed Consensus | `sync.Mutex`, `time.Timer`, `time.Ticker`, channels | `hashicorp/raft`, `etcd/raft` |
| RPC Networking | `net.Listen`, `net.DialTimeout`, `encoding/gob` | `google.golang.org/grpc` |
| Durable Storage | `os.OpenFile`, `encoding/binary`, `hash/crc32`, `file.Sync()` | `boltdb/bolt`, `cockroachdb/pebble` |
| Serialization | `encoding/gob`, `bytes.Buffer` | `google.golang.org/protobuf` |
| CLI Parsing | `flag` | `spf13/cobra` |
| Structured Logging | `log/slog` | `uber-go/zap`, `sirupsen/logrus` |
| Testing Assertions | `testing` (`t.Fatalf`, `t.TempDir`) | `stretchr/testify` |
| Atomic Operations | `sync/atomic` | `uber-go/atomic` |

Detailed substitution rationale and performance notes are documented in [`STDLIB.md`](STDLIB.md).

---

## Makefile Reference

```bash
make build        # Compile orrisd and orrisctl binaries into bin/
make test         # Run all unit and integration tests
make check        # Run go vet static analysis
make demo         # Execute automated end-to-end consensus demo
make visualizer   # Launch interactive terminal visualizer
make start        # Boot default 3-node cluster
make stop         # Stop all cluster node processes
make clean        # Remove bin/ binaries and runtime data/ directories
```

---

## License

MIT License. See `LICENSE` for details.
