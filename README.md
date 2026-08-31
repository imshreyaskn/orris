# orris — Distributed Key-Value Store with Raft Consensus & WAL

> **Zero Dependency 2026 Submission**  
> **Track D — Data & Storage** (Secondary: Track C — Web & Network)  
> **Language:** Go 1.21+ (Standard Library Only — Zero Third-Party Runtime Dependencies)

---

## 📌 Overview

**`orris`** is a production-grade, distributed, fault-tolerant key-value store built entirely from scratch with **zero third-party dependencies**. 

It implements a clean, robust Raft consensus engine, an append-only Write-Ahead Log (WAL) with CRC32 checksums, binary length-prefixed Gob encoding, and `fsync` persistence, asynchronous state-machine application, and a text-based TCP client protocol with an accompanying CLI & visualizer dashboard (`orrisctl`).

---

## 🏆 Hackathon Alignment & Highlights

- **Zero Runtime Dependencies:** Fully verified empty `go.mod` (no `require` block). Everything is built purely on the Go standard library (`net`, `encoding/gob`, `encoding/binary`, `hash/crc32`, `os`, `sync`, `sync/atomic`, `time`, `io`, `bufio`, `flag`, `log/slog`).
- **Track D (Data & Storage):** Demonstrates strict persistence, durability with `fsync`, CRC32 checksum verification, suffix truncation recovery, linearizable state machine replication, leader election, and automatic failover.
- **Package Killer Bonus (+3):** Replaces `hashicorp/raft` + `etcd/raft` + `grpc` + `boltdb` + `zap` with standard library constructs.
- **STDLIB Log Bonus (+3):** 12 comprehensive package-to-stdlib substitutions documented in [`STDLIB.md`](STDLIB.md).
- **Single-Command Build & Check:** Builds instantly with standard `go build ./...` and verifies with `go test -v ./...`.

---

## 🏗️ Architecture & Component Design

```
                     +---------------------------------------+
                     |           Client Applications         |
                     +---------------------------------------+
                                        | (Text TCP)
                                        v
+---------------------------------------------------------------------------------+
| orris Node                                                                      |
|                                                                                 |
|   +---------------------+      Proposals      +-----------------------------+   |
|   |   Client Server     | -----------------> |         Raft Core           |   |
|   | (:9001-9003)        |                     | - Randomized Election Timer |   |
|   | SET/GET/DEL/PING    |                     | - RequestVote & AppendEntry |   |
|   +---------------------+                     | - Quorum & Match Index      |   |
|              ^                                +-----------------------------+   |
|              | Reads                                  |               |         |
|              |                                        | Commits       | Logs    |
|   +---------------------+   Apply committed entries   |               v         |
|   |     KV Store        | <---------------------------+       +---------------+ |
|   | (In-Memory RWMutex) |                                     |      WAL      | |
|   +---------------------+                                     | - Gob Encode  | |
|                                                               | - Length-Pref | |
|                                                               | - CRC32 Check | |
|                                                               | - fsync       | |
|                                                               +---------------+ |
+---------------------------------------------------------------------------------+
           |                                                        |
           | TCP RPC (AppendEntries / RequestVote)                  | (Disk)
           v                                                        v
   Other Raft Nodes                                            ./data/wal.log
```

1. **Storage Layer (`internal/storage/` & `internal/kv/`)**:
   - Length-prefixed binary records with CRC32 checksums and Gob encoding.
   - Durability guaranteed through synchronous `file.Sync()` on state and entry writes.
   - Explicit `TRUNCATE` records to solve the Raft log suffix overwrite problem.
   - Replay-driven state machine recovery on startup.

2. **Transport Layer (`internal/transport/`)**:
   - High-performance TCP transport framing with Go's native Gob encoder/decoder.
   - Bounded connection and read/write timeouts (`net.DialTimeout`, `SetDeadline`).

3. **Consensus Layer (`internal/raft/`)**:
   - Raft leader election with randomized timeouts (300ms–600ms).
   - Log replication with `NextIndex` backtracking and term verification.
   - Quorum-based commit index advancement (`(N_peers + 1) / 2`).
   - Event-driven apply loop decoupling consensus from state execution.

4. **Client Interface (`internal/client/` & `cmd/`)**:
   - Plaintext TCP line protocol (`SET`, `GET`, `DEL`, `PING`, `STATUS`, `KEYS`, `LOGS`).
   - `orrisctl` interactive TUI dashboard and command-line client.

---

## 🚀 Standardized Single-Command Operations

All cluster lifecycle and visualizer operations are unified into **single, cross-platform commands** in `orrisctl`:

```bash
# ⚡ 1-Step Launch: Auto-boots 3-node cluster and opens interactive dashboard
go run ./cmd/orrisctl
```

*(If the cluster isn't running, it auto-spawns all 3 nodes, elects a leader, and drops you straight into the live interactive console!)*

| Task | Pure Go Standard Command | Makefile Equivalent |
| :--- | :--- | :--- |
| **All-in-One Launch (Auto-Start + UI)** | `go run ./cmd/orrisctl` | `make visualizer` |
| **Run Full E2E Demo & Failover** | `go run ./cmd/orrisctl demo` | `make demo` |
| **Kill Leader (Chaos Engineering)** | `go run ./cmd/orrisctl kill-leader` | `make kill-leader` |
| **Explicit Start Cluster** | `go run ./cmd/orrisctl start [N]` | `make start` |
| **Stop Cluster** | `go run ./cmd/orrisctl stop` | `make stop` |
| **Run Unit & Integration Tests** | `go test -v ./...` | `make test` |
| **Run Static Analysis & Formatting**| `go vet ./...` | `make check` |
| **Build Binaries** | `go build ./...` | `make build` |

---

## 💻 Interactive Visual Console

Inside the visual console (`orris>`), you can run:
- `set <k> <v>` $\rightarrow$ Proposes write to the active leader, verifies quorum commit, and updates the KV table live.
- `get <k>` $\rightarrow$ Reads a key from active nodes and prints latency.
- `del <k>` $\rightarrow$ Deletes a key across the cluster.
- `bench [n] [c]` $\rightarrow$ Blasts $n$ writes with $c$ concurrent workers to measure commit throughput (ops/sec).
- `kill-leader` $\rightarrow$ Simulates a leader crash to test real-time failover.
- `start [N]` / `stop` $\rightarrow$ Controls cluster processes on the fly.
- `watch` $\rightarrow$ Enters live 1-second auto-refreshing dashboard mode.

---

## 🧪 Testing

Run all unit, concurrency, corruption resilience, and cluster failover tests:

```bash
go test -v -count=1 ./...
```

---

## 🔍 Dependency Proof

Verify that no external packages exist in the module:

```bash
# Inspect module dependencies
go list -m all
```
*Output:*
```
orris
```

*(No `github.com/...` or third-party dependencies are required or fetched.)*

---

## 📁 Repository Structure

```
orris/
├── cmd/
├── orrisd/main.go          # Node daemon with slog & graceful shutdown
└── orrisctl/main.go        # CLI client & dynamic multi-node visualizer
├── internal/
│   ├── client/server.go        # Text-based TCP client protocol (SET, GET, DEL, PING)
│   ├── kv/store.go             # In-memory thread-safe KV state machine
│   ├── raft/
│   │   ├── apply.go            # Event-driven state machine apply loop
│   │   ├── client.go           # Client request & proposal handling
│   │   ├── election.go         # Election timers & RequestVote
│   │   ├── node.go             # Node lifecycle & WAL recovery
│   │   ├── replication.go      # Log replication & AppendEntries
│   │   └── state.go            # Raft state definitions
│   ├── storage/wal.go          # WAL persistence, CRC32, Gob encoding, fsync
│   └── transport/
│       ├── rpc.go              # Consensus RPC types
│       └── tcp.go              # TCP RPC server & client engine
├── tests/
│   ├── raft_test.go            # State machine concurrency tests
│   ├── wal_test.go             # WAL persistence & CRC32 corruption resilience tests
│   └── integration_test.go     # 3-node in-process cluster & failover tests
├── go.mod                      # Go 1.21 zero-dependency manifest
├── Makefile                    # Standard make targets (check, test, build, demo)
├── README.md                   # Project documentation
└── STDLIB.md                   # Standard library substitution log
```
