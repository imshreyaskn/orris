# Orris

> **A fault-tolerant distributed key-value store with Raft consensus and durable Write-Ahead Logging — built entirely from Go's standard library with zero third-party dependencies.**

🌐 **Live Interactive Web Simulation (Click & Play):** [https://orrisraft.vercel.app](https://orrisraft.vercel.app/)  
⚡ **Instant Local Launch:** `go run ./cmd/orrisctl`

*Boot a 3-node Raft cluster, elect a leader, and explore consensus with live visual dashboards — online in your browser or locally in your terminal.*

---

## The Problem

Most distributed systems tutorials teach you to `go get github.com/hashicorp/raft` and call it a day. You end up with a working system, but you have no idea what actually happens when a node crashes, why a stale leader steps down, or how data survives a power failure.

The real problems in distributed storage are subtle and deeply unsexy:

- **Consensus without external libraries.** Who elects the leader when the current one dies? How do you prevent split-brain? How do you ensure no two nodes think they're leader in the same term?
- **Durability without a storage engine.** Commits acknowledged to the client must survive a crash. You need `fsync`, checksums, and a log that can be safely replayed — not just a map in memory.
- **Linearizability.** A `GET` after a committed `SET` must return the new value, on any node in the cluster, every time. This requires understanding exactly when a value is "applied" versus just "received."
- **Safe recovery.** When a crashed node restarts, it must replay its Write-Ahead Log to reconstruct state exactly. A corrupt record in the middle shouldn't destroy the entire history.
- **Zero-dependency constraint.** Every serious distributed systems library (`hashicorp/raft`, `etcd`, `grpc`, `bolt`, `zap`, `protobuf`) is off-limits. Everything must be built from primitives.

**Orris solves all of this using only Go's standard library.**

---

## What Orris Is

Orris (`orris`) is a distributed, fault-tolerant key-value store implementing the [Raft consensus algorithm](https://raft.github.io/) from scratch. It provides:

- **Strong consistency**: all reads and writes are linearizable across the cluster
- **Automatic leader election** with randomized timeouts to prevent split votes
- **Log replication** with quorum commit and `NextIndex` backtracking
- **Crash recovery** via a durable Write-Ahead Log with CRC32 checksums and `fsync`
- **Fault tolerance**: the cluster continues operating as long as a majority of nodes are alive (3-node: tolerates 1 crash; 5-node: tolerates 2 crashes)
- **A live interactive visualizer** to watch everything happen in real-time

And it does all of this with **zero third-party dependencies**. The `go.mod` has no `require` block. It verifies with `go list -m all` returning a single line.

---

## How It Works

### The Raft Consensus Algorithm

Raft solves distributed consensus by decomposing it into three relatively independent sub-problems:

#### 1. Leader Election

Every node starts as a **Follower**. If no heartbeat is received within a randomized timeout (300–600ms), the node becomes a **Candidate**, increments its term, and requests votes from peers. A node wins if it receives votes from a majority of the cluster. The randomization prevents two candidates from always racing each other indefinitely.

```
FOLLOWER --[election timeout]--> CANDIDATE --[quorum votes]--> LEADER
   ^                                                               |
   |                               [term out-of-date / new leader]|
   +---------------------------------------------------------------+
```

If a split vote occurs (two candidates get equal votes), the election times out and restarts with a new incremented term. Raft's guarantee: **at most one leader per term**.

#### 2. Log Replication

The Leader receives all writes. When a client sends `SET user Alice`:

1. Leader appends the entry to its own log with the current term
2. Leader sends `AppendEntries` RPCs to all followers in parallel
3. Each follower appends the entry to its own log and acknowledges
4. Once a **majority** (quorum) acknowledges, the Leader advances its `CommitIndex`
5. The entry is applied to the in-memory KV state machine
6. The client receives `OK COMMITTED`

Followers never apply entries until the Leader tells them via the next heartbeat's `LeaderCommit` field. This ensures linearizability: a value is only readable after it's been committed by a majority.

#### 3. Safety

The key safety property: **a leader must have all committed entries**. Orris enforces this by having candidates include their last log index and term in vote requests. A node only votes for a candidate whose log is at least as up-to-date as its own. This prevents a node with stale data from becoming leader and overwriting committed entries.

---

### The Write-Ahead Log (WAL)

In-memory state dies on crash. The WAL is what makes Orris durable.

Every change to Raft state is written to `data/nodeX/wal.log` **before** it's acknowledged. The WAL stores four record types:

| Record Type | When Written | Contents |
| :--- | :--- | :--- |
| `STATE` | On term increment or vote cast | `CurrentTerm`, `VotedFor` |
| `ENTRY` | On log append | Full `LogEntry` (index, term, op, key, value) |
| `COMMIT` | On `CommitIndex` advancement | New `CommitIndex` |
| `TRUNCATE` | On log conflict resolution | Truncation point index |

Each record is framed as:

```
[ 4 bytes: payload length ][ 4 bytes: IEEE CRC32 checksum ][ N bytes: Gob-encoded payload ]
```

On startup, `ReadAll()` replays every record in order. If any record has a checksum mismatch (e.g., from an incomplete write during a power failure), replay stops and the corrupt tail is truncated. The node recovers to the last known-good state and rejoins the cluster, catching up via Raft replication.

This means: **an `OK COMMITTED` response guarantees the write survived a crash**.

---

### The Client Protocol

Orris uses a simple line-delimited plaintext TCP protocol on ports 9001-9003:

```
SET key value        →  OK COMMITTED
GET key              →  VALUE <value> | ERR NOT_FOUND
DEL key              →  OK COMMITTED
PING                 →  PONG
STATUS               →  NODE node1 ROLE LEADER TERM 3 COMMIT 14 APPLIED 14 LOGLEN 15
KEYS                 →  KEYS user=Alice | role=Admin
LOGS                 →  LOGS [0]T1:SET:user=Alice | [1]T1:SET:role=Admin
LEADER               →  LEADER node1 127.0.0.1:9001
```

Write operations (`SET`, `DEL`) are automatically routed to the active leader by `orrisctl`. Read operations (`GET`, `STATUS`, `KEYS`) can be served by any node.

---

## Architecture

```
                     ┌─────────────────────────────────────┐
                     │           orrisctl (CLI/TUI)         │
                     │  set/get/del/wals/kill/spawn/bench   │
                     └──────────────┬──────────────────────┘
                                    │ TCP plaintext protocol
                     ┌──────────────▼──────────────────────┐
                     │          Client Server               │
                     │    (internal/client/server.go)       │
                     └──────────────┬──────────────────────┘
                                    │ Proposal channel
                     ┌──────────────▼──────────────────────┐
                     │            Raft Core                 │
                     │  ┌──────────────────────────────┐   │
                     │  │  Election   │  Replication   │   │
                     │  │  (election.go) (replication.go)  │
                     │  └──────────────────────────────┘   │
                     │  ┌──────────────────────────────┐   │
                     │  │   Apply Loop  (apply.go)     │   │
                     │  │   Event-driven, not polling  │   │
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

### Package Structure

```
orris/
├── cmd/
│   ├── orrisd/main.go       # Node daemon: flags, signal handling, slog, lifecycle
│   └── orrisctl/main.go     # CLI + live TUI dashboard + cluster management
│
├── internal/
│   ├── storage/
│   │   └── wal.go           # WAL: length-prefix + CRC32 + Gob + fsync + replay
│   ├── kv/
│   │   └── store.go         # Thread-safe in-memory KV state machine (RWMutex)
│   ├── raft/
│   │   ├── state.go         # Node struct, NodeState enum, accessor methods
│   │   ├── node.go          # Lifecycle: NewNode, Start, Stop, WAL recovery
│   │   ├── election.go      # Election timer, RequestVote RPC, vote counting
│   │   ├── replication.go   # AppendEntries RPC, NextIndex backtracking, commit
│   │   ├── apply.go         # Event-driven apply loop (applyNotify channel)
│   │   └── client.go        # Client proposal handler with deadline & routing
│   └── transport/
│       ├── rpc.go           # Consensus RPC types (RequestVote, AppendEntries)
│       └── tcp.go           # TCP server/client, Gob framing, connection timeouts
│
├── tests/
│   ├── wal_test.go          # WAL persistence & CRC32 corruption resilience
│   ├── raft_test.go         # 20-goroutine concurrent state machine tests
│   └── integration_test.go  # 3-node in-process cluster: write, replicate, failover
│
├── go.mod                   # Go 1.21 — zero require statements
├── Makefile                 # build, test, demo, check targets
├── STDLIB.md                # Full standard library substitution log
└── README.md
```

---

## Getting Started

### Prerequisites

- Go 1.21 or later
- No other dependencies — ever

### One-Command Launch

```bash
git clone https://github.com/imshreyaskn/orris.git
cd orris
go run ./cmd/orrisctl
```

Orris auto-detects whether the cluster is running. If not, it spawns all nodes, waits for leader election, and opens the interactive console automatically.

### Build Binaries

```bash
go build -o bin/orrisd ./cmd/orrisd
go build -o bin/orrisctl ./cmd/orrisctl
```

Or use Make:

```bash
make build
```

### Start & Stop the Cluster Manually

```bash
# Start a 3-node cluster
go run ./cmd/orrisctl start

# Start a 5-node cluster
go run ./cmd/orrisctl start 5

# Stop all nodes
go run ./cmd/orrisctl stop
```

---

## Interactive Visual Console

You can run the live dashboard in two ways:
1. **In your browser (zero setup):** Open [https://orrisraft.vercel.app](https://orrisraft.vercel.app/) to click and play immediately.
2. **In your terminal:** Run `go run ./cmd/orrisctl` to interact with real background Go processes.

```text
╔══════════════════════════════════════════════════════════════════════════════════════╗
║  ORRIS CLUSTER VISUALIZER  [Zero-Dependency Raft Consensus]          TIME: 18:04:15  ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

--- [ CLUSTER NODES & RAFT STATE ] ---------------------------------------------------
 * Node node1 on 127.0.0.1:9001 (ping: 1.2ms)
   Role:  [LEADER]    Term: 3   CommitIndex: 12   Applied: 12   LogEntries: 13

 * Node node2 on 127.0.0.1:9002 (ping: 0.9ms)
   Role:  [FOLLOWER]  Term: 3   CommitIndex: 12   Applied: 12   LogEntries: 13

 * Node node3 on 127.0.0.1:9003 (ping: 1.1ms)
   Role:  [FOLLOWER]  Term: 3   CommitIndex: 12   Applied: 12   LogEntries: 13

--- [ REPLICATED STATE MACHINE (KV STORE) ] -------------------------------------------
   KEY              | node1            | node2            | node3
   -----------------------------------------------------------------------
   author           | Shreyas          | Shreyas          | Shreyas
   project          | orris            | orris            | orris

--- [ RAFT WAL & LOG ENTRIES (Recent 6 of 13 Entries) ] --------------------------------
   INDEX    | TERM     | OP       | KEY / VALUE                     | COMMIT STATE
   -------------------------------------------------------------------------
   #8       | Term 2   | SET      | author = Shreyas                | [COMMITTED]
   #9       | Term 2   | SET      | project = orris                 | [COMMITTED]

COMMANDS: set <k> <v> | get <k> | del <k> | keys | wals | wal <node> | kill <node|leader> | spawn <node> | bench [n] [c] | help | exit
orris>
```

### Full Command Reference

**KV Operations**

| Command | Description |
| :--- | :--- |
| `set <key> <value>` | Route write to leader, commit to quorum, update dashboard |
| `get <key>` | Read from any online node |
| `del <key>` | Delete key via consensus |
| `keys` / `dump` | List all keys and their values across all nodes |

**Cluster Management**

| Command | Description |
| :--- | :--- |
| `start [N]` | Spawn an N-node cluster (default: 3) |
| `stop` | Gracefully terminate all nodes |
| `kill <node\|leader>` | Crash a specific node or the current leader |
| `spawn <node>` | Restart a crashed node, it replays its WAL and rejoins |
| `status` / `info` | Print live Role, Term, CommitIndex, Latency for all nodes |
| `ping` | Ping all nodes and show round-trip latencies |
| `leader` | Identify the current consensus leader |

**WAL & Diagnostics**

| Command | Description |
| :--- | :--- |
| `wals` | Compare on-disk WAL file sizes and record counts across all nodes |
| `wal <node>` | Inspect raw on-disk WAL records (STATE, ENTRY, COMMIT, TRUNCATE) |
| `logs` | Print the full replicated log timeline with commit status |

**Tools**

| Command | Description |
| :--- | :--- |
| `bench [n] [c]` | Concurrent write benchmark: `n` total writes, `c` parallel workers |
| `watch` / `top` | Live 1-second auto-refreshing dashboard mode |
| `help` | Full command reference |
| `exit` | Quit |

---

## Chaos Engineering

The most interesting way to run Orris is to break it and watch it self-heal:

```bash
# Terminal 1: Open the visualizer
go run ./cmd/orrisctl

# Inside the console:
orris> set user Alice
[COMMITTED] Set 'user' = 'Alice' via 127.0.0.1:9001 (latency: 11ms)

orris> kill leader
[CHAOS] Killed active leader node1 (127.0.0.1:9001, PID: 12345)

# Watch the dashboard: Term increments, node2 or node3 becomes leader
# The cluster continues to serve writes within ~600ms

orris> set user Bob
[COMMITTED] Set 'user' = 'Bob' via 127.0.0.1:9002 (latency: 13ms)

orris> spawn node1
 [OK] Started node1 (Consensus: 127.0.0.1:8001, Client: 127.0.0.1:9001, PID: 12400)

# node1 replays its WAL, catches up with the cluster, and returns as FOLLOWER
```

Or run the full automated demo:

```bash
go run ./cmd/orrisctl demo
```

This executes all four steps automatically: cluster bootstrap, data replication, leader kill, and post-failover state verification.

---

## Benchmarking

```bash
# 100 writes, 10 concurrent workers
go run ./cmd/orrisctl bench 100 10
```

Or inside the console:

```text
orris> bench 200 20
Running concurrent benchmark: 200 writes with concurrency 20...
[OK] Benchmark Finished: 200/200 committed in 4.2s (~47.6 ops/sec, 0 failed)
```

Throughput is bounded by Raft's consensus round-trip (leader → quorum → commit → apply) rather than raw disk I/O. Each committed write requires a full `fsync` to disk on the leader before acknowledgement.

---

## Testing

```bash
go test -v -count=1 ./...
```

Tests cover:

| Test | File | What It Verifies |
| :--- | :--- | :--- |
| `TestWALPersistence` | `wal_test.go` | Write, close, reopen, and verify all records survive |
| `TestWALCorruptionResilience` | `wal_test.go` | Inject byte-level corruption, verify truncation at corrupt tail |
| `TestKVStoreConcurrency` | `raft_test.go` | 20 goroutines hammering the state machine concurrently |
| `TestClusterReplicationAndFailover` | `integration_test.go` | Full 3-node in-process cluster: write → replicate → kill leader → elect new leader → verify state |

---

## Zero Dependencies — Verified

```bash
go list -m all
```

Output:

```
orris
```

That's it. No `hashicorp/raft`. No `etcd`. No `grpc`. No `bolt`. No `protobuf`. No `zap`. No `testify`.

| What You'd Normally Import | What Orris Uses Instead |
| :--- | :--- |
| `hashicorp/raft` / `etcd/raft` | `sync`, `time`, goroutines, channels |
| `google.golang.org/grpc` | `net.Listen` + `net.DialTimeout` + `encoding/gob` |
| `boltdb/bolt` / `pebble` | `os.OpenFile` + `binary.BigEndian` + `hash/crc32` + `file.Sync()` |
| `google/protobuf` / `msgpack` | `encoding/gob` + `bytes.Buffer` |
| `spf13/cobra` | `flag` |
| `uber-go/zap` / `logrus` | `log/slog` |
| `stretchr/testify` | `testing` |
| `uber-go/atomic` | `sync/atomic` |

See [`STDLIB.md`](STDLIB.md) for the full annotated substitution log.

---

## Key Design Decisions

**Why plaintext TCP instead of HTTP/gRPC?**
Simplicity and zero dependencies. A `bufio.Scanner` reading newline-delimited commands is auditable in 10 lines. HTTP adds framing, headers, and encoding overhead. gRPC requires protobuf compilation and a runtime dependency.

**Why Gob encoding for consensus RPCs?**
`encoding/gob` is part of the standard library, type-safe, and handles nested structs. It's not the most efficient serialization format, but it eliminates the protobuf compiler toolchain entirely.

**Why CRC32 and not CRC64 or SHA256?**
CRC32 catches the entire class of corruption we care about: incomplete writes, partial overwrites, and disk bit flips. It's in the standard library (`hash/crc32`), 4 bytes per record, and fast enough to add no measurable overhead on modern hardware. Cryptographic hashing would be overkill for write integrity on trusted storage.

**Why event-driven apply instead of a polling loop?**
The original implementation used `time.After(50ms)` to periodically check if `CommitIndex > LastApplied`. This creates GC pressure (a new timer allocation every 50ms, never garbage collected if not drained) and adds up to 50ms of unnecessary commit latency. The current implementation uses an `applyNotify` channel. When `CommitIndex` advances, the replication goroutine signals the apply loop, which wakes up instantly and applies all committed entries.

**Why randomized election timeouts?**
If all nodes used the same election timeout and the leader crashed, they'd all start elections simultaneously and vote-split indefinitely. Randomization in the 300–600ms range ensures that, statistically, one node fires first and wins before the others start their elections.

---

## Makefile Targets

```bash
make build        # Build both binaries into bin/
make test         # Run all tests (go test -v ./...)
make check        # go vet + go build (static analysis)
make demo         # Full end-to-end demo with failover
make visualizer   # Launch interactive dashboard
make start        # Boot 3-node cluster
make stop         # Stop all cluster nodes
make clean        # Remove bin/ and data/
```

---

## License

MIT

---

*Built from scratch. No shortcuts. All primitives.*
