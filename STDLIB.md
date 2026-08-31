# Standard Library Substitution Log (STDLIB.md)

This document details how **`orris`** achieves 100% zero third-party dependencies by replacing common external Go libraries and distributed frameworks with native standard library packages.

---

## 📊 Summary of Substitutions

| # | Normally Used Package | Standard Library Replacement | Domain / Purpose |
|---|---|---|---|
| 1 | `hashicorp/raft` / `etcd/raft` | `sync`, `time`, `math/rand`, goroutines, channels | Distributed Consensus & Leader Election |
| 2 | `google.golang.org/grpc` / `net/rpc` | `net` (TCP) + `encoding/gob` | Inter-node consensus RPC transport |
| 3 | `boltdb/bolt` / `dgraph-io/badger` | `os` (`O_CREATE`, `file.Sync`), `encoding/binary`, `hash/crc32`, `io` | Durable Write-Ahead Log (WAL) with CRC32 and `fsync` |
| 4 | `google/protobuf` / `vmihailenco/msgpack` | `encoding/gob` + `bytes.Buffer` | Structured binary serialization for disk & network |
| 5 | `spf13/cobra` / `spf13/pflag` | `flag` | Command-line argument parsing |
| 6 | `gorilla/mux` / custom framing | `bufio.Scanner` + `strings` | Client wire protocol parser (`SET`, `GET`, `DEL`, `PING`, `STATUS`) |
| 7 | `uber-go/zap` / `sirupsen/logrus` | `log/slog` | High-performance structured diagnostic logging |
| 8 | `stretchr/testify` (`assert`, `require`) | `testing` (`t.Fatal`, `t.TempDir`, `t.Run`) | Unit & integration testing fixtures |
| 9 | `go-redis/redis` | `net.DialTimeout` + `bufio.Scanner` + `time.Duration` | Client TCP connection & wire protocol client |
| 10 | `patrickmn/go-cache` / custom caches | `sync.RWMutex` + Go native `map[string]string` | Thread-safe in-memory replicated state machine |
| 11 | `pkg/errors` | `errors` + `fmt.Errorf` | Error handling and context wrapping |
| 12 | `uber-go/atomic` | `sync/atomic` | Thread-safe benchmark counters and statistics |

---

## 🛠️ Detailed Substitution Analysis

### 1. Distributed Consensus & Raft Protocol
- **Normally Used:** `github.com/hashicorp/raft` or `go.etcd.io/etcd/raft/v3`
- **Standard Library Equivalent:** `sync.Mutex`, `time.Timer`, `time.Ticker`, `math/rand`, native goroutines and channels (`chan struct{}`, `chan ClientReq`).
- **Why & How:** Rather than pulling in multi-thousand-line consensus dependencies, `orris` implements the Raft specification directly in `internal/raft/`. Randomized election timeouts (300ms–600ms) prevent split votes, heartbeats run on `time.NewTicker`, and state machine transitions (`Follower` -> `Candidate` -> `Leader`) are strictly synchronized with mutexes and event-driven commit notifications.

---

### 2. Inter-Node RPC Networking
- **Normally Used:** `google.golang.org/grpc` (gRPC + Protocol Buffers)
- **Standard Library Equivalent:** `net.Listen`, `net.DialTimeout`, `net.Conn.SetDeadline`, `encoding/gob`.
- **Why & How:** Instead of heavy protobuf compilers and gRPC runtime dependencies, `internal/transport/tcp.go` implements a lightweight framing layer over raw TCP using Go's built-in `encoding/gob`. Timeouts are enforced via `net.DialTimeout` (500ms) and connection read/write deadlines (`SetDeadline(1s)`).

---

### 3. Write-Ahead Log (WAL) & Storage Engine
- **Normally Used:** `github.com/boltdb/bolt` or `github.com/cockroachdb/pebble`
- **Standard Library Equivalent:** `os.OpenFile(O_CREATE|O_RDWR)`, `binary.BigEndian`, `hash/crc32`, `io.ReadFull`, and `file.Sync()`.
- **Why & How:** In `internal/storage/wal.go`, log records are framed with a 4-byte length prefix, a 4-byte IEEE CRC32 checksum, and Gob-encoded payload bytes. Every write is followed immediately by `file.Sync()` (`fsync` syscall) to guarantee physical disk durability before acknowledging consensus. Truncation and corruption resilience are handled via CRC validation and explicit `TRUNCATE` records.

---

### 4. Binary Serialization
- **Normally Used:** `google.golang.org/protobuf` or `github.com/vmihailenco/msgpack`
- **Standard Library Equivalent:** `encoding/gob` and `bytes.Buffer`.
- **Why & How:** Go's `encoding/gob` is built into the standard library and serializes Go native structs (`LogEntry`, `WALRecord`, `AppendEntriesRequest`, `RequestVoteResponse`) with minimal boilerplate and high efficiency.

---

### 5. Structured Diagnostics & Logging
- **Normally Used:** `go.uber.org/zap` or `github.com/sirupsen/logrus`
- **Standard Library Equivalent:** `log/slog` (Standard Library in Go 1.21+).
- **Why & How:** Standard library `log/slog` provides structured key-value logging with zero external dependencies and zero heap allocations for fast telemetry.

---

### 6. Client Wire Protocol
- **Normally Used:** Redis protocol parser packages or HTTP web frameworks
- **Standard Library Equivalent:** `bufio.Scanner`, `net.Listener`, `strings.SplitN`, `strings.ToUpper`.
- **Why & How:** `internal/client/server.go` implements a human-readable line-based wire protocol (`SET <k> <v>`, `GET <k>`, `DEL <k>`, `PING`, `STATUS`, `KEYS`, `LOGS`). Clients can connect via `orrisctl`, `telnet`, or `netcat`.

---

## 🏆 Package Killer Footprint

### Replaces: `hashicorp/raft` + `grpc` + `boltdb` + `zap` + `testify`

In modern Go architectures, building a replicated storage system typically requires adding:
- `github.com/hashicorp/raft` (~25k LOC + transitive deps)
- `google.golang.org/grpc` (~100k+ LOC + transitive deps)
- `go.etcd.io/bbolt` (~10k LOC)
- `go.uber.org/zap` (~12k LOC)

**`orris` standard library footprint:** Clean, transparent Go standard library code accomplishing:
1. Leader election with split-vote protection and majority quorum.
2. Suffix conflict detection & truncation.
3. Majority quorum matching & commit tracking.
4. CRC32-checksummed binary length-prefixed `fsync` WAL persistence.
5. In-memory state machine replay on crash recovery.
6. Real-time interactive TUI dashboard and client CLI.
