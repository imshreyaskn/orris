package raft

import (
	"orris/internal/kv"
	"orris/internal/storage"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

type Node struct {
	mu sync.Mutex

	ID    string
	Addr  string
	Peers map[string]string

	State NodeState

	CurrentTerm int
	VotedFor    string

	Log         []storage.LogEntry
	CommitIndex int
	LastApplied int

	NextIndex  map[string]int
	MatchIndex map[string]int

	KVStore *kv.Store

	electionTimeout time.Duration
	electionTimer   *time.Timer

	wal *storage.WAL

	stopCh      chan struct{}
	clientCh    chan ClientReq
	applyNotify chan struct{} // signaled when CommitIndex advances
}

func (n *Node) GetState(key string) (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.KVStore.Get(key)
}

func (n *Node) GetRole() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.State.String()
}

func (n *Node) GetTerm() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.CurrentTerm
}

func (n *Node) GetCommit() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.CommitIndex
}

func (n *Node) GetLastApplied() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.LastApplied
}

func (n *Node) GetLogEntries() []storage.LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	entries := make([]storage.LogEntry, len(n.Log))
	copy(entries, n.Log)
	return entries
}

func (n *Node) GetAllKeys() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.KVStore.GetAll()
}

// Client Requests
type ClientReq struct {
	Op    storage.OpType
	Key   string
	Value string
	ResCh chan error
}
