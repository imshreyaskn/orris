package raft

import (
	"log"
	"math/rand"
	"orris/internal/kv"
	"orris/internal/storage"
	"time"
)

func NewNode(id, addr string, peers map[string]string, walPath string) (*Node, error) {
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	n := &Node{
		ID:          id,
		Addr:        addr,
		Peers:       peers,
		State:       Follower,
		CurrentTerm: 0,
		VotedFor:    "",
		Log:         make([]storage.LogEntry, 0),
		CommitIndex: -1,
		LastApplied: -1,
		NextIndex:   make(map[string]int),
		MatchIndex:  make(map[string]int),
		KVStore:     kv.NewStore(),
		wal:         wal,
		stopCh:      make(chan struct{}),
		clientCh:    make(chan ClientReq, 100),
		applyNotify: make(chan struct{}, 1),
	}

	n.recoverState()
	n.resetElectionTimer()

	go n.applyLoop()
	go n.clientHandler()
	go n.heartbeatLoop()

	return n, nil
}

func (n *Node) recoverState() {
	records, err := n.wal.ReadAll()
	if err != nil {
		log.Printf("[WAL] Recovery read warning for %s: %v", n.ID, err)
		return
	}

	for _, rec := range records {
		switch rec.Type {
		case storage.RecordState:
			n.CurrentTerm = rec.Term
			n.VotedFor = rec.VotedFor
		case storage.RecordEntry:
			if rec.Entry != nil {
				n.Log = append(n.Log, *rec.Entry)
			}
		case storage.RecordTruncate:
			truncIdx := rec.CommitIndex + 1
			if truncIdx >= 0 && truncIdx < len(n.Log) {
				n.Log = n.Log[:truncIdx]
			} else if truncIdx <= 0 {
				n.Log = n.Log[:0]
			}
		case storage.RecordCommit:
			if rec.CommitIndex > n.CommitIndex {
				n.CommitIndex = rec.CommitIndex
			}
		}
	}

	// Replay all committed entries into state machine
	for i := 0; i <= n.CommitIndex && i < len(n.Log); i++ {
		entry := n.Log[i]
		if entry.Operation == storage.OpSet {
			n.KVStore.Set(entry.Key, entry.Value)
		} else if entry.Operation == storage.OpDelete {
			n.KVStore.Delete(entry.Key)
		}
		n.LastApplied = i
	}

	log.Printf("[Recovery] Node %s recovered: Term=%d, LogLen=%d, CommitIndex=%d, LastApplied=%d",
		n.ID, n.CurrentTerm, len(n.Log), n.CommitIndex, n.LastApplied)
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// Randomized timeout between 300ms and 600ms to avoid split votes
	timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
	n.electionTimer = time.AfterFunc(timeout, n.startElection)
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			isLeader := n.State == Leader
			n.mu.Unlock()
			if isLeader {
				n.sendHeartbeats()
			}
		}
	}
}

func (n *Node) Stop() {
	select {
	case <-n.stopCh:
		return // already stopped
	default:
		close(n.stopCh)
	}

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	_ = n.wal.Close()
}
