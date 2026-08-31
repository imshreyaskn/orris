package raft

import (
	"errors"
	"orris/internal/storage"
	"time"
)

var (
	ErrNotLeader      = errors.New("NOT_LEADER")
	ErrLostLeadership = errors.New("LOST_LEADERSHIP")
	ErrTimeout        = errors.New("TIMEOUT")
)

func (n *Node) Submit(req ClientReq) {
	select {
	case n.clientCh <- req:
	case <-n.stopCh:
		req.ResCh <- errors.New("NODE_STOPPED")
	}
}

func (n *Node) clientHandler() {
	for {
		select {
		case <-n.stopCh:
			return
		case req := <-n.clientCh:
			n.handleClientReq(req)
		}
	}
}

func (n *Node) handleClientReq(req ClientReq) {
	n.mu.Lock()

	if n.State != Leader {
		n.mu.Unlock()
		req.ResCh <- ErrNotLeader
		return
	}

	idx := len(n.Log)
	term := n.CurrentTerm
	entry := storage.LogEntry{
		Index:     idx,
		Term:      term,
		Operation: req.Op,
		Key:       req.Key,
		Value:     req.Value,
	}

	n.Log = append(n.Log, entry)
	_ = n.wal.Append(storage.WALRecord{
		Type:  storage.RecordEntry,
		Entry: &entry,
	})
	n.mu.Unlock()

	// Proactively trigger replication to peers
	go n.sendHeartbeats()

	// Wait for entry to be committed or timeout
	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			req.ResCh <- errors.New("NODE_STOPPED")
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.State != Leader || n.CurrentTerm != term {
				n.mu.Unlock()
				req.ResCh <- ErrLostLeadership
				return
			}
			if n.CommitIndex >= idx {
				n.mu.Unlock()
				req.ResCh <- nil
				return
			}
			n.mu.Unlock()

			if time.Now().After(deadline) {
				req.ResCh <- ErrTimeout
				return
			}
		}
	}
}
