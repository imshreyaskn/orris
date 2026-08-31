package raft

import (
	"orris/internal/storage"
	"orris/internal/transport"
	"sync"
)

func (n *Node) notifyApply() {
	select {
	case n.applyNotify <- struct{}{}:
	default:
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.State != Leader {
		n.mu.Unlock()
		return
	}
	term := n.CurrentTerm
	commitIndex := n.CommitIndex

	type peerInfo struct {
		id   string
		addr string
		next int
	}
	var peers []peerInfo
	for id, addr := range n.Peers {
		peers = append(peers, peerInfo{id, addr, n.NextIndex[id]})
	}
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(p peerInfo) {
			defer wg.Done()
			n.replicateToPeer(p.id, p.addr, p.next, term, commitIndex)
		}(p)
	}
	wg.Wait()
}

func (n *Node) replicateToPeer(peerID, addr string, nextIdx, term, commitIndex int) {
	n.mu.Lock()
	if n.State != Leader || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(n.Log) {
		prevLogTerm = n.Log[prevLogIndex].Term
	}

	var entries []storage.LogEntry
	if nextIdx >= 0 && nextIdx < len(n.Log) {
		entries = append(entries, n.Log[nextIdx:]...)
	}

	req := transport.AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}
	n.mu.Unlock()

	var resp transport.AppendEntriesResponse
	err := transport.SendRPC(addr, transport.RPCAppendEntriesReq, req, &resp)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Leader || n.CurrentTerm != term {
		return
	}

	if resp.Term > n.CurrentTerm {
		n.stepDown(resp.Term)
		return
	}

	if resp.Success {
		if len(entries) > 0 {
			n.MatchIndex[peerID] = prevLogIndex + len(entries)
			n.NextIndex[peerID] = n.MatchIndex[peerID] + 1
			n.advanceCommitIndex()
		}
	} else {
		// Log conflict or term mismatch, decrement nextIndex and retry
		if nextIdx > 0 {
			n.NextIndex[peerID] = nextIdx - 1
		}
	}
}

func (n *Node) advanceCommitIndex() {
	for N := len(n.Log) - 1; N > n.CommitIndex; N-- {
		if n.Log[N].Term != n.CurrentTerm {
			continue
		}

		count := 1 // self
		for _, match := range n.MatchIndex {
			if match >= N {
				count++
			}
		}

		if count > (len(n.Peers)+1)/2 {
			n.CommitIndex = N
			_ = n.wal.Append(storage.WALRecord{
				Type:        storage.RecordCommit,
				CommitIndex: N,
			})
			n.notifyApply()
			break
		}
	}
}

func (n *Node) HandleAppendEntries(req transport.AppendEntriesRequest) transport.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.CurrentTerm {
		return transport.AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
	}

	if req.Term > n.CurrentTerm {
		n.stepDown(req.Term)
	} else {
		n.State = Follower
	}
	n.resetElectionTimer()

	// Verify log consistency at prevLogIndex
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(n.Log) || n.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
			truncateIdx := req.PrevLogIndex
			if truncateIdx > len(n.Log) {
				truncateIdx = len(n.Log)
			}
			n.Log = n.Log[:truncateIdx]
			_ = n.wal.Append(storage.WALRecord{Type: storage.RecordTruncate, CommitIndex: truncateIdx - 1})
			return transport.AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
		}
	} else if req.PrevLogIndex == -1 {
		// Starting fresh or overwriting entire log if mismatch
		if len(req.Entries) > 0 && len(n.Log) > 0 && n.Log[0].Term != req.Entries[0].Term {
			n.Log = n.Log[:0]
			_ = n.wal.Append(storage.WALRecord{Type: storage.RecordTruncate, CommitIndex: -1})
		}
	}

	// Append any new entries not already in the log
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + 1 + i
		if idx < len(n.Log) {
			if n.Log[idx].Term != entry.Term {
				n.Log = n.Log[:idx]
				_ = n.wal.Append(storage.WALRecord{Type: storage.RecordTruncate, CommitIndex: idx - 1})
				// Append this and all remaining entries
				for _, remainingEntry := range req.Entries[i:] {
					n.Log = append(n.Log, remainingEntry)
					_ = n.wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &remainingEntry})
				}
				break
			}
		} else {
			n.Log = append(n.Log, entry)
			_ = n.wal.Append(storage.WALRecord{Type: storage.RecordEntry, Entry: &entry})
		}
	}

	// Update commit index if leader's commit index is higher
	if req.LeaderCommit > n.CommitIndex {
		lastLogIdx := len(n.Log) - 1
		if req.LeaderCommit < lastLogIdx {
			n.CommitIndex = req.LeaderCommit
		} else {
			n.CommitIndex = lastLogIdx
		}
		_ = n.wal.Append(storage.WALRecord{
			Type:        storage.RecordCommit,
			CommitIndex: n.CommitIndex,
		})
		n.notifyApply()
	}

	return transport.AppendEntriesResponse{Term: n.CurrentTerm, Success: true, MatchIndex: len(n.Log) - 1}
}

func (n *Node) HandleRequestVote(req transport.RequestVoteRequest) transport.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.CurrentTerm {
		return transport.RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: false}
	}

	if req.Term > n.CurrentTerm {
		n.stepDown(req.Term)
	}

	canVote := (n.VotedFor == "" || n.VotedFor == req.CandidateID)

	lastLogIdx := -1
	lastLogTerm := 0
	if len(n.Log) > 0 {
		lastLogIdx = n.Log[len(n.Log)-1].Index
		lastLogTerm = n.Log[len(n.Log)-1].Term
	}

	// Raft rule: log is up-to-date if term is higher or terms match and candidate log is at least as long
	logUpToDate := false
	if req.LastLogTerm > lastLogTerm {
		logUpToDate = true
	} else if req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIdx {
		logUpToDate = true
	}

	if canVote && logUpToDate {
		n.VotedFor = req.CandidateID
		_ = n.wal.Append(storage.WALRecord{
			Type:     storage.RecordState,
			Term:     n.CurrentTerm,
			VotedFor: n.VotedFor,
		})
		n.resetElectionTimer()
		return transport.RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: true}
	}

	return transport.RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: false}
}
