package raft

import (
	"log"
	"orris/internal/storage"
	"orris/internal/transport"
	"sync"
)

func (n *Node) startElection() {
	n.mu.Lock()
	if n.State == Leader {
		n.mu.Unlock()
		return
	}

	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	term := n.CurrentTerm
	lastLogIndex := -1
	lastLogTerm := 0
	if len(n.Log) > 0 {
		lastLogIndex = n.Log[len(n.Log)-1].Index
		lastLogTerm = n.Log[len(n.Log)-1].Term
	}

	_ = n.wal.Append(storage.WALRecord{
		Type:     storage.RecordState,
		Term:     n.CurrentTerm,
		VotedFor: n.VotedFor,
	})
	n.resetElectionTimer()
	n.mu.Unlock()

	log.Printf("[Consensus] Node %s starting election for Term %d", n.ID, term)

	var wg sync.WaitGroup
	votes := 1 // Vote for self
	var voteMu sync.Mutex
	peersCount := len(n.Peers) + 1

	for peerID, peerAddr := range n.Peers {
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			req := transport.RequestVoteRequest{
				Term:         term,
				CandidateID:  n.ID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			var resp transport.RequestVoteResponse
			err := transport.SendRPC(addr, transport.RPCRequestVoteReq, req, &resp)
			if err != nil {
				return
			}

			n.mu.Lock()
			// If term or state changed, discard response
			if n.CurrentTerm != term || n.State != Candidate {
				n.mu.Unlock()
				return
			}

			if resp.Term > n.CurrentTerm {
				n.stepDown(resp.Term)
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			if resp.VoteGranted {
				voteMu.Lock()
				votes++
				// If majority reached early, we can trigger leader promotion
				if votes > peersCount/2 {
					n.mu.Lock()
					if n.State == Candidate && n.CurrentTerm == term {
						n.becomeLeader()
					}
					n.mu.Unlock()
				}
				voteMu.Unlock()
			}
		}(peerID, peerAddr)
	}

	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Candidate || n.CurrentTerm != term {
		return
	}

	if votes > peersCount/2 {
		n.becomeLeader()
	} else {
		n.resetElectionTimer()
	}
}

func (n *Node) stepDown(term int) {
	n.State = Follower
	n.CurrentTerm = term
	n.VotedFor = ""
	_ = n.wal.Append(storage.WALRecord{
		Type:     storage.RecordState,
		Term:     term,
		VotedFor: "",
	})
	n.resetElectionTimer()
}

func (n *Node) becomeLeader() {
	if n.State == Leader {
		return
	}
	n.State = Leader
	log.Printf("[Consensus] Node %s became LEADER for Term %d", n.ID, n.CurrentTerm)

	nextIdx := len(n.Log)
	for id := range n.Peers {
		n.NextIndex[id] = nextIdx
		n.MatchIndex[id] = -1
	}
	n.resetElectionTimer()
	go n.sendHeartbeats()
}
