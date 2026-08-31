package raft

import (
	"orris/internal/storage"
	"time"
)

func (n *Node) applyLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyNotify:
			n.applyCommittedEntries()
		case <-ticker.C:
			n.applyCommittedEntries()
		}
	}
}

func (n *Node) applyCommittedEntries() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for n.LastApplied < n.CommitIndex && n.LastApplied < len(n.Log)-1 {
		n.LastApplied++
		entry := n.Log[n.LastApplied]
		if entry.Operation == storage.OpSet {
			n.KVStore.Set(entry.Key, entry.Value)
		} else if entry.Operation == storage.OpDelete {
			n.KVStore.Delete(entry.Key)
		}
	}
}
