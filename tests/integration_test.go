package tests

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"orris/internal/client"
	"orris/internal/raft"
	"orris/internal/transport"
)

type testNode struct {
	id         string
	addr       string
	clientAddr string
	node       *raft.Node
	stopCh     chan struct{}
}

func sendCmd(addr, cmd string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if !strings.HasSuffix(cmd, "\n") {
		cmd += "\n"
	}
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	return "", fmt.Errorf("empty response")
}

func startTestCluster(t *testing.T, basePort int) []*testNode {
	t.Helper()
	dir := t.TempDir()

	nodeConfigs := []struct {
		id         string
		addr       string
		clientAddr string
	}{
		{"node1", fmt.Sprintf("127.0.0.1:%d", basePort), fmt.Sprintf("127.0.0.1:%d", basePort+100)},
		{"node2", fmt.Sprintf("127.0.0.1:%d", basePort+1), fmt.Sprintf("127.0.0.1:%d", basePort+101)},
		{"node3", fmt.Sprintf("127.0.0.1:%d", basePort+2), fmt.Sprintf("127.0.0.1:%d", basePort+102)},
	}

	nodes := make([]*testNode, len(nodeConfigs))

	for i, cfg := range nodeConfigs {
		peers := make(map[string]string)
		for _, other := range nodeConfigs {
			if other.id != cfg.id {
				peers[other.id] = other.addr
			}
		}

		walPath := filepath.Join(dir, fmt.Sprintf("%s.wal", cfg.id))
		rn, err := raft.NewNode(cfg.id, cfg.addr, peers, walPath)
		if err != nil {
			t.Fatalf("Failed to create node %s: %v", cfg.id, err)
		}

		stopCh := make(chan struct{})
		if err := transport.StartRPCServer(cfg.addr, rn, stopCh); err != nil {
			t.Fatalf("Failed to start RPC server %s: %v", cfg.addr, err)
		}
		if err := client.StartClientServer(cfg.clientAddr, rn, stopCh); err != nil {
			t.Fatalf("Failed to start client server %s: %v", cfg.clientAddr, err)
		}

		nodes[i] = &testNode{
			id:         cfg.id,
			addr:       cfg.addr,
			clientAddr: cfg.clientAddr,
			node:       rn,
			stopCh:     stopCh,
		}
	}

	return nodes
}

func stopTestCluster(nodes []*testNode) {
	for _, n := range nodes {
		if n.stopCh != nil {
			close(n.stopCh)
			n.node.Stop()
		}
	}
}

func TestClusterReplicationAndFailover(t *testing.T) {
	nodes := startTestCluster(t, 18500)
	defer stopTestCluster(nodes)

	// 1. Wait for Leader Election
	var leader *testNode
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.node.GetRole() == "LEADER" {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if leader == nil {
		t.Fatal("No leader elected within deadline")
	}

	// 2. Propose SET command to leader
	res, err := sendCmd(leader.clientAddr, "SET cluster_name orris-masterclass")
	if err != nil {
		t.Fatalf("Failed to send SET command: %v", err)
	}
	if !strings.HasPrefix(res, "OK") {
		t.Fatalf("Expected OK response from leader, got: %s", res)
	}

	// 3. Verify replication across all followers
	time.Sleep(300 * time.Millisecond)
	for _, n := range nodes {
		val, err := sendCmd(n.clientAddr, "GET cluster_name")
		if err != nil {
			t.Fatalf("Node %s GET failed: %v", n.id, err)
		}
		if !strings.Contains(val, "orris-masterclass") {
			t.Fatalf("Node %s state mismatch: expected orris-masterclass, got %s", n.id, val)
		}
	}

	// 4. Test PING command
	pingRes, err := sendCmd(leader.clientAddr, "PING")
	if err != nil || pingRes != "PONG" {
		t.Fatalf("Expected PONG, got %s (err: %v)", pingRes, err)
	}

	// 5. Trigger Leader Crash / Failover
	oldLeaderID := leader.id
	close(leader.stopCh)
	leader.node.Stop()
	leader.stopCh = nil // prevent double close

	// 6. Wait for new leader election among remaining 2 nodes
	var newLeader *testNode
	deadline = time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.id != oldLeaderID && n.stopCh != nil && n.node.GetRole() == "LEADER" {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if newLeader == nil {
		t.Fatal("Failover failed: No new leader elected")
	}

	// 7. Verify committed state persists on new leader
	val, err := sendCmd(newLeader.clientAddr, "GET cluster_name")
	if err != nil || !strings.Contains(val, "orris-masterclass") {
		t.Fatalf("New leader state corrupted after failover: %s (err: %v)", val, err)
	}

	// 8. Write new key to new leader
	res2, err := sendCmd(newLeader.clientAddr, "SET failover_success true")
	if err != nil || !strings.HasPrefix(res2, "OK") {
		t.Fatalf("Failed to write to new leader: %s (err: %v)", res2, err)
	}
}
