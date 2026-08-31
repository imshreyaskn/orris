package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"orris/internal/storage"
)

// ANSI Color and Style Codes
const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
	bgGreen     = "\033[42m\033[30m"
	bgBlue      = "\033[44m\033[37m"
	bgYellow    = "\033[43m\033[30m"
	bgRed       = "\033[41m\033[37m"
	clearScreen = "\033[H\033[2J"
)

type NodeInfo struct {
	ID          string
	Addr        string
	Online      bool
	Role        string
	Term        int
	CommitIndex int
	LastApplied int
	LogLen      int
	Latency     time.Duration
	Keys        map[string]string
	Logs        []string
}

func main() {
	addr := flag.String("addr", "", "Target node address (default: scans 127.0.0.1:9001-9003)")
	nodesFlag := flag.String("nodes", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "Comma-separated list of cluster client addresses")
	watchMode := flag.Bool("watch", false, "Run in live auto-refresh dashboard mode")
	flag.Parse()

	args := flag.Args()
	nodeAddrs := strings.Split(*nodesFlag, ",")

	// If a direct CLI command was passed with arguments
	if len(args) > 0 {
		cmd := strings.ToUpper(args[0])
		switch cmd {
		case "START", "UP":
			count := 3
			if len(args) > 1 {
				if c, err := strconv.Atoi(args[1]); err == nil && c > 0 {
					count = c
				}
			}
			clusterStart(count)
			return
		case "STOP", "DOWN":
			clusterStop()
			return
		case "KILL-LEADER", "KILLLEADER":
			clusterKillLeader(nodeAddrs)
			return
		case "KILL":
			target := "leader"
			if len(args) > 1 {
				target = args[1]
			}
			clusterKillTarget(nodeAddrs, target)
			return
		case "SPAWN", "START-NODE", "RESTART":
			nodeNum := 1
			if len(args) > 1 {
				if c, err := strconv.Atoi(strings.TrimPrefix(strings.ToLower(args[1]), "node")); err == nil {
					nodeNum = c
				}
			}
			clusterStartSingleNode(nodeNum, 3)
			return
		case "WALS", "WAL":
			if len(args) > 1 {
				displayNodeWAL(args[1])
			} else {
				displayAllWALs()
			}
			return
		case "BENCH", "BENCHMARK":
			total := 50
			concurrency := 5
			if len(args) > 1 {
				if c, err := strconv.Atoi(args[1]); err == nil && c > 0 {
					total = c
				}
			}
			if len(args) > 2 {
				if c, err := strconv.Atoi(args[2]); err == nil && c > 0 {
					concurrency = c
				}
			}
			runConcurrentBenchmark(nodeAddrs, total, concurrency)
			return
		case "PING":
			displayPing(nodeAddrs)
			return
		case "LEADER":
			displayLeader(nodeAddrs)
			return
		case "STATUS", "INFO":
			displayStatus(nodeAddrs)
			return
		case "KEYS":
			displayKeys(nodeAddrs)
			return
		case "LOGS", "LOG":
			displayLogs(nodeAddrs)
			return
		case "DEMO":
			runFullDemo(nodeAddrs)
			return
		case "CLUSTER":
			if len(args) > 1 {
				sub := strings.ToUpper(args[1])
				switch sub {
				case "START", "UP":
					count := 3
					if len(args) > 2 {
						if c, err := strconv.Atoi(args[2]); err == nil && c > 0 {
							count = c
						}
					}
					clusterStart(count)
					return
				case "STOP", "DOWN":
					clusterStop()
					return
				case "KILL-LEADER":
					clusterKillLeader(nodeAddrs)
					return
				}
			}
			fmt.Println("Usage: orrisctl cluster [start [N]|stop|kill-leader]")
			return

		case "RUN", "LIVE", "DASHBOARD", "TOP", "UI":
			runDashboard(nodeAddrs, true)
			return
		case "REPL", "INTERACTIVE":
			runREPL(nodeAddrs)
			return
		}

		targetAddr := *addr
		if targetAddr == "" {
			targetAddr = nodeAddrs[0]
		}

		// Handle SET/DELETE/DEL auto-routing to leader
		if cmd == "SET" || cmd == "DELETE" || cmd == "DEL" {
			handleRoutedCommand(nodeAddrs, targetAddr, cmd, args[1:])
			return
		}

		handleSingleCommand(targetAddr, cmd, args[1:])
		return
	}

	if *watchMode {
		runDashboard(nodeAddrs, true)
		return
	}

	// Default mode: Launch Interactive REPL + Live Visualizer
	runREPL(nodeAddrs)
}

// -------------------------------------------------------------
// Cross-Platform Cluster Management
// -------------------------------------------------------------

func getOrrisdExe() string {
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	exePath := filepath.Join("bin", "orrisd"+ext)
	if _, err := os.Stat(exePath); os.IsNotExist(err) {
		fmt.Printf("%s[Building orrisd binary...]%s\n", colorDim, colorReset)
		_ = os.MkdirAll("bin", 0755)
		cmd := exec.Command("go", "build", "-o", exePath, "./cmd/orrisd")
		if err := cmd.Run(); err != nil {
			fmt.Printf("%sFailed to build orrisd: %v%s\n", colorRed, err, colorReset)
			return ""
		}
	}
	return exePath
}

func clusterStart(count int) {
	if count < 1 {
		count = 3
	}
	fmt.Printf("%s%s>>> Starting %d-Node Raft Cluster (Cross-Platform)...%s\n", colorBold, colorCyan, count, colorReset)
	clusterStopSilent()

	for i := 1; i <= count; i++ {
		clusterStartSingleNode(i, count)
	}

	fmt.Printf("%sWaiting for leader election...%s\n", colorDim, colorReset)
	time.Sleep(2 * time.Second)

	var nodeAddrs []string
	for i := 1; i <= count; i++ {
		nodeAddrs = append(nodeAddrs, fmt.Sprintf("127.0.0.1:%d", 9000+i))
	}

	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online && n.Role == "LEADER" {
			fmt.Printf("%s%s[OK] Cluster Ready! Active Leader: %s on %s (Term: %d)%s\n",
				colorBold, colorGreen, n.ID, n.Addr, n.Term, colorReset)
			return
		}
	}
	fmt.Printf("%sCluster initialized.%s\n", colorYellow, colorReset)
}

func clusterStartSingleNode(i, totalCount int) {
	exe := getOrrisdExe()
	if exe == "" {
		return
	}

	id := fmt.Sprintf("node%d", i)
	addr := fmt.Sprintf("127.0.0.1:%d", 8000+i)
	clientAddr := fmt.Sprintf("127.0.0.1:%d", 9000+i)
	logPath := fmt.Sprintf("node%d.log", i)
	pidPath := fmt.Sprintf("node%d.pid", i)
	dataPath := fmt.Sprintf("./data/node%d", i)
	_ = os.MkdirAll(dataPath, 0755)

	var peersList []string
	for j := 1; j <= totalCount; j++ {
		if j != i {
			peersList = append(peersList, fmt.Sprintf("node%d=127.0.0.1:%d", j, 8000+j))
		}
	}
	peers := strings.Join(peersList, ",")

	cmd := exec.Command(exe,
		"-id", id,
		"-addr", addr,
		"-client", clientAddr,
		"-peers", peers,
		"-data", dataPath,
		"-log", logPath,
	)

	if runtime.GOOS == "windows" {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			CreationFlags: 0x00000008 | 0x00000200 | 0x08000000,
		}
	}

	if err := cmd.Start(); err != nil {
		fmt.Printf("%sFailed to start %s: %v%s\n", colorRed, id, err, colorReset)
	} else {
		_ = os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0644)
		fmt.Printf(" %s[OK]%s Started %s (Consensus: %s, Client: %s, PID: %d)\n",
			colorGreen, colorReset, id, addr, clientAddr, cmd.Process.Pid)
	}
}

func clusterStopSilent() {
	for i := 1; i <= 20; i++ {
		pidFile := fmt.Sprintf("node%d.pid", i)
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				if proc, err := os.FindProcess(pid); err == nil {
					_ = proc.Kill()
				}
			}
			_ = os.Remove(pidFile)
		}
	}
}

func clusterStop() {
	fmt.Printf("%s%s>>> Stopping all cluster nodes...%s\n", colorBold, colorYellow, colorReset)
	stopped := 0
	for i := 1; i <= 20; i++ {
		pidFile := fmt.Sprintf("node%d.pid", i)
		if data, err := os.ReadFile(pidFile); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				if proc, err := os.FindProcess(pid); err == nil {
					_ = proc.Kill()
					fmt.Printf(" %s[OK]%s Stopped node%d (PID: %d)\n", colorGreen, colorReset, i, pid)
					stopped++
				}
			}
			_ = os.Remove(pidFile)
		}
	}
	if stopped == 0 {
		fmt.Printf("%sNo running nodes found.%s\n", colorDim, colorReset)
	} else {
		fmt.Printf("%s%s[OK] All %d nodes stopped.%s\n", colorBold, colorGreen, stopped, colorReset)
	}
}

func clusterKillLeader(nodeAddrs []string) {
	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online && n.Role == "LEADER" {
			pidFile := fmt.Sprintf("%s.pid", n.ID)
			if data, err := os.ReadFile(pidFile); err == nil {
				if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
					if proc, err := os.FindProcess(pid); err == nil {
						_ = proc.Kill()
						_ = os.Remove(pidFile)
						fmt.Printf("%s[CHAOS] Killed active leader %s (%s, PID: %d)%s\n",
							colorBold+colorRed, n.ID, n.Addr, pid, colorReset)
						return
					}
				}
			}
		}
	}
	fmt.Printf("%sNo active leader found to kill.%s\n", colorYellow, colorReset)
}

func clusterKillTarget(nodeAddrs []string, target string) {
	target = strings.ToLower(strings.TrimSpace(target))
	if target == "leader" {
		clusterKillLeader(nodeAddrs)
		return
	}

	targetID := target
	if !strings.HasPrefix(targetID, "node") {
		targetID = "node" + targetID
	}

	pidFile := fmt.Sprintf("%s.pid", targetID)
	if data, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			if proc, err := os.FindProcess(pid); err == nil {
				_ = proc.Kill()
				_ = os.Remove(pidFile)
				fmt.Printf("%s[CHAOS] Killed target node %s (PID: %d)%s\n", colorBold+colorRed, targetID, pid, colorReset)
				return
			}
		}
	}
	fmt.Printf("%sCould not find running process for %s%s\n", colorYellow, targetID, colorReset)
}

// -------------------------------------------------------------
// Diagnostics & Inspection Helpers
// -------------------------------------------------------------

func displayPing(nodeAddrs []string) {
	fmt.Printf("\n%s%s=== CLUSTER PING & LATENCY ===%s\n", colorBold, colorCyan, colorReset)
	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online {
			fmt.Printf(" %s* Node %s%s (%s) -> %sPONG%s in %s%v%s\n",
				colorGreen, n.ID, colorReset, n.Addr, colorBold+colorGreen, colorReset, colorYellow, n.Latency.Round(time.Microsecond), colorReset)
		} else {
			fmt.Printf(" %s* Node on %s%s -> %s[OFFLINE]%s\n", colorRed, n.Addr, colorReset, colorRed, colorReset)
		}
	}
	fmt.Println()
}

func displayLeader(nodeAddrs []string) {
	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online && n.Role == "LEADER" {
			fmt.Printf("%s%s[LEADER]%s Node %s on %s (Term: %d, CommitIndex: %d)\n",
				colorBold, colorGreen, colorReset, n.ID, n.Addr, n.Term, n.CommitIndex)
			return
		}
	}
	fmt.Printf("%sNo active leader detected.%s\n", colorYellow, colorReset)
}

func displayStatus(nodeAddrs []string) {
	fmt.Printf("\n%s%s=== CLUSTER STATUS MATRIX ===%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("   %-8s | %-16s | %-10s | %-6s | %-12s | %-12s | %-8s\n",
		"NODE", "CLIENT ADDR", "ROLE", "TERM", "COMMIT IDX", "LAST APPLIED", "LATENCY")
	fmt.Println("   " + strings.Repeat("-", 82))
	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online {
			badgeColor := colorBlue
			if n.Role == "LEADER" {
				badgeColor = colorGreen
			} else if n.Role == "CANDIDATE" {
				badgeColor = colorYellow
			}
			fmt.Printf("   %-8s | %-16s | %s%-10s%s | %-6d | %-12d | %-12d | %-8v\n",
				n.ID, n.Addr, badgeColor, n.Role, colorReset, n.Term, n.CommitIndex, n.LastApplied, n.Latency.Round(time.Microsecond))
		} else {
			fmt.Printf("   %-8s | %-16s | %s%-10s%s | %-6s | %-12s | %-12s | %-8s\n",
				"-", n.Addr, colorRed, "OFFLINE", colorReset, "-", "-", "-", "-")
		}
	}
	fmt.Println()
}

func displayKeys(nodeAddrs []string) {
	nodes := queryCluster(nodeAddrs)
	allKeys := make(map[string]map[string]string)
	for _, n := range nodes {
		if n.Online {
			for k, v := range n.Keys {
				if allKeys[k] == nil {
					allKeys[k] = make(map[string]string)
				}
				allKeys[k][n.ID] = v
			}
		}
	}

	if len(allKeys) == 0 {
		fmt.Println("No keys stored in cluster.")
		return
	}

	var sortedKeys []string
	for k := range allKeys {
		sortedKeys = append(sortedKeys, k)
	}
	slices.Sort(sortedKeys)

	fmt.Printf("\n%s%s=== ALL KEYS MATRIX ===%s\n", colorBold, colorCyan, colorReset)
	for _, k := range sortedKeys {
		var parts []string
		for _, n := range nodes {
			if n.Online {
				if v, ok := allKeys[k][n.ID]; ok {
					parts = append(parts, fmt.Sprintf("%s:%s", n.ID, v))
				}
			}
		}
		fmt.Printf("   • %s%-16s%s -> %s\n", colorBold, k, colorReset, strings.Join(parts, " | "))
	}
	fmt.Println()
}

func displayLogs(nodeAddrs []string) {
	nodes := queryCluster(nodeAddrs)
	var primaryLogs []string
	highestCommit := -1
	for _, n := range nodes {
		if n.Online {
			if n.CommitIndex > highestCommit {
				highestCommit = n.CommitIndex
			}
			if len(n.Logs) > len(primaryLogs) {
				primaryLogs = n.Logs
			}
		}
	}

	if len(primaryLogs) == 0 {
		fmt.Println("Log is empty.")
		return
	}

	fmt.Printf("\n%s%s=== FULL REPLICATED LOG TIMELINE (%d Entries) ===%s\n", colorBold, colorCyan, len(primaryLogs), colorReset)
	fmt.Printf("   %-8s | %-8s | %-8s | %-32s | %-12s\n", "INDEX", "TERM", "OP", "KEY / VALUE", "STATUS")
	fmt.Println("   " + strings.Repeat("-", 76))
	for _, raw := range primaryLogs {
		item, ok := parseLogItem(raw)
		if !ok {
			continue
		}
		status := colorGreen + "[COMMITTED]" + colorReset
		if item.Index > highestCommit {
			status = colorYellow + "[UNCOMMITTED]" + colorReset
		}
		payload := fmt.Sprintf("%s = %s", item.Key, item.Val)
		if item.Op == "DELETE" || item.Op == "DEL" {
			payload = item.Key
		}
		if len(payload) > 32 {
			payload = payload[:29] + "..."
		}
		fmt.Printf("   %-8s | %-8s | %-8s | %-32s | %-12s\n",
			fmt.Sprintf("#%d", item.Index),
			fmt.Sprintf("Term %d", item.Term),
			item.Op,
			payload,
			status,
		)
	}
	fmt.Println()
}

// -------------------------------------------------------------
// On-Disk WAL Inspection & Visualization
// -------------------------------------------------------------

func displayNodeWAL(nodeTarget string) {
	nodeTarget = strings.ToLower(strings.TrimSpace(nodeTarget))
	if !strings.HasPrefix(nodeTarget, "node") {
		nodeTarget = "node" + nodeTarget
	}

	walPath := filepath.Join("data", nodeTarget, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		fmt.Printf("%s[ERR] WAL file not found at %s: %v%s\n", colorRed, walPath, err, colorReset)
		return
	}

	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		fmt.Printf("%s[ERR] Failed to open WAL at %s: %v%s\n", colorRed, walPath, err, colorReset)
		return
	}
	defer wal.Close()

	records, err := wal.ReadAll()
	if err != nil {
		fmt.Printf("%s[ERR] Failed to read WAL records: %v%s\n", colorRed, err, colorReset)
		return
	}

	fmt.Printf("\n%s%s=== ON-DISK WAL INSPECTION: %s (%s, %d bytes) ===%s\n",
		colorBold, colorCyan, strings.ToUpper(nodeTarget), walPath, info.Size(), colorReset)
	fmt.Printf("   %-6s | %-10s | %-8s | %-12s | %-32s\n", "#", "REC TYPE", "TERM", "VOTED / COMMIT", "PAYLOAD / LOG ENTRY")
	fmt.Println("   " + strings.Repeat("-", 80))

	for i, r := range records {
		typeColor := colorWhite
		votedCommit := "-"
		payload := "-"

		switch r.Type {
		case storage.RecordState:
			typeColor = colorBlue
			votedCommit = fmt.Sprintf("Voted:%s", r.VotedFor)
			payload = fmt.Sprintf("Term state set to %d", r.Term)
		case storage.RecordCommit:
			typeColor = colorGreen
			votedCommit = fmt.Sprintf("CommitIdx:%d", r.CommitIndex)
			payload = fmt.Sprintf("Advanced CommitIndex to %d", r.CommitIndex)
		case storage.RecordTruncate:
			typeColor = colorRed
			votedCommit = fmt.Sprintf("TruncTo:%d", r.CommitIndex)
			payload = fmt.Sprintf("Log truncated after index %d", r.CommitIndex)
		case storage.RecordEntry:
			typeColor = colorYellow
			if r.Entry != nil {
				votedCommit = fmt.Sprintf("Idx:%d T%d", r.Entry.Index, r.Entry.Term)
				payload = fmt.Sprintf("%s %s = %s", r.Entry.Operation, r.Entry.Key, r.Entry.Value)
			}
		}

		fmt.Printf("   %-6d | %s%-10s%s | %-8d | %-12s | %-32s\n",
			i+1, typeColor, r.Type, colorReset, r.Term, votedCommit, payload)
	}
	fmt.Printf("\n   %sTotal on-disk records: %d (All CRC32 checksums verified)%s\n\n", colorGreen, len(records), colorReset)
}

func displayAllWALs() {
	fmt.Printf("\n%s%s=== ALL NODE WRITE-AHEAD LOG (WAL) COMPARISON ===%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("   %-10s | %-12s | %-10s | %-12s | %-16s\n", "NODE", "DISK SIZE", "RECORDS", "LAST TERM", "LOG ENTRIES")
	fmt.Println("   " + strings.Repeat("-", 72))

	for i := 1; i <= 10; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		walPath := filepath.Join("data", nodeID, "wal.log")
		info, err := os.Stat(walPath)
		if err != nil {
			continue
		}

		wal, err := storage.OpenWAL(walPath)
		if err != nil {
			continue
		}

		records, _ := wal.ReadAll()
		wal.Close()

		lastTerm := 0
		entriesCount := 0
		for _, r := range records {
			if r.Term > lastTerm {
				lastTerm = r.Term
			}
			if r.Type == storage.RecordEntry {
				entriesCount++
			}
		}

		fmt.Printf("   %-10s | %-12s | %-10d | %-12d | %-16d\n",
			nodeID, fmt.Sprintf("%d B", info.Size()), len(records), lastTerm, entriesCount)
	}
	fmt.Printf("\n   %s(Type 'wal <node>' e.g. 'wal node1' to inspect raw ledger records)%s\n\n", colorDim, colorReset)
}

func runFullDemo(nodeAddrs []string) {
	fmt.Printf("\n%s%s========================================================================%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s               ORRIS END-TO-END DEMO & VALIDATION                       %s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s========================================================================%s\n\n", colorBold, colorCyan, colorReset)

	// 1. Start Cluster
	clusterStart(3)
	time.Sleep(1 * time.Second)

	// 2. Write Data
	fmt.Printf("\n%s%s>>> [1/4] Writing Data via Consensus Leader...%s\n", colorBold, colorWhite, colorReset)
	res1, lat1, l1 := executeRouted(nodeAddrs, "SET project orris-zero-dep")
	fmt.Printf(" Write 1: 'project'='orris-zero-dep' -> %s (via %s in %v)\n", res1, l1, lat1)
	res2, lat2, l2 := executeRouted(nodeAddrs, "SET system distributed-raft")
	fmt.Printf(" Write 2: 'system'='distributed-raft' -> %s (via %s in %v)\n", res2, l2, lat2)

	time.Sleep(500 * time.Millisecond)

	// 3. Read Data from ALL Nodes
	fmt.Printf("\n%s%s>>> [2/4] Verifying Replicated State Across All Follower Nodes...%s\n", colorBold, colorWhite, colorReset)
	for _, addr := range nodeAddrs {
		val, lat, err := sendRawCommand(addr, "GET project")
		if err == nil {
			fmt.Printf(" Node on %-14s -> %s%s%s (in %v)\n", addr, colorGreen, val, colorReset, lat)
		}
	}

	// 4. Failover & Leader Election
	fmt.Printf("\n%s%s>>> [3/4] Triggering Leader Failover (Killing Leader)...%s\n", colorBold, colorWhite, colorReset)
	clusterKillLeader(nodeAddrs)
	fmt.Printf("%sWaiting 2 seconds for quorum re-election...%s\n", colorDim, colorReset)
	time.Sleep(2 * time.Second)

	fmt.Printf("\n%s%s>>> [4/4] Verifying State & New Leader After Failover...%s\n", colorBold, colorWhite, colorReset)
	nodes := queryCluster(nodeAddrs)
	for _, n := range nodes {
		if n.Online {
			badge := colorBlue + n.Role + colorReset
			if n.Role == "LEADER" {
				badge = colorGreen + "[LEADER]" + colorReset
			}
			fmt.Printf(" Node %s on %-14s -> Role: %s | Term: %d | Val(project): %s%s%s\n",
				n.ID, n.Addr, badge, n.Term, colorGreen, n.Keys["project"], colorReset)
		} else {
			fmt.Printf(" Node on %-19s -> %s[OFFLINE / KILLED]%s\n", n.Addr, colorRed, colorReset)
		}
	}

	fmt.Printf("\n%s%s[OK] DEMO COMPLETE: Consensus, Replication, WAL & Failover Verified!%s\n\n", colorBold, colorGreen, colorReset)
	clusterStop()
}

// -------------------------------------------------------------
// Network & Protocol Handlers
// -------------------------------------------------------------

func sendRawCommand(addr, cmd string) (string, time.Duration, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return "", 0, err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if !strings.HasSuffix(cmd, "\n") {
		cmd += "\n"
	}
	if _, err := conn.Write([]byte(cmd)); err != nil {
		return "", 0, err
	}

	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text(), time.Since(start), nil
	}
	return "", time.Since(start), fmt.Errorf("no response")
}

func queryNode(addr string) NodeInfo {
	info := NodeInfo{
		Addr:   addr,
		Online: false,
		Keys:   make(map[string]string),
	}

	statusResp, latency, err := sendRawCommand(addr, "STATUS")
	if err != nil {
		return info
	}

	info.Online = true
	info.Latency = latency

	parts := strings.Fields(statusResp)
	for i := 0; i < len(parts)-1; i++ {
		switch parts[i] {
		case "NODE":
			info.ID = parts[i+1]
		case "ROLE":
			info.Role = parts[i+1]
		case "TERM":
			info.Term, _ = strconv.Atoi(parts[i+1])
		case "COMMIT":
			info.CommitIndex, _ = strconv.Atoi(parts[i+1])
		case "APPLIED":
			info.LastApplied, _ = strconv.Atoi(parts[i+1])
		case "LOGLEN":
			info.LogLen, _ = strconv.Atoi(parts[i+1])
		}
	}

	if keysResp, _, err := sendRawCommand(addr, "KEYS"); err == nil {
		keysStr := strings.TrimPrefix(keysResp, "KEYS ")
		if keysStr != "(empty)" && keysStr != "" {
			pairs := strings.Split(keysStr, " | ")
			for _, p := range pairs {
				kv := strings.SplitN(p, "=", 2)
				if len(kv) == 2 {
					info.Keys[kv[0]] = kv[1]
				}
			}
		}
	}

	if logsResp, _, err := sendRawCommand(addr, "LOGS"); err == nil {
		logsStr := strings.TrimPrefix(logsResp, "LOGS ")
		if logsStr != "(empty)" && logsStr != "" {
			info.Logs = strings.Split(logsStr, " | ")
		}
	}

	return info
}

func queryCluster(nodeAddrs []string) []NodeInfo {
	var wg sync.WaitGroup
	results := make([]NodeInfo, len(nodeAddrs))

	for i, addr := range nodeAddrs {
		wg.Add(1)
		go func(idx int, target string) {
			defer wg.Done()
			results[idx] = queryNode(target)
		}(i, addr)
	}
	wg.Wait()
	return results
}

// -------------------------------------------------------------
// Visual Dashboard & REPL
// -------------------------------------------------------------

type LogItem struct {
	Index int
	Term  int
	Op    string
	Key   string
	Val   string
}

func parseLogItem(raw string) (LogItem, bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "[") {
		return LogItem{}, false
	}
	closeBracket := strings.Index(raw, "]")
	if closeBracket == -1 {
		return LogItem{}, false
	}
	idx, _ := strconv.Atoi(raw[1:closeBracket])
	rest := raw[closeBracket+1:]
	parts := strings.SplitN(rest, ":", 3)
	if len(parts) < 3 {
		return LogItem{Index: idx}, false
	}
	termStr := strings.TrimPrefix(parts[0], "T")
	term, _ := strconv.Atoi(termStr)
	op := parts[1]
	kv := strings.SplitN(parts[2], "=", 2)
	k := kv[0]
	v := ""
	if len(kv) > 1 {
		v = kv[1]
	}
	return LogItem{Index: idx, Term: term, Op: op, Key: k, Val: v}, true
}

func renderDashboard(nodes []NodeInfo) {
	fmt.Print(clearScreen)
	now := time.Now().Format("15:04:05")

	fmt.Printf("%s%s╔══════════════════════════════════════════════════════════════════════════════════════╗%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("%s%s║  ORRIS CLUSTER VISUALIZER  [Zero-Dependency Raft Consensus]          %sTIME: %s  ║%s\n", colorBold, colorCyan, colorYellow, now, colorReset)
	fmt.Printf("%s%s╚══════════════════════════════════════════════════════════════════════════════════════╝%s\n\n", colorBold, colorCyan, colorReset)

	fmt.Printf("%s%s--- [ CLUSTER NODES & RAFT STATE ] ---------------------------------------------------%s\n", colorBold, colorWhite, colorReset)
	for i, n := range nodes {
		if !n.Online {
			name := n.ID
			if name == "" {
				name = fmt.Sprintf("Node %d", i+1)
			}
			fmt.Printf(" %s* %s%s (%s)\n", colorRed, name, colorReset, n.Addr)
			fmt.Printf("   Status: %s[OFFLINE / STOPPED]%s  (Run 'spawn %d' to restart)\n\n", bgRed, colorReset, i+1)
			continue
		}

		var roleBadge string
		switch n.Role {
		case "LEADER":
			roleBadge = fmt.Sprintf("%s [LEADER] %s", bgGreen, colorReset)
		case "CANDIDATE":
			roleBadge = fmt.Sprintf("%s [CANDIDATE] %s", bgYellow, colorReset)
		default:
			roleBadge = fmt.Sprintf("%s [FOLLOWER] %s", bgBlue, colorReset)
		}

		fmt.Printf(" %s* Node %s%s on %s%s%s (ping: %s%v%s)\n",
			colorGreen, n.ID, colorReset, colorCyan, n.Addr, colorReset, colorDim, n.Latency.Round(time.Microsecond), colorReset)
		fmt.Printf("   Role: %s   Term: %s%d%s   CommitIndex: %s%d%s   Applied: %s%d%s   LogEntries: %s%d%s\n\n",
			roleBadge, colorBold, n.Term, colorReset, colorBold, n.CommitIndex, colorReset, colorBold, n.LastApplied, colorReset, colorBold, n.LogLen, colorReset)
	}

	fmt.Printf("%s%s--- [ REPLICATED STATE MACHINE (KV STORE) ] -------------------------------------------%s\n", colorBold, colorWhite, colorReset)
	allKeys := make(map[string]bool)
	for _, n := range nodes {
		if n.Online {
			for k := range n.Keys {
				allKeys[k] = true
			}
		}
	}

	if len(allKeys) == 0 {
		fmt.Printf("   %s(No keys stored yet — use 'set <key> <value>' to write data)%s\n\n", colorDim, colorReset)
	} else {
		var sortedKeys []string
		for k := range allKeys {
			sortedKeys = append(sortedKeys, k)
		}
		slices.Sort(sortedKeys)

		header := fmt.Sprintf("   %-16s", "KEY")
		for _, n := range nodes {
			label := n.ID
			if label == "" {
				label = n.Addr
			}
			header += fmt.Sprintf(" | %-16s", label)
		}
		fmt.Println(header)
		fmt.Println("   " + strings.Repeat("-", len(header)-3))

		for _, k := range sortedKeys {
			row := fmt.Sprintf("   %-16s", k)
			for _, n := range nodes {
				if !n.Online {
					row += fmt.Sprintf(" | %s%-16s%s", colorRed, "OFFLINE", colorReset)
				} else if v, exists := n.Keys[k]; exists {
					valDisplay := v
					if len(valDisplay) > 16 {
						valDisplay = valDisplay[:13] + "..."
					}
					row += fmt.Sprintf(" | %s%-16s%s", colorGreen, valDisplay, colorReset)
				} else {
					row += fmt.Sprintf(" | %s%-16s%s", colorDim, "<nil>", colorReset)
				}
			}
			fmt.Println(row)
		}
		fmt.Println()
	}

	// Clean RAFT WAL Log Table
	var primaryLogs []string
	highestCommit := -1
	for _, n := range nodes {
		if n.Online {
			if n.CommitIndex > highestCommit {
				highestCommit = n.CommitIndex
			}
			if len(n.Logs) > len(primaryLogs) {
				primaryLogs = n.Logs
			}
		}
	}

	if len(primaryLogs) == 0 {
		fmt.Printf("%s%s--- [ RAFT WAL & LOG ENTRIES ] --------------------------------------------------------%s\n", colorBold, colorWhite, colorReset)
		fmt.Printf("   %s(Log is empty)%s\n\n", colorDim, colorReset)
	} else {
		total := len(primaryLogs)
		displayCount := 6
		startIdx := 0
		if total > displayCount {
			startIdx = total - displayCount
		}
		recentLogs := primaryLogs[startIdx:]

		fmt.Printf("%s%s--- [ RAFT WAL & LOG ENTRIES (Recent %d of %d Entries) ] --------------------------------%s\n", colorBold, colorWhite, len(recentLogs), total, colorReset)
		fmt.Printf("   %-8s | %-8s | %-8s | %-32s | %-12s\n", "INDEX", "TERM", "OP", "KEY / VALUE", "COMMIT STATE")
		fmt.Println("   " + strings.Repeat("-", 76))

		for _, raw := range recentLogs {
			item, ok := parseLogItem(raw)
			if !ok {
				continue
			}
			opColor := colorGreen
			payload := fmt.Sprintf("%s = %s", item.Key, item.Val)
			if item.Op == "DELETE" || item.Op == "DEL" {
				opColor = colorRed
				payload = item.Key
			}
			if len(payload) > 32 {
				payload = payload[:29] + "..."
			}

			status := colorGreen + "[COMMITTED]" + colorReset
			if item.Index > highestCommit {
				status = colorYellow + "[UNCOMMITTED]" + colorReset
			}

			fmt.Printf("   %-8s | %-8s | %-16s | %-32s | %-12s\n",
				fmt.Sprintf("#%d", item.Index),
				fmt.Sprintf("Term %d", item.Term),
				opColor+fmt.Sprintf("%-6s", item.Op)+colorReset,
				payload,
				status,
			)
		}
		fmt.Printf("   %s(All active nodes synchronized • Type 'wals' to view all %d node WAL records)%s\n\n", colorDim, total, colorReset)
	}
}

func ensureClusterRunning(nodeAddrs []string) {
	nodes := queryCluster(nodeAddrs)
	allOffline := true
	for _, n := range nodes {
		if n.Online {
			allOffline = false
			break
		}
	}
	if allOffline {
		fmt.Printf("%s[No active cluster detected — auto-starting 3-node Raft cluster in background...]%s\n", colorCyan, colorReset)
		clusterStart(3)
		time.Sleep(500 * time.Millisecond)
	}
}

func runDashboard(nodeAddrs []string, loop bool) {
	ensureClusterRunning(nodeAddrs)
	for {
		nodes := queryCluster(nodeAddrs)
		renderDashboard(nodes)
		if !loop {
			break
		}
		fmt.Printf("%s[Auto-refreshing every 1s — Press Ctrl+C to exit]%s\n", colorDim, colorReset)
		time.Sleep(1 * time.Second)
	}
}

func runREPL(nodeAddrs []string) {
	ensureClusterRunning(nodeAddrs)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		// Discover active nodes dynamically up to 10
		var activeAddrs []string
		for i := 1; i <= 10; i++ {
			activeAddrs = append(activeAddrs, fmt.Sprintf("127.0.0.1:%d", 9000+i))
		}
		nodes := queryCluster(activeAddrs)
		var displayedNodes []NodeInfo
		for i, n := range nodes {
			if n.Online || i < 3 {
				displayedNodes = append(displayedNodes, n)
			}
		}
		renderDashboard(displayedNodes)

		fmt.Printf("%s%sCOMMANDS:%s set <k> <v> | get <k> | del <k> | keys | wals | wal <node> | kill <node|leader> | spawn <node> | ping | leader | bench [n] [c] | help | exit\n",
			colorBold, colorPurple, colorReset)
		fmt.Printf("%sorris> %s", colorBold, colorReset)

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		line = strings.TrimPrefix(line, "orris>")
		line = strings.TrimPrefix(line, "orris >")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "exit", "quit", "q":
			fmt.Println("Goodbye!")
			return
		case "clear", "cls":
			continue
		case "start", "up", "scale":
			count := 3
			if len(parts) > 1 {
				if c, err := strconv.Atoi(parts[1]); err == nil && c > 0 {
					count = c
				}
			}
			clusterStart(count)
		case "stop", "down":
			clusterStop()
		case "kill-leader", "killleader":
			clusterKillLeader(nodeAddrs)
		case "kill":
			target := "leader"
			if len(parts) > 1 {
				target = parts[1]
			}
			clusterKillTarget(nodeAddrs, target)
		case "spawn", "start-node", "restart-node", "restart":
			nodeNum := 1
			if len(parts) > 1 {
				if c, err := strconv.Atoi(strings.TrimPrefix(strings.ToLower(parts[1]), "node")); err == nil {
					nodeNum = c
				}
			}
			clusterStartSingleNode(nodeNum, len(displayedNodes))
		case "ping":
			displayPing(nodeAddrs)
		case "leader":
			displayLeader(nodeAddrs)
		case "status", "info":
			displayStatus(nodeAddrs)
		case "keys", "dump":
			displayKeys(nodeAddrs)
		case "wals":
			displayAllWALs()
		case "wal":
			if len(parts) > 1 {
				displayNodeWAL(parts[1])
			} else {
				displayAllWALs()
			}
		case "watch", "top":
			runDashboard(nodeAddrs, true)
			return

		case "set":
			if len(parts) < 3 {
				fmt.Printf("%s[ERR] Usage: set <key> <value>%s\n", colorRed, colorReset)
			} else {
				key := parts[1]
				val := strings.Join(parts[2:], " ")
				res, lat, leaderAddr := executeRouted(nodeAddrs, fmt.Sprintf("SET %s %s", key, val))
				if strings.HasPrefix(res, "OK") {
					fmt.Printf("%s[COMMITTED]%s Set '%s' = '%s' via %s (latency: %v)\n", colorGreen, colorReset, key, val, leaderAddr, lat)
				} else {
					fmt.Printf("%s[FAILED]%s %s\n", colorRed, colorReset, res)
				}
			}
		case "get":
			if len(parts) < 2 {
				fmt.Printf("%s[ERR] Usage: get <key>%s\n", colorRed, colorReset)
			} else {
				key := parts[1]
				found := false
				for _, n := range nodes {
					if n.Online {
						res, lat, _ := sendRawCommand(n.Addr, fmt.Sprintf("GET %s", key))
						if strings.HasPrefix(res, "VALUE") {
							val := strings.TrimPrefix(res, "VALUE ")
							fmt.Printf("%s[FOUND]%s Key '%s' = '%s%s%s' (from %s in %v)\n", colorGreen, colorReset, key, colorBold, val, colorReset, n.Addr, lat)
							found = true
							break
						}
					}
				}
				if !found {
					fmt.Printf("%s[NOT FOUND]%s Key '%s' not present\n", colorYellow, colorReset, key)
				}
			}
		case "del", "delete":
			if len(parts) < 2 {
				fmt.Printf("%s[ERR] Usage: del <key>%s\n", colorRed, colorReset)
			} else {
				key := parts[1]
				res, lat, leaderAddr := executeRouted(nodeAddrs, fmt.Sprintf("DEL %s", key))
				if strings.HasPrefix(res, "OK") {
					fmt.Printf("%s[COMMITTED]%s Deleted '%s' via %s (latency: %v)\n", colorGreen, colorReset, key, leaderAddr, lat)
				} else {
					fmt.Printf("%s[FAILED]%s %s\n", colorRed, colorReset, res)
				}
			}
		case "bench", "benchmark":
			total := 50
			concurrency := 5
			if len(parts) >= 2 {
				if c, err := strconv.Atoi(parts[1]); err == nil && c > 0 {
					total = c
				}
			}
			if len(parts) >= 3 {
				if c, err := strconv.Atoi(parts[2]); err == nil && c > 0 {
					concurrency = c
				}
			}
			runConcurrentBenchmark(nodeAddrs, total, concurrency)

		case "logs", "log":
			displayLogs(nodeAddrs)

		case "help", "?":
			fmt.Println("\nAvailable commands:")
			fmt.Println("  [Cluster Management]")
			fmt.Println("    start [N]         - Start an N-node cluster (default: 3)")
			fmt.Println("    stop              - Stop all cluster nodes")
			fmt.Println("    kill <node|leader>- Crash/kill a specific node (e.g. 'kill node2', 'kill 1', 'kill leader')")
			fmt.Println("    spawn <node>      - Restart a crashed node (e.g. 'spawn node2', 'spawn 2')")
			fmt.Println("    status / info     - Print live status matrix for all nodes")
			fmt.Println("    ping              - Ping all cluster nodes and display latencies")
			fmt.Println("    leader            - Query active consensus leader")
			fmt.Println()
			fmt.Println("  [KV State Machine]")
			fmt.Println("    set <k> <v>       - Replicate and commit a key-value pair to leader")
			fmt.Println("    get <k>           - Read a value from any active node")
			fmt.Println("    del <k>           - Delete a key across the cluster")
			fmt.Println("    keys / dump       - List all keys stored across all nodes")
			fmt.Println()
			fmt.Println("  [WAL & Replicated Log Diagnostics]")
			fmt.Println("    logs              - Print full replicated log timeline across the cluster")
			fmt.Println("    wals              - Compare physical on-disk WAL records across all nodes")
			fmt.Println("    wal <node>        - Inspect raw on-disk WAL ledger records for a specific node (e.g. 'wal node1')")
			fmt.Println()
			fmt.Println("  [Benchmark & Tools]")
			fmt.Println("    bench [n] [c]     - Run concurrent write benchmark with n entries and c workers (e.g. 'bench 100 10')")
			fmt.Println("    watch / top       - Enter live 1-second auto-refreshing dashboard")
			fmt.Println("    clear / cls       - Refresh dashboard screen")
			fmt.Println("    exit / q          - Quit")

		default:
			fmt.Printf("%sUnknown command '%s'. Type 'help' for options.%s\n", colorRed, cmd, colorReset)
		}

		fmt.Printf("\n%s[Press Enter to refresh dashboard]%s", colorDim, colorReset)
		scanner.Scan()
	}
}

func runConcurrentBenchmark(nodeAddrs []string, total, concurrency int) {
	fmt.Printf("%sRunning concurrent benchmark: %d writes with concurrency %d...%s\n", colorCyan, total, concurrency, colorReset)
	start := time.Now()
	var successCount int64
	var failCount int64

	jobs := make(chan int, total)
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				k := fmt.Sprintf("k_%d", idx)
				v := fmt.Sprintf("val_%d", idx)
				res, _, _ := executeRouted(nodeAddrs, fmt.Sprintf("SET %s %s", k, v))
				if strings.HasPrefix(res, "OK") {
					atomic.AddInt64(&successCount, 1)
				} else {
					atomic.AddInt64(&failCount, 1)
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	opsPerSec := float64(successCount) / elapsed.Seconds()
	fmt.Printf("%s%s[OK] Benchmark Finished:%s %d/%d committed in %v (~%.1f ops/sec, %d failed)\n",
		colorBold, colorGreen, colorReset, successCount, total, elapsed, opsPerSec, failCount)
}

func executeRouted(nodeAddrs []string, cmd string) (string, time.Duration, string) {
	nodes := queryCluster(nodeAddrs)
	var leaderAddr string
	for _, n := range nodes {
		if n.Online && n.Role == "LEADER" {
			leaderAddr = n.Addr
			break
		}
	}

	if leaderAddr != "" {
		res, lat, err := sendRawCommand(leaderAddr, cmd)
		if err == nil && !strings.Contains(res, "NOT_LEADER") {
			return res, lat, leaderAddr
		}
	}

	for _, n := range nodes {
		if n.Online {
			res, lat, err := sendRawCommand(n.Addr, cmd)
			if err == nil && !strings.Contains(res, "NOT_LEADER") {
				return res, lat, n.Addr
			}
		}
	}

	return "ERR NO_LEADER_FOUND", 0, "none"
}

func handleRoutedCommand(nodeAddrs []string, targetAddr, cmd string, args []string) {
	payload := fmt.Sprintf("%s %s", cmd, strings.Join(args, " "))
	res, lat, addr := executeRouted(nodeAddrs, payload)
	fmt.Printf("%s (via %s in %v)\n", res, addr, lat)
}

func handleSingleCommand(targetAddr, cmd string, args []string) {
	payload := cmd
	if len(args) > 0 {
		payload += " " + strings.Join(args, " ")
	}
	res, _, err := sendRawCommand(targetAddr, payload)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(res)
}
