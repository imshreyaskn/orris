package client

import (
	"bufio"
	"fmt"
	"net"
	"orris/internal/raft"
	"orris/internal/storage"
	"strings"
	"time"
)

func StartClientServer(addr string, n *raft.Node, stopCh chan struct{}) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		<-stopCh
		_ = ln.Close()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-stopCh:
					return
				default:
					continue
				}
			}
			go handleClient(conn, n)
		}
	}()
	return nil
}

func handleClient(conn net.Conn, n *raft.Node) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		var resp string

		switch cmd {
		case "PING":
			resp = "PONG\n"

		case "SET":
			if len(parts) < 3 {
				resp = "ERR INVALID_ARGS\n"
			} else {
				req := raft.ClientReq{
					Op:    storage.OpSet,
					Key:   parts[1],
					Value: parts[2],
					ResCh: make(chan error, 1),
				}
				n.Submit(req)
				err := <-req.ResCh
				if err != nil {
					resp = fmt.Sprintf("ERR %s\n", err.Error())
				} else {
					resp = "OK COMMITTED\n"
				}
			}

		case "DELETE", "DEL":
			if len(parts) < 2 {
				resp = "ERR INVALID_ARGS\n"
			} else {
				req := raft.ClientReq{
					Op:    storage.OpDelete,
					Key:   parts[1],
					ResCh: make(chan error, 1),
				}
				n.Submit(req)
				err := <-req.ResCh
				if err != nil {
					resp = fmt.Sprintf("ERR %s\n", err.Error())
				} else {
					resp = "OK COMMITTED\n"
				}
			}

		case "GET":
			if len(parts) < 2 {
				resp = "ERR INVALID_ARGS\n"
			} else {
				val, ok := n.GetState(parts[1])
				if ok {
					resp = fmt.Sprintf("VALUE %s\n", val)
				} else {
					resp = "ERR NOT_FOUND\n"
				}
			}

		case "STATUS":
			resp = fmt.Sprintf("NODE %s ROLE %s TERM %d COMMIT %d APPLIED %d LOGLEN %d\n",
				n.ID, n.GetRole(), n.GetTerm(), n.GetCommit(), n.GetLastApplied(), len(n.GetLogEntries()))

		case "LEADER":
			resp = fmt.Sprintf("ROLE %s TERM %d\n", n.GetRole(), n.GetTerm())

		case "KEYS":
			all := n.GetAllKeys()
			if len(all) == 0 {
				resp = "KEYS (empty)\n"
			} else {
				var pairs []string
				for k, v := range all {
					pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
				}
				resp = fmt.Sprintf("KEYS %s\n", strings.Join(pairs, " | "))
			}

		case "LOGS":
			entries := n.GetLogEntries()
			if len(entries) == 0 {
				resp = "LOGS (empty)\n"
			} else {
				var items []string
				for _, e := range entries {
					items = append(items, fmt.Sprintf("[%d]T%d:%s:%s=%s", e.Index, e.Term, e.Operation, e.Key, e.Value))
				}
				resp = fmt.Sprintf("LOGS %s\n", strings.Join(items, " | "))
			}

		default:
			resp = "ERR UNKNOWN_CMD\n"
		}

		_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write([]byte(resp)); err != nil {
			return
		}
	}
}
