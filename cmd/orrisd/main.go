package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"orris/internal/client"
	"orris/internal/raft"
	"orris/internal/transport"
)

const version = "1.0.0"

func main() {
	id := flag.String("id", "node1", "Node ID")
	addr := flag.String("addr", "127.0.0.1:8001", "Consensus TCP address")
	clientAddr := flag.String("client", "127.0.0.1:9001", "Client TCP address")
	peersStr := flag.String("peers", "", "Comma-separated list of peer=id:addr")
	dataDir := flag.String("data", "./data/node1", "Data directory")
	logFile := flag.String("log", "", "Path to log file")
	showVersion := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("orrisd version %s (zero-dependency Raft distributed KV)\n", version)
		os.Exit(0)
	}

	if *logFile != "" {
		if f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644); err == nil {
			log.SetOutput(f)
			slog.SetDefault(slog.New(slog.NewTextHandler(f, nil)))
		}
	}

	peers := make(map[string]string)
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.SplitN(p, "=", 2)
			if len(parts) == 2 {
				peers[parts[0]] = parts[1]
			}
		}
	}

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		slog.Error("Failed to create data directory", "path", *dataDir, "err", err)
		os.Exit(1)
	}

	walPath := filepath.Join(*dataDir, "wal.log")
	n, err := raft.NewNode(*id, *addr, peers, walPath)
	if err != nil {
		slog.Error("Failed to initialize Raft node", "id", *id, "err", err)
		os.Exit(1)
	}

	stopCh := make(chan struct{})

	if err := transport.StartRPCServer(*addr, n, stopCh); err != nil {
		slog.Error("Failed to start consensus RPC server", "addr", *addr, "err", err)
		os.Exit(1)
	}

	if err := client.StartClientServer(*clientAddr, n, stopCh); err != nil {
		slog.Error("Failed to start client TCP server", "clientAddr", *clientAddr, "err", err)
		os.Exit(1)
	}

	slog.Info("Node running",
		"id", *id,
		"consensus", *addr,
		"client", *clientAddr,
		"peers", len(peers),
		"data", *dataDir,
	)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down node...", "id", *id)
	close(stopCh)
	n.Stop()
	slog.Info("Node stopped gracefully", "id", *id)
}
