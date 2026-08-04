package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sshivamanand/distributed-task-framework/internal/leader"
	"github.com/sshivamanand/distributed-task-framework/internal/raft"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

func main() {
	id := flag.String("id", "", "unique node id")
	addr := flag.String("addr", ":8080", "address to listen on")
	peers := flag.String("peers", "", "comma-separated addresses of the other leader-eligible nodes in the cluster")
	flag.Parse()

	if *id == "" {
		log.Fatal("leader: -id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("leader: listen %s: %v", *addr, err)
	}

	var peerAddrs []string
	if *peers != "" {
		peerAddrs = strings.Split(*peers, ",")
	}

	raftNode := raft.NewNode(*id, peerAddrs)
	queue := task.NewQueue(64)
	results := task.NewResultStore()
	srv := leader.NewServer(queue, results, raftNode)

	go raftNode.Run(ctx)

	log.Printf("leader: node %s listening on %s (peers: %v)", *id, *addr, peerAddrs)
	if err := srv.Serve(ctx, ln); err != nil {
		log.Fatalf("leader: %v", err)
	}
}
