package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sshivamanand/distributed-task-framework/internal/worker"
)

func main() {
	id := flag.String("id", "", "unique worker id")
	leaderAddr := flag.String("leader", "localhost:8080", "leader address")
	concurrency := flag.Int("concurrency", 4, "number of concurrent task slots")
	flag.Parse()

	if *id == "" {
		log.Fatal("worker: -id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &worker.Client{ID: *id, LeaderAddr: *leaderAddr, Concurrency: *concurrency}
	if err := c.Run(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
