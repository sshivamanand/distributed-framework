package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sshivamanand/distributed-task-framework/internal/worker"
)

func main() {
	id := flag.String("id", "", "unique worker id")
	leaders := flag.String("leaders", "localhost:8080", "comma-separated addresses of the leader-eligible nodes; the worker discovers whichever one is currently leader")
	concurrency := flag.Int("concurrency", 4, "number of concurrent task slots")
	flag.Parse()

	if *id == "" {
		log.Fatal("worker: -id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c := &worker.Client{ID: *id, LeaderAddrs: strings.Split(*leaders, ","), Concurrency: *concurrency}
	if err := c.Run(ctx); err != nil {
		log.Fatalf("worker: %v", err)
	}
}
