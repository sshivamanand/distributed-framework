package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/sshivamanand/distributed-task-framework/internal/leader"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

func main() {
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("leader: listen %s: %v", *addr, err)
	}

	queue := task.NewQueue(64)
	results := task.NewResultStore()
	srv := leader.NewServer(queue, results)

	log.Printf("leader: listening on %s", *addr)
	if err := srv.Serve(ctx, ln); err != nil {
		log.Fatalf("leader: %v", err)
	}
}
