package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sshivamanand/distributed-task-framework/internal/client"
	"github.com/sshivamanand/distributed-task-framework/internal/task"
)

func main() {
	leaders := flag.String("leaders", "localhost:8080", "comma-separated addresses of the leader-eligible nodes")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	c := &client.Client{LeaderAddrs: strings.Split(*leaders, ",")}

	switch args[0] {
	case "submit":
		submit(c, args[1:])
	case "result":
		result(c, args[1:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  client [-leaders <addrs>] submit [-id <id>] <command> [args...]
  client [-leaders <addrs>] result <id>`)
	os.Exit(1)
}

func submit(c *client.Client, args []string) {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	id := fs.String("id", "", "task id (default: generated from the current time)")
	fs.Parse(args)

	rest := fs.Args()
	if len(rest) < 1 {
		usage()
	}
	if *id == "" {
		*id = fmt.Sprintf("task-%d", time.Now().UnixNano())
	}

	t := task.Task{ID: *id, Command: rest[0], Args: rest[1:]}
	if err := c.Submit(t); err != nil {
		log.Fatalf("client: submit: %v", err)
	}
	fmt.Println(*id)
}

func result(c *client.Client, args []string) {
	if len(args) < 1 {
		usage()
	}
	res, found, err := c.QueryResult(args[0])
	if err != nil {
		log.Fatalf("client: result: %v", err)
	}
	if !found {
		fmt.Println("PENDING (still running, unknown task id, or its leader changed since — see information.md)")
		return
	}
	fmt.Printf("status: %s\n", res.Status)
	if res.Output != "" {
		fmt.Printf("output: %s", res.Output)
	}
	if res.Error != "" {
		fmt.Printf("error: %s\n", res.Error)
	}
}
