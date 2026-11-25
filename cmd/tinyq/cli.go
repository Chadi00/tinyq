package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/Chadi00/tinyq/client"
	"github.com/Chadi00/tinyq/internal/server"
	"github.com/Chadi00/tinyq/internal/wal"
)

const usage = `Usage:
  tinyq [--partition NAME] COMMAND
  tinyq serve [flags]      Start the server in the foreground
  tinyq enqueue PAYLOAD    Enqueue one argument as bytes; use - to read stdin
  tinyq next [--ttl 5m] [--receipt PATH]  Claim a job and print its payload and receipt
  tinyq done RECEIPT       Complete this claim after saving the result
  tinyq renew RECEIPT      Extend this claim by its original duration
  tinyq work -- CMD [ARG...]  Feed one job to CMD on stdin; renew and ack on success
  tinyq lease              Claim a job, or return null if the queue is empty
  tinyq renew ID TOKEN     Extend a lease by its original duration
  tinyq ack ID TOKEN       Mark a leased job complete
  tinyq stats              Show queue counts
  tinyq empty              Discard all jobs and reset this partition

Queue commands print one JSON value on success. Errors go to stderr and exit nonzero.
TINYQ_PARTITION selects the queue (default: default). --partition overrides it.
Local queue commands start the server if needed. Set TINYQ_URL to override
http://127.0.0.1:8080. For server flags: tinyq serve -h.
`

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		_, err := io.WriteString(stdout, usage)
		return err
	}
	name := os.Getenv("TINYQ_PARTITION")
	if name == "" {
		name = "default"
	}
	if args[0] == "--partition" || args[0] == "-p" {
		if len(args) < 3 {
			return errors.New("usage: tinyq --partition NAME COMMAND")
		}
		name, args = args[1], args[2:]
	}
	command, args := args[0], args[1:]
	switch command {
	case "help", "-h", "--help":
		_, err := io.WriteString(stdout, usage)
		return err
	case "serve":
		return serve(args)
	}
	if strings.HasPrefix(command, "-") {
		return serve(append([]string{command}, args...))
	}
	if len(args) == 1 && (args[0] == "-h" || args[0] == "--help") {
		_, err := io.WriteString(stdout, usage)
		return err
	}
	ctx := context.Background()
	if command == "done" || command == "renew" && (len(args) == 1 || len(args) == 2 && args[0] == "--receipt") {
		if len(args) > 0 && args[0] == "--receipt" {
			args = args[1:]
		}
		if len(args) != 1 {
			return fmt.Errorf("usage: tinyq %s RECEIPT", command)
		}
		if err := useReceipt(ctx, command, args[0]); err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(map[string]bool{"ok": true})
	}

	if !server.ValidPartition(name) {
		return errors.New("invalid partition: use 1–64 lowercase letters, digits, hyphens or underscores; start with a letter or digit")
	}
	url := strings.TrimRight(os.Getenv("TINYQ_URL"), "/")
	if url == "" {
		url = "http://127.0.0.1:8080"
	}
	c := queueClient(url, name)
	var result any
	var err error
	switch command {
	case "next":
		result, err = next(ctx, c, url, name, args)
	case "work":
		if len(args) < 2 || args[0] != "--" {
			return errors.New("usage: tinyq work -- CMD [ARG...]")
		}
		result, err = work(ctx, c, args[1:])
	case "enqueue":
		if len(args) != 1 {
			return errors.New("usage: tinyq enqueue PAYLOAD (use - for stdin)")
		}
		payload := []byte(args[0])
		if args[0] == "-" {
			payload, err = io.ReadAll(io.LimitReader(stdin, wal.MaxPayloadBytes+1))
			if err != nil {
				return err
			}
		}
		if len(payload) > wal.MaxPayloadBytes {
			return fmt.Errorf("payload exceeds %d bytes", wal.MaxPayloadBytes)
		}
		var id uint64
		id, err = c.Enqueue(ctx, payload)
		result = map[string]uint64{"id": id}
	case "lease", "stats", "empty":
		if len(args) != 0 {
			return fmt.Errorf("usage: tinyq %s", command)
		}
		if command == "empty" {
			err = c.Empty(ctx)
			result = map[string]bool{"ok": true}
		} else if command == "stats" {
			result, err = c.Stats(ctx)
		} else {
			result, err = c.Lease(ctx)
			if errors.Is(err, client.ErrEmptyQueue) {
				result, err = nil, nil
			}
		}
	case "ack", "renew":
		if len(args) != 2 {
			return fmt.Errorf("usage: tinyq %s ID TOKEN", command)
		}
		id, parseErr := strconv.ParseUint(args[0], 10, 64)
		if parseErr != nil || id == 0 || args[1] == "" {
			return errors.New("expected a positive job ID and a nonempty lease token")
		}
		op := c.Ack
		if command == "renew" {
			op = c.Renew
		}
		err = op(ctx, id, args[1])
		result = map[string]bool{"ok": true}
	default:
		return fmt.Errorf("unknown command %q; see tinyq --help", command)
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}
