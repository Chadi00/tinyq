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
	"github.com/Chadi00/tinyq/internal/wal"
)

const usage = `Usage:
  tinyq serve [flags]      Start the local server (also the default with no command)
  tinyq enqueue PAYLOAD    Enqueue one argument as bytes; use - to read stdin
  tinyq lease              Claim a job, or return null if the queue is empty
  tinyq renew ID TOKEN     Extend a lease by 30 seconds from now
  tinyq ack ID TOKEN       Mark a leased job complete
  tinyq stats              Show queue counts

Queue commands print one JSON value on success. Errors go to stderr and exit nonzero.
Set TINYQ_URL to override http://127.0.0.1:8080. For server flags: tinyq serve -h.
`

func run(args []string, stdin io.Reader, stdout io.Writer) error {
	if len(args) == 0 {
		return serve(nil)
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

	url := os.Getenv("TINYQ_URL")
	if url == "" {
		url = "http://127.0.0.1:8080"
	}
	c := client.New(url)
	ctx := context.Background()
	var result any
	var err error
	switch command {
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
	case "lease", "stats":
		if len(args) != 0 {
			return fmt.Errorf("usage: tinyq %s", command)
		}
		if command == "stats" {
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
