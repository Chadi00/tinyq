package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"github.com/Chadi00/tinyq/client"
)

type cliClient struct {
	binary, url string
}

func (c cliClient) run(ctx context.Context, input []byte, result any, args ...string) error {
	cmd := exec.CommandContext(ctx, c.binary, args...)
	cmd.Env = append(os.Environ(), "TINYQ_URL="+c.url, "TINYQ_PARTITION=default")
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("tinyq %s: %w", args[0], err)
	}
	return json.Unmarshal(out, result)
}

func (c cliClient) Enqueue(ctx context.Context, data []byte) (uint64, error) {
	var job struct{ ID uint64 }
	err := c.run(ctx, data, &job, "enqueue", "-")
	return job.ID, err
}

func (c cliClient) Lease(ctx context.Context) (client.Lease, error) {
	var lease *client.Lease
	if err := c.run(ctx, nil, &lease, "lease"); err != nil {
		return client.Lease{}, err
	}
	if lease == nil {
		return client.Lease{}, client.ErrEmptyQueue
	}
	return *lease, nil
}

func (c cliClient) Renew(ctx context.Context, id uint64, token string) error {
	return c.update(ctx, "renew", id, token)
}

func (c cliClient) Ack(ctx context.Context, id uint64, token string) error {
	return c.update(ctx, "ack", id, token)
}

func (c cliClient) update(ctx context.Context, command string, id uint64, token string) error {
	var result struct{ OK bool }
	if err := c.run(ctx, nil, &result, command, strconv.FormatUint(id, 10), token); err != nil {
		return err
	}
	if !result.OK {
		return fmt.Errorf("tinyq %s did not report success", command)
	}
	return nil
}

func (c cliClient) Stats(ctx context.Context) (client.Stats, error) {
	var stats client.Stats
	err := c.run(ctx, nil, &stats, "stats")
	return stats, err
}
