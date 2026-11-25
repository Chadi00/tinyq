package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chadi00/tinyq/client"
)

func work(ctx context.Context, c *client.Client, args []string) (any, error) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	l, err := c.Lease(ctx)
	if errors.Is(err, client.ErrEmptyQueue) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	interval := time.Until(l.ExpiresAt) / 3
	if interval <= 0 {
		return nil, errors.New("lease expired before starting worker")
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(workerCtx, args[0], args[1:]...)
	cmd.Stdin = bytes.NewReader(l.Job.Payload)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case err := <-finished:
			if err != nil {
				return nil, fmt.Errorf("worker failed; job not acknowledged: %w", err)
			}
			if err := c.Ack(ctx, l.Job.ID, l.Token); err != nil {
				return nil, fmt.Errorf("worker succeeded, but acknowledgment was not confirmed: %w", err)
			}
			return map[string]any{"id": l.Job.ID, "ok": true}, nil
		case <-tick.C:
			renewCtx, end := context.WithTimeout(ctx, interval)
			err := c.Renew(renewCtx, l.Job.ID, l.Token)
			end()
			if err != nil {
				cancel()
				<-finished
				return nil, fmt.Errorf("worker stopped; could not renew lease: %w", err)
			}
		}
	}
}
