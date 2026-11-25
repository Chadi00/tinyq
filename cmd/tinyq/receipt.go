package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/Chadi00/tinyq/client"
	"github.com/Chadi00/tinyq/internal/server"
)

type receipt struct {
	Server, Partition, Token string
	ID                       uint64
}

func next(ctx context.Context, c *client.Client, endpoint, partition string, args []string) (any, error) {
	flags := flag.NewFlagSet("next", flag.ContinueOnError)
	ttl := flags.Duration("ttl", 5*time.Minute, "lease duration (1s–24h)")
	path := flags.String("receipt", "", "save receipt to this new file")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	if flags.NArg() != 0 {
		return nil, errors.New("usage: tinyq next [--ttl 5m] [--receipt PATH]")
	}
	var f *os.File
	var err error
	if *path == "" {
		f, err = os.CreateTemp("", "tinyq-claim-*.json")
	} else {
		f, err = os.OpenFile(*path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	}
	if err != nil {
		return nil, fmt.Errorf("create receipt: %w", err)
	}
	saved := false
	defer func() {
		if !saved {
			f.Close()
			os.Remove(f.Name())
		}
	}()
	absolute, err := filepath.Abs(f.Name())
	if err != nil {
		return nil, err
	}
	l, err := c.LeaseFor(ctx, *ttl)
	if errors.Is(err, client.ErrEmptyQueue) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.NewEncoder(f).Encode(receipt{endpoint, partition, l.Token, l.Job.ID}); err != nil {
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	saved = true
	result := map[string]any{"id": l.Job.ID, "receipt": absolute, "expires_at": l.ExpiresAt}
	if utf8.Valid(l.Job.Payload) {
		result["payload"] = string(l.Job.Payload)
	} else {
		result["encoding"] = "base64"
		result["payload"] = base64.StdEncoding.EncodeToString(l.Job.Payload)
	}
	return result, nil
}

func useReceipt(ctx context.Context, command, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var r receipt
	if err := json.NewDecoder(f).Decode(&r); err != nil {
		return fmt.Errorf("read receipt: %w", err)
	}
	if r.Server == "" || !server.ValidPartition(r.Partition) || r.ID == 0 || r.Token == "" {
		return errors.New("invalid tinyq receipt")
	}
	c := queueClient(r.Server, r.Partition)
	if command == "renew" {
		return c.Renew(ctx, r.ID, r.Token)
	}
	if err := c.Ack(ctx, r.ID, r.Token); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("job completed, but could not remove receipt: %w", err)
	}
	return nil
}
