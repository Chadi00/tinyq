package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"syscall"
	"time"

	"github.com/Chadi00/tinyq/client"
)

type benchmark struct {
	c         *client.Client
	n         int
	workers   int
	completed int
	terminal  bool
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tinyq bench:", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "tinyq bench — measure a durable queue over HTTP and the CLI\n\nUsage: bench [options]\n  bench -suite full\n  bench -url http://127.0.0.1:8081 -suite cycles -workers 4\n\nOptions:")
		flag.PrintDefaults()
	}
	server := flag.String("server", "./bin/tinyq", "tinyq executable for disposable servers and CLI workloads")
	url := flag.String("url", "", "Use a dedicated, empty running queue instead")
	suite := flag.String("suite", "quick", "quick (API, CLI, cycles, local WAL), full (also expiry), api, cli, cycles, storage, or expiry")
	n := flag.Int("n", 200, "Samples per workload (1–1000000; CLI capped at 64, large payloads and backlog also capped)")
	workers := flag.Int("workers", 8, "Concurrent clients in parallel workloads (1–64)")
	flag.Parse()
	if flag.NArg() != 0 || !slices.Contains([]string{"quick", "full", "api", "cli", "cycles", "storage", "expiry"}, *suite) {
		return errors.New("unknown workload or argument; use -h for usage")
	}
	if *n < 1 || *n > 1_000_000 || *workers < 1 || *workers > 64 {
		return errors.New("-n must be 1–1000000 and -workers must be 1–64")
	}
	local := *url == ""
	if !local && *suite == "storage" {
		return errors.New("storage needs a disposable local WAL; omit -url and use -server")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	info, err := os.Stdout.Stat()
	b := benchmark{n: *n, workers: *workers, terminal: err == nil && info.Mode()&os.ModeCharDevice != 0 && os.Getenv("TERM") != "dumb"}
	b.section("tinyq bench  /  " + *suite)
	fmt.Printf("%d requested samples  ·  %d parallel workers  ·  %s/%s client\n", *n, *workers, runtime.GOOS, runtime.GOARCH)
	started := time.Now()
	var dir string
	if local {
		dir, err = os.MkdirTemp("", "tinyq-bench-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		fmt.Println("Local WAL:", dir)
	}
	standard := *suite == "quick" || *suite == "full"
	if *suite != "storage" {
		var p *process
		if local {
			p, err = start(ctx, *server, filepath.Join(dir, "http.wal"))
			if err != nil {
				return err
			}
			defer p.close()
			*url = p.url
		}
		fmt.Println("Endpoint: ", *url)
		b.c = client.New(*url)
		stats, err := b.c.Stats(ctx)
		if err != nil {
			return err
		}
		if stats.Pending != 0 || stats.Leased != 0 {
			return errors.New("benchmark requires a dedicated, empty queue")
		}
		b.completed = stats.Completed
		if standard || *suite == "api" {
			b.heading("HTTP operations  /  FIFO backlog")
			if err := b.operations(ctx, b.c, b.n); err != nil {
				return err
			}
		}
		if standard || *suite == "cli" {
			b.heading("CLI operations  /  one process per command")
			if err := b.operations(ctx, cliClient{*server, *url}, min(b.n, 64)); err != nil {
				return err
			}
		}
		if standard || *suite == "cycles" {
			if err := b.cycles(ctx); err != nil {
				return err
			}
		}
		if *suite == "full" || *suite == "expiry" {
			if err := b.expiry(ctx); err != nil {
				return err
			}
		}
		if local {
			p.close()
			p.footprint("HTTP server")
		}
	}
	if standard || *suite == "storage" {
		if local {
			if err := b.storage(ctx, *server, filepath.Join(dir, "storage.wal")); err != nil {
				return fmt.Errorf("storage: %w", err)
			}
		} else {
			fmt.Println("\nStorage skipped: process and WAL measurements need a disposable server.")
		}
	}
	fmt.Printf("\nDone in %s.\n", time.Since(started).Round(time.Millisecond))
	if *suite == "quick" {
		fmt.Println("Use -suite full to include timed lease expiry and redelivery.")
	}
	return nil
}

func (b *benchmark) drained(ctx context.Context, added int) error {
	stats, err := b.c.Stats(ctx)
	if err != nil {
		return err
	}
	b.completed += added
	if stats.Pending != 0 || stats.Leased != 0 || stats.Completed != b.completed {
		return fmt.Errorf("expected a drained queue with %d completed jobs, got %+v", b.completed, stats)
	}
	return nil
}
