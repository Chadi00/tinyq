package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Chadi00/tinyq/client"
)

func (b *benchmark) storage(ctx context.Context, server, wal string) error {
	fmt.Println()
	b.section("Local storage  /  disposable 2 MiB queue")
	p, err := start(ctx, server, wal, "-max-queue-mib", "2")
	if err != nil {
		return err
	}
	defer p.close()
	c := client.New(p.url)
	large, live := payload(1<<20), payload(4<<10)
	ids := make([]uint64, max(2, min(b.n, 256)))
	for i := range 16 {
		if _, err := c.Enqueue(ctx, large); err != nil {
			return err
		}
		if i == 15 {
			break
		}
		l, err := take(ctx, c, large)
		if err != nil {
			return err
		}
		if err := c.Ack(ctx, l.Job.ID, l.Token); err != nil {
			return err
		}
	}
	for i := range ids {
		ids[i], err = c.Enqueue(ctx, live)
		if err != nil {
			return err
		}
	}
	last, err := take(ctx, c, large)
	if err != nil {
		return err
	}
	before, err := os.Stat(wal)
	if err != nil {
		return err
	}
	t := time.Now()
	if err := c.Ack(ctx, last.Job.ID, last.Token); err != nil {
		return err
	}
	after, err := waitForSnapshot(ctx, wal, before.Size())
	if err != nil {
		return err
	}
	fmt.Printf("  Compaction observed   %s  ·  %.2f → %.2f MiB WAL (includes maintenance wait)\n",
		latency(time.Since(t)), float64(before.Size())/(1<<20), float64(after)/(1<<20))

	old, err := take(ctx, c, live)
	if err != nil {
		return err
	}
	if old.Job.ID != ids[0] {
		return errors.New("unexpected live job before restart")
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	p.close()
	t = time.Now()
	restarted, err := start(ctx, server, wal, "-max-queue-mib", "2")
	if err != nil {
		return err
	}
	defer restarted.close()
	c = client.New(restarted.url)
	stats, err := c.Stats(ctx)
	if err != nil {
		return err
	}
	if stats.Pending != len(ids) || stats.Leased != 0 {
		return fmt.Errorf("unexpected replay state: %+v", stats)
	}
	fmt.Printf("  Restart to ready      %s  ·  %d jobs, including one in flight\n", latency(time.Since(t)), len(ids))
	b.heading("After restart  /  FIFO recovery and capacity")
	err = b.measure(ctx, "Drain recovered job", len(ids), 1, len(live), func(ctx context.Context, i int) error {
		l, err := take(ctx, c, live)
		if err != nil {
			return err
		}
		if l.Job.ID != ids[i] || l.Token == old.Token {
			return errors.New("unexpected recovered ID or lease token")
		}
		return c.Ack(ctx, l.Job.ID, l.Token)
	})
	if err != nil {
		return err
	}
	full := make([]uint64, 2)
	for i := range full {
		full[i], err = c.Enqueue(ctx, large)
		if err != nil {
			return err
		}
	}
	httpClient := &http.Client{Timeout: 10 * time.Second}
	err = b.measure(ctx, "Full queue · 503", min(b.n, 256), 1, 0, func(ctx context.Context, _ int) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, restarted.url+"/enqueue", strings.NewReader("x"))
		if err != nil {
			return err
		}
		res, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusServiceUnavailable {
			return fmt.Errorf("expected full queue, got %s", res.Status)
		}
		_, err = io.Copy(io.Discard, res.Body)
		return err
	})
	if err != nil {
		return err
	}
	for _, id := range full {
		l, err := take(ctx, c, large)
		if err != nil {
			return err
		}
		if l.Job.ID != id {
			return errors.New("unexpected job after capacity check")
		}
		if err := c.Ack(ctx, id, l.Token); err != nil {
			return err
		}
	}
	check := benchmark{c: c, completed: stats.Completed}
	if err := check.drained(ctx, len(ids)+len(full)); err != nil {
		return err
	}
	restarted.close()
	p.footprint("Compaction server")
	restarted.footprint("Recovery server")
	return nil
}

func waitForSnapshot(ctx context.Context, wal string, before int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		info, err := os.Stat(wal)
		if err != nil {
			return 0, err
		}
		if info.Size() < before/2 {
			return info.Size(), nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("waiting for compacted WAL: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
