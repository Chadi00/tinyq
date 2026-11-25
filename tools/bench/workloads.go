package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/Chadi00/tinyq/client"
)

func payload(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

func take(ctx context.Context, c *client.Client, data []byte) (client.Lease, error) {
	l, err := c.Lease(ctx)
	if err == nil && !bytes.Equal(l.Job.Payload, data) {
		err = errors.New("unexpected leased payload; job was not acknowledged")
	}
	return l, err
}

func (b *benchmark) api(ctx context.Context) error {
	b.heading("HTTP operations  /  FIFO backlog")
	n := min(b.n, 256)
	ids := make([]uint64, n)
	leases := make([]client.Lease, n)
	data := payload(128)
	for _, step := range []struct {
		name string
		n    int
		op   func(context.Context, int) error
	}{
		{"Enqueue · 128 B", n, func(ctx context.Context, i int) error {
			id, err := b.c.Enqueue(ctx, data)
			ids[i] = id
			return err
		}},
		{"Lease from backlog", n, func(ctx context.Context, i int) error {
			l, err := take(ctx, b.c, data)
			if err == nil && l.Job.ID != ids[i] {
				return fmt.Errorf("FIFO: got job %d, want %d", l.Job.ID, ids[i])
			}
			leases[i] = l
			return err
		}},
		{"Renew lease", n, func(ctx context.Context, i int) error {
			return b.c.Renew(ctx, ids[i], leases[i].Token)
		}},
		{"Ack", n, func(ctx context.Context, i int) error {
			return b.c.Ack(ctx, ids[i], leases[i].Token)
		}},
		{"Stats", b.n, func(ctx context.Context, _ int) error {
			_, err := b.c.Stats(ctx)
			return err
		}},
		{"Poll empty queue", b.n, func(ctx context.Context, _ int) error {
			_, err := b.c.Lease(ctx)
			if errors.Is(err, client.ErrEmptyQueue) {
				return nil
			}
			if err == nil {
				return errors.New("expected an empty queue")
			}
			return err
		}},
	} {
		if err := b.measure(ctx, step.name, step.n, 1, 0, step.op); err != nil {
			return err
		}
	}
	return b.drained(ctx, n)
}

func (b *benchmark) cycles(ctx context.Context) error {
	b.heading("Durable job cycles  /  enqueue → lease → ack")
	for _, w := range []struct {
		name             string
		size, n, workers int
	}{
		{"128 B serial", 128, b.n, 1},
		{"128 B parallel", 128, b.n, b.workers},
		{"64 KiB serial", 64 << 10, b.n, 1},
		{"64 KiB parallel", 64 << 10, b.n, b.workers},
		{"1 MiB maximum", 1 << 20, min(b.n, 16), 1},
	} {
		data := payload(w.size)
		if err := b.measure(ctx, w.name, w.n, w.workers, w.size, func(ctx context.Context, _ int) error {
			if _, err := b.c.Enqueue(ctx, data); err != nil {
				return err
			}
			l, err := take(ctx, b.c, data)
			if err != nil {
				return err
			}
			return b.c.Ack(ctx, l.Job.ID, l.Token)
		}); err != nil {
			return err
		}
		if err := b.drained(ctx, w.n); err != nil {
			return err
		}
	}
	return nil
}

func (b *benchmark) expiry(ctx context.Context) error {
	b.heading("Lease expiry  /  time to redelivery, including the 30 s lease")
	data := payload(128)
	id, err := b.c.Enqueue(ctx, data)
	if err != nil {
		return err
	}
	old, err := take(ctx, b.c, data)
	if err != nil {
		return err
	}
	if old.Job.ID != id {
		return errors.New("unexpected initial lease ID")
	}
	var next client.Lease
	err = b.measure(ctx, "Redelivery", 1, 1, 0, func(ctx context.Context, _ int) error {
		ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
		defer cancel()
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-tick.C:
				l, err := take(ctx, b.c, data)
				if errors.Is(err, client.ErrEmptyQueue) {
					continue
				}
				if err == nil && (l.Job.ID != id || l.Token == old.Token) {
					return errors.New("expected a fresh lease for the original job")
				}
				next = l
				return err
			}
		}
	})
	if err != nil {
		return err
	}
	if err := b.c.Ack(ctx, id, next.Token); err != nil {
		return err
	}
	return b.drained(ctx, 1)
}
