package queue_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Chadi00/tinyq/client"
	"github.com/Chadi00/tinyq/internal/queue"
	"github.com/Chadi00/tinyq/internal/server"
)

func BenchmarkQueueCycle(b *testing.B) {
	for _, tc := range []struct {
		name  string
		size  int
		depth int
	}{
		{"128B", 128, 0},
		{"64KiB", 64 << 10, 0},
		{"128B/Backlog16K", 128, 16 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			q := queue.New()
			payload := make([]byte, tc.size)
			for range tc.depth {
				if _, err := q.Enqueue(payload); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			for b.Loop() {
				if err := queueCycle(q, payload); err != nil {
					b.Fatal(err)
				}
			}
			if stats := q.Stats(); stats.Pending != tc.depth || stats.Leased != 0 || stats.Completed != b.N {
				b.Fatalf("unexpected final stats: %+v", stats)
			}
		})
	}
}

func BenchmarkQueueCycleParallel128B(b *testing.B) {
	q := queue.New()
	payload := make([]byte, 128)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := queueCycle(q, payload); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	if stats := q.Stats(); stats.Pending != 0 || stats.Leased != 0 || stats.Completed != b.N {
		b.Fatalf("unexpected final stats: %+v", stats)
	}
}

func queueCycle(q *queue.Queue, payload []byte) error {
	if _, err := q.Enqueue(payload); err != nil {
		return err
	}
	return drainOne(q)
}

func drainOne(q *queue.Queue) error {
	lease, err := q.Lease()
	if err != nil {
		return err
	}
	return q.Ack(lease.Job.ID, lease.Token)
}

func BenchmarkEnqueueHandler(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"64KiB", 64 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			q := queue.New()
			handler := server.New(q)
			payload := make([]byte, tc.size)

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					req := httptest.NewRequest(http.MethodPost, "/enqueue", bytes.NewReader(payload))
					res := httptest.NewRecorder()
					handler.ServeHTTP(res, req)
					if res.Code != http.StatusCreated {
						b.Errorf("enqueue status = %d, want %d", res.Code, http.StatusCreated)
						return
					}
					if err := drainOne(q); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.StopTimer()
			if stats := q.Stats(); stats.Completed != b.N {
				b.Fatalf("unexpected final stats: %+v", stats)
			}
		})
	}
}

func BenchmarkHTTPCycle(b *testing.B) {
	for _, tc := range []struct {
		name string
		size int
	}{
		{"128B", 128},
		{"64KiB", 64 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			q := queue.New()
			srv := httptest.NewServer(server.New(q))
			b.Cleanup(srv.Close)
			c := client.New(srv.URL)
			ctx := b.Context()
			payload := make([]byte, tc.size)
			if _, err := c.Stats(ctx); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			for b.Loop() {
				id, err := c.Enqueue(ctx, payload)
				if err != nil {
					b.Fatal(err)
				}
				lease, err := c.Lease(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if lease.Job.ID != id || len(lease.Job.Payload) != len(payload) {
					b.Fatalf("unexpected lease: job %d, payload size %d", lease.Job.ID, len(lease.Job.Payload))
				}
				if err := c.Ack(ctx, lease.Job.ID, lease.Token); err != nil {
					b.Fatal(err)
				}
			}
			if stats := q.Stats(); stats.Pending != 0 || stats.Leased != 0 || stats.Completed != b.N {
				b.Fatalf("unexpected final stats: %+v", stats)
			}
		})
	}
}

func BenchmarkHTTPControl(b *testing.B) {
	q := queue.New()
	if _, err := q.Enqueue(nil); err != nil {
		b.Fatal(err)
	}
	lease, err := q.Lease()
	if err != nil {
		b.Fatal(err)
	}
	srv := httptest.NewServer(server.New(q))
	b.Cleanup(srv.Close)
	c := client.New(srv.URL)
	if _, err := c.Stats(b.Context()); err != nil {
		b.Fatal(err)
	}

	b.Run("Renew", func(b *testing.B) {
		ctx := b.Context()
		b.ReportAllocs()
		for b.Loop() {
			if err := c.Renew(ctx, lease.Job.ID, lease.Token); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Stats", func(b *testing.B) {
		ctx := b.Context()
		b.ReportAllocs()
		for b.Loop() {
			stats, err := c.Stats(ctx)
			if err != nil {
				b.Fatal(err)
			}
			if stats.Leased != 1 || stats.Pending != 0 {
				b.Fatalf("unexpected stats: %+v", stats)
			}
		}
	})
}

func BenchmarkExpireSweep(b *testing.B) {
	for _, tc := range []struct {
		name   string
		leased int
	}{
		{"Live1K", 1 << 10},
		{"Live16K", 16 << 10},
	} {
		b.Run(tc.name, func(b *testing.B) {
			q := queue.New()
			for range tc.leased {
				if _, err := q.Enqueue(nil); err != nil {
					b.Fatal(err)
				}
			}
			for range tc.leased {
				if _, err := q.Lease(); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			for b.Loop() {
				q.Expire()
			}
			if stats := q.Stats(); stats.Leased != tc.leased || stats.LeaseExpirations != 0 {
				b.Fatalf("unexpected final stats: %+v", stats)
			}
		})
	}
}
