package queue

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func enqueue(t *testing.T, q *Queue, payload string) uint64 {
	t.Helper()
	id, err := q.Enqueue([]byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func lease(t *testing.T, q *Queue) Lease {
	t.Helper()
	l, err := q.Lease()
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestRenewalDelaysRedelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		id := enqueue(t, q, "work")
		old := lease(t, q)
		time.Sleep(leaseTTL / 2)
		if err := q.Renew(id, old.Token); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(old.ExpiresAt))
		q.Expire()
		if _, err := q.Lease(); err != ErrEmptyQueue {
			t.Fatalf("at original deadline: Lease returned %v, want empty queue", err)
		}

		time.Sleep(leaseTTL / 2)
		if err := q.Renew(id, old.Token); err != ErrExpiredLease {
			t.Fatalf("at renewed deadline: Renew returned %v, want expired lease", err)
		}
		if err := q.Ack(id, old.Token); err != ErrExpiredLease {
			t.Fatalf("at renewed deadline: Ack returned %v, want expired lease", err)
		}
		q.Expire()
		next := lease(t, q)
		if next.Job.ID != id || string(next.Job.Payload()) != "work" || next.Token == old.Token {
			t.Fatalf("redelivery: want original job %d and a fresh token", id)
		}
		if err := q.Renew(id, old.Token); err != ErrInvalidLease {
			t.Fatalf("stale worker: Renew returned %v, want invalid lease", err)
		}
		if err := q.Ack(id, old.Token); err != ErrInvalidLease {
			t.Fatalf("stale worker: Ack returned %v, want invalid lease", err)
		}
		if err := q.Ack(id, next.Token); err != nil {
			t.Fatal(err)
		}
		if got := q.Stats(); got != (Stats{Completed: 1, LeaseExpirations: 1}) {
			t.Fatalf("after redelivery: %+v, want one expiration and one completion", got)
		}
	})
}

func TestShortClaimExpiresBeforeEarlierLongClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := New()
		enqueue(t, q, "long")
		enqueue(t, q, "short")
		if _, err := q.LeaseFor(0); err != ErrLeaseDuration {
			t.Fatalf("invalid duration: %v", err)
		}
		long, err := q.LeaseFor(5 * time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		short := lease(t, q)
		time.Sleep(leaseTTL)
		q.Expire()
		if got := lease(t, q); got.Job.ID != short.Job.ID {
			t.Fatalf("redelivered %d, want %d", got.Job.ID, short.Job.ID)
		}
		if err := q.Renew(long.Job.ID, long.Token); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(long.ExpiresAt))
		if err := q.Ack(long.Job.ID, long.Token); err != nil {
			t.Fatalf("renewed long claim ended too soon: %v", err)
		}
	})
}

func TestCapacityReleasedOnFailedReadAndAck(t *testing.T) {
	q := NewWithLimit(4)
	if _, err := q.EnqueueFrom(strings.NewReader("x"), 4); err != io.ErrUnexpectedEOF {
		t.Fatalf("short body: got %v, want unexpected EOF", err)
	}
	if _, err := q.EnqueueFrom(strings.NewReader("12345"), -1); err != ErrFull {
		t.Fatalf("oversized stream: got %v, want full queue", err)
	}
	enqueue(t, q, "1234")
	l := lease(t, q)
	if _, err := q.Enqueue([]byte("x")); err != ErrFull {
		t.Fatalf("while leased: got %v, want full queue", err)
	}
	if err := q.Ack(l.Job.ID, l.Token); err != nil {
		t.Fatal(err)
	}
	enqueue(t, q, "5678")
}

func TestEmptyWaitsForAdmittedUpload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := NewWithLimit(1)
		r, w := io.Pipe()
		defer r.Close()
		defer w.Close()
		uploaded := make(chan error, 1)
		go func() {
			_, err := q.EnqueueFrom(r, 1)
			uploaded <- err
		}()
		synctest.Wait()
		go func() {
			if _, err := w.Write([]byte("a")); err != nil {
				t.Error(err)
			}
		}()
		if err := q.Empty(); err != nil {
			t.Fatal(err)
		}
		if err := <-uploaded; err != nil {
			t.Fatal(err)
		}
		if _, err := q.Lease(); err != ErrEmptyQueue {
			t.Fatalf("upload survived empty: %v", err)
		}
		if id := enqueue(t, q, "b"); id != 1 {
			t.Fatalf("fresh job ID = %d, want 1", id)
		}
	})
}

func TestRecoveryReplaysOnlyUnacknowledgedJobs(t *testing.T) {
	open := func(path string) *Queue {
		t.Helper()
		q, err := Open(path, 1024)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := q.Close(); err != nil {
				t.Error(err)
			}
		})
		return q
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "queue.wal")
	q := open(path)
	enqueue(t, q, "done")
	done := lease(t, q)
	if err := q.Ack(done.Job.ID, done.Token); err != nil {
		t.Fatal(err)
	}
	inFlight := enqueue(t, q, "in flight")
	old := lease(t, q)
	pending := enqueue(t, q, "pending")

	enqueue(t, q, "interrupted")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(dir, "restart.wal")
	if len(data) == 0 {
		t.Fatal("empty WAL after enqueue")
	}
	if err := os.WriteFile(copyPath, data[:len(data)-1], 0600); err != nil {
		t.Fatal(err)
	}
	recovered := open(copyPath)
	for _, want := range []struct {
		id      uint64
		payload string
	}{{inFlight, "in flight"}, {pending, "pending"}} {
		l := lease(t, recovered)
		if l.Job.ID != want.id || string(l.Job.Payload()) != want.payload {
			t.Fatalf("recovered (%d, %q), want (%d, %q)", l.Job.ID, l.Job.Payload(), want.id, want.payload)
		}
	}
	if _, err := recovered.Lease(); err != ErrEmptyQueue {
		t.Fatalf("after recovered jobs: got %v, want empty queue", err)
	}
	if err := recovered.Ack(inFlight, old.Token); err != ErrInvalidLease {
		t.Fatalf("pre-restart token: got %v, want invalid lease", err)
	}
	if id := enqueue(t, recovered, "new"); id <= pending {
		t.Fatalf("new job ID %d did not advance past %d", id, pending)
	}
}
