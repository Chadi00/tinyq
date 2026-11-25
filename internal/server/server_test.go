package server_test

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/Chadi00/tinyq/client"
	"github.com/Chadi00/tinyq/internal/queue"
	"github.com/Chadi00/tinyq/internal/server"
)

func TestWorkersReceiveJobsInFIFOOrder(t *testing.T) {
	s := httptest.NewServer(server.New(queue.New()))
	defer s.Close()
	c := client.New(s.URL)
	ctx := t.Context()
	payloads := [][]byte{[]byte("first"), bytes.Repeat([]byte{0xff, 0x00}, 4096)}
	var ids []uint64
	for _, payload := range payloads {
		id, err := c.Enqueue(ctx, payload)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	var leases []client.Lease
	for i, id := range ids {
		l, err := c.Lease(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if l.Job.ID != id || !bytes.Equal(l.Job.Payload, payloads[i]) {
			t.Fatalf("lease %d: want job %d with its original payload, got job %d", i, id, l.Job.ID)
		}
		leases = append(leases, l)
	}
	if _, err := c.Lease(ctx); err != client.ErrEmptyQueue {
		t.Fatalf("all jobs in flight: Lease returned %v, want empty queue", err)
	}
	if err := c.Renew(ctx, ids[0], leases[0].Token); err != nil {
		t.Fatal(err)
	}
	for _, l := range leases {
		if err := c.Ack(ctx, l.Job.ID, l.Token); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := c.Stats(ctx); err != nil || got != (client.Stats{Completed: len(ids)}) {
		t.Fatalf("after acknowledgments: stats=%+v, err=%v; want only %d completed", got, err, len(ids))
	}
}
