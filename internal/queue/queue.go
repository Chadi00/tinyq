package queue

import "sync"

const DefaultMaxMiB = 64

type Queue struct {
	mu           sync.Mutex
	pending      []*Job
	leased       map[uint64]Lease
	nextID       uint64
	completed    int
	leaseExpired int
	maxBytes     int
	payloadBytes int
	outstanding  int
}

type Stats struct {
	Pending          int
	Leased           int
	Completed        int
	LeaseExpirations int
}

func New() *Queue {
	return NewWithLimit(DefaultMaxMiB << 20)
}

func NewWithLimit(maxBytes int) *Queue {
	return &Queue{
		leased:   make(map[uint64]Lease),
		maxBytes: maxBytes,
	}
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return Stats{
		Pending:          len(q.pending),
		Leased:           len(q.leased),
		Completed:        q.completed,
		LeaseExpirations: q.leaseExpired,
	}
}
