package queue

import (
	"sync"
	"time"

	"github.com/Chadi00/tinyq/internal/wal"
)

const DefaultMaxMiB = 64

type Queue struct {
	mutations    sync.RWMutex
	mu           sync.Mutex
	walMu        sync.Mutex
	compactRetry time.Time
	log          *wal.Log
	writes       []*mutation
	flushing     bool
	pending      []Job
	pendingHead  int
	pendingCount int
	leased       map[uint64]activeLease
	nextExpiry   time.Time
	leaseEntropy [16 * leaseTokenBytes]byte
	entropyLeft  int
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
		leased:   make(map[uint64]activeLease),
		maxBytes: maxBytes,
	}
}

func (q *Queue) Stats() Stats {
	q.mu.Lock()
	defer q.mu.Unlock()

	return Stats{
		Pending:          q.pendingCount,
		Leased:           len(q.leased),
		Completed:        q.completed,
		LeaseExpirations: q.leaseExpired,
	}
}

func (q *Queue) push(job Job) {
	if q.pendingCount == len(q.pending) {
		size := len(q.pending)
		switch {
		case size == 0:
			size = 1
		case size < 1024:
			size *= 4
		default:
			size *= 2
		}
		pending := make([]Job, size)
		n := copy(pending, q.pending[q.pendingHead:])
		copy(pending[n:], q.pending[:q.pendingHead])
		q.pending = pending
		q.pendingHead = 0
	}

	tail := q.pendingHead + q.pendingCount
	if tail >= len(q.pending) {
		tail -= len(q.pending)
	}
	q.pending[tail] = job
	q.pendingCount++
}

func (q *Queue) pop() Job {
	head := q.pendingHead
	job := q.pending[head]
	q.pending[head] = Job{}
	q.pendingHead++
	if q.pendingHead == len(q.pending) {
		q.pendingHead = 0
	}
	q.pendingCount--
	return job
}
