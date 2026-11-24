package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"
)

const (
	DefaultMaxMiB = 64
	maxJobs       = 64 << 10
	maxJobBytes   = 1 << 20
)

var (
	ErrEmptyQueue      = errors.New("queue is empty")
	ErrInvalidLease    = errors.New("invalid lease")
	ErrExpiredLease    = errors.New("lease has expired")
	ErrFull            = errors.New("queue is full")
	ErrPayloadTooLarge = errors.New("payload too large")
)

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

func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	if len(payload) > maxJobBytes {
		return 0, ErrPayloadTooLarge
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.reserve(len(payload)); !ok {
		return 0, ErrFull
	}
	return q.enqueue(bytes.Clone(payload)), nil
}

func (q *Queue) EnqueueFrom(r io.Reader, contentLength int64) (uint64, error) {
	if contentLength > maxJobBytes {
		return 0, ErrPayloadTooLarge
	}

	q.mu.Lock()
	limit, ok := q.reserve(int(contentLength))
	q.mu.Unlock()
	if !ok {
		return 0, ErrFull
	}

	var payload []byte
	var err error
	if contentLength >= 0 {
		payload = make([]byte, limit)
		_, err = io.ReadFull(r, payload)
	} else {
		payload, err = io.ReadAll(io.LimitReader(r, int64(limit)+1))
		if err == nil && len(payload) > limit {
			err = ErrFull
			if limit == maxJobBytes {
				err = ErrPayloadTooLarge
			}
		} else if err == nil && cap(payload) != len(payload) {
			payload = bytes.Clone(payload)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if err != nil {
		q.payloadBytes -= limit
		q.outstanding--
		return 0, err
	}
	q.payloadBytes -= limit - len(payload)
	return q.enqueue(payload), nil
}

func (q *Queue) reserve(size int) (int, bool) {
	available := q.maxBytes - q.payloadBytes
	if size < 0 {
		size = min(maxJobBytes, available)
	}
	if size > available || q.outstanding >= maxJobs {
		return 0, false
	}
	q.payloadBytes += size
	q.outstanding++
	return size, true
}

func (q *Queue) enqueue(payload []byte) uint64 {
	q.nextID++
	q.pending = append(q.pending, &Job{ID: q.nextID, Payload: payload})
	return q.nextID
}

func (q *Queue) Lease() (Lease, error) {
	q.mu.Lock()

	if len(q.pending) == 0 {
		q.mu.Unlock()
		return Lease{}, ErrEmptyQueue
	}

	var token [16]byte
	rand.Read(token[:])

	job := q.pending[0]

	q.pending[0] = nil
	q.pending = q.pending[1:]

	lease := Lease{
		Job:       job,
		Token:     hex.EncodeToString(token[:]),
		ExpiresAt: time.Now().Add(leaseTTL),
	}

	q.leased[job.ID] = lease
	q.mu.Unlock()

	lease.Job = &Job{
		ID:      job.ID,
		Payload: bytes.Clone(job.Payload),
	}

	return lease, nil
}

func (q *Queue) Renew(id uint64, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.leased[id]

	if !ok || lease.Token != token {
		return ErrInvalidLease
	}

	now := time.Now()

	if !now.Before(lease.ExpiresAt) {
		return ErrExpiredLease
	}

	lease.ExpiresAt = now.Add(leaseTTL)
	q.leased[id] = lease

	return nil
}

func (q *Queue) Ack(id uint64, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.leased[id]

	if !ok || lease.Token != token {
		return ErrInvalidLease
	}

	if !time.Now().Before(lease.ExpiresAt) {
		return ErrExpiredLease
	}

	delete(q.leased, id)
	q.payloadBytes -= len(lease.Job.Payload)
	q.outstanding--
	q.completed++

	return nil
}

func (q *Queue) Expire() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()

	for id, lease := range q.leased {
		if !now.Before(lease.ExpiresAt) {
			delete(q.leased, id)
			q.pending = append(q.pending, lease.Job)
			q.leaseExpired++
		}
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
