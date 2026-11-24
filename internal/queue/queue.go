package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)


var (
ErrEmptyQueue = errors.New("queue is empty")
ErrInvalidLease = errors.New("invalid lease")
ErrExpiredLease = errors.New("lease has expired")
)

type Queue struct{
	mu sync.Mutex
	pending []*Job
	leased map[uint64]Lease
	nextID uint64
	completed int
	leaseExpired int
}

type Stats struct{
	Pending int
	Leased int
	Completed int
	LeaseExpirations int
}

func New() *Queue {
	return &Queue{
		leased: make(map[uint64]Lease),
	}
}

func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.nextID++

	job := &Job{
		ID: q.nextID,
		Payload: bytes.Clone(payload),
	}

	q.pending = append(q.pending, job)

	return job.ID, nil
}

func (q *Queue) Lease() (Lease, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return Lease{}, ErrEmptyQueue
	}

	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return Lease{}, err
	}

	job := q.pending[0]

	q.pending[0] = nil
	q.pending = q.pending[1:]

	lease := Lease{
		Job: job,
		Token: hex.EncodeToString(token[:]),
		ExpiresAt: time.Now().Add(leaseTTL),
	}

	q.leased[job.ID] = lease

	lease.Job = &Job{
		ID: job.ID,
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
		Pending: len(q.pending),
		Leased: len(q.leased),
		Completed: q.completed,
		LeaseExpirations: q.leaseExpired,
	}
}
