package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

const leaseTTL = 30 * time.Second

var (
	ErrEmptyQueue   = errors.New("queue is empty")
	ErrInvalidLease = errors.New("invalid lease")
	ErrExpiredLease = errors.New("lease has expired")
)

type Lease struct {
	Job       *Job
	Token     string
	ExpiresAt time.Time
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
