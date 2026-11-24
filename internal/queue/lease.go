package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"
)

const (
	leaseTTL        = 30 * time.Second
	leaseTokenBytes = 16
)

var (
	ErrEmptyQueue   = errors.New("queue is empty")
	ErrInvalidLease = errors.New("invalid lease")
	ErrExpiredLease = errors.New("lease has expired")
)

type Lease struct {
	Job       Job
	Token     string
	ExpiresAt time.Time
}

type activeLease struct {
	token     string
	expiresAt time.Time
	payload   []byte
}

func (l Lease) EncodeJSON(w io.Writer) error {
	type jobJSON struct {
		ID      uint64
		Payload []byte
	}
	return json.NewEncoder(w).Encode(struct {
		Job       jobJSON
		Token     string
		ExpiresAt time.Time
	}{
		Job:       jobJSON{ID: l.Job.ID, Payload: l.Job.payload},
		Token:     l.Token,
		ExpiresAt: l.ExpiresAt,
	})
}

func (q *Queue) Lease() (Lease, error) {
	q.mu.Lock()

	if q.pendingCount == 0 {
		q.mu.Unlock()
		return Lease{}, ErrEmptyQueue
	}

	if q.entropyLeft == 0 {
		rand.Read(q.leaseEntropy[:])
		q.entropyLeft = len(q.leaseEntropy)
	}
	q.entropyLeft -= leaseTokenBytes
	token := q.leaseEntropy[q.entropyLeft : q.entropyLeft+leaseTokenBytes]

	job := q.pop()
	lease := activeLease{
		token:     hex.EncodeToString(token),
		expiresAt: time.Now().Add(leaseTTL),
		payload:   job.payload,
	}

	if len(q.leased) == 0 {
		q.nextExpiry = lease.expiresAt
	}
	q.leased[job.ID] = lease
	q.mu.Unlock()

	return Lease{Job: job, Token: lease.token, ExpiresAt: lease.expiresAt}, nil
}

func (q *Queue) Renew(id uint64, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.leased[id]

	if !ok || lease.token != token {
		return ErrInvalidLease
	}

	now := time.Now()

	if !now.Before(lease.expiresAt) {
		return ErrExpiredLease
	}

	lease.expiresAt = now.Add(leaseTTL)
	q.leased[id] = lease

	return nil
}

func (q *Queue) Ack(id uint64, token string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	lease, ok := q.leased[id]

	if !ok || lease.token != token {
		return ErrInvalidLease
	}

	if time.Until(lease.expiresAt) <= 0 {
		return ErrExpiredLease
	}

	delete(q.leased, id)
	q.payloadBytes -= len(lease.payload)
	q.outstanding--
	q.completed++

	return nil
}

func (q *Queue) Expire() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.leased) == 0 {
		return
	}

	now := time.Now()
	if now.Before(q.nextExpiry) {
		return
	}

	var next time.Time
	for id, lease := range q.leased {
		if !now.Before(lease.expiresAt) {
			delete(q.leased, id)
			q.push(Job{ID: id, payload: lease.payload})
			q.leaseExpired++
		} else if next.IsZero() || lease.expiresAt.Before(next) {
			next = lease.expiresAt
		}
	}
	q.nextExpiry = next
}
