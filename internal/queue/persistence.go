package queue

import (
	"errors"
	"time"

	"github.com/Chadi00/tinyq/internal/wal"
)

var ErrPersistence = errors.New("queue storage unavailable")

func Open(path string, maxBytes int) (*Queue, error) {
	log, state, err := wal.Open(path)
	if err != nil {
		return nil, err
	}
	q := NewWithLimit(maxBytes)
	q.log = log
	q.nextID = state.LastID
	for _, job := range state.Jobs {
		q.push(Job{ID: job.ID, payload: job.Payload})
		q.payloadBytes += len(job.Payload)
		q.outstanding++
	}
	return q, nil
}

func (q *Queue) Compact() error {
	q.walMu.Lock()
	defer q.walMu.Unlock()
	if q.log == nil {
		return nil
	}
	if err := q.log.Err(); err != nil {
		return errors.Join(ErrPersistence, err)
	}
	if time.Now().Before(q.compactRetry) {
		return nil
	}

	q.mu.Lock()
	liveJobs := q.pendingCount + len(q.leased)
	if !q.log.ShouldCompact(liveJobs, q.payloadBytes) {
		q.mu.Unlock()
		return nil
	}
	jobs := make([]wal.Job, 0, liveJobs)
	for i := range q.pendingCount {
		job := q.pending[(q.pendingHead+i)%len(q.pending)]
		jobs = append(jobs, wal.Job{ID: job.ID, Payload: job.payload})
	}
	for id, lease := range q.leased {
		jobs = append(jobs, wal.Job{ID: id, Payload: lease.payload})
	}
	lastID := q.nextID
	q.mu.Unlock()

	err := q.log.Compact(wal.State{Jobs: jobs, LastID: lastID})
	if fatal := q.log.Err(); fatal != nil {
		return errors.Join(ErrPersistence, fatal)
	}
	if err != nil {
		q.compactRetry = time.Now().Add(time.Minute)
	}
	return err
}

func (q *Queue) Empty() error {
	q.mutations.Lock()
	defer q.mutations.Unlock()
	q.walMu.Lock()
	defer q.walMu.Unlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.log != nil {
		if err := q.log.Compact(wal.State{}); err != nil {
			return errors.Join(ErrPersistence, err)
		}
	}
	q.pending = nil
	q.pendingHead, q.pendingCount = 0, 0
	q.leased = make(map[uint64]activeLease)
	q.outstanding, q.payloadBytes = 0, 0
	q.nextID, q.completed, q.leaseExpired = 0, 0, 0
	q.nextExpiry, q.compactRetry = time.Time{}, time.Time{}
	return nil
}

func (q *Queue) Close() error {
	q.walMu.Lock()
	defer q.walMu.Unlock()
	if q.log == nil {
		return nil
	}
	return q.log.Close()
}

type mutation struct {
	id      uint64
	ack     bool
	payload []byte
	done    chan struct{}
	err     error
}

func (q *Queue) commit(m *mutation) (uint64, error) {
	m.done = make(chan struct{})
	q.writes = append(q.writes, m)
	if !q.flushing {
		q.flushing = true
		go q.flush()
	}
	q.mu.Unlock()
	<-m.done
	q.mu.Lock()
	if m.err != nil {
		return 0, m.err
	}
	return m.id, nil
}

func (q *Queue) flush() {
	var batch []*mutation
	for {
		q.walMu.Lock()
		q.mu.Lock()
		if len(q.writes) == 0 {
			q.flushing = false
			q.mu.Unlock()
			q.walMu.Unlock()
			return
		}
		batch, q.writes = q.writes, batch[:0]
		nextID := q.nextID
		q.mu.Unlock()

		nextID, err := q.writeBatch(batch, nextID)

		q.mu.Lock()
		if err == nil {
			q.nextID = nextID
		}
		for _, m := range batch {
			if err != nil {
				m.err = err
			}
			if m.ack {
				lease := q.leased[m.id]
				if m.err == nil {
					q.complete(m.id, lease)
				} else {
					lease.acking = false
					q.leased[m.id] = lease
				}
			} else if m.err == nil {
				q.push(Job{ID: m.id, payload: m.payload})
			} else {
				q.release(len(m.payload))
			}
			close(m.done)
		}
		clear(batch)
		q.mu.Unlock()
		q.walMu.Unlock()
	}
}

func (q *Queue) writeBatch(batch []*mutation, nextID uint64) (uint64, error) {
	for _, m := range batch {
		var err error
		if m.ack {
			err = q.log.AppendAck(m.id)
		} else if nextID+1 == 0 {
			m.err = ErrFull
		} else {
			nextID++
			m.id = nextID
			err = q.log.AppendEnqueue(m.id, m.payload)
		}
		if err != nil {
			return 0, errors.Join(ErrPersistence, err)
		}
	}
	if err := q.log.Sync(); err != nil {
		return 0, errors.Join(ErrPersistence, err)
	}
	return nextID, nil
}
