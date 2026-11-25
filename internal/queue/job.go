package queue

import (
	"bytes"
	"errors"
	"io"

	"github.com/Chadi00/tinyq/internal/wal"
)

const (
	maxJobs     = 64 << 10
	maxJobBytes = wal.MaxPayloadBytes
)

var (
	ErrFull            = errors.New("queue is full")
	ErrPayloadTooLarge = errors.New("payload too large")
)

type Job struct {
	ID      uint64
	payload []byte
}

func (j Job) Payload() []byte {
	return bytes.Clone(j.payload)
}

func (j Job) PayloadSize() int {
	return len(j.payload)
}

func (q *Queue) Enqueue(payload []byte) (uint64, error) {
	if len(payload) > maxJobBytes {
		return 0, ErrPayloadTooLarge
	}
	q.mutations.RLock()
	defer q.mutations.RUnlock()
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.reserve(len(payload)); !ok {
		return 0, ErrFull
	}
	return q.enqueue(bytes.Clone(payload))
}

func (q *Queue) EnqueueFrom(r io.Reader, contentLength int64) (uint64, error) {
	if contentLength > maxJobBytes {
		return 0, ErrPayloadTooLarge
	}

	q.mutations.RLock()
	defer q.mutations.RUnlock()
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
		q.release(limit)
		return 0, err
	}
	q.payloadBytes -= limit - len(payload)
	return q.enqueue(payload)
}

func (q *Queue) reserve(size int) (int, bool) {
	available := q.maxBytes - q.payloadBytes
	if available < 0 || q.outstanding >= maxJobs {
		return 0, false
	}
	if size < 0 {
		size = min(maxJobBytes, available)
	}
	if size > available {
		return 0, false
	}
	q.payloadBytes += size
	q.outstanding++
	return size, true
}

func (q *Queue) release(size int) {
	q.payloadBytes -= size
	q.outstanding--
}

func (q *Queue) enqueue(payload []byte) (uint64, error) {
	if q.log != nil {
		return q.commit(&mutation{payload: payload})
	}
	id := q.nextID + 1
	if id == 0 {
		q.release(len(payload))
		return 0, ErrFull
	}
	q.nextID = id
	q.push(Job{ID: id, payload: payload})
	return id, nil
}
