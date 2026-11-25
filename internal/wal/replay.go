package wal

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
)

func replay(f *os.File) (State, int64, error) {
	var header [len(magic)]byte
	n, err := io.ReadFull(f, header[:])
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return State{}, 0, err
	}
	if string(header[:n]) != magic[:n] {
		return State{}, 0, errors.New("invalid WAL format")
	}
	if n < len(magic) {
		if _, err := f.WriteString(magic[n:]); err != nil {
			return State{}, 0, err
		}
		if err := f.Sync(); err != nil {
			return State{}, 0, err
		}
	}

	r := bufio.NewReaderSize(f, bufferSize)
	live := make(map[uint64]Job)
	var state State
	end := int64(len(magic))
	for {
		kind, job, err := readRecord(r)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err := f.Truncate(end); err != nil {
				return State{}, 0, err
			}
			if err := f.Sync(); err != nil {
				return State{}, 0, err
			}
			break
		}
		if err != nil {
			return State{}, 0, fmt.Errorf("WAL at byte %d: %w", end, err)
		}
		switch kind {
		case enqueue:
			if job.ID <= state.LastID {
				return State{}, 0, fmt.Errorf("WAL at byte %d: out-of-order enqueue ID %d", end, job.ID)
			}
			state.LastID = job.ID
			live[job.ID] = job
		case ack:
			if _, ok := live[job.ID]; !ok {
				return State{}, 0, fmt.Errorf("WAL at byte %d: ACK for unknown job %d", end, job.ID)
			}
			delete(live, job.ID)
		case checkpoint:
			if job.ID < state.LastID {
				return State{}, 0, fmt.Errorf("WAL at byte %d: invalid checkpoint ID %d", end, job.ID)
			}
			state.LastID = job.ID
		default:
			return State{}, 0, fmt.Errorf("WAL at byte %d: invalid record kind %d", end, kind)
		}
		end += recordSize + int64(len(job.Payload))
	}

	state.Jobs = make([]Job, 0, len(live))
	for _, job := range live {
		state.Jobs = append(state.Jobs, job)
	}
	sortJobs(state.Jobs)
	return state, end, nil
}

func sortJobs(jobs []Job) {
	slices.SortFunc(jobs, func(a, b Job) int { return cmp.Compare(a.ID, b.ID) })
}
