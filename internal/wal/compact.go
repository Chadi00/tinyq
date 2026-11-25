package wal

import (
	"bufio"
	"errors"
	"fmt"
	"os"
)

func (l *Log) ShouldCompact(liveJobs, livePayloadBytes int) bool {
	live := int64(len(magic)) + int64(liveJobs+1)*recordSize + int64(livePayloadBytes)
	garbage := l.size - live
	return garbage >= 16<<20 && garbage >= live
}

func (l *Log) Compact(state State) error {
	if l.err != nil {
		return l.err
	}
	sortJobs(state.Jobs)
	tmp := l.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			f.Close()
			os.Remove(tmp)
		}
	}()

	w := bufio.NewWriterSize(f, bufferSize)
	if _, err := w.WriteString(magic); err != nil {
		return err
	}
	size := int64(len(magic)) + recordSize
	for _, job := range state.Jobs {
		if err := writeRecord(w, enqueue, job.ID, job.Payload); err != nil {
			return err
		}
		size += recordSize + int64(len(job.Payload))
	}
	if err := writeRecord(w, checkpoint, state.LastID, nil); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := os.Rename(tmp, l.path); err != nil {
		return err
	}

	old := l.file
	l.file, l.writer, l.size = f, w, size
	installed = true
	if err := errors.Join(l.dir.Sync(), old.Close()); err != nil {
		l.err = fmt.Errorf("install compacted WAL: %w", err)
		return l.err
	}
	return nil
}
