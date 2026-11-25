package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const (
	magic      = "tinyqwal\x01"
	bufferSize = 64 << 10
)

type Job struct {
	ID      uint64
	Payload []byte
}

type State struct {
	Jobs   []Job
	LastID uint64
}

type Log struct {
	path   string
	file   *os.File
	dir    *os.File
	lock   *os.File
	writer *bufio.Writer
	size   int64
	err    error
}

func Open(path string) (_ *Log, _ State, err error) {
	if path == "" {
		return nil, State{}, errors.New("WAL path is empty")
	}
	l := &Log{path: path}
	defer func() {
		if err != nil {
			l.Close()
		}
	}()

	l.lock, err = openSingleFile(path + ".lock")
	if err != nil {
		return nil, State{}, err
	}
	if err := syscall.Flock(int(l.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, State{}, fmt.Errorf("lock WAL: %w", err)
	}
	l.dir, err = os.Open(filepath.Dir(path))
	if err != nil {
		return nil, State{}, err
	}
	l.file, err = openSingleFile(path)
	if err != nil {
		return nil, State{}, err
	}

	state, end, err := replay(l.file)
	if err != nil {
		return nil, State{}, err
	}
	if _, err := l.file.Seek(end, io.SeekStart); err != nil {
		return nil, State{}, err
	}
	if err := os.Remove(path + ".compact"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, State{}, err
	}
	if err := l.dir.Sync(); err != nil {
		return nil, State{}, err
	}
	l.size = end
	l.writer = bufio.NewWriterSize(l.file, bufferSize)
	return l, state, nil
}

func openSingleFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err == nil && (!info.Mode().IsRegular() || info.Sys().(*syscall.Stat_t).Nlink != 1) {
		err = fmt.Errorf("%s must be a regular file with a single link", path)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (l *Log) AppendEnqueue(id uint64, payload []byte) error {
	return l.append(enqueue, id, payload)
}

func (l *Log) AppendAck(id uint64) error {
	return l.append(ack, id, nil)
}

func (l *Log) append(kind byte, id uint64, payload []byte) error {
	if l.err != nil {
		return l.err
	}
	if err := writeRecord(l.writer, kind, id, payload); err != nil {
		return l.fail(err)
	}
	l.size += recordSize + int64(len(payload))
	return nil
}

func (l *Log) Sync() error {
	if l.err != nil {
		return l.err
	}
	if err := l.writer.Flush(); err != nil {
		return l.fail(err)
	}
	if err := l.file.Sync(); err != nil {
		return l.fail(err)
	}
	return nil
}

func (l *Log) fail(err error) error {
	l.err = fmt.Errorf("commit WAL: %w", err)
	return l.err
}

func (l *Log) Err() error {
	return l.err
}

func (l *Log) Close() error {
	l.err = os.ErrClosed
	var err error
	for _, f := range []*os.File{l.file, l.dir, l.lock} {
		if f != nil {
			err = errors.Join(err, f.Close())
		}
	}
	return err
}
