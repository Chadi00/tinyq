package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	MaxPayloadBytes = 1 << 20
	recordSize      = 21

	enqueue    byte = 1
	ack        byte = 2
	checkpoint byte = 3
)

var checksumTable = crc32.MakeTable(crc32.Castagnoli)

func writeRecord(w *bufio.Writer, kind byte, id uint64, payload []byte) error {
	var header [recordSize]byte
	header[0] = kind
	binary.LittleEndian.PutUint64(header[1:9], id)
	binary.LittleEndian.PutUint32(header[9:13], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[13:17], crc32.Checksum(payload, checksumTable))
	binary.LittleEndian.PutUint32(header[17:21], crc32.Checksum(header[:17], checksumTable))
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func readRecord(r *bufio.Reader) (byte, Job, error) {
	var header [recordSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, Job{}, err
	}
	if crc32.Checksum(header[:17], checksumTable) != binary.LittleEndian.Uint32(header[17:21]) {
		return 0, Job{}, errors.New("invalid header checksum")
	}
	kind := header[0]
	id := binary.LittleEndian.Uint64(header[1:9])
	size := binary.LittleEndian.Uint32(header[9:13])
	if size > MaxPayloadBytes || (kind != enqueue && size != 0) {
		return 0, Job{}, errors.New("invalid payload size")
	}

	payload := make([]byte, size)
	if size != 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, Job{}, err
		}
	}
	if crc32.Checksum(payload, checksumTable) != binary.LittleEndian.Uint32(header[13:17]) {
		return 0, Job{}, errors.New("invalid payload checksum")
	}
	return kind, Job{ID: id, Payload: payload}, nil
}
