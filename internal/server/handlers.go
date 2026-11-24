package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/Chadi00/tinyq/internal/queue"
)

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	id, err := s.q.EnqueueFrom(r.Body, r.ContentLength)
	var status int
	switch err {
	case nil:
		writeJSON(w, http.StatusCreated, struct {
			ID uint64 `json:"id"`
		}{id})
		return
	case queue.ErrFull:
		status = http.StatusServiceUnavailable
	case queue.ErrPayloadTooLarge:
		status = http.StatusRequestEntityTooLarge
	default:
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	w.Header().Set("Connection", "close")
	http.Error(w, err.Error(), status)
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	lease, err := s.q.Lease()

	if errors.Is(err, queue.ErrEmptyQueue) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if size := lease.Job.PayloadSize(); size >= 8<<10 {
		w.Header().Set("X-Tinyq-Payload-Size", strconv.Itoa(size))
	}
	w.WriteHeader(http.StatusOK)
	lease.EncodeJSON(w)
}

func (s *Server) renew(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID    uint64 `json:"id"`
		Token string `json:"token"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	err := s.q.Renew(req.ID, req.Token)

	if errors.Is(err, queue.ErrInvalidLease) ||
		errors.Is(err, queue.ErrExpiredLease) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID    uint64 `json:"id"`
		Token string `json:"token"`
	}

	r.Body = http.MaxBytesReader(w, r.Body, 4096)

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	err := s.q.Ack(req.ID, req.Token)

	if errors.Is(err, queue.ErrInvalidLease) ||
		errors.Is(err, queue.ErrExpiredLease) {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.q.Stats())
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
