package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Chadi00/tinyq/internal/queue"
)

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	id, err := s.q.Enqueue(payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]uint64{"id": id})
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

	writeJSON(w, http.StatusOK, lease)
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
