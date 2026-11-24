package server

import (
	"net/http"

	"github.com/Chadi00/tinyq/internal/queue"
)

type Server struct{
	q *queue.Queue
}

func New(q *queue.Queue) http.Handler {
	s := &Server{q: q}
	mux := http.NewServeMux()

	mux.HandleFunc("POST /enqueue", s.enqueue)
	mux.HandleFunc("POST /lease", s.lease)
	mux.HandleFunc("POST /renew", s.renew)
	mux.HandleFunc("POST /ack", s.ack)
	mux.HandleFunc("GET /stats", s.stats)

	return mux
}
