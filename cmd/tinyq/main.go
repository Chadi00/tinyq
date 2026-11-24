package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chadi00/tinyq/internal/queue"
	"github.com/Chadi00/tinyq/internal/server"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	addr := flag.String("addr", ":8080", "Listen address")
	flag.Parse()

	q := queue.New()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(q),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	errc := make(chan error, 1)

	go func() {
		log.Printf("tinyq listening on %s", *addr)
		errc <- srv.ListenAndServe()
	}()

	for {
		select {
		case <-ticker.C:
			q.Expire()

		case err := <-errc:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err

		case <-ctx.Done():
			ctx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cancel()

			return srv.Shutdown(ctx)
		}
	}
}
