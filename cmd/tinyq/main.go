package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

func run() (err error) {
	addr := flag.String("addr", "127.0.0.1:8080", "Listen address")
	walPath := flag.String("wal", "tinyq.wal", "Path to the persistent queue log")
	queueMiB, ok := os.LookupEnv("TINYQ_MAX_QUEUE_MIB")
	if !ok {
		queueMiB = strconv.Itoa(queue.DefaultMaxMiB)
	}
	maxQueueMiB := flag.String("max-queue-mib", queueMiB, "Maximum unfinished payload and upload reservations in MiB (env: TINYQ_MAX_QUEUE_MIB)")
	flag.Parse()
	mib, err := strconv.Atoi(*maxQueueMiB)
	if err != nil || mib <= 0 || mib > math.MaxInt>>20 {
		return errors.New("-max-queue-mib or TINYQ_MAX_QUEUE_MIB must be a positive MiB integer that fits in an int after conversion to bytes")
	}

	q, err := queue.Open(*walPath, mib<<20)
	if err != nil {
		return fmt.Errorf("open WAL %q: %w", *walPath, err)
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(q),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
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

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	log.Printf("tinyq listening on %s", listener.Addr())
	go func() { errc <- srv.Serve(listener) }()

	for {
		select {
		case <-ticker.C:
			q.Expire()
			if err := q.Compact(); err != nil {
				if errors.Is(err, queue.ErrPersistence) {
					srv.Close()
					return err
				}
				log.Printf("WAL compaction: %v", err)
			}

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
