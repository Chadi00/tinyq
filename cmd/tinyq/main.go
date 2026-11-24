package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"math"
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

func run() error {
	addr := flag.String("addr", ":8080", "Listen address")
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

	q := queue.NewWithLimit(mib << 20)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           server.New(q),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
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
