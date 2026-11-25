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
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Chadi00/tinyq/internal/queue"
	"github.com/Chadi00/tinyq/internal/server"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serve(args []string) (err error) {
	flags := flag.NewFlagSet("tinyq serve", flag.ContinueOnError)
	addr := flags.String("addr", "127.0.0.1:8080", "Listen address")
	walPath := flags.String("wal", os.Getenv("TINYQ_WAL"), "Queue log (env: TINYQ_WAL; default: ~/.local/share/tinyq/tinyq.wal)")
	idleDefault := os.Getenv("TINYQ_IDLE_TIMEOUT")
	if idleDefault == "" {
		idleDefault = "15m"
	}
	idle := flags.String("idle-timeout", idleDefault, "Stop after this long without requests; 0 disables (env: TINYQ_IDLE_TIMEOUT)")
	queueMiB, ok := os.LookupEnv("TINYQ_MAX_QUEUE_MIB")
	if !ok {
		queueMiB = strconv.Itoa(queue.DefaultMaxMiB)
	}
	maxQueueMiB := flags.String("max-queue-mib", queueMiB, "Maximum unfinished payload and upload reservations in MiB (env: TINYQ_MAX_QUEUE_MIB)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected server argument: %q", flags.Arg(0))
	}
	mib, err := strconv.Atoi(*maxQueueMiB)
	if err != nil || mib <= 0 || mib > math.MaxInt>>20 {
		return errors.New("-max-queue-mib or TINYQ_MAX_QUEUE_MIB must be a positive MiB integer that fits in an int after conversion to bytes")
	}

	timeout, err := time.ParseDuration(*idle)
	if err != nil || timeout < 0 {
		return errors.New("idle timeout must be a nonnegative duration, e.g. 15m")
	}
	if *walPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}
		dir := filepath.Join(home, ".local", "share", "tinyq")
		if err := os.MkdirAll(dir, 0700); err != nil {
			return err
		}
		*walPath = filepath.Join(dir, "tinyq.wal")
	}
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	q, err := server.Open(*walPath, mib<<20)
	// The previous server can release its listener just before it releases its WAL.
	for deadline := time.Now().Add(5 * time.Second); errors.Is(err, syscall.EWOULDBLOCK) && time.Now().Before(deadline); {
		time.Sleep(25 * time.Millisecond)
		q, err = server.Open(*walPath, mib<<20)
	}
	if err != nil {
		return fmt.Errorf("open WAL %q: %w", *walPath, err)
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	listenerClosed := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("/", q)
	mux.Handle("POST /stop", http.NewCrossOriginProtection().Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopbackAddress(r.RemoteAddr) || !loopbackAddress(r.Host) {
			http.Error(w, "stop requires a local connection", http.StatusForbidden)
			return
		}
		stop()
		<-listenerClosed // The next queue command can now start a fresh server.
		w.WriteHeader(http.StatusNoContent)
	})))
	activity := &activityHandler{handler: mux, last: time.Now()}
	srv := &http.Server{
		Addr:              *addr,
		Handler:           activity,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
	}
	srv.RegisterOnShutdown(func() { close(listenerClosed) })

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var idleTimer *time.Timer
	var idleC <-chan time.Time
	if timeout > 0 {
		idleTimer = time.NewTimer(timeout)
		defer idleTimer.Stop()
		idleC = idleTimer.C
	}

	errc := make(chan error, 1)

	log.Printf("tinyq listening on %s", listener.Addr())
	go func() { errc <- srv.Serve(listener) }()

	for {
		select {
		case <-idleC:
			if remaining := activity.remaining(timeout); remaining > 0 {
				idleTimer.Reset(remaining)
			} else {
				stop()
			}
		case <-ticker.C:
			if err := q.Maintain(); err != nil {
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

type activityHandler struct {
	handler http.Handler
	mu      sync.Mutex
	active  int
	last    time.Time
}

func (a *activityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.active++
	a.mu.Unlock()
	defer func() {
		a.mu.Lock()
		a.active--
		a.last = time.Now()
		a.mu.Unlock()
	}()
	a.handler.ServeHTTP(w, r)
}

func (a *activityHandler) remaining(timeout time.Duration) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active > 0 {
		return timeout
	}
	return timeout - time.Since(a.last)
}
