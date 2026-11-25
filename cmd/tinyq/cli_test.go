package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Chadi00/tinyq/client"
	"github.com/Chadi00/tinyq/internal/server"
)

func TestMain(m *testing.M) {
	if os.Getenv("TINYQ_TEST_EXEC") == "1" {
		time.AfterFunc(30*time.Second, func() { os.Exit(1) })
		main()
		return
	}
	os.Exit(m.Run())
}

func TestIndependentReceiptsInSharedDirectory(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("TMPDIR", dir)
	q, err := server.Open(filepath.Join(dir, "queue.wal"), 32)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	s := httptest.NewServer(q)
	defer s.Close()
	t.Setenv("TINYQ_URL", s.URL)
	t.Setenv("TINYQ_PARTITION", "agents")
	call := func(args ...string) (string, error) {
		var out bytes.Buffer
		err := run(args, nil, &out)
		return out.String(), err
	}
	must := func(args ...string) string {
		t.Helper()
		out, err := call(args...)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if !strings.Contains(must(), "Usage:") {
		t.Fatal("bare tinyq did not show help")
	}
	must("enqueue", "hello")
	must("enqueue", "\xff")
	if _, err := call("enqueue", strings.Repeat("x", 32)); err == nil || !strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("missing capacity error: %v", err)
	}
	type claim struct {
		ID                         uint64
		Receipt, Payload, Encoding string
		Expires                    time.Time `json:"expires_at"`
	}
	results := make(chan claim, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Go(func() {
			out, err := call("next", "--ttl", "2m")
			var v claim
			if err != nil || json.Unmarshal([]byte(out), &v) != nil {
				t.Errorf("next: %q: %v", out, err)
				return
			}
			results <- v
		})
	}
	group.Wait()
	close(results)
	claims := make(map[uint64]claim)
	for c := range results {
		claims[c.ID] = c
		info, err := os.Stat(c.Receipt)
		if err != nil || info.Mode().Perm() != 0600 || time.Until(c.Expires) < time.Minute {
			t.Fatalf("invalid receipt for %+v: %v", c, err)
		}
	}
	a, b := claims[1], claims[2]
	if a.Payload != "hello" || b.Payload != "/w==" || b.Encoding != "base64" || a.Receipt == b.Receipt {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	must("enqueue", "third")
	if _, err := call("next", "--receipt", a.Receipt); !errors.Is(err, os.ErrExist) {
		t.Fatalf("receipt overwrite: %v", err)
	}
	must("next", "--receipt", "third.json")
	if got := must("next", "--receipt", "unused"); got != "null\n" {
		t.Fatalf("expected empty queue: %s", got)
	}
	if _, err := os.Stat("unused"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("empty claim left a file: %v", err)
	}
	t.Setenv("TINYQ_PARTITION", "another")
	must("done", b.Receipt)
	must("renew", "--receipt", a.Receipt)
	must("done", a.Receipt)
	must("done", "third.json")
	if _, err := os.Stat(a.Receipt); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed receipt not removed: %v", err)
	}
}

func TestWorkerAcknowledgesOnlyAfterSuccessfulProcessing(t *testing.T) {
	for _, test := range []struct {
		name, script string
		loseLease    bool
		wantAck      bool
	}{
		{"success", `read -r line && test "$line" = "hello" || exit 1; while [ ! -f "$1" ]; do sleep 0.05; done`, false, true},
		{"failure", `exit 7`, false, false},
		{"lost lease", `sleep 30`, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var acked, renewed atomic.Bool
			heartbeat := filepath.Join(t.TempDir(), "renewed")
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/lease":
					json.NewEncoder(w).Encode(client.Lease{Job: client.Job{ID: 1, Payload: []byte("hello\n")}, Token: "owner", ExpiresAt: time.Now().Add(time.Second)})
				case "/renew":
					renewed.Store(true)
					if test.loseLease {
						http.Error(w, "invalid lease", http.StatusConflict)
						return
					}
					if err := os.WriteFile(heartbeat, nil, 0600); err != nil {
						t.Error(err)
					}
					w.WriteHeader(http.StatusNoContent)
				case "/ack":
					acked.Store(true)
					w.WriteHeader(http.StatusNoContent)
				}
			}))
			defer s.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Second)
			defer cancel()
			_, err := work(ctx, client.New(s.URL), []string{"sh", "-c", test.script, "worker", heartbeat})
			if acked.Load() != test.wantAck || (err == nil) != test.wantAck || test.loseLease && (!renewed.Load() || !strings.Contains(err.Error(), "invalid lease")) {
				t.Fatalf("ack=%v renew=%v err=%v", acked.Load(), renewed.Load(), err)
			}
		})
	}
}

func TestAgentsAutoStartResetAndResume(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	listener.Close()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TINYQ_TEST_EXEC", "1")
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	t.Setenv("TINYQ_URL", "http://"+addr)
	t.Setenv("TINYQ_WAL", filepath.Join(t.TempDir(), "queue.wal"))
	t.Setenv("TINYQ_IDLE_TIMEOUT", "1s")
	t.Setenv("TINYQ_PARTITION", "writer")
	run := func(args ...string) []byte {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), exe, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%v: %v: %s", args, err, out)
		}
		return out
	}
	take := func(args ...string) client.Lease {
		t.Helper()
		var l client.Lease
		if err := json.Unmarshal(run(append(args, "lease")...), &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	waitForIdleShutdown := func() {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if errors.Is(err, syscall.ECONNREFUSED) {
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			if time.Now().After(deadline) {
				t.Fatal("server still listening after idle timeout")
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	t.Cleanup(waitForIdleShutdown)
	run("enqueue", "leased")
	old := take()
	run("enqueue", "done")
	done := take()
	run("ack", strconv.FormatUint(done.Job.ID, 10), done.Token)
	run("enqueue", "pending")
	run("--partition", "reader", "enqueue", "keep")
	run("empty")
	var stats client.Stats
	if err := json.Unmarshal(run("stats"), &stats); err != nil || stats != (client.Stats{}) {
		t.Fatalf("after empty: stats=%+v err=%v", stats, err)
	}

	waitForIdleShutdown()
	if string(run("lease")) != "null\n" {
		t.Fatal("emptied work returned after restart")
	}
	run("enqueue", "new")
	fresh := take()
	if fresh.Job.ID != 1 || old.Job.ID != 1 || string(fresh.Job.Payload) != "new" {
		t.Fatalf("expected a fresh queue starting at ID 1, got %+v", fresh.Job)
	}
	id := strconv.FormatUint(fresh.Job.ID, 10)
	if err := exec.CommandContext(t.Context(), exe, "ack", id, old.Token).Run(); err == nil {
		t.Fatal("pre-reset token acknowledged the new job")
	}
	run("ack", id, fresh.Token)
	req, err := http.NewRequest("POST", "http://"+addr+"/stop", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://example.org")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin stop: %s", res.Status)
	}
	run("stop")
	run("stop")
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("stop left the server listening: %v", err)
	}
	listener.Close()
	if other := take("--partition", "reader"); string(other.Job.Payload) != "keep" || other.Job.ID != 1 {
		t.Fatalf("other partition changed: %+v", other.Job)
	}
}
