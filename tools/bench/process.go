package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"time"
)

type process struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	url    string
}

func start(ctx context.Context, server, wal string, args ...string) (*process, error) {
	ctx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(ctx, server, append([]string{"-addr", "127.0.0.1:0", "-wal", wal}, args...)...)
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	stderr, err := cmd.StderrPipe()
	if err == nil {
		err = cmd.Start()
	}
	if err != nil {
		cancel()
		return nil, err
	}
	p := &process{cmd: cmd, cancel: cancel}
	r := bufio.NewReader(stderr)
	timer := time.AfterFunc(10*time.Second, cancel)
	line, err := r.ReadString('\n')
	timer.Stop()
	_, addr, ok := strings.Cut(strings.TrimSpace(line), "tinyq listening on ")
	host, _, addrErr := net.SplitHostPort(addr)
	if err != nil || !ok || addrErr != nil || host != "127.0.0.1" {
		p.close()
		return nil, fmt.Errorf("server did not announce a loopback address: %q", strings.TrimSpace(line))
	}
	go io.Copy(os.Stderr, r)
	p.url = "http://" + addr
	return p, nil
}

func (p *process) close() {
	if p.cmd.ProcessState == nil {
		p.cancel()
		p.cmd.Wait()
	}
}

func (p *process) footprint(label string) {
	state := p.cmd.ProcessState
	if state == nil {
		return
	}
	rss := int64(state.SysUsage().(*syscall.Rusage).Maxrss)
	if runtime.GOOS == "linux" {
		rss *= 1024
	}
	fmt.Printf("\n%s  ·  %.2f MiB peak RSS  ·  %s CPU", label, float64(rss)/(1<<20), (state.UserTime() + state.SystemTime()).Round(time.Millisecond))
	if info, err := os.Stat(p.cmd.Path); err == nil {
		fmt.Printf("  ·  %.2f MiB binary", float64(info.Size())/(1<<20))
	}
	fmt.Println()
}
