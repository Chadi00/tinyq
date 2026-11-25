package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

func (b *benchmark) section(title string) {
	if b.terminal && os.Getenv("NO_COLOR") == "" {
		fmt.Printf("\033[1m%s\033[0m\n", title)
	} else {
		fmt.Println(title)
	}
}

func (b *benchmark) heading(title string) {
	fmt.Println()
	b.section(title)
	fmt.Printf("%-19s %7s %2s %9s %7s %8s %8s %8s\n", "Workload", "Ops", "W", "Ops/s", "MiB/s", "p50", "p95", "p99")
}

func latency(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.0fµs", float64(d)/float64(time.Microsecond))
	}
}

func (b *benchmark) measure(ctx context.Context, name string, n, workers, payloadBytes int, op func(context.Context, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	workers = min(n, workers)
	samples := make([]time.Duration, n)
	var completed atomic.Int64
	done := make(chan struct{}, workers)
	started := time.Now()
	for worker := range workers {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := worker; i < n && ctx.Err() == nil; i += workers {
				t := time.Now()
				if err := op(ctx, i); err != nil {
					cancel(err)
					return
				}
				samples[i] = time.Since(t)
				completed.Add(1)
			}
		}()
	}
	var ticks <-chan time.Time
	if b.terminal {
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()
		ticks = ticker.C
	}
	frame := 0
	for remaining := workers; remaining > 0; {
		select {
		case <-done:
			remaining--
		case <-ticks:
			count := completed.Load()
			filled := int(16 * count / int64(n))
			fmt.Printf("\r\033[K%c %-20s [%s%s] %d/%d  %s", "|/-\\"[frame%4], name, strings.Repeat("=", filled), strings.Repeat(" ", 16-filled), count, n, time.Since(started).Round(time.Second))
			frame++
		}
	}
	elapsed := time.Since(started)
	if b.terminal {
		fmt.Print("\r\033[K")
	}
	if err := context.Cause(ctx); err != nil {
		return fmt.Errorf("%s after %d/%d: %w", name, completed.Load(), n, err)
	}
	slices.Sort(samples)
	p := func(percent int) string { return latency(samples[(percent*n-1)/100]) }
	rate := float64(n) / elapsed.Seconds()
	bandwidth := "–"
	if payloadBytes > 0 {
		bandwidth = fmt.Sprintf("%.2f", rate*float64(payloadBytes)/(1<<20))
	}
	fmt.Printf("%-19s %7d %2d %9.2f %7s %8s %8s %8s\n", name, n, workers, rate, bandwidth, p(50), p(95), p(99))
	return nil
}
