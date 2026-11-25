package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/Chadi00/tinyq/client"
)

func queueClient(endpoint, partition string) *client.Client {
	base := endpoint
	if partition != "default" {
		base += "/partitions/" + partition
	}
	localAddr := localAddress(endpoint)
	if localAddr == "" {
		return client.New(base)
	}
	dialer := &net.Dialer{}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if addr != localAddr || !errors.Is(err, syscall.ECONNREFUSED) {
			return conn, err
		}
		if err := startLocalServer(addr); err != nil {
			return nil, err
		}
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ticker := time.NewTicker(25 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("tinyq did not start at %s: %w; run tinyq serve -addr %s for diagnostics", addr, ctx.Err(), addr)
			case <-ticker.C:
				conn, err := dialer.DialContext(ctx, network, addr)
				if !errors.Is(err, syscall.ECONNREFUSED) {
					return conn, err
				}
			}
		}
	}}
	return client.NewWithHTTPClient(base, &http.Client{Transport: transport, Timeout: 10 * time.Second})
}

func localAddress(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Port() == "" || !loopbackAddress(u.Host) {
		return ""
	}
	return u.Host
}

func loopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	return err == nil && (host == "localhost" || net.ParseIP(host).IsLoopback())
}

func stopLocalServer(endpoint string) error {
	if localAddress(endpoint) == "" {
		return errors.New("tinyq stop requires a local HTTP URL with a port")
	}
	c := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	res, err := c.Post(endpoint+"/stop", "", nil)
	if errors.Is(err, syscall.ECONNREFUSED) {
		return nil
	}
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		return fmt.Errorf("tinyq stop: %s", res.Status)
	}
	return nil
}

func startLocalServer(addr string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "serve", "-addr", addr)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return err
	}
	return cmd.Process.Release()
}
