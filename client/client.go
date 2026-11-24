package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var ErrEmptyQueue = errors.New("queue is empty")

type Client struct {
	baseURL string
	http    *http.Client
}

type Job struct {
	ID      uint64
	Payload []byte
}

type Lease struct {
	Job       Job
	Token     string
	ExpiresAt time.Time
}

type Stats struct {
	Pending          int
	Leased           int
	Completed        int
	LeaseExpirations int
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) Enqueue(ctx context.Context, payload []byte) (uint64, error) {
	var result struct {
		ID uint64 `json:"id"`
	}

	err := c.request(ctx, "POST", "/enqueue",
		payload, "application/octet-stream",
		http.StatusCreated, &result)

	return result.ID, err
}

func (c *Client) Lease(ctx context.Context) (Lease, error) {
	var lease Lease

	err := c.request(ctx, "POST", "/lease",
		nil, "", http.StatusOK, &lease)

	return lease, err
}

func (c *Client) Renew(ctx context.Context, id uint64, token string) error {
	body, err := json.Marshal(struct {
		ID    uint64 `json:"id"`
		Token string `json:"token"`
	}{id, token})

	if err != nil {
		return err
	}

	return c.request(ctx, "POST", "/renew",
		body, "application/json",
		http.StatusNoContent, nil)
}

func (c *Client) Ack(ctx context.Context, id uint64, token string) error {
	body, err := json.Marshal(struct {
		ID    uint64 `json:"id"`
		Token string `json:"token"`
	}{id, token})

	if err != nil {
		return err
	}

	return c.request(ctx, "POST", "/ack",
		body, "application/json",
		http.StatusNoContent, nil)
}

func (c *Client) Stats(ctx context.Context) (Stats, error) {
	var stats Stats

	err := c.request(ctx, "GET", "/stats",
		nil, "", http.StatusOK, &stats)

	return stats, err
}

func (c *Client) request(
	ctx context.Context,
	method, path string,
	body []byte,
	contentType string,
	expectedStatus int,
	result any,
) error {
	req, err := http.NewRequestWithContext(
		ctx, method, c.baseURL+path, bytes.NewReader(body),
	)
	if err != nil {
		return err
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if path == "/lease" && res.StatusCode == http.StatusNoContent {
		return ErrEmptyQueue
	}

	if res.StatusCode != expectedStatus {
		return fmt.Errorf("tinyq: %s", res.Status)
	}

	if result != nil {
		return json.NewDecoder(res.Body).Decode(result)
	}

	_, err = io.Copy(io.Discard, res.Body)
	return err
}
