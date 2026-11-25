package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
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
	return NewWithHTTPClient(baseURL, &http.Client{Timeout: 10 * time.Second})
}

func NewWithHTTPClient(baseURL string, httpClient *http.Client) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpClient,
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
	return c.lease(ctx, "/lease")
}

func (c *Client) LeaseFor(ctx context.Context, duration time.Duration) (Lease, error) {
	return c.lease(ctx, "/lease?ttl="+duration.String())
}

func (c *Client) lease(ctx context.Context, path string) (Lease, error) {
	var lease Lease

	err := c.request(ctx, "POST", path,
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

func (c *Client) Empty(ctx context.Context) error {
	return c.request(ctx, "POST", "/empty", nil, "", http.StatusNoContent, nil)
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

	resource, _, _ := strings.Cut(path, "?")
	if resource == "/lease" && res.StatusCode == http.StatusNoContent {
		return ErrEmptyQueue
	}

	if res.StatusCode != expectedStatus {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		if detail := strings.TrimSpace(string(body)); detail != "" {
			return fmt.Errorf("tinyq: %s: %s", res.Status, detail)
		}
		return fmt.Errorf("tinyq: %s", res.Status)
	}

	if result != nil {
		if resource == "/lease" {
			if hint := res.Header.Get("X-Tinyq-Payload-Size"); hint != "" {
				return decodeLease(res.Body, hint, result)
			}
		}
		return json.NewDecoder(res.Body).Decode(result)
	}

	_, err = io.Copy(io.Discard, res.Body)
	return err
}

func decodeLease(r io.Reader, hint string, result any) error {
	size, err := strconv.Atoi(hint)
	if err != nil || size < 8<<10 || size > 1<<20 {
		return json.NewDecoder(r).Decode(result)
	}

	data := make([]byte, base64.StdEncoding.EncodedLen(size)+256)
	n, err := io.ReadFull(r, data)
	switch err {
	case nil:
		return json.NewDecoder(io.MultiReader(bytes.NewReader(data), r)).Decode(result)
	case io.EOF, io.ErrUnexpectedEOF:
		data = data[:n]
		if err := json.Unmarshal(data, result); err == nil {
			return nil
		}
		return json.NewDecoder(bytes.NewReader(data)).Decode(result)
	default:
		return err
	}
}
