// Package client is the raftlab client library: it finds the leader, retries
// on timeouts and leader changes, and numbers writes so retries are
// deduplicated by the servers (exactly-once effect).
package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Client talks to one cluster. It is safe for concurrent use, but writes are
// serialised: the dedup protocol allows one outstanding write per client ID.
type Client struct {
	addrs   []string
	http    *http.Client
	id      uint64
	Timeout time.Duration // give up on an operation after this long

	mu     sync.Mutex
	seq    uint64
	leader string
	wmu    sync.Mutex
}

// New creates a client with a random ID.
func New(addrs []string) *Client {
	return &Client{
		addrs:   addrs,
		http:    &http.Client{Timeout: 3 * time.Second},
		id:      rand.Uint64()>>1 | 1,
		Timeout: 10 * time.Second,
		leader:  addrs[0],
	}
}

type response struct {
	Value  string `json:"value"`
	Error  string `json:"error"`
	Leader string `json:"leader"`
}

// Get reads a key with a linearizable read.
func (c *Client) Get(key string) (string, error) {
	return c.retry(func(addr string) (*http.Request, error) {
		return http.NewRequest(http.MethodGet, "http://"+addr+"/kv/"+url.PathEscape(key), nil)
	})
}

// Put sets key to value.
func (c *Client) Put(key, value string) error {
	_, err := c.write(http.MethodPut, key, "", value)
	return err
}

// Append appends value to key.
func (c *Client) Append(key, value string) error {
	_, err := c.write(http.MethodPost, key, "/append", value)
	return err
}

func (c *Client) write(method, key, suffix, value string) (string, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.mu.Lock()
	c.seq++
	seq := c.seq
	c.mu.Unlock()
	return c.retry(func(addr string) (*http.Request, error) {
		u := fmt.Sprintf("http://%s/kv/%s%s?client=%d&seq=%d", addr, url.PathEscape(key), suffix, c.id, seq)
		return http.NewRequest(method, u, bytes.NewReader([]byte(value)))
	})
}

// retry sends the same request (same seq for writes) until it succeeds.
func (c *Client) retry(build func(addr string) (*http.Request, error)) (string, error) {
	deadline := time.Now().Add(c.Timeout)
	backoff := 5 * time.Millisecond
	var lastErr error
	for i := 0; time.Now().Before(deadline); i++ {
		c.mu.Lock()
		addr := c.leader
		c.mu.Unlock()
		req, err := build(addr)
		if err != nil {
			return "", err
		}
		resp, err := c.http.Do(req)
		if err == nil {
			var body response
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			json.Unmarshal(data, &body)
			switch resp.StatusCode {
			case http.StatusOK:
				return body.Value, nil
			case http.StatusMisdirectedRequest:
				lastErr = errors.New(body.Error)
				c.mu.Lock()
				if body.Leader != "" {
					c.leader = body.Leader
				} else {
					c.leader = c.next(addr)
				}
				c.mu.Unlock()
				if body.Leader != "" {
					continue // try the hinted leader at once
				}
			case http.StatusBadRequest:
				return "", fmt.Errorf("bad request: %s", data)
			default:
				lastErr = errors.New(body.Error)
				c.rotate(addr)
			}
		} else {
			lastErr = err
			c.rotate(addr)
		}
		time.Sleep(backoff)
		backoff = min(backoff*2, 200*time.Millisecond)
	}
	return "", fmt.Errorf("gave up after %v: %w", c.Timeout, lastErr)
}

func (c *Client) rotate(addr string) {
	c.mu.Lock()
	if c.leader == addr {
		c.leader = c.next(addr)
	}
	c.mu.Unlock()
}

func (c *Client) next(addr string) string {
	for i, a := range c.addrs {
		if a == addr {
			return c.addrs[(i+1)%len(c.addrs)]
		}
	}
	return c.addrs[0]
}
