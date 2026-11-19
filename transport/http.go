// Package transport carries Raft messages between real servers over HTTP.
//
// Raft tolerates lost, duplicated and reordered messages, so the transport is
// deliberately simple: one queue and one sender goroutine per peer, messages
// batched into a single POST, and anything that cannot be sent is dropped.
package transport

import (
	"bytes"
	"encoding/gob"
	"io"
	"net/http"
	"sync"
	"time"

	"raftlab/raft"
)

// Path is the HTTP endpoint that receives Raft message batches.
const Path = "/raft"

// HTTP sends messages to peers by address.
type HTTP struct {
	client *http.Client
	queues map[raft.NodeID]chan raft.Message
	quit   chan struct{}
	wg     sync.WaitGroup
}

// NewHTTP starts one sender per peer. addrs maps node IDs to host:port.
func NewHTTP(self raft.NodeID, addrs map[raft.NodeID]string) *HTTP {
	t := &HTTP{
		client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4}},
		queues: map[raft.NodeID]chan raft.Message{},
		quit:   make(chan struct{}),
	}
	for id, addr := range addrs {
		if id == self {
			continue
		}
		q := make(chan raft.Message, 8192)
		t.queues[id] = q
		t.wg.Add(1)
		go t.sender("http://"+addr+Path, q)
	}
	return t
}

// Send queues messages without blocking; a full queue drops them.
func (t *HTTP) Send(msgs []raft.Message) {
	for _, m := range msgs {
		select {
		case t.queues[m.To] <- m:
		default:
		}
	}
}

// Close stops the senders.
func (t *HTTP) Close() {
	close(t.quit)
	t.wg.Wait()
}

func (t *HTTP) sender(url string, q chan raft.Message) {
	defer t.wg.Done()
	var batch []raft.Message
	for {
		select {
		case <-t.quit:
			return
		case m := <-q:
			batch = append(batch[:0], m)
		}
	drain:
		for len(batch) < 512 {
			select {
			case m := <-q:
				batch = append(batch, m)
			default:
				break drain
			}
		}
		if err := t.post(url, batch); err != nil {
			// Peer unreachable: back off briefly so a dead peer does not
			// burn CPU. Raft retries on its own.
			select {
			case <-time.After(20 * time.Millisecond):
			case <-t.quit:
				return
			}
		}
	}
}

func (t *HTTP) post(url string, batch []raft.Message) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(batch); err != nil {
		return err
	}
	resp, err := t.client.Post(url, "application/octet-stream", &buf)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	return resp.Body.Close()
}

// Handler decodes message batches and passes each message to deliver.
func Handler(deliver func(raft.Message)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []raft.Message
		if err := gob.NewDecoder(r.Body).Decode(&batch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, m := range batch {
			deliver(m)
		}
		w.WriteHeader(http.StatusNoContent)
	})
}
