package harness

import (
	"errors"
	"fmt"
	"time"

	"raftlab/check"
	"raftlab/kv"
	"raftlab/peer"
	"raftlab/raft"
)

// client behaves like a real client library: one operation at a time, a
// guess of who the leader is, a timeout per attempt, and retries with the
// same sequence number so the server can deduplicate.
type client struct {
	c       *cluster
	idx     int
	cid     uint64
	seq     uint64
	guess   raft.NodeID
	attempt uint64

	busy   bool
	in     check.Input
	cmd    kv.Command
	called time.Duration
}

func (c *cluster) startClient(i int) {
	cl := &client{c: c, idx: i, cid: uint64(i + 1), guess: c.ids[i%len(c.ids)]}
	c.clients = append(c.clients, cl)
	c.w.After(time.Duration(c.crng.IntN(10))*time.Millisecond, fmt.Sprintf("client %d start", i), cl.next)
}

// next starts a new operation.
func (cl *client) next() {
	c := cl.c
	if c.failed || c.w.Now() >= c.stopClients {
		return
	}
	key := fmt.Sprintf("k%d", c.crng.IntN(c.opt.Keys))
	switch r := c.crng.IntN(10); {
	case r < 3:
		cl.in = check.Input{Kind: check.Get, Key: key}
	case r < 6:
		cl.seq++
		cl.in = check.Input{Kind: check.Put, Key: key, Value: fmt.Sprintf("%d.%d;", cl.cid, cl.seq)}
		cl.cmd = kv.Command{ClientID: cl.cid, Seq: cl.seq, Op: kv.OpPut, Key: key, Value: cl.in.Value}
	default:
		cl.seq++
		cl.in = check.Input{Kind: check.Append, Key: key, Value: fmt.Sprintf("%d.%d;", cl.cid, cl.seq)}
		cl.cmd = kv.Command{ClientID: cl.cid, Seq: cl.seq, Op: kv.OpAppend, Key: key, Value: cl.in.Value}
	}
	cl.busy = true
	cl.called = c.w.Now()
	c.res.Ops++
	cl.try()
}

// clientHop is one direction of the client <-> server link: a short delay,
// and the same loss rate as the cluster network.
func (c *cluster) clientHop(label string, fn func()) {
	if c.crng.Float64() < c.net.Config().DropRate {
		return
	}
	c.w.After(time.Duration(1+c.crng.IntN(5))*time.Millisecond, label, fn)
}

// try sends one attempt of the current operation to the guessed leader.
func (cl *client) try() {
	c := cl.c
	cl.attempt++
	attempt := cl.attempt
	target := cl.guess
	c.w.After(c.opt.ClientTimeout, fmt.Sprintf("client %d timeout", cl.idx), func() {
		if cl.busy && cl.attempt == attempt {
			cl.guess = cl.nextNode(target)
			cl.try()
		}
	})
	var arrive func()
	arrive = func() {
		p := c.peers[target]
		if p == nil {
			return // lost; the timeout retries elsewhere
		}
		if c.paused(target) {
			// A paused process handles queued requests when it wakes up.
			c.w.After(c.pause[target]-c.w.Now()+c.jitter(), "deferred request", arrive)
			return
		}
		reply := func(r peer.Result) {
			c.clientHop(fmt.Sprintf("%d -> client %d", target, cl.idx), func() { cl.onReply(attempt, target, r) })
		}
		c.guard(target, func() {
			if cl.in.Kind == check.Get {
				p.Read(cl.in.Key, reply)
			} else {
				p.Submit(cl.cmd, reply)
			}
		})
	}
	c.clientHop(fmt.Sprintf("client %d -> %d", cl.idx, target), arrive)
}

func (cl *client) nextNode(id raft.NodeID) raft.NodeID {
	ids := cl.c.ids
	for i, x := range ids {
		if x == id {
			return ids[(i+1)%len(ids)]
		}
	}
	return ids[0]
}

func (cl *client) onReply(attempt uint64, from raft.NodeID, r peer.Result) {
	c := cl.c
	if !cl.busy || attempt != cl.attempt {
		return // a late answer to an attempt we already gave up on
	}
	if r.Err != nil {
		var nl *peer.NotLeaderError
		if errors.As(r.Err, &nl) && nl.Leader != raft.None && nl.Leader != from {
			cl.guess = nl.Leader
		} else {
			cl.guess = cl.nextNode(from)
		}
		cl.attempt++ // retire this attempt's timeout
		c.w.After(10*time.Millisecond, fmt.Sprintf("client %d retry", cl.idx), cl.try)
		return
	}
	cl.guess = from
	cl.busy = false
	cl.attempt++
	out := check.Output{}
	if cl.in.Kind == check.Get {
		out.Value = r.Value
	}
	c.hist.Add(check.Op{Client: cl.idx, Input: cl.in, Output: out, Call: cl.called, Return: c.w.Now()})
	c.res.Completed++
	c.w.After(time.Duration(c.crng.IntN(10))*time.Millisecond, fmt.Sprintf("client %d next", cl.idx), cl.next)
}

// finishClients records operations still in flight at the end as unknown.
func (c *cluster) finishClients() {
	for _, cl := range c.clients {
		if cl.busy {
			cl.busy = false
			c.hist.Add(check.Op{Client: cl.idx, Input: cl.in, Output: check.Output{Unknown: true}, Call: cl.called, Return: c.w.Now()})
			c.res.Unknown++
		}
	}
}
