// Package server runs a Peer as a real network service: one goroutine owns
// the peer (so its single-threaded contract holds), a ticker drives time, an
// HTTP transport carries Raft messages and an HTTP API serves clients.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"raftlab/kv"
	"raftlab/peer"
	"raftlab/raft"
	"raftlab/transport"
	"raftlab/vfs"
)

// Config for one server.
type Config struct {
	ID            raft.NodeID
	Addrs         map[raft.NodeID]string // every member's host:port, including this one
	DataDir       string
	Tick          time.Duration // default 10ms
	ElectionTick  int           // default 10 ticks
	SnapshotEvery uint64        // default 10000 entries
	NoSync        bool          // skip fsync (benchmarks only: unsafe)
	MaxBatch      int           // client writes proposed together per fsync; 1 disables batching
	ReqTimeout    time.Duration // default 2s
}

// Response is the JSON body of every client API reply.
type Response struct {
	Value  string `json:"value,omitempty"`
	Error  string `json:"error,omitempty"`
	Leader string `json:"leader,omitempty"` // address hint when this node is not the leader
}

type request struct {
	cmd  kv.Command
	read bool
	key  string
	resp chan peer.Result
}

// Server is a running node.
type Server struct {
	cfg   Config
	p     *peer.Peer
	tr    *transport.HTTP
	inbox chan raft.Message
	reqs  chan request
	quit  chan struct{}
	done  chan struct{}
	ln    net.Listener
	http  *http.Server

	mu     sync.Mutex
	status raft.Status
}

// Start opens the data directory, restores state and begins serving.
func Start(cfg Config) (*Server, error) {
	if cfg.Tick == 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10000
	}
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 256
	}
	if cfg.ReqTimeout == 0 {
		cfg.ReqTimeout = 2 * time.Second
	}
	fs, err := vfs.NewOS(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addrs[cfg.ID])
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:   cfg,
		tr:    transport.NewHTTP(cfg.ID, cfg.Addrs),
		inbox: make(chan raft.Message, 16384),
		reqs:  make(chan request, 16384),
		quit:  make(chan struct{}),
		done:  make(chan struct{}),
		ln:    ln,
	}
	var ids []raft.NodeID
	for id := range cfg.Addrs {
		ids = append(ids, id)
	}
	var mut peerMutations
	if cfg.NoSync {
		mut = skipFsync
	}
	s.p, err = peer.New(peer.Config{
		ID: cfg.ID, Peers: ids,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: 1,
		FS:            fs,
		Rand:          rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), cfg.ID)),
		Mutations:     mut,
		SnapshotEvery: cfg.SnapshotEvery,
		CheckQuorum:   true,
		Send:          s.tr.Send,
	})
	if err != nil {
		ln.Close()
		s.tr.Close()
		return nil, err
	}
	s.status = s.p.Status()

	mux := http.NewServeMux()
	mux.Handle("POST "+transport.Path, transport.Handler(func(m raft.Message) {
		select {
		case s.inbox <- m:
		default: // overloaded: drop, Raft will retry
		}
	}))
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handleWrite(kv.OpPut))
	mux.HandleFunc("POST /kv/{key}/append", s.handleWrite(kv.OpAppend))
	mux.HandleFunc("GET /status", s.handleStatus)
	s.http = &http.Server{Handler: mux}

	go s.loop()
	go s.http.Serve(ln)
	return s, nil
}

// Stop shuts the server down (like a clean kill: no state is flushed beyond
// what was already fsynced).
func (s *Server) Stop() {
	s.http.Close()
	close(s.quit)
	<-s.done
	s.tr.Close()
	s.p.Close()
}

// Status returns the latest protocol state (updated by the loop).
func (s *Server) Status() raft.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// loop is the only goroutine that touches the peer.
func (s *Server) loop() {
	defer close(s.done)
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	var cmds []kv.Command
	var cbs []peer.Callback
	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.p.Tick()
		case m := <-s.inbox:
			s.p.Receive(m)
		case r := <-s.reqs:
			cmds, cbs = cmds[:0], cbs[:0]
			s.collect(r, &cmds, &cbs)
			// Group whatever else is already queued into one proposal batch:
			// one fsync and one message per follower for all of them.
		more:
			for len(cmds) < s.cfg.MaxBatch {
				select {
				case r := <-s.reqs:
					s.collect(r, &cmds, &cbs)
				default:
					break more
				}
			}
			if len(cmds) > 0 {
				s.p.SubmitBatch(cmds, cbs)
			}
		}
		st := s.p.Status()
		s.mu.Lock()
		s.status = st
		s.mu.Unlock()
	}
}

func (s *Server) collect(r request, cmds *[]kv.Command, cbs *[]peer.Callback) {
	cb := func(res peer.Result) { r.resp <- res }
	if r.read {
		s.p.Read(r.key, cb)
		return
	}
	*cmds = append(*cmds, r.cmd)
	*cbs = append(*cbs, cb)
}

func (s *Server) do(r request) peer.Result {
	r.resp = make(chan peer.Result, 1)
	select {
	case s.reqs <- r:
	default:
		return peer.Result{Err: errors.New("server overloaded")}
	}
	select {
	case res := <-r.resp:
		return res
	case <-time.After(s.cfg.ReqTimeout):
		return peer.Result{Err: errors.New("timeout")}
	}
}

func (s *Server) reply(w http.ResponseWriter, res peer.Result) {
	w.Header().Set("Content-Type", "application/json")
	var nl *peer.NotLeaderError
	switch {
	case res.Err == nil:
		json.NewEncoder(w).Encode(Response{Value: res.Value})
	case errors.As(res.Err, &nl):
		w.WriteHeader(http.StatusMisdirectedRequest)
		json.NewEncoder(w).Encode(Response{Error: res.Err.Error(), Leader: s.cfg.Addrs[nl.Leader]})
	default:
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(Response{Error: res.Err.Error()})
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	s.reply(w, s.do(request{read: true, key: r.PathValue("key")}))
}

func (s *Server) handleWrite(op kv.Op) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		client, err1 := strconv.ParseUint(r.URL.Query().Get("client"), 10, 64)
		seq, err2 := strconv.ParseUint(r.URL.Query().Get("seq"), 10, 64)
		if err1 != nil || err2 != nil || client == 0 || seq == 0 {
			http.Error(w, "client and seq query parameters are required", http.StatusBadRequest)
			return
		}
		var body []byte
		if r.ContentLength > 0 {
			body = make([]byte, r.ContentLength)
			if _, err := ioReadFull(r, body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		cmd := kv.Command{ClientID: client, Seq: seq, Op: op, Key: r.PathValue("key"), Value: string(body)}
		s.reply(w, s.do(request{cmd: cmd}))
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	st := s.Status()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id": st.ID, "state": st.State.String(), "term": st.Term, "leader": st.Lead,
		"commit": st.Commit, "applied": st.Applied, "lastIndex": st.LastIndex, "snapIndex": st.SnapIndex,
	})
}

// ParseAddrs parses "1=host:port,2=host:port".
func ParseAddrs(s string) (map[raft.NodeID]string, error) {
	out := map[raft.NodeID]string{}
	for _, part := range splitComma(s) {
		var id uint64
		var addr string
		if _, err := fmt.Sscanf(part, "%d=%s", &id, &addr); err != nil || id == 0 {
			return nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		out[id] = addr
	}
	return out, nil
}
