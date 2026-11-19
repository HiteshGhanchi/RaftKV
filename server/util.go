package server

import (
	"io"
	"net/http"
	"strings"

	"raftlab/mutation"
)

type peerMutations = mutation.Set

// skipFsync is reused for the benchmark's no-fsync mode: same code path as
// the bug zoo entry, so "what does fsync cost" is measured honestly.
const skipFsync = mutation.SkipFsync

func ioReadFull(r *http.Request, buf []byte) (int, error) { return io.ReadFull(r.Body, buf) }

func splitComma(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
