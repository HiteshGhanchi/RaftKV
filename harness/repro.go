package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"raftlab/check"
)

// ReproVersion is the version of the repro file format.
const ReproVersion = 1

// Repro is a saved run: options (including the seed, mutations and a fault
// schedule) plus the safety property it is expected to violate. Running it
// replays the same execution bit for bit.
type Repro struct {
	Version int     `json:"repro_version"`
	Expect  string  `json:"expect"` // property expected to fail, "" if the run must pass
	Note    string  `json:"note,omitempty"`
	Opts    Options `json:"options"`
}

// Options returns the run's options.
func (r Repro) Options() Options { return r.Opts }

// LoadRepro reads a repro file.
func LoadRepro(path string) (Repro, error) {
	var r Repro
	b, err := os.ReadFile(path)
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("%s: %w", path, err)
	}
	if r.Version != ReproVersion {
		return r, fmt.Errorf("%s: repro_version %d, want %d", path, r.Version, ReproVersion)
	}
	return r, nil
}

// Save writes the repro as indented JSON.
func (r Repro) Save(path string) error {
	r.Version = ReproVersion
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// Property names the safety property a result violated: the part of the
// violation before the colon, "Linearizability" for a non-linearizable
// history, or "" if the run passed.
func Property(r Result) string {
	if r.Violation != "" {
		name, _, _ := strings.Cut(r.Violation, ":")
		return name
	}
	if r.Linearizability == check.NotLinearizable {
		return "Linearizability"
	}
	return ""
}
