// Package vfs is the small file-system interface the storage layer is written
// against. It has a real implementation (OS) and a simulated one (sim.Disk)
// that can lose writes which were never fsynced.
package vfs

import (
	"io/fs"
	"os"
	"path/filepath"
)

// File is an append-only file handle.
type File interface {
	Write(p []byte) (int, error)
	// Sync makes everything written so far durable.
	Sync() error
	Close() error
}

// FS is the set of file operations the write-ahead log needs.
type FS interface {
	// OpenAppend opens name for appending, creating it if needed.
	OpenAppend(name string) (File, error)
	// ReadFile returns the file's contents, or an error wrapping
	// fs.ErrNotExist when it does not exist.
	ReadFile(name string) ([]byte, error)
	// WriteFileAtomic replaces name with data. When it returns nil the new
	// contents are durable and a crash can never expose a partial file.
	WriteFileAtomic(name string, data []byte) error
	// Truncate shortens name to size bytes, durably.
	Truncate(name string, size int64) error
}

// ErrNotExist is re-exported so callers do not need to import io/fs.
var ErrNotExist = fs.ErrNotExist

// OS is the real file system, rooted at Dir.
type OS struct{ Dir string }

// NewOS creates the directory if needed.
func NewOS(dir string) (*OS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &OS{Dir: dir}, nil
}

func (o *OS) path(name string) string { return filepath.Join(o.Dir, name) }

// OpenAppend implements FS.
func (o *OS) OpenAppend(name string) (File, error) {
	return os.OpenFile(o.path(name), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

// ReadFile implements FS.
func (o *OS) ReadFile(name string) ([]byte, error) { return os.ReadFile(o.path(name)) }

// WriteFileAtomic writes a temporary file, fsyncs it, renames it over the
// target and fsyncs the directory so the rename itself survives a crash.
func (o *OS) WriteFileAtomic(name string, data []byte) error {
	tmp := o.path(name + ".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, o.path(name)); err != nil {
		return err
	}
	return o.syncDir()
}

// Truncate implements FS.
func (o *OS) Truncate(name string, size int64) error {
	f, err := os.OpenFile(o.path(name), os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return err
	}
	return f.Sync()
}

func (o *OS) syncDir() error {
	d, err := os.Open(o.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
