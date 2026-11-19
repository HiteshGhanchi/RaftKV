package sim

import "raftlab/vfs"

func osfs(dir string) (*vfs.OS, error) { return vfs.NewOS(dir) }

// Compile-time check that the simulated disk satisfies the storage interface.
var _ vfs.FS = (*Disk)(nil)
