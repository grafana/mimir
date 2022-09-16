//go:build !linux
// +build !linux

package bufferpool

import "syscall"

const openFileFlags = syscall.O_RDONLY
