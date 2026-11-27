// SPDX-License-Identifier: AGPL-3.0-only
//go:build unix

package mmap

import (
	"os"

	"golang.org/x/sys/unix"
)

const Supported = true

func Map(file *os.File, size int, writable bool) ([]byte, error) {
	protection := unix.PROT_READ
	if writable {
		protection |= unix.PROT_WRITE
	}
	return unix.Mmap(int(file.Fd()), 0, size, protection, unix.MAP_SHARED)
}

func Anonymous(size int) ([]byte, error) {
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
}

func Flush(data []byte) error { return unix.Msync(data, unix.MS_SYNC) }
func Unmap(data []byte) error { return unix.Munmap(data) }
