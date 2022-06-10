// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap_unix.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

//go:build !darwin && !windows && !plan9
// +build !darwin,!windows,!plan9

package fileutil

import (
	"os"

	"golang.org/x/sys/unix"
)

func mmap(f *os.File, length int, populate bool) ([]byte, error) {
	flags := unix.MAP_SHARED
	if populate {
		flags |= unix.MAP_POPULATE
	}
	return unix.Mmap(int(f.Fd()), 0, length, unix.PROT_READ, flags)
}

func munmap(b []byte) (err error) {
	return unix.Munmap(b)
}
