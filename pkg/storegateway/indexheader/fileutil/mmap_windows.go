// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap_windows.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package fileutil

import (
	"os"
	"syscall"
	"unsafe"
)

func mmap(f *os.File, size int, populate bool) ([]byte, error) {
	low, high := uint32(size), uint32(size>>32)
	h, errno := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, high, low, nil)
	if h == 0 {
		return nil, os.NewSyscallError("CreateFileMapping", errno)
	}

	addr, errno := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	if addr == 0 {
		return nil, os.NewSyscallError("MapViewOfFile", errno)
	}

	if err := syscall.CloseHandle(syscall.Handle(h)); err != nil {
		return nil, os.NewSyscallError("CloseHandle", err)
	}

	return (*[maxMapSize]byte)(unsafe.Pointer(addr))[:size], nil
}

func munmap(b []byte) error {
	if err := syscall.UnmapViewOfFile((uintptr)(unsafe.Pointer(&b[0]))); err != nil {
		return os.NewSyscallError("UnmapViewOfFile", err)
	}
	return nil
}
