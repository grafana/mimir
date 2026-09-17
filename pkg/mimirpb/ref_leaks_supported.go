// SPDX-License-Identifier: AGPL-3.0-only

//go:build linux || darwin

package mimirpb

import (
	"syscall"
)

const refLeaksInstrumentationSupported = true

func mmapAnon(len int) ([]byte, error) {
	return syscall.Mmap(-1, 0, len, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
}

func mprotectNone(b []byte) error {
	return syscall.Mprotect(b, syscall.PROT_NONE)
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}
