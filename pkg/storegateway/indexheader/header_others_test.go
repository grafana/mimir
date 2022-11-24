// SPDX-License-Identifier: AGPL-3.0-only

//go:build !linux

package indexheader

func flushFile(_ string) error {
	// fadvise is not supported on non-Linux platforms.
	return nil
}
