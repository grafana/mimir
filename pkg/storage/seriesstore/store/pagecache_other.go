// SPDX-License-Identifier: AGPL-3.0-only

//go:build !linux

package store

import "os"

// dropPageCache does nothing: only Linux, where the ingesters run, has posix_fadvise in golang.org/x/sys/unix.
func dropPageCache(*os.File) error {
	return nil
}
