// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropPageCache drops the file's synced pages from the page cache.
func dropPageCache(file *os.File) error {
	return unix.Fadvise(int(file.Fd()), 0, 0, unix.FADV_DONTNEED)
}
