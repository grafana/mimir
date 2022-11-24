// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"golang.org/x/sys/unix"
)

func flushFile(file string) error {
	f, err := fileutil.OpenMmapFile(file)
	if err != nil {
		return err
	}
	defer f.Close()
	src := f.Bytes()

	const POSIX_FADV_WILLNEED = 3
	const POSIX_FADV_DONTNEED = 4
	err = unix.Fadvise(int(f.File().Fd()), 0, int64(len(src)), POSIX_FADV_DONTNEED)
	if err != nil {
		return err
	}
	//fmt.Printf("Flushed\n")

	f.Close()
	return nil
}
