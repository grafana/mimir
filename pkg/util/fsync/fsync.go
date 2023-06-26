// SPDX-License-Identifier: AGPL-3.0-only

package fsync

import (
	"os"
	"path"

	"github.com/grafana/dskit/multierror"
)

// CreateFile create a file and execute writeFunc to write data followed by some
// fsync operation to make sure the file and its content are created atomically.
func CreateFile(file *os.File, writeFunc func(f *os.File) (int, error)) error {
	// Write the file, fsync it, then fsync the containing directory in order to guarantee
	// it is persisted to disk. From https://man7.org/linux/man-pages/man2/fsync.2.html
	//
	// > Calling fsync() does not necessarily ensure that the entry in the
	// > directory containing the file has also reached disk.  For that an
	// > explicit fsync() on a file descriptor for the directory is also
	// > needed.

	merr := multierror.New()
	_, err := writeFunc(file)
	merr.Add(err)
	merr.Add(file.Sync())
	merr.Add(file.Close())

	if err := merr.Err(); err != nil {
		return err
	}

	dir, err := os.OpenFile(path.Dir(file.Name()), os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	merr.Add(dir.Sync())
	merr.Add(dir.Close())
	return merr.Err()
}
