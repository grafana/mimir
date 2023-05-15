// SPDX-License-Identifier: AGPL-3.0-only

package shutdownmarker

import (
	"os"
	"path"
	"time"

	"github.com/grafana/dskit/multierror"
)

const shutdownMarkerFilename = "shutdown-requested.txt"

// Create writes a marker file on the given path to indicate that a component is
// going to be scaled down in the future. The presence of this file means that a component
// should perform some operations specified by the component itself before being shutdown.
func Create(p string) error {
	// Write the file, fsync it, then fsync the containing directory in order to guarantee
	// it is persisted to disk. From https://man7.org/linux/man-pages/man2/fsync.2.html
	//
	// > Calling fsync() does not necessarily ensure that the entry in the
	// > directory containing the file has also reached disk.  For that an
	// > explicit fsync() on a file descriptor for the directory is also
	// > needed.
	file, err := os.Create(p)
	if err != nil {
		return err
	}

	merr := multierror.New()
	_, err = file.WriteString(time.Now().UTC().Format(time.RFC3339))
	merr.Add(err)
	merr.Add(file.Sync())
	merr.Add(file.Close())

	if err := merr.Err(); err != nil {
		return err
	}

	dir, err := os.OpenFile(path.Dir(p), os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	merr.Add(dir.Sync())
	merr.Add(dir.Close())
	return merr.Err()
}

// Remove removes the shutdown marker file on the given path if it exists.
func Remove(p string) error {
	err := os.Remove(p)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	dir, err := os.OpenFile(path.Dir(p), os.O_RDONLY, 0777)
	if err != nil {
		return err
	}

	merr := multierror.New()
	merr.Add(dir.Sync())
	merr.Add(dir.Close())
	return merr.Err()
}

// Exists returns true if the shutdown marker file exists on the given path, false otherwise
func Exists(p string) (bool, error) {
	s, err := os.Stat(p)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return s.Mode().IsRegular(), nil
}

// GetPath returns the absolute path of the shutdown marker file
func GetPath(dirPath string) string {
	return path.Join(dirPath, shutdownMarkerFilename)
}
