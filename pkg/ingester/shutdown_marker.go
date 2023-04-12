// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"os"
	"path"
	"time"
)

// ShutdownMarker is responsible for writing a marker file to disk to indicate
// that an ingester is going to be scaled down in the future. The presence of this
// file means that an ingester should flush and upload all data when stopping. This
// file is never cleaned up because the ingester pod will be deleted as part of
// scaling down.
type ShutdownMarker struct {
	Path string
}

func NewShutdownMarker(path string) *ShutdownMarker {
	return &ShutdownMarker{path}
}

func (m *ShutdownMarker) Create() error {
	var (
		dir       *os.File
		file      *os.File
		closeDir  = true
		closeFile = true

		err error
	)

	defer func() {
		if closeFile && file != nil {
			_ = file.Close()
		}

		if closeDir && dir != nil {
			_ = dir.Close()
		}
	}()

	// Write the file, fsync it, then fsync the containing directory in order to guarantee
	// it is persisted to disk. From https://man7.org/linux/man-pages/man2/fsync.2.html
	//
	// > Calling fsync() does not necessarily ensure that the entry in the
	// > directory containing the file has also reached disk.  For that an
	// > explicit fsync() on a file descriptor for the directory is also
	// > needed.

	file, err = os.Create(m.Path)
	if err != nil {
		return fmt.Errorf("unable to create shutdown marker %s: %w", m.Path, err)
	}

	_, err = file.WriteString(time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("unable to write shutdown marker contents %s: %w", m.Path, err)
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("unable to fsync shutdown marker %s: %w", m.Path, err)
	}

	closeFile = false
	if err = file.Close(); err != nil {
		return fmt.Errorf("unable to close shutdown marker %s: %w", m.Path, err)
	}

	dir, err = os.OpenFile(path.Dir(m.Path), os.O_RDONLY, 0777)
	if err != nil {
		return fmt.Errorf("unable to open shutdown marker directory %s: %w", m.Path, err)
	}

	if err = dir.Sync(); err != nil {
		return fmt.Errorf("unable to fsync shutdown marker directory %s: %w", m.Path, err)
	}

	closeDir = false
	if err = dir.Close(); err != nil {
		return fmt.Errorf("unable to close shutdown marker directory %s: %w", m.Path, err)
	}

	return nil
}

func (m *ShutdownMarker) Exists() (bool, error) {
	s, err := os.Stat(m.Path)
	if err != nil && os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return s.Mode().IsRegular(), nil
}
