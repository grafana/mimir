// SPDX-License-Identifier: AGPL-3.0-only

package atomicfs

import (
	"bytes"
	"io"
	"os"
	"path"
	"runtime"
)

// // CreateFile creates a file in the filePath, write the data into the file and then execute
// // fsync operation to make sure the file and its content are stored atomically. If the file already
// // exists, it will be overwritten.
// func CreateFile(filePath string, data io.Reader) error {
// 	// Write the file, fsync it, then fsync the containing directory in order to guarantee
// 	// it is persisted to disk. From https://man7.org/linux/man-pages/man2/fsync.2.html
// 	//
// 	// > Calling fsync() does not necessarily ensure that the entry in the
// 	// > directory containing the file has also reached disk.  For that an
// 	// > explicit fsync() on a file descriptor for the directory is also
// 	// > needed.
// 	file, err := os.Create(filePath)
// 	if err != nil {
// 		return err
// 	}

// 	merr := multierror.New()
// 	buf := new(bytes.Buffer)
// 	_, err = buf.ReadFrom(data)
// 	merr.Add(err)
// 	_, err = file.Write(buf.Bytes())
// 	merr.Add(err)
// 	merr.Add(file.Sync())
// 	merr.Add(file.Close())

// 	if err := merr.Err(); err != nil {
// 		return err
// 	}

// 	dir, err := os.OpenFile(path.Dir(file.Name()), os.O_RDONLY, 0777)
// 	if err != nil {
// 		return err
// 	}

// 	merr.Add(dir.Sync())
// 	merr.Add(dir.Close())
// 	return merr.Err()
// }

func CreateFile(filePath string, data io.Reader) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure file is closed regardless of the errors below

	// Read data into buffer before writing to file
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(data); err != nil {
		return err
	}

	// Write buffer to file
	if _, err := file.Write(buf.Bytes()); err != nil {
		return err
	}

	// Sync file to ensure data is written to disk
	if err := file.Sync(); err != nil {
		return err
	}

	// Skip directory sync on Windows
	if runtime.GOOS != "windows" {
		dir, err := os.OpenFile(path.Dir(filePath), os.O_RDONLY, 0777)
		if err != nil {
			return err
		}
		defer dir.Close() // Ensure directory is closed regardless of the errors below

		if err := dir.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// CreateFileAndMove creates a file in the tmpPath, write the data into the file and then execute
// fsync operation to make sure the file and its content are stored atomically. After that it will move
// file to the finalPath to make sure if there is a failure in writing to the tmpPath, we can retry and
// ensure integrity of the file in the finalPath.
func CreateFileAndMove(tmpPath, finalPath string, data io.Reader) error {
	if err := CreateFile(tmpPath, data); err != nil {
		return err
	}
	defer os.Remove(tmpPath)
	// we rely on the atomicity of this on Unix systems for this method to behave correctly
	return os.Rename(tmpPath, finalPath)
}
