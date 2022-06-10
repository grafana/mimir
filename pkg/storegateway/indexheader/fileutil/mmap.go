// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/fileutil/mmap.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package fileutil

import (
	"os"

	"github.com/pkg/errors"
)

type MmapFile struct {
	f *os.File
	b []byte
}

func OpenMmapFile(path string, populate bool) (*MmapFile, error) {
	return OpenMmapFileWithSize(path, 0, populate)
}

func OpenMmapFileWithSize(path string, size int, populate bool) (mf *MmapFile, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "try lock file")
	}
	defer func() {
		if retErr != nil {
			f.Close()
		}
	}()
	if size <= 0 {
		info, err := f.Stat()
		if err != nil {
			return nil, errors.Wrap(err, "stat")
		}
		size = int(info.Size())
	}

	b, err := mmap(f, size, populate)
	if err != nil {
		return nil, errors.Wrapf(err, "mmap, size %d", size)
	}

	return &MmapFile{f: f, b: b}, nil
}

func (f *MmapFile) Close() error {
	err0 := munmap(f.b)
	err1 := f.f.Close()

	if err0 != nil {
		return err0
	}
	return err1
}

func (f *MmapFile) File() *os.File {
	return f.f
}

func (f *MmapFile) Bytes() []byte {
	return f.b
}
