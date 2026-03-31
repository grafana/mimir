// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/filepool"
)

type DecbufFactory interface {
	// NewDecbufAtChecked returns a new binary decoding reader positioned at offset + 4 bytes.
	// It expects the first 4 bytes after offset to hold the big-endian-encoded content length,
	// followed by the contents and the expected checksum.
	// This method MUST check the CRC of the content and return an errored Decbuf if validation fails.
	NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf

	// NewDecbufAtUnchecked returns a new binary decoding reader positioned at offset + 4 bytes.
	// It expects the first 4 bytes after offset to hold the big endian encoded content length,
	// followed by the contents and the expected checksum.
	// This method MUST NOT validate or compute the CRC of the content.
	// To check the CRC of the content, use NewDecbufAtChecked.
	NewDecbufAtUnchecked(offset int) Decbuf

	// NewRawDecbuf returns a new binary decoding reader positioned at the beginning of the underlying data,
	// and spanning the entire length of the data segment.
	// It MUST NOT make any assumptions about the layout of the underlying data w.r.t checksums, TOC, etc.
	// and it MUST NOT validate or compute the CRC of the content.
	// To create a binary decoding reader for some subset of the data or to perform integrity checks,
	// use NewDecbufAtUnchecked or NewDecbufAtChecked.
	NewRawDecbuf() Decbuf

	Close() error
}

// FilePoolDecbufFactory creates new file-backed Decbuf instances
// for a specific index-header file on local disk.
type FilePoolDecbufFactory struct {
	files *filepool.FilePool
}

func NewFilePoolDecbufFactory(
	path string,
	maxIdleFileHandles uint,
	metrics *filepool.FilePoolMetrics,
) *FilePoolDecbufFactory {
	return &FilePoolDecbufFactory{
		files: filepool.NewFilePool(
			path,
			maxIdleFileHandles,
			metrics,
		),
	}
}

func (df *FilePoolDecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	f, err := df.files.Get()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a BufReader for our Decbuf, we are responsible
	// for putting the file handle back in the pool.
	closeFile := true
	defer func() {
		if closeFile {
			_ = df.files.Put(f)
		}
	}()

	// TODO: A particular index-header only has symbols and posting offsets. We should only need to read
	//  the length of each of those a single time per index-header (DecbufFactory). Should the factory
	//  cache the length? Should the table of contents be passed to the factory?
	lengthBytes := make([]byte, 4)
	n, err := f.ReadAt(lengthBytes, int64(offset))
	if err != nil {
		return Decbuf{E: err}
	}
	if n != 4 {
		return Decbuf{E: errors.Wrapf(ErrInvalidSize, "insufficient bytes read for size (got %d, wanted %d)", n, 4)}
	}

	contentLength := int(binary.BigEndian.Uint32(lengthBytes))
	bufferLength := len(lengthBytes) + contentLength + crc32.Size
	r, err := NewFileReader(f, offset, bufferLength, df.files)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "create file reader")}
	}

	closeFile = false
	d := Decbuf{r: r}

	if d.ResetAt(4); d.Err() != nil {
		return d
	}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(4)
	}

	return d
}

func (df *FilePoolDecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return df.NewDecbufAtChecked(offset, nil)
}

func (df *FilePoolDecbufFactory) NewRawDecbuf() Decbuf {
	f, err := df.files.Get()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a BufReader for our Decbuf, we are responsible
	// for putting the file handle back in the pool.
	closeFile := true
	defer func() {
		if closeFile {
			_ = df.files.Put(f)
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "stat file for decbuf")}
	}

	fileSize := stat.Size()
	reader, err := NewFileReader(f, 0, int(fileSize), df.files)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "file reader for decbuf")}
	}

	closeFile = false
	return Decbuf{r: reader}
}

// Close cleans up resources associated with this DecbufFactory
func (df *FilePoolDecbufFactory) Close() error {
	df.files.Stop()
	return nil
}
