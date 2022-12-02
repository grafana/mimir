// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/pkg/errors"
)

// DecbufFactory creates new file-backed decoding buffer instances for a specific index-header file.
type DecbufFactory struct {
	// TODO: After profiling, it might make sense to have a `sync.Pool` of `bufio.Reader`s here
	path string
}

func NewDecbufFactory(path string) *DecbufFactory {
	return &DecbufFactory{path: path}
}

// NewDecbufAtChecked returns a new file-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method checks the CRC of the content and will
// return an error Decbuf if it does not match the expected CRC.
func (df *DecbufFactory) NewDecbufAtChecked(offset int, table *crc32.Table) Decbuf {
	f, err := os.Open(df.path)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a Reader for our Decbuf, we are responsible
	// for actually closing the file handle.
	closeFile := true
	defer func() {
		if closeFile {
			_ = f.Close()
		}
	}()

	lengthBytes := make([]byte, 4)
	n, err := f.ReadAt(lengthBytes, int64(offset))
	if err != nil {
		return Decbuf{E: err}
	}
	if n != 4 {
		return Decbuf{E: errors.Wrapf(ErrInvalidSize, "insufficient bytes read for size (got %d, wanted %d)", n, 4)}
	}

	contentLength := int(binary.BigEndian.Uint32(lengthBytes))
	bufferLength := len(lengthBytes) + contentLength + 4
	r, err := NewFileReader(f, offset, bufferLength)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "create file reader")}
	}

	d := Decbuf{r: r}
	closeFile = false
	if d.ResetAt(4); d.Err() != nil {
		return d
	}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// Reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(4)
	}

	return d
}

// NewDecbufAtUnchecked returns a new file-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method does NOT compute the CRC of the content.
// To check the CRC of the content, use NewDecbufAtChecked.
func (df *DecbufFactory) NewDecbufAtUnchecked(offset int) Decbuf {
	return df.NewDecbufAtChecked(offset, nil)
}

// NewRawDecbuf returns a new file-backed decoding buffer positioned at the beginning of the file,
// spanning the entire length of the file. It does not make any assumptions about the contents of the
// file, nor does it perform any form of integrity check. To create a decoding buffer for some subset
// of the file or perform integrity checks or use NewDecbufAtUnchecked or NewDecbufAtChecked.
func (df *DecbufFactory) NewRawDecbuf() Decbuf {
	f, err := os.Open(df.path)
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "open file for decbuf")}
	}

	// If we return early and don't include a Reader for our Decbuf, we are responsible
	// for actually closing the file handle.
	closeFile := true
	defer func() {
		if closeFile {
			_ = f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "stat file for decbuf")}
	}

	fileSize := stat.Size()
	reader, err := NewFileReader(f, 0, int(fileSize))
	if err != nil {
		return Decbuf{E: errors.Wrap(err, "file reader for decbuf")}
	}

	closeFile = false
	return Decbuf{r: reader}
}
