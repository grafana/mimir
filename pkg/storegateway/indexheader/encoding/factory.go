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

// NewDecbufAt returns a new file-backed decoding buffer positioned at offset + 4 bytes.
// It expects the first 4 bytes after offset to hold the big endian encoded content length, followed
// by the contents and the expected checksum. This method does NOT compute the CRC of the content.
// To check the CRC of the content, use NewDecbufAtChecked.
func (df *DecbufFactory) NewDecbufAt(offset int) Decbuf {
	return df.NewDecbufAtChecked(offset, nil)
}
