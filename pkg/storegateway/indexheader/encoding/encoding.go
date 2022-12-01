// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/encoding/encoding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/dennwc/varint"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/math"
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// Decbuf provides safe methods to extract data from a byte slice. It does all
// necessary bounds checking and advancing of the byte slice.
// Several datums can be extracted without checking for errors. However, before using
// any datum, the Err() method must be checked.
type Decbuf struct {
	r *FileReader
	E error
}

// NewDecbufFromFile returns a new decoding buffer for f. It expects the first 4 bytes
// after offset to hold the big endian encoded content length, followed by the contents and the expected
// checksum.
// If castagnoliTable is non-nil, the integrity of the contents of f are checked against the expected
// checksum.
// TODO: might be able to save a small amount of time by not reading the size every time we create a
// Decbuf if we've read it previously
func NewDecbufFromFile(f *os.File, offset int, castagnoliTable *crc32.Table) Decbuf {
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

	// Skip to the beginning of the content.
	if err := r.ResetAt(4); err != nil {
		return Decbuf{E: err}
	}

	d := Decbuf{r: r}

	if castagnoliTable != nil {
		d.CheckCrc32(castagnoliTable)

		if d.Err() != nil {
			return d
		}

		// Return to the beginning of the content.
		if err := r.ResetAt(4); err != nil {
			return Decbuf{E: err}
		}
	}

	return d
}

// NewRawDecbuf returns a new decoding buffer for r.
// Unlike NewDecbufFromFile, it does not make any assumptions about the contents of r,
// nor does it perform any form of integrity check.
func NewRawDecbuf(r *FileReader) Decbuf {
	err := r.Reset()
	if err != nil {
		return Decbuf{E: err}
	}
	return Decbuf{r: r}
}

func (d *Decbuf) Uvarint() int { return int(d.Uvarint64()) }
func (d *Decbuf) Be32int() int { return int(d.Be32()) }

// CheckCrc32 checks the integrity of the contents of this Decbuf,
// comparing the contents with the CRC32 checksum stored in the last four bytes.
// CheckCrc32 consumes the contents of this Decbuf.
func (d *Decbuf) CheckCrc32(castagnoliTable *crc32.Table) {
	if d.r.Len() <= 4 {
		d.E = ErrInvalidSize
		return
	}

	hash := crc32.New(castagnoliTable)
	bytesToRead := d.r.Len() - 4

	for bytesToRead > 0 {
		maxChunkSize := 1024 * 1024 // TODO: what is a sensible size to use here?
		chunkSize := math.Min(bytesToRead, maxChunkSize)

		// TODO: pull byte slices from a pool rather than creating a new one every time?
		b, err := d.r.Read(chunkSize)
		if err != nil {
			d.E = errors.Wrap(err, "read contents for CRC32 calculation")
			return
		}

		if n, err := hash.Write(b); err != nil {
			d.E = errors.Wrap(err, "write bytes to CRC32 calculation")
			return
		} else if n != len(b) {
			d.E = fmt.Errorf("CRC32 calculation only wrote %v bytes, expected to write %v bytes", n, len(b))
			return
		}

		bytesToRead -= len(b)
	}

	actual := hash.Sum32()
	expected := d.Be32()

	if actual != expected {
		d.E = ErrInvalidChecksum
	}
}

// Skip advances the pointer of the underlying FileReader by the given number
// of bytes. Skip-ing beyond the end of the underlying FileReader will set E
// to an error and not advance the pointer of the FileReader.
func (d *Decbuf) Skip(l int) {
	err := d.r.Skip(l)
	if err != nil {
		d.E = err
	}
}

// ResetAt sets the pointer of the underlying FileReader to the absolute
// offset and discards any buffered data. If E is non-nil, this method has
// no effect.
func (d *Decbuf) ResetAt(off int) {
	if d.E != nil {
		return
	}

	err := d.r.ResetAt(off)
	if err != nil {
		d.E = err
	}
}

func (d *Decbuf) UvarintStr() string {
	return string(d.UvarintBytes())
}

// The return value becomes invalid if the byte slice goes away.
// Compared to UvarintStr, this avoid allocations.
func (d *Decbuf) UvarintBytes() []byte {
	l := d.Uvarint64()
	if d.E != nil {
		return []byte{}
	}

	b, err := d.r.Read(int(l))
	if err != nil {
		d.E = err
		return []byte{}
	}

	if len(b) < int(l) {
		d.E = ErrInvalidSize
		return []byte{}
	}

	return b
}

func (d *Decbuf) Uvarint64() uint64 {
	if d.E != nil {
		return 0
	}
	b, err := d.r.Peek(10)
	if err != nil {
		d.E = err
		return 0
	}

	x, n := varint.Uvarint(b)
	if n < 1 {
		d.E = ErrInvalidSize
		return 0
	}

	_, err = d.r.Read(n)
	if err != nil {
		d.E = err
		return 0
	}

	return x
}

func (d *Decbuf) Be64() uint64 {
	if d.E != nil {
		return 0
	}

	b, err := d.r.Read(8)
	if err != nil {
		d.E = err
		return 0
	}

	if len(b) != 8 {
		d.E = ErrInvalidSize
		return 0
	}

	return binary.BigEndian.Uint64(b)
}

func (d *Decbuf) Be32() uint32 {
	if d.E != nil {
		return 0
	}

	b, err := d.r.Read(4)
	if err != nil {
		d.E = err
		return 0
	}

	if len(b) != 4 {
		d.E = ErrInvalidSize
		return 0
	}

	return binary.BigEndian.Uint32(b)
}

func (d *Decbuf) Err() error { return d.E }
func (d *Decbuf) Len() int   { return d.r.Len() }

func (d *Decbuf) Close() error {
	if d.r != nil {
		return d.r.Close()
	}

	return nil
}
