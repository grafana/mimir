// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/encoding/encoding.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/dennwc/varint"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/math"
)

var (
	ErrInvalidSize     = errors.New("invalid size")
	ErrInvalidChecksum = errors.New("invalid checksum")
)

// Decbuf provides safe methods to extract data from a binary file. It does all
// necessary bounds checking and advancing of the binary file. Several datums can
// be extracted without checking for errors. However, before using any datum, the
// Err() method must be checked. New file-backed Decbuf instances must be created
// via DecbufFactory
type Decbuf struct {
	r *FileReader
	E error
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
// of bytes. If E is non-nil, this method has no effect. Skip-ing beyond the
// end of the underlying FileReader will set E to an error and not advance the
// pointer of the FileReader.
func (d *Decbuf) Skip(l int) {
	if d.E != nil {
		return
	}

	err := d.r.Skip(l)
	if err != nil {
		d.E = err
	}
}

// ResetAt sets the pointer of the underlying FileReader to the absolute
// offset and discards any buffered data. If E is non-nil, this method has
// no effect. ResetAt-ing beyond the end of the underlying FileReader will set
// E to an error and not advance the pointer of FileReader.
func (d *Decbuf) ResetAt(off int) {
	if d.E != nil {
		return
	}

	err := d.r.ResetAt(off)
	if err != nil {
		d.E = err
	}
}

// UvarintStr reads varint prefixed bytes into a string and consumes them. The string
// returned allocates its own memory may be used after subsequent reads from the Decbuf.
// If E is non-nil, this method returns an empty string.
func (d *Decbuf) UvarintStr() string {
	return string(d.UvarintBytes())
}

// UvarintBytes reads varint prefixed bytes into a byte slice consuming them but without
// allocating. The bytes returned are no longer valid after subsequent reads from the Decbuf.
// If E is non-nil, this method returns an empty byte slice.
func (d *Decbuf) UvarintBytes() []byte {
	l := d.Uvarint64()
	if d.E != nil {
		return []byte{}
	}

	b, err := d.r.Peek(int(l))
	if err != nil {
		d.E = err
		return []byte{}
	}

	if len(b) != int(l) {
		d.E = ErrInvalidSize
		return []byte{}
	}

	if b == nil {
		return []byte{}
	}

	err = d.r.Skip(len(b))
	if err != nil {
		d.E = err
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

	err = d.r.Skip(n)
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

	return binary.BigEndian.Uint32(b)
}

func (d *Decbuf) Byte() byte {
	if d.E != nil {
		return 0
	}

	b, err := d.r.Read(1)
	if err != nil {
		d.E = err
		return 0
	}

	return b[0]
}

func (d *Decbuf) Err() error { return d.E }
func (d *Decbuf) Len() int   { return d.r.Len() }

// close cleans up any resources associated with this Decbuf. This method
// is private to ensure that all resource management is handled by DecbufFactory
// which pools resources.
func (d *Decbuf) close() error {
	if d.r != nil {
		return d.r.close()
	}

	return nil
}
