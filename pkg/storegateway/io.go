// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/io.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"fmt"
	"io"

	"github.com/pkg/errors"
)

const (
	readerBufferSize = 32 * 1024
)

// byteRange holds information about a single byte range.
type byteRange struct {
	offset int
	length int
}

// byteRanges holds a list of non-overlapping byte ranges sorted by offset.
type byteRanges []byteRange

// size returns the total number of bytes in the byte ranges.
func (r byteRanges) size() int {
	size := 0
	for _, c := range r {
		size += c.length
	}
	return size
}

// areContiguous returns whether all byte ranges are contiguous (no gaps).
func (r byteRanges) areContiguous() bool {
	if len(r) < 2 {
		return true
	}

	for off, idx := r[0].offset+r[0].length, 1; idx < len(r); idx++ {
		if r[idx].offset != off {
			return false
		}
		off += r[idx].length
	}
	return true
}

// readByteRanges reads the provided byteRanges from src and append them to dst. The provided
// byteRanges must be sorted by offset and non overlapping. The byteRanges offset must be
// relative to the beginning of the provided src (offset 0 == first byte will be read from src).
func readByteRanges(src io.Reader, dst []byte, byteRanges byteRanges) ([]byte, error) {
	if len(byteRanges) == 0 {
		return nil, nil
	}

	// Ensure the provided dst buffer has enough capacity.
	expectedSize := byteRanges.size()
	if cap(dst) < expectedSize {
		return nil, io.ErrShortBuffer
	}

	// Size the destination buffer accordingly.
	dst = dst[0:expectedSize]

	// Optimisation for the case all ranges are contiguous.
	if byteRanges[0].offset == 0 && byteRanges.areContiguous() {
		// We get an ErrUnexpectedEOF if EOF is reached before we fill allocated dst slice.
		// Due to how the reading logic works in the bucket store, we may try to overread at
		// the end of an object, so we consider it legit.
		if _, err := io.ReadFull(src, dst); err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, err
		}
		return dst, nil
	}

	// To keep implementation easier we frequently call Read() for short lengths.
	// In such scenario, having a buffered reader improves performances at the cost
	// of 1 more buffer allocation and memory copy.
	reader := bufio.NewReaderSize(src, readerBufferSize)

	for dstOffset, idx := 0, 0; idx < len(byteRanges); idx++ {
		curr := byteRanges[idx]

		// Read and discard all bytes before the current chunk offset.
		discard := 0
		if idx == 0 {
			discard = curr.offset
		} else {
			prev := byteRanges[idx-1]
			discard = curr.offset - (prev.offset + prev.length)
		}

		if _, err := reader.Discard(discard); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, errors.Wrap(err, "discard bytes")
		}

		// At this point the next byte to read from the reader is the current chunk,
		// so we'll read it fully. io.ReadFull() returns an error if less bytes than
		// expected have been read.
		readBytes, err := io.ReadFull(reader, dst[dstOffset:dstOffset+curr.length])
		if readBytes > 0 {
			dstOffset += readBytes
		}
		if err != nil {
			// We get an ErrUnexpectedEOF if EOF is reached before we fill the slice.
			// Due to how the reading logic works in the bucket store, we may try to overread
			// the last byte range so, if the error occurrs on the last one, we consider it legit.
			if errors.Is(err, io.ErrUnexpectedEOF) && idx == len(byteRanges)-1 {
				return dst, nil
			}

			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, errors.Wrap(err, "read byte range")
		}
	}

	return dst, nil
}

type offsetTrackingReader struct {
	offset uint64
	r      *bufio.Reader
}

// SkipTo skips to the provided offset.
func (r *offsetTrackingReader) SkipTo(at uint64) error {
	if diff := int(at - r.offset); diff < 0 {
		return fmt.Errorf("cannot reverse reader: offset %d, at %d", r.offset, at)
	} else if diff > 0 {
		n, err := r.r.Discard(diff)
		r.offset += uint64(n)
		if err != nil {
			return fmt.Errorf("discarding sequence reader: %w", err)
		}
	}
	return nil
}

// ReadByte implement io.ByteReader for compatibility with binary.ReadUvarint.
func (r *offsetTrackingReader) ReadByte() (byte, error) {
	r.offset++
	return r.r.ReadByte()
}

// Read implements io.Reader.
func (r *offsetTrackingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.offset += uint64(n)
	return n, err
}

func (r *offsetTrackingReader) Reset(offset uint64, reader io.Reader) {
	r.r.Reset(reader)
	r.offset = offset
}

// Release removes the reference to the underlying reader
func (r *offsetTrackingReader) Release() {
	r.Reset(0, nil)
}
