// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/io.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"fmt"
	"io"
)

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
