// SPDX-License-Identifier: AGPL-3.0-only

package bufferpool

import (
	"os"
)

// ByteSlice is a slice of bytes backed by a buffer pool.
type ByteSlice struct {
	len int
	f   *os.File
	p   *Pool
}

// NewByteSlice returns and initializes a new ByteSlice instance.
func NewByteSlice(f *os.File, p *Pool) (*ByteSlice, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	return &ByteSlice{
		len: int(fi.Size()),
		f:   f,
		p:   p,
	}, nil
}

// Len returns the length of the slice.
func (bs *ByteSlice) Len() int {
	return bs.len
}

// Range returns a slice of the given range.
func (bs *ByteSlice) Range(start, end int) []byte {
	return nil
}
