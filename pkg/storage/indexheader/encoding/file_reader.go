// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// readerBufferSize is the size of the buffer used for reading index-header files. This
// value is arbitrary and will likely change in the future based on profiling results.
const readerBufferSize = 4096

type poolCloser interface {
	put(*os.File) error
}

type fileReader struct {
	file   *os.File
	closer poolCloser
	buf    *bufio.Reader
	base   int
	length int
	pos    int
	stats  *BufReaderStats
}

var bufferPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, readerBufferSize)
	},
}

// newFileReader creates a new fileReader for the segment of file beginning at base bytes,
// extending length bytes, and closing the handle with closer.
func newFileReader(file *os.File, base, length int, closer poolCloser) (*fileReader, error) {
	f := &fileReader{
		file:   file,
		closer: closer,
		buf:    bufferPool.Get().(*bufio.Reader),
		base:   base,
		length: length,
		stats:  &BufReaderStats{},
	}

	err := f.Reset()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Reset moves the cursor position to the beginning of the file segment including the
// base set when the fileReader was created.
func (f *fileReader) Reset() error {
	return f.ResetAt(0)
}

// ResetAt moves the cursor position to the given offset in the file segment including
// the base set when the fileReader was created. Attempting to ResetAt to the end of the
// file segment is valid. Attempting to ResetAt _beyond_ the end of the file segment will
// return an error.
func (f *fileReader) ResetAt(off int) error {
	if off > f.length {
		return ErrInvalidSize
	}

	_, err := f.file.Seek(int64(f.base+off), io.SeekStart)
	if err != nil {
		return err
	}

	f.buf.Reset(f.file)
	f.pos = off

	return nil
}

// Skip advances the cursor position by the given number of bytes in the file segment.
// Attempting to Skip to the end of the file segment is valid. Attempting to Skip _beyond_
// the end of the file segment will return an error.
func (f *fileReader) Skip(l int) error {
	if l > f.Len() {
		return ErrInvalidSize
	}

	n, err := f.buf.Discard(l)
	if n > 0 {
		f.pos += n
		f.stats.BytesDiscarded.Add(uint64(n))
	}

	return err
}

// Peek returns at most the given number of bytes from the file segment
// without consuming them. The bytes returned become invalid at the next
// Read. It is valid to Peek beyond the end of the file segment. In this
// case the available bytes are returned with a nil error.
func (f *fileReader) Peek(n int) ([]byte, error) {
	b, err := f.buf.Peek(n)
	// bufio.Reader still returns what it Read when it hits EOF and callers
	// expect to be able to Peek past the end of a file.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if len(b) > 0 {
		return b, nil
	}

	return nil, nil
}

// Read returns the given number of bytes from the file segment, consuming them. It is
// NOT valid to Read beyond the end of the file segment. In this case, a nil byte slice
// and ErrInvalidSize error will be returned, and the remaining bytes are consumed.
func (f *fileReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := f.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// ReadInto reads len(b) bytes from the file segment into b, consuming them. It is
// NOT valid to ReadInto beyond the end of the file segment. In this case, an ErrInvalidSize
// error will be returned and the remaining bytes are consumed.
func (f *fileReader) ReadInto(b []byte) error {
	r, err := io.ReadFull(f.buf, b)
	if r > 0 {
		f.pos += r
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(b), err)
	} else if err != nil {
		return err
	}

	return nil
}

// Size returns the length of the underlying buffer in bytes.
func (f *fileReader) Size() int {
	return f.buf.Size()
}

// Len returns the remaining number of bytes in the file segment owned by this reader.
func (f *fileReader) Len() int {
	return f.length - f.pos
}

// Position returns the current position of this fileReader in f, relative to base.
func (f *fileReader) Position() int {
	return f.pos
}

// Buffered returns the number of bytes that can be Read from the fileReader which are already in memory.
func (f *fileReader) Buffered() int {
	return f.buf.Buffered()
}

func (f *fileReader) Stats() *BufReaderStats {
	return f.stats
}

// Close cleans up the underlying resources used by this fileReader.
func (f *fileReader) Close() error {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bufferPool.Put(f.buf)
	// File handles are pooled, so we don't actually Close the handle here, just return it.
	return f.closer.put(f.file)
}
