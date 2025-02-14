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
	}

	err := f.reset()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// reset moves the cursor position to the beginning of the file segment including the
// base set when the fileReader was created.
func (f *fileReader) reset() error {
	return f.resetAt(0)
}

// resetAt moves the cursor position to the given offset in the file segment including
// the base set when the fileReader was created. Attempting to resetAt to the end of the
// file segment is valid. Attempting to resetAt _beyond_ the end of the file segment will
// return an error.
func (f *fileReader) resetAt(off int) error {
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

// skip advances the cursor position by the given number of bytes in the file segment.
// Attempting to skip to the end of the file segment is valid. Attempting to skip _beyond_
// the end of the file segment will return an error.
func (f *fileReader) skip(l int) error {
	if l > f.len() {
		return ErrInvalidSize
	}

	n, err := f.buf.Discard(l)
	if n > 0 {
		f.pos += n
	}

	return err
}

// peek returns at most the given number of bytes from the file segment
// without consuming them. The bytes returned become invalid at the next
// read. It is valid to peek beyond the end of the file segment. In this
// case the available bytes are returned with a nil error.
func (f *fileReader) peek(n int) ([]byte, error) {
	b, err := f.buf.Peek(n)
	// bufio.Reader still returns what it read when it hits EOF and callers
	// expect to be able to peek past the end of a file.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if len(b) > 0 {
		return b, nil
	}

	return nil, nil
}

// read returns the given number of bytes from the file segment, consuming them. It is
// NOT valid to read beyond the end of the file segment. In this case, a nil byte slice
// and ErrInvalidSize error will be returned, and the remaining bytes are consumed.
func (f *fileReader) read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := f.readInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// readInto reads len(b) bytes from the file segment into b, consuming them. It is
// NOT valid to readInto beyond the end of the file segment. In this case, an ErrInvalidSize
// error will be returned and the remaining bytes are consumed.
func (f *fileReader) readInto(b []byte) error {
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

// size returns the length of the underlying buffer in bytes.
func (f *fileReader) size() int {
	return f.buf.Size()
}

// len returns the remaining number of bytes in the file segment owned by this reader.
func (f *fileReader) len() int {
	return f.length - f.pos
}

// position returns the current position of this fileReader in f, relative to base.
func (f *fileReader) position() int {
	return f.pos
}

// buffered returns the number of bytes that can be read from the fileReader which are already in memory.
func (f *fileReader) buffered() int {
	return f.buf.Buffered()
}

// close cleans up the underlying resources used by this fileReader.
func (f *fileReader) close() error {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bufferPool.Put(f.buf)
	// File handles are pooled, so we don't actually close the handle here, just return it.
	return f.closer.put(f.file)
}
