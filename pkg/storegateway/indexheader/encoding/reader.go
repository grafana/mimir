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

type FileReader struct {
	file   *os.File
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

// NewFileReader creates a new FileReader for the segment of file beginning at base bytes
// extending length bytes using the supplied buffered reader.
func NewFileReader(file *os.File, base, length int) (*FileReader, error) {
	f := &FileReader{
		file:   file,
		buf:    bufferPool.Get().(*bufio.Reader),
		base:   base,
		length: length,
	}

	err := f.Reset()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Reset moves the cursor position to the beginning of the file segment including the
// base set when the FileReader was created.
func (f *FileReader) Reset() error {
	return f.ResetAt(0)
}

// ResetAt moves the cursor position to the given offset in the file segment including
// the base set when the FileReader was created. Attempting to ResetAt to the end of the
// file segment is valid. Attempting to ResetAt _beyond_ the end of the file segment will
// return an error.
func (f *FileReader) ResetAt(off int) error {
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
func (f *FileReader) Skip(l int) error {
	if l > f.Len() {
		return ErrInvalidSize
	}

	n, err := f.buf.Discard(l)
	if n > 0 {
		f.pos += n
	}

	return err
}

// Peek returns at most the given number of bytes from the file segment
// without consuming them. The bytes returned become invalid at the next
// read. It is valid to Peek beyond the end of the file segment. In this
// case the available bytes are returned with a nil error.
func (f *FileReader) Peek(n int) ([]byte, error) {
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

// Read returns the given number of bytes from the file segment, consuming them. It is
// NOT valid to Read beyond the end of the file segment. In this case, a nil byte slice
// and ErrInvalidSize error will be returned, and the remaining bytes are consumed.
func (f *FileReader) Read(n int) ([]byte, error) {
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
func (f *FileReader) ReadInto(b []byte) error {
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
func (f *FileReader) Size() int {
	return f.buf.Size()
}

// Len returns the remaining number of bytes in the file segment owned by this reader.
func (f *FileReader) Len() int {
	return f.length - f.pos
}

// close closes the underlying resources used by this FileReader. This method
// is unexported to ensure that all resource management is handled by DecbufFactory
// which pools resources.
func (f *FileReader) close() error {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bufferPool.Put(f.buf)

	return f.file.Close()
}
