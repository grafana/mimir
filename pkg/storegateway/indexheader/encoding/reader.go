// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"errors"
	"io"
	"os"
)

type FileReader struct {
	file   *os.File
	buf    *bufio.Reader
	base   int
	length int
	pos    int
}

// NewFileReader creates a new FileReader for the segment of file beginning at base
// with length length.
func NewFileReader(file *os.File, base, length int) (*FileReader, error) {
	f := &FileReader{
		file:   file,
		buf:    bufio.NewReader(file),
		base:   base,
		length: length,
	}

	// TODO: Audit everywhere we create a new reader and see if this is really required
	err := f.Reset()
	if err != nil {
		return nil, err
	}

	return f, nil
}

// Reset moves the cursor position to the beginning of the file segment.
func (f *FileReader) Reset() error {
	return f.ResetAt(0)
}

// ResetAt moves the cursor position to the given absolute offset in the file segment.
// Attempting to ResetAt beyond the end of the file segment will return an error.
func (f *FileReader) ResetAt(off int) error {
	if off >= f.length {
		return ErrInvalidSize
	}

	pos, err := f.file.Seek(int64(f.base+off), io.SeekStart)
	if err != nil {
		return err
	}

	f.buf.Reset(f.file)
	f.pos = int(pos) - f.base

	return nil
}

// Skip advances the cursor position by the given number of bytes in the file segment.
// Attempting to Skip beyond the end of the file segment will return an error.
func (f *FileReader) Skip(l int) error {
	if l >= f.Len() {
		return ErrInvalidSize
	}

	n, err := f.buf.Discard(l)
	if n > 0 {
		f.pos += n
	}

	return err
}

// Peek returns at most the given number of bytes from the file segment
// without consuming them. It is valid to Peek beyond the end of the file
// segment. In this case the available bytes are returned with a nil error.
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

// Read returns at most the given number of bytes from the file segment, consuming
// them. It is valid to Read beyond the end of the file segment. In this case the
// available bytes are returned with a nil error.
func (f *FileReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)
	r, err := f.buf.Read(b)
	if r > 0 {
		f.pos += r
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return b[:r], nil
}

// Len returns the remaining number of bytes in the file segment.
func (f *FileReader) Len() int {
	return f.length - f.pos
}

// Close closes the underlying resources used by this FileReader.
func (f *FileReader) Close() error {
	return f.file.Close()
}
