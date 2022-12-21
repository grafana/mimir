// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/grafana/mimir/pkg/util/math"
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
	buf    *bufferingFileReader
	base   int
	length int
	pos    int
}

var bufferPool = sync.Pool{
	New: func() any {
		return newBufferingFileReader(readerBufferSize)
	},
}

// newFileReader creates a new fileReader for the segment of file beginning at base bytes,
// extending length bytes, and closing the handle with closer.
func newFileReader(file *os.File, base, length int, closer poolCloser) (*fileReader, error) {
	f := &fileReader{
		file:   file,
		closer: closer,
		buf:    bufferPool.Get().(*bufferingFileReader),
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

	f.buf.Seek(f.file, f.base+off)
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

	f.buf.Discard(l)
	f.pos += l

	return nil
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

// close cleans up the underlying resources used by this fileReader.
func (f *fileReader) close() error {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bufferPool.Put(f.buf)
	// File handles are pooled, so we don't actually close the handle here, just return it.
	return f.closer.put(f.file)
}

// bufferingFileReader is conceptually the same as bufio.Reader, but optimised
// for reading files. In particular, it does not discard its buffer when seeking
// to an earlier position in the same file where possible.
type bufferingFileReader struct {
	f *os.File
	b []byte

	offsetFirstByteInB    int
	currentPositionInFile int
	haveRead              bool
}

func newBufferingFileReader(bufferSize int) *bufferingFileReader {
	return &bufferingFileReader{
		b: make([]byte, 0, bufferSize),
	}
}

func (b *bufferingFileReader) Seek(f *os.File, off int) {
	b.currentPositionInFile = off

	if f != b.f {
		b.f = f
		b.offsetFirstByteInB = off
		b.haveRead = false
	}
}

func (b *bufferingFileReader) Discard(n int) {
	b.currentPositionInFile += n
}

func (b *bufferingFileReader) Peek(n int) ([]byte, error) {
	if err := b.fillIfRequired(n); err != nil {
		return nil, err
	}

	firstIndex := b.currentPositionInFile - b.offsetFirstByteInB
	available := len(b.b) - firstIndex

	if available == 0 {
		return nil, io.EOF
	}

	n = math.Min(n, available)

	return b.b[firstIndex : firstIndex+n], nil
}

func (b *bufferingFileReader) fillIfRequired(minDesired int) error {
	if minDesired > cap(b.b) {
		return fmt.Errorf("tried to fill %v bytes into buffer, but buffer capacity is only %v bytes", minDesired, cap(b.b))
	}

	if b.haveRead && b.currentPositionInFile >= b.offsetFirstByteInB && b.currentPositionInFile+minDesired <= b.offsetFirstByteInB+len(b.b) {
		return nil
	}

	if _, err := b.f.Seek(int64(b.currentPositionInFile), io.SeekStart); err != nil {
		return err
	}

	b.b = b.b[:cap(b.b)]
	n, err := io.ReadFull(b.f, b.b)
	b.b = b.b[:n]
	b.offsetFirstByteInB = b.currentPositionInFile

	if err != nil {
		if err == io.ErrUnexpectedEOF {
			b.haveRead = true

			if n == 0 {
				return io.EOF
			}
		} else {
			return err
		}
	}

	b.haveRead = true

	return nil
}

func (b *bufferingFileReader) Size() int {
	return cap(b.b)
}

func (b *bufferingFileReader) Read(dest []byte) (int, error) {
	// TODO: could potentially read directly into dest in some cases (eg. we have no available bytes buffered and we're doing a read larger than the capacity of the buffer)

	if err := b.fillIfRequired(1); err != nil {
		return 0, err
	}

	firstIndex := b.currentPositionInFile - b.offsetFirstByteInB
	available := len(b.b) - firstIndex

	if available == 0 {
		return 0, io.EOF
	}

	n := math.Min(len(dest), available)
	copy(dest[:n], b.b[firstIndex:firstIndex+n])
	b.currentPositionInFile += n

	return n, nil
}
