package encoding

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

type Reader interface {
	// Reset moves the cursor position to the beginning of the underlying store.
	Reset() error

	// ResetAt moves the cursor position to the given offset in the underlying store.
	ResetAt(off int) error

	Read(int) []byte

	// Peek returns at most the given number of bytes from the underlying store
	// without consuming them. It is valid to Peek beyond the end of the underlying
	// store. In this case the available bytes are returned with a nil error.
	Peek(int) ([]byte, error)
	Len() int

	// TODO: Seems like we need a "Remaining()" method here because Decbuf
	//  expects to be able to do `d.B = d.B[n:]` when it consumes parts of the
	//  underlying byte slice.

	// TODO: Add Seek method here to allow us to skip bytes without
	//  needing to read them or allocate? Easy to implement for BufReader
	//  and supported by bufio.Reader used in FileReader via Discard()
}

type BufReader struct {
	initial []byte
	b       []byte
}

func NewBufReader(bs ByteSlice) *BufReader {
	b := bs.Range(0, bs.Len())
	r := &BufReader{initial: b}
	_ = r.Reset()
	return r
}

func (b *BufReader) Reset() error {
	b.b = b.initial
	return nil
}

func (b *BufReader) ResetAt(off int) error {
	b.b = b.initial
	if off >= len(b.b) {
		return ErrInvalidSize
	}
	b.b = b.b[off:]
	return nil
}

func (b *BufReader) Peek(n int) ([]byte, error) {
	if len(b.b) < n {
		n = len(b.b)
	}
	res := b.b[:n]
	return res, nil
}

func (b *BufReader) Read(n int) []byte {
	if len(b.b) < n {
		n = len(b.b)
	}
	res := b.b[:n]
	b.b = b.b[n:]
	return res
}

func (b *BufReader) Len() int {
	return len(b.b)
}

type FileReader struct {
	file   *os.File
	buf    *bufio.Reader
	base   int
	length int
	pos    int
}

func NewFileReader(file *os.File, base, length int) (*FileReader, error) {
	f := &FileReader{
		file:   file,
		buf:    bufio.NewReader(file),
		base:   base,
		length: length,
	}

	err := f.Reset()
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *FileReader) Reset() error {
	return f.ResetAt(0)
}

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

func (f *FileReader) Read(n int) []byte {
	b := make([]byte, n)
	n, err := f.buf.Read(b)
	fmt.Printf("read: %v\n", err)
	if n > 0 {
		f.pos += n
		return b[:n]
	}
	return nil
}

func (f *FileReader) Len() int {
	return f.length - f.pos
}
