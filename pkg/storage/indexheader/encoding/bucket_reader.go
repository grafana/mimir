// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/thanos-io/objstore"
)

type BucketReader struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int
}

func NewBucketReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketReader {
	return &BucketReader{
		ctx:    ctx,
		bkt:    bkt,
		name:   name,
		base:   base,
		length: length,
	}
}

func (r *BucketReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.off >= r.length {
		return 0, io.EOF
	}
	toRead := len(p)
	remaining := r.length - r.off
	if toRead > remaining {
		toRead = remaining
	}
	rc, err := r.bkt.GetRange(r.ctx, r.name, int64(r.base+r.off), int64(toRead))
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err = io.ReadFull(rc, p[:toRead])
	r.off += n
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (r *BucketReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("invalid Seek whence: %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek to negative offset %d", offset)
	}
	r.off = int(offset)
	return offset, nil
}

var bucketBufPool = sync.Pool{
	New: func() any {
		// 1MiB buffer chosen as starting point;
		// we could make this configurable and benchmark.
		return bufio.NewReaderSize(nil, 1<<20)
	},
}

type BucketBufReader struct {
	ctx         context.Context
	bkt         objstore.BucketReader
	name        string
	base        int
	length      int
	off         int
	r           *BucketReader
	resetReader func(off int) error
	buf         *bufio.Reader
	// Hold a reference to the pool for returning on Close - allows tests to use different pool.
	bufPool *sync.Pool
}

func resetReaderFunc(bufReader *BucketBufReader) func(off int) error {
	return func(off int) error {
		r := NewBucketReader(bufReader.ctx, bufReader.bkt, bufReader.name, bufReader.base, bufReader.length)
		_, err := r.Seek(int64(off), io.SeekStart)
		if err != nil {
			return err
		}
		bufReader.r = r
		return nil
	}
}

func NewBucketBufReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufReader {
	return newBucketBufReader(ctx, &bucketBufPool, bkt, name, base, length)
}

func newBucketBufReader(
	ctx context.Context, bufioPool *sync.Pool, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufReader {
	reader := NewBucketReader(ctx, bkt, name, base, length)
	bufioReader := bufioPool.Get().(*bufio.Reader)
	bufioReader.Reset(reader)

	bufReader := &BucketBufReader{
		ctx:     ctx,
		bkt:     bkt,
		name:    name,
		base:    base,
		length:  length,
		r:       reader,
		buf:     bufioReader,
		bufPool: bufioPool,
	}

	bufReader.resetReader = resetReaderFunc(bufReader)
	return bufReader
}

func (bbr *BucketBufReader) Reset() error {
	return bbr.ResetAt(0)
}

func (bbr *BucketBufReader) ResetAt(off int) error {
	if off > bbr.length {
		return ErrInvalidSize
	}

	if dist := off - bbr.off; dist > 0 && dist < bbr.Buffered() {
		// skip ahead by discarding the distance bytes
		return bbr.Skip(dist)
	}

	if err := bbr.resetReader(off); err != nil {
		return err
	}

	bbr.buf.Reset(bbr.r)
	bbr.off = off

	return nil
}

func (bbr *BucketBufReader) Skip(l int) error {
	if l > bbr.Len() {
		return ErrInvalidSize
	}

	n, err := bbr.buf.Discard(l)
	if n > 0 {
		bbr.off += n
	}

	return err
}

func (bbr *BucketBufReader) Peek(n int) ([]byte, error) {
	b, err := bbr.buf.Peek(n)
	// bufio.Reader still returns what it Read when it hits EOF and callers
	// expect to be able to peek past the end of a file.
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if len(b) > 0 {
		return b, nil
	}

	return nil, nil
}

func (bbr *BucketBufReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := bbr.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (bbr *BucketBufReader) ReadInto(b []byte) error {
	n, err := io.ReadFull(bbr.buf, b)
	if n > 0 {
		bbr.off += n
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(b), err)
	} else if err != nil {
		return err
	}

	return nil
}

func (bbr *BucketBufReader) Size() int {
	return bbr.buf.Size()
}

func (bbr *BucketBufReader) Len() int {
	return bbr.length - bbr.off
}

func (bbr *BucketBufReader) Offset() int {
	return bbr.off
}

func (bbr *BucketBufReader) Buffered() int {
	return bbr.buf.Buffered()
}

func (bbr *BucketBufReader) Close() error {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bbr.bufPool.Put(bbr.buf)
	// The BucketReader does not need closed -
	// it closes the reader generated from bkt.GetRange on each Read call.
	return nil
}
