// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/thanos-io/objstore"
)

var errBufferFull = errors.New("encoding: buffer full")

type BucketReader struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int // fetch position: how many bytes have been read from the object store

	buf     []byte
	bufR    int   // read position in buf
	bufW    int   // write position in buf
	lastErr error // sticky error from last fill
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

func (r *BucketReader) readErr() error {
	err := r.lastErr
	r.lastErr = nil
	return err
}

func (r *BucketReader) fill() {
	if r.bufR > 0 {
		copy(r.buf, r.buf[r.bufR:r.bufW])
		r.bufW -= r.bufR
		r.bufR = 0
	}

	if r.bufW >= len(r.buf) {
		return
	}

	if r.off >= r.length {
		r.lastErr = io.EOF
		return
	}

	toRead := len(r.buf) - r.bufW
	remaining := r.length - r.off
	if toRead > remaining {
		toRead = remaining
	}

	rc, err := r.bkt.GetRange(r.ctx, r.name, int64(r.base+r.off), int64(toRead))
	if err != nil {
		r.lastErr = err
		return
	}
	defer rc.Close()

	n, err := io.ReadFull(rc, r.buf[r.bufW:r.bufW+toRead])
	r.bufW += n
	r.off += n
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			r.lastErr = io.EOF
		} else {
			r.lastErr = err
		}
	}
}

func (r *BucketReader) readDirect(p []byte) (int, error) {
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
	n, err := io.ReadFull(rc, p[:toRead])
	r.off += n
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (r *BucketReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	if r.buf == nil {
		return r.readDirect(p)
	}

	if r.bufR == r.bufW {
		if r.lastErr != nil {
			return 0, r.readErr()
		}
		if len(p) >= len(r.buf) {
			return r.readDirect(p)
		}
		r.fill()
		if r.bufR == r.bufW {
			return 0, r.readErr()
		}
	}
	n = copy(p, r.buf[r.bufR:r.bufW])
	r.bufR += n
	return n, nil
}

func (r *BucketReader) Peek(n int) ([]byte, error) {
	for r.bufW-r.bufR < n && r.bufW-r.bufR < len(r.buf) && r.lastErr == nil {
		r.fill()
	}

	var err error
	if avail := r.bufW - r.bufR; n > avail {
		n = avail
		if r.lastErr != nil {
			err = r.readErr()
		} else {
			err = errBufferFull
		}
	}
	return r.buf[r.bufR : r.bufR+n], err
}

func (r *BucketReader) Discard(n int) (int, error) {
	if n == 0 {
		return 0, nil
	}
	remain := n
	for {
		skip := r.Buffered()
		if skip == 0 {
			r.fill()
			skip = r.Buffered()
		}
		if skip > remain {
			skip = remain
		}
		r.bufR += skip
		remain -= skip
		if remain == 0 {
			return n, nil
		}
		if r.lastErr != nil {
			return n - remain, r.readErr()
		}
	}
}

func (r *BucketReader) Size() int {
	return len(r.buf)
}

func (r *BucketReader) Buffered() int {
	return r.bufW - r.bufR
}

func (r *BucketReader) Reset(off int) {
	r.off = off
	r.bufR = 0
	r.bufW = 0
	r.lastErr = nil
}

func (r *BucketReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("invalid Seek whence: %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek to negative offset %d", offset)
	}
	r.off = int(offset)
	r.bufR = 0
	r.bufW = 0
	r.lastErr = nil
	return offset, nil
}

var bucketBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 1<<20)
	},
}

type BucketBufReader struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int
	r      *BucketReader
	// Hold a reference to the pool for returning on Close - allows tests to use different pool.
	bufPool *sync.Pool
}

func NewBucketBufReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufReader {
	return newBucketBufReader(ctx, &bucketBufPool, bkt, name, base, length)
}

func newBucketBufReader(
	ctx context.Context, bufPool *sync.Pool, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufReader {
	reader := NewBucketReader(ctx, bkt, name, base, length)
	reader.buf = bufPool.Get().([]byte)

	return &BucketBufReader{
		ctx:     ctx,
		bkt:     bkt,
		name:    name,
		base:    base,
		length:  length,
		r:       reader,
		bufPool: bufPool,
	}
}

func (bbr *BucketBufReader) Reset() error {
	return bbr.ResetAt(0)
}

func (bbr *BucketBufReader) ResetAt(off int) error {
	if off > bbr.length {
		return ErrInvalidSize
	}

	if dist := off - bbr.off; dist > 0 && dist < bbr.Buffered() {
		return bbr.Skip(dist)
	}

	bbr.r.Reset(off)
	bbr.off = off

	return nil
}

func (bbr *BucketBufReader) Skip(l int) error {
	if l > bbr.Len() {
		return ErrInvalidSize
	}

	n, err := bbr.r.Discard(l)
	if n > 0 {
		bbr.off += n
	}

	return err
}

func (bbr *BucketBufReader) Peek(n int) ([]byte, error) {
	b, err := bbr.r.Peek(n)
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
	n, err := io.ReadFull(bbr.r, b)
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
	return bbr.r.Size()
}

func (bbr *BucketBufReader) Len() int {
	return bbr.length - bbr.off
}

func (bbr *BucketBufReader) Offset() int {
	return bbr.off
}

func (bbr *BucketBufReader) Buffered() int {
	return bbr.r.Buffered()
}

func (bbr *BucketBufReader) Close() error {
	bbr.bufPool.Put(bbr.r.buf)
	bbr.r.buf = nil
	return nil
}
