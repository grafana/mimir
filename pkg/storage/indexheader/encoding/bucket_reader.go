// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	list "github.com/bahlo/generic-list-go"
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

func (r *BucketReader) Read(buf []byte) (n int, err error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if r.off >= r.length {
		return 0, io.EOF
	}
	toRead := len(buf)
	remaining := r.length - r.off
	if toRead > remaining {
		toRead = remaining
	}
	rc, err := r.bkt.GetRange(r.ctx, r.name, int64(r.base+r.off), int64(toRead))
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	n, err = io.ReadFull(rc, buf[:toRead])
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

type BucketReaderAsyncReadAhead struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int

	queuedLen atomic.Int64
	curr      *bucketReadPromise
	bufQueue  *list.List[*bucketReadPromise]
}

func NewBucketReaderAsyncReadAhead(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketReaderAsyncReadAhead {
	r := &BucketReaderAsyncReadAhead{
		ctx:      ctx,
		bkt:      bkt,
		name:     name,
		base:     base,
		length:   length,
		bufQueue: list.New[*bucketReadPromise](),
	}
	r.queueReadAhead()
	return r
}

func (r *BucketReaderAsyncReadAhead) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("invalid Seek whence: %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek to negative offset %d", offset)
	}
	r.off = int(offset)
	return offset, nil
}

func (r *BucketReaderAsyncReadAhead) Read(buf []byte) (n int, err error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if r.off >= r.length {
		return 0, io.EOF
	}
	toRead := len(buf)
	remaining := r.length - r.off
	if toRead > remaining {
		toRead = remaining
	}

	bytesRead := 0
	for bytesRead < toRead && err == nil {
		// Get current read promise if we do not have one.
		if r.curr == nil {
			elem := r.bufQueue.Front()
			if elem != nil {
				r.curr = elem.Value
				r.bufQueue.Remove(elem)
			}
		}

		// Now read from current read promise
		bytes, readErr := r.curr.ReadN(toRead)
		copy(buf, bytes)
		// Update trackers even if there was an error.
		r.off += len(bytes)
		bytesRead += len(bytes)
		err = readErr
	}
	return bytesRead, err
}

func (r *BucketReaderAsyncReadAhead) queueReadAhead() {
	inFlight := r.bufQueue.Len()
	if r.curr != nil {
		inFlight++
	}
	for inFlight < asyncReadAheadMaxInFlight {
		queued := r.queuedLen.Load()
		remaining := int64(r.length) - queued
		if remaining <= 0 {
			return
		}
		chunkLen := int64(asyncReadAheadChunkSize)
		if chunkLen > remaining {
			chunkLen = remaining
		}
		buf := make([]byte, chunkLen)
		promise := newBucketReadAheadPromise(
			r.ctx, r.bkt, r.name,
			r.base+int(queued), int(chunkLen),
			buf,
		)
		r.bufQueue.PushBack(promise)
		r.queuedLen.Add(chunkLen)
		inFlight++
	}
}

const (
	asyncReadAheadMaxInFlight = 4
	asyncReadAheadChunkSize   = 32 * 1024
)

type bucketReadPromise struct {
	ctx       context.Context
	bkt       objstore.BucketReader
	name      string
	base      int
	length    int
	wg        sync.WaitGroup
	resultBuf []byte
	resultErr error
	readOff   int
}

func newBucketReadAheadPromise(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int, buf []byte,
) *bucketReadPromise {

	promise := &bucketReadPromise{
		ctx:    ctx,
		bkt:    bkt,
		name:   name,
		base:   base,
		length: length,
		wg:     sync.WaitGroup{},
	}
	promise.wg.Go(func() {
		promise.fill(buf)
	})

	return promise
}

func (p *bucketReadPromise) ReadN(n int) ([]byte, error) {
	p.wg.Wait()

	if p.readOff >= len(p.resultBuf) {
		return nil, io.EOF
	}

	toRead := n
	remaining := len(p.resultBuf) - p.readOff
	if toRead > remaining {
		toRead = remaining
	}
	b := make([]byte, toRead)
	copy(b, p.resultBuf[p.readOff:])
	p.readOff += toRead
	return b, p.resultErr
}

func (p *bucketReadPromise) fill(buf []byte) {
	finalize := func(b []byte, e error) {
		p.resultBuf = b
		p.resultErr = e
	}

	toRead := cap(buf)
	if toRead > p.length {
		toRead = p.length
	}
	buf = buf[:toRead]
	clear(buf)

	if toRead == 0 {
		finalize(buf, nil)
		return
	}

	rc, err := p.bkt.GetRange(p.ctx, p.name, int64(p.base), int64(toRead))
	if err != nil {
		finalize(buf, err)
		return
	}
	defer rc.Close()

	n, err := io.ReadFull(rc, buf)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	buf = buf[:n]
	finalize(buf, err)
	return
}

const DefaultBucketBufPoolSize = 1024 * 1024 // 1 MiB

var (
	bucketBufPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, DefaultBucketBufPoolSize)
		},
	}
	bucketBufPoolOnce sync.Once
)

func InitBucketBufPool(bufferSizeBytes int) {
	bucketBufPoolOnce.Do(func() {
		bucketBufPool = sync.Pool{
			New: func() any {
				return bufio.NewReaderSize(nil, bufferSizeBytes)
			},
		}
	})
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
