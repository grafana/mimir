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
	defer func() {
		if err != nil && r.curr != nil {
			_ = r.curr.Close()
			r.curr = nil
		}
	}()
	for bytesRead < toRead {
		// Get current read promise if we do not have one. If the queue is empty we
		// have nothing more to serve and the caller has hit the end of the range.
		if r.curr == nil {
			elem := r.bufQueue.Front()
			if elem == nil {
				err = io.EOF
				return bytesRead, err
			}
			r.curr = elem.Value
			r.bufQueue.Remove(elem)
		}

		chunk, readErr := r.curr.ReadN(toRead - bytesRead)
		n := copy(buf[bytesRead:], chunk)
		r.off += n
		bytesRead += n

		// Empty chunk means the promise is exhausted (or failed). Discard it and
		// either propagate the error or top up the pipeline and continue.
		if len(chunk) == 0 {
			_ = r.curr.Close()
			r.curr = nil
			if readErr != nil {
				err = readErr
				return bytesRead, err
			}
			r.queueReadAhead()
		} else if readErr != nil {
			// Non-EOF error surfaced alongside data; surface it now rather than
			// continuing to drain a buffer that came from a failed fill.
			err = readErr
			return bytesRead, err
		}
	}
	return bytesRead, err
}

func (r *BucketReaderAsyncReadAhead) Close() error {
	if r.curr != nil {
		_ = r.curr.Close()
		r.curr = nil
	}
	for elem := r.bufQueue.Front(); elem != nil; elem = elem.Next() {
		_ = elem.Value.Close()
	}
	r.bufQueue.Init()
	return nil
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
		buf := asyncReadAheadBufPool.Get().([]byte)
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
	asyncReadAheadMaxInFlight = 8

	asyncReadAheadChunkSize = 128 * 1024
)

var asyncReadAheadBufPool = sync.Pool{
	New: func() any {
		return make([]byte, asyncReadAheadChunkSize)
	},
}

type bucketReadPromise struct {
	ctx       context.Context
	bkt       objstore.BucketReader
	name      string
	base      int
	length    int
	wg        sync.WaitGroup
	buf       []byte
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
		buf:    buf,
	}
	promise.wg.Go(func() {
		promise.fill()
	})

	return promise
}

func (p *bucketReadPromise) Close() error {
	p.wg.Wait()
	if p.buf == nil {
		return nil
	}
	asyncReadAheadBufPool.Put(p.buf[:cap(p.buf)])
	p.buf = nil
	return nil
}

// ReadN returns up to n bytes from the promise's buffer. When the buffer is exhausted
// it returns (nil, resultErr): a non-nil err signals a real failure from fill(); a nil
// err signals natural exhaustion and the caller should advance to the next promise.
func (p *bucketReadPromise) ReadN(n int) ([]byte, error) {
	p.wg.Wait()

	if p.readOff >= len(p.buf) {
		return nil, p.resultErr
	}

	toRead := n
	remaining := len(p.buf) - p.readOff
	if toRead > remaining {
		toRead = remaining
	}
	b := make([]byte, toRead)
	copy(b, p.buf[p.readOff:])
	p.readOff += toRead
	// Surface non-EOF errors immediately even if there are still buffered bytes:
	// fill() may have produced a partial result before failing, and any caller
	// blindly draining the rest of the buffer would mask a real failure.
	if p.resultErr != nil && !errors.Is(p.resultErr, io.EOF) {
		return b, p.resultErr
	}
	return b, nil
}

func (p *bucketReadPromise) fill() {
	toRead := cap(p.buf)
	if toRead > p.length {
		toRead = p.length
	}
	p.buf = p.buf[:toRead]
	clear(p.buf)

	if toRead == 0 {
		return
	}

	rc, err := p.bkt.GetRange(p.ctx, p.name, int64(p.base), int64(toRead))
	if err != nil {
		p.buf = p.buf[:0]
		p.resultErr = err
		return
	}
	defer rc.Close()

	n, err := io.ReadFull(rc, p.buf)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	p.buf = p.buf[:n]
	p.resultErr = err
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

type BucketBufAsyncReader struct {
	ctx         context.Context
	bkt         objstore.BucketReader
	name        string
	base        int
	length      int
	off         int
	r           *BucketReaderAsyncReadAhead
	resetReader func(off int) error
	buf         *bufio.Reader
	// Hold a reference to the pool for returning on Close - allows tests to use different pool.
	bufPool *sync.Pool
}

func resetAsyncReaderFunc(bufReader *BucketBufAsyncReader) func(off int) error {
	return func(off int) error {
		if bufReader.r != nil {
			_ = bufReader.r.Close()
		}
		// Construct the new reader at base+off so its readahead pipeline starts
		// at the desired position. BucketReaderAsyncReadAhead queues its first
		// promise at construction time, so a post-construction Seek would be too late.
		bufReader.r = NewBucketReaderAsyncReadAhead(
			bufReader.ctx, bufReader.bkt, bufReader.name,
			bufReader.base+off, bufReader.length-off,
		)
		return nil
	}
}

func NewBucketBufAsyncReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufAsyncReader {
	return newBucketBufAsyncReader(ctx, &bucketBufPool, bkt, name, base, length)
}

func newBucketBufAsyncReader(
	ctx context.Context, bufioPool *sync.Pool, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufAsyncReader {
	reader := NewBucketReaderAsyncReadAhead(ctx, bkt, name, base, length)
	bufioReader := bufioPool.Get().(*bufio.Reader)
	bufioReader.Reset(reader)

	bufReader := &BucketBufAsyncReader{
		ctx:     ctx,
		bkt:     bkt,
		name:    name,
		base:    base,
		length:  length,
		r:       reader,
		buf:     bufioReader,
		bufPool: bufioPool,
	}

	bufReader.resetReader = resetAsyncReaderFunc(bufReader)
	return bufReader
}

func (bbr *BucketBufAsyncReader) Reset() error {
	return bbr.ResetAt(0)
}

func (bbr *BucketBufAsyncReader) ResetAt(off int) error {
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

func (bbr *BucketBufAsyncReader) Skip(l int) error {
	if l > bbr.Len() {
		return ErrInvalidSize
	}

	n, err := bbr.buf.Discard(l)
	if n > 0 {
		bbr.off += n
	}

	return err
}

func (bbr *BucketBufAsyncReader) Peek(n int) ([]byte, error) {
	b, err := bbr.buf.Peek(n)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	if len(b) > 0 {
		return b, nil
	}

	return nil, nil
}

func (bbr *BucketBufAsyncReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := bbr.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (bbr *BucketBufAsyncReader) ReadInto(b []byte) error {
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

func (bbr *BucketBufAsyncReader) Size() int {
	return bbr.buf.Size()
}

func (bbr *BucketBufAsyncReader) Len() int {
	return bbr.length - bbr.off
}

func (bbr *BucketBufAsyncReader) Offset() int {
	return bbr.off
}

func (bbr *BucketBufAsyncReader) Buffered() int {
	return bbr.buf.Buffered()
}

func (bbr *BucketBufAsyncReader) Close() error {
	bbr.bufPool.Put(bbr.buf)
	if bbr.r != nil {
		_ = bbr.r.Close()
		bbr.r = nil
	}
	return nil
}
