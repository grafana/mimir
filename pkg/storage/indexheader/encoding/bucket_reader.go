// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	list "github.com/bahlo/generic-list-go"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
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

// BucketReaderAsyncReadAhead reads a byte range from object storage using
// pipelined readahead. It maintains a queue of in-flight bucketReadPromises
// filled concurrently and serves Peek/ReadInto/Skip/Read directly out of the
// current promise's buffer, advancing to the next promise when the current
// one is exhausted. There is no intermediate bufio.Reader: the user buffer
// is filled with a single copy from the promise buffer in the common case.
type BucketReaderAsyncReadAhead struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int

	// bufPool produces fixed-size []byte chunks of length chunkSize. Held on
	// the reader (rather than referenced via a package global) so tests and
	// benchmarks can swap the pool/chunk-size pair per-construction, the same
	// way BucketBufReader takes a *sync.Pool of bufio.Readers.
	bufPool   *sync.Pool
	chunkSize int

	queuedLen atomic.Int64
	curr      *bucketReadPromise
	bufQueue  *list.List[*bucketReadPromise]

	// holdOver buffers bytes that have been pulled out of promises (advancing
	// their readOff) but are not yet consumed by the user. It is populated by
	// Peek when the requested range spans a promise boundary; subsequent
	// Read/ReadInto/Skip drain holdOver before pulling from curr. The fast
	// path - peeks/reads that fit inside the current promise - never touches
	// holdOver and pays no copy beyond the user's destination buffer.
	holdOver    []byte
	holdOverOff int
}

func NewBucketReaderAsyncReadAhead(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketReaderAsyncReadAhead {
	return newBucketReaderAsyncReadAhead(ctx, &asyncReadAheadBufPool, asyncReadAheadChunkSize, bkt, name, base, length)
}

func newBucketReaderAsyncReadAhead(
	ctx context.Context, bufPool *sync.Pool, chunkSize int,
	bkt objstore.BucketReader, name string, base int, length int,
) *BucketReaderAsyncReadAhead {
	r := &BucketReaderAsyncReadAhead{
		ctx:       ctx,
		bkt:       bkt,
		name:      name,
		base:      base,
		length:    length,
		bufPool:   bufPool,
		chunkSize: chunkSize,
		bufQueue:  list.New[*bucketReadPromise](),
	}
	r.queueReadAhead()
	return r
}

// fastCurr returns curr with at least one byte available. It pops the next
// promise from the queue when the current one is exhausted, propagates any
// non-EOF error from a failed fill, and tops up the pipeline. Returns
// (nil, nil) when both curr and the queue are exhausted.
func (r *BucketReaderAsyncReadAhead) fastCurr() (*bucketReadPromise, error) {
	for {
		if r.curr == nil {
			elem := r.bufQueue.Front()
			if elem == nil {
				return nil, nil
			}
			r.curr = elem.Value
			r.bufQueue.Remove(elem)
			r.queueReadAhead()
		}
		r.curr.wg.Wait()
		if r.curr.readOff < len(r.curr.buf) {
			return r.curr, nil
		}
		err := r.curr.resultErr
		_ = r.curr.Close()
		r.curr = nil
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
}

// Peek returns the next n bytes without advancing the user offset. The fast
// path returns a slice directly into the current promise's buffer (zero copy).
// When the request spans a promise boundary, bytes are drained from successive
// promises into holdOver and returned from there; subsequent reads consume
// holdOver first so the user-visible offset accounting stays consistent.
func (r *BucketReaderAsyncReadAhead) Peek(n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}
	if avail := len(r.holdOver) - r.holdOverOff; avail >= n {
		return r.holdOver[r.holdOverOff : r.holdOverOff+n], nil
	} else if avail == 0 {
		// Fast path: no holdOver and curr alone can satisfy the peek.
		p, err := r.fastCurr()
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, nil
		}
		if len(p.buf)-p.readOff >= n {
			return p.buf[p.readOff : p.readOff+n], nil
		}
	}
	// Slow path: collect bytes across promise boundaries into holdOver.
	if r.holdOverOff > 0 {
		r.holdOver = append(r.holdOver[:0], r.holdOver[r.holdOverOff:]...)
		r.holdOverOff = 0
	}
	for len(r.holdOver) < n {
		p, err := r.fastCurr()
		if err != nil {
			return nil, err
		}
		if p == nil {
			if len(r.holdOver) == 0 {
				return nil, nil
			}
			return r.holdOver, nil
		}
		need := n - len(r.holdOver)
		avail := len(p.buf) - p.readOff
		take := need
		if take > avail {
			take = avail
		}
		r.holdOver = append(r.holdOver, p.buf[p.readOff:p.readOff+take]...)
		p.readOff += take
	}
	return r.holdOver[:n], nil
}

// ReadInto fills buf with bytes from the reader, advancing the user offset.
// In the common case the entire buf is filled with one copy from the current
// promise's buffer; ReadInto loops only when crossing promise boundaries or
// when consuming bytes that were previously stashed in holdOver by Peek.
func (r *BucketReaderAsyncReadAhead) ReadInto(buf []byte) (int, error) {
	total := 0
	if avail := len(r.holdOver) - r.holdOverOff; avail > 0 {
		nc := copy(buf, r.holdOver[r.holdOverOff:])
		r.holdOverOff += nc
		r.off += nc
		total += nc
		if r.holdOverOff >= len(r.holdOver) {
			r.holdOver = r.holdOver[:0]
			r.holdOverOff = 0
		}
		buf = buf[nc:]
	}
	for len(buf) > 0 {
		p, err := r.fastCurr()
		if err != nil {
			return total, err
		}
		if p == nil {
			return total, io.EOF
		}
		nc := copy(buf, p.buf[p.readOff:])
		p.readOff += nc
		r.off += nc
		total += nc
		buf = buf[nc:]
	}
	return total, nil
}

// Read returns the next n bytes as a freshly allocated slice. Callers that can
// take a destination buffer should prefer ReadInto; this method exists to
// match the existing Decbuf API and to handle requests larger than a peek.
func (r *BucketReaderAsyncReadAhead) Read(n int) ([]byte, error) {
	b := make([]byte, n)
	got, err := r.ReadInto(b)
	if err != nil {
		return nil, err
	}
	return b[:got], nil
}

// Skip advances the user offset by n bytes without copying.
func (r *BucketReaderAsyncReadAhead) Skip(n int) error {
	if n <= 0 {
		return nil
	}
	if avail := len(r.holdOver) - r.holdOverOff; avail > 0 {
		drop := n
		if drop > avail {
			drop = avail
		}
		r.holdOverOff += drop
		r.off += drop
		n -= drop
		if r.holdOverOff >= len(r.holdOver) {
			r.holdOver = r.holdOver[:0]
			r.holdOverOff = 0
		}
	}
	for n > 0 {
		p, err := r.fastCurr()
		if err != nil {
			return err
		}
		if p == nil {
			return io.EOF
		}
		avail := len(p.buf) - p.readOff
		if avail >= n {
			p.readOff += n
			r.off += n
			return nil
		}
		p.readOff = len(p.buf)
		r.off += avail
		n -= avail
	}
	return nil
}

// Buffered reports an upper bound on bytes that can be served without issuing
// a new GetRange. It includes holdOver, the current promise's remaining bytes,
// and the queued (possibly still-filling) promises by their requested length.
func (r *BucketReaderAsyncReadAhead) Buffered() int {
	n := len(r.holdOver) - r.holdOverOff
	if r.curr != nil {
		// Use length rather than len(buf) to avoid racing with an in-flight fill.
		n += r.curr.length - r.curr.readOff
	}
	for elem := r.bufQueue.Front(); elem != nil; elem = elem.Next() {
		n += elem.Value.length
	}
	return n
}

// Size returns the size of an individual readahead chunk. Decbuf uses Size to
// decide whether a Peek of length l can be satisfied by a contiguous slice,
// so reporting the chunk size keeps it on the zero-copy fast path.
func (r *BucketReaderAsyncReadAhead) Size() int {
	return r.chunkSize
}

func (r *BucketReaderAsyncReadAhead) Len() int {
	return r.length - r.off
}

func (r *BucketReaderAsyncReadAhead) Offset() int {
	return r.off
}

// Reset rewinds the reader to offset 0, tearing down the readahead pipeline.
func (r *BucketReaderAsyncReadAhead) Reset() error {
	return r.ResetAt(0)
}

// ResetAt repositions the reader to off. If the new position lies within the
// already-buffered window we Skip ahead to keep the pipeline warm; otherwise
// the queue is torn down and a fresh pipeline is queued from the new offset.
func (r *BucketReaderAsyncReadAhead) ResetAt(off int) error {
	if off > r.length {
		return ErrInvalidSize
	}
	if dist := off - r.off; dist > 0 && dist < r.Buffered() {
		return r.Skip(dist)
	}
	r.tearDown()
	r.off = off
	r.queuedLen.Store(int64(off))
	r.queueReadAhead()
	return nil
}

func (r *BucketReaderAsyncReadAhead) Close() error {
	r.tearDown()
	return nil
}

func (r *BucketReaderAsyncReadAhead) tearDown() {
	if r.curr != nil {
		_ = r.curr.Close()
		r.curr = nil
	}
	for elem := r.bufQueue.Front(); elem != nil; elem = elem.Next() {
		_ = elem.Value.Close()
	}
	r.bufQueue.Init()
	r.holdOver = r.holdOver[:0]
	r.holdOverOff = 0
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
		chunkLen := int64(r.chunkSize)
		if chunkLen > remaining {
			chunkLen = remaining
		}
		bufPtr := r.bufPool.Get().(*[]byte)
		promise := newBucketReadAheadPromise(
			r.ctx, r.bkt, r.name,
			r.base+int(queued), int(chunkLen),
			bufPtr, r.bufPool,
		)
		r.bufQueue.PushBack(promise)
		r.queuedLen.Add(chunkLen)
		inFlight++
	}
}

const (
	asyncReadAheadMaxInFlight = 16

	asyncReadAheadChunkSize = DefaultBucketBufPoolSize / asyncReadAheadMaxInFlight
)

// asyncReadAheadBufPool stores *[]byte rather than []byte so Put doesn't have
// to box the 24-byte slice header (staticcheck SA6002): the pointer fits in
// an interface word with no per-call allocation. The pointed-to slice is
// re-sliced inside fill() for partial reads, then restored to full cap on
// return so the next Get() sees a usable buffer.
var asyncReadAheadBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, asyncReadAheadChunkSize)
		return &b
	},
}

type bucketReadPromise struct {
	ctx    context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	wg     sync.WaitGroup
	// buf is the working view into the pooled backing array (re-sliced by
	// fill() to cover the bytes actually read). bufPtr holds the same
	// underlying array for return to the pool; storing a pointer in the pool
	// avoids the slice-header allocation on Put (staticcheck SA6002).
	buf       []byte
	bufPtr    *[]byte
	bufPool   *sync.Pool
	resultErr error
	readOff   int
}

func newBucketReadAheadPromise(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
	bufPtr *[]byte, bufPool *sync.Pool,
) *bucketReadPromise {
	promise := &bucketReadPromise{
		ctx:     ctx,
		bkt:     bkt,
		name:    name,
		base:    base,
		length:  length,
		wg:      sync.WaitGroup{},
		buf:     *bufPtr,
		bufPtr:  bufPtr,
		bufPool: bufPool,
	}
	promise.wg.Go(func() {
		promise.fill()
	})

	return promise
}

func (p *bucketReadPromise) Close() error {
	p.wg.Wait()
	if p.bufPtr == nil {
		return nil
	}
	// Restore the pooled slice to full capacity before returning it so the
	// next Get sees a usable buffer.
	*p.bufPtr = (*p.bufPtr)[:cap(*p.bufPtr)]
	p.bufPool.Put(p.bufPtr)
	p.bufPtr = nil
	p.buf = nil
	return nil
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

// BucketBufAsyncReader is a thin Decbuf-shaped wrapper around
// BucketReaderAsyncReadAhead. The async reader serves Peek/ReadInto/Skip/Read
// directly from its readahead buffers, so the wrapper exists only to expose
// length-checked Read variants that match the BucketBufReader API.
type BucketBufAsyncReader struct {
	r *BucketReaderAsyncReadAhead
}

func NewBucketBufAsyncReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufAsyncReader {
	return newBucketBufAsyncReader(ctx, &asyncReadAheadBufPool, asyncReadAheadChunkSize, bkt, name, base, length)
}

func newBucketBufAsyncReader(
	ctx context.Context, bufPool *sync.Pool, chunkSize int,
	bkt objstore.BucketReader, name string, base int, length int,
) *BucketBufAsyncReader {
	return &BucketBufAsyncReader{
		r: newBucketReaderAsyncReadAhead(ctx, bufPool, chunkSize, bkt, name, base, length),
	}
}

func (bbr *BucketBufAsyncReader) Reset() error          { return bbr.r.Reset() }
func (bbr *BucketBufAsyncReader) ResetAt(off int) error { return bbr.r.ResetAt(off) }
func (bbr *BucketBufAsyncReader) Size() int             { return bbr.r.Size() }
func (bbr *BucketBufAsyncReader) Len() int              { return bbr.r.Len() }
func (bbr *BucketBufAsyncReader) Offset() int           { return bbr.r.Offset() }
func (bbr *BucketBufAsyncReader) Buffered() int         { return bbr.r.Buffered() }

func (bbr *BucketBufAsyncReader) Skip(l int) error {
	if l > bbr.r.Len() {
		return ErrInvalidSize
	}
	return bbr.r.Skip(l)
}

func (bbr *BucketBufAsyncReader) Peek(n int) ([]byte, error) {
	return bbr.r.Peek(n)
}

func (bbr *BucketBufAsyncReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)
	if err := bbr.ReadInto(b); err != nil {
		return nil, err
	}
	return b, nil
}

func (bbr *BucketBufAsyncReader) ReadInto(b []byte) error {
	n, err := bbr.r.ReadInto(b)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n < len(b) {
		return fmt.Errorf("%w reading %d bytes: got %d", ErrInvalidSize, len(b), n)
	}
	return nil
}

func (bbr *BucketBufAsyncReader) Close() error {
	if bbr.r != nil {
		_ = bbr.r.Close()
		bbr.r = nil
	}
	return nil
}
