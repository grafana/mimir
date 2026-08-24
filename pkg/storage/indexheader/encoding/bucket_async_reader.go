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

const (
	// asyncReadAheadChunkSize is the byte range that one background request fetches.
	asyncReadAheadChunkSize = 1 << 20 // 1 MiB

	// asyncReadAheadMaxInFlight is the number of chunks the pipeline keeps queued,
	// counting the chunk the consumer currently reads from.
	asyncReadAheadMaxInFlight = 4
)

// asyncReadAheadBufPool holds chunk buffers. It stores *[]byte rather than []byte
// so that Put does not allocate a slice header (staticcheck SA6002).
var asyncReadAheadBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, asyncReadAheadChunkSize)
		return &b
	},
}

// errAsyncReaderTornDown is the cancellation cause for the fetches of a pipeline
// generation that no consumer will read.
var errAsyncReaderTornDown = errors.New("async read-ahead reader torn down")

// BucketAsyncBufReader is a BufReader over object storage that keeps several
// range requests in flight. It queues up to maxInFlight chunks of chunkSize
// bytes, fetches each on its own goroutine, and serves Peek, Read, ReadInto and
// Skip out of the front chunk. When the front chunk is drained the reader moves
// to the next one, which is usually already in memory.
//
// There is no intermediate bufio.Reader. In the common case a read is a single
// copy out of a chunk buffer, and a peek is a slice into one.
type BucketAsyncBufReader struct {
	parent context.Context
	bkt    objstore.BucketReader
	name   string
	base   int
	length int
	off    int

	// bufPool produces the chunk buffers, and chunkSize is the range that each
	// promise requests. Both are fields rather than package globals so that tests
	// can build a reader with a small chunk size, the same way newBucketBufReader
	// takes a sync.Pool of bufio.Readers.
	bufPool     *sync.Pool
	chunkSize   int
	maxInFlight int

	// genCtx covers one pipeline generation. tearDown cancels it, so an abandoned
	// reader does not wait for requests that nobody will read. ResetAt must mint a
	// fresh one, because a cancelled context cannot be reused.
	genCtx    context.Context
	genCancel context.CancelCauseFunc

	queueOff int // next byte offset to queue, relative to base
	queue    promiseRing

	curr      *bucketReadPromise
	currReady bool // curr was waited on, so the length of its buffer is stable

	// prev holds the promise that retired just before curr. Peek hands out slices
	// into a chunk buffer, and Decbuf.UnsafeUvarintBytes keeps such a slice across
	// a Skip that can cross into the next chunk. Holding the retired promise one
	// step longer keeps that memory out of the pool while the slice is still live.
	prev *bucketReadPromise

	// holdOver buffers bytes that were pulled out of promises but not yet consumed.
	// Peek fills it when the requested range spans a chunk boundary. Read, ReadInto
	// and Skip drain it first. The fast path never touches it.
	holdOver    []byte
	holdOverOff int

	done   bool  // the pipeline reached the end of the range, or it failed
	err    error // sticky: every call after a failed fetch reports it
	closed bool
}

var _ BufReader = (*BucketAsyncBufReader)(nil)

// NewBucketAsyncBufReader returns a read-ahead reader over the length bytes that
// start at base in the named object.
func NewBucketAsyncBufReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketAsyncBufReader {
	return newBucketAsyncBufReader(
		ctx, &asyncReadAheadBufPool, asyncReadAheadChunkSize, asyncReadAheadMaxInFlight,
		bkt, name, base, length,
	)
}

func newBucketAsyncBufReader(
	ctx context.Context, bufPool *sync.Pool, chunkSize int, maxInFlight int,
	bkt objstore.BucketReader, name string, base int, length int,
) *BucketAsyncBufReader {
	r := &BucketAsyncBufReader{
		parent:      ctx,
		bkt:         bkt,
		name:        name,
		base:        base,
		length:      length,
		bufPool:     bufPool,
		chunkSize:   chunkSize,
		maxInFlight: maxInFlight,
		queue:       newPromiseRing(maxInFlight),
	}
	r.newGeneration()
	r.queueReadAhead()
	return r
}

// newGeneration derives the context that covers the current set of in-flight
// fetches. tearDown cancels it, so every reposition needs a new one.
func (r *BucketAsyncBufReader) newGeneration() {
	r.genCtx, r.genCancel = context.WithCancelCause(r.parent)
}

// queueReadAhead tops the pipeline back up to maxInFlight promises and starts a
// background fetch for each new one. It stops on the length boundary and shrinks
// the last chunk to the bytes that remain.
func (r *BucketAsyncBufReader) queueReadAhead() {
	if r.done {
		return
	}

	inFlight := r.queue.len()
	if r.curr != nil {
		inFlight++
	}
	for inFlight < r.maxInFlight && r.queueOff < r.length {
		chunkLen := min(r.chunkSize, r.length-r.queueOff)
		r.queue.push(newBucketReadPromise(
			r.genCtx, r.bkt, r.name, r.base+r.queueOff, chunkLen, r.bufPool,
		))
		r.queueOff += chunkLen
		inFlight++
	}
}

// nextPromise returns the promise that holds the next unread byte, or (nil, nil)
// when the reader is at the end of its range. It waits for the front promise to
// fill, retires the promises that are drained, and tops the pipeline back up.
func (r *BucketAsyncBufReader) nextPromise() (*bucketReadPromise, error) {
	for {
		if r.err != nil {
			return nil, r.err
		}

		if r.curr == nil {
			if r.done {
				return nil, nil
			}
			p := r.queue.pop()
			if p == nil {
				r.terminate()
				return nil, nil
			}
			r.curr = p
			r.currReady = false
			r.queueReadAhead()
		}

		r.curr.wg.Wait()
		r.currReady = true

		// Report a transport error immediately, even when the fetch delivered some
		// bytes: io.ReadFull can return both. If the error waits until the buffer
		// drains, a consumer that stops inside the partial range never sees it.
		if err := r.curr.err; err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			r.fail(err)
			return nil, err
		}

		if r.curr.remaining() > 0 {
			return r.curr, nil
		}

		// The promise is drained. A chunk that delivered fewer bytes than it
		// requested ends the stream. Chunk offsets come from requested lengths, so
		// a move to the next promise would join two ranges that are not adjacent,
		// advance the offset as though nothing was lost, and report no error.
		if r.curr.short() {
			r.terminate()
			return nil, nil
		}
		r.retire()
	}
}

// retire moves curr into prev and releases the promise that prev held. Holding
// one retired promise keeps a live Peek slice out of the buffer pool. Refer to
// the comment on the prev field.
func (r *BucketAsyncBufReader) retire() {
	if r.prev != nil {
		r.prev.release()
	}
	r.prev = r.curr
	r.curr = nil
	r.currReady = false
}

// terminate ends the pipeline. It queues no more chunks, and it cancels and
// releases every promise that is still waiting.
func (r *BucketAsyncBufReader) terminate() {
	r.done = true
	r.genCancel(errAsyncReaderTornDown)
	if r.curr != nil {
		r.retire()
	}
	for p := r.queue.pop(); p != nil; p = r.queue.pop() {
		p.release()
	}
}

// fail terminates the pipeline and records err for every later call.
func (r *BucketAsyncBufReader) fail(err error) {
	r.err = err
	r.terminate()
}

// Peek returns the next n bytes and does not advance the reader. The fast path
// returns a slice into the current chunk buffer. When the request spans a chunk
// boundary, the bytes go into holdOver instead, and the reads that follow drain
// holdOver first, so that the offset accounting stays correct.
//
// A peek past the end of the range is valid. Peek then returns the bytes that
// are available and a nil error.
func (r *BucketAsyncBufReader) Peek(n int) ([]byte, error) {
	if n <= 0 {
		return nil, nil
	}

	if avail := len(r.holdOver) - r.holdOverOff; avail >= n {
		return r.holdOver[r.holdOverOff : r.holdOverOff+n], nil
	} else if avail == 0 {
		// Fast path: holdOver is empty and the current chunk alone can satisfy the peek.
		p, err := r.nextPromise()
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, nil
		}
		if p.remaining() >= n {
			return p.buf[p.readOff : p.readOff+n], nil
		}
	}

	// Slow path: gather the bytes across chunk boundaries into holdOver. Compact
	// first, so that the bytes already consumed do not grow the buffer without bound.
	if r.holdOverOff > 0 {
		r.holdOver = append(r.holdOver[:0], r.holdOver[r.holdOverOff:]...)
		r.holdOverOff = 0
	}
	for len(r.holdOver) < n {
		p, err := r.nextPromise()
		if err != nil {
			return nil, err
		}
		if p == nil {
			if len(r.holdOver) == 0 {
				return nil, nil
			}
			// holdOverOff is 0 here: the compaction above cleared it, and nothing in
			// this loop advances it.
			return r.holdOver, nil
		}
		take := min(n-len(r.holdOver), p.remaining())
		r.holdOver = append(r.holdOver, p.buf[p.readOff:p.readOff+take]...)
		p.readOff += take
	}
	return r.holdOver[:n], nil
}

// Read returns the next n bytes as a new slice and advances the reader. Callers
// that already hold a destination buffer must prefer ReadInto.
func (r *BucketAsyncBufReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)
	if err := r.ReadInto(b); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadInto fills b with the next len(b) bytes and advances the reader. A read
// past the end of the range is not valid. ReadInto then consumes the bytes that
// remain and returns ErrInvalidSize.
func (r *BucketAsyncBufReader) ReadInto(b []byte) error {
	total := 0
	dst := b

	if avail := len(r.holdOver) - r.holdOverOff; avail > 0 {
		n := copy(dst, r.holdOver[r.holdOverOff:])
		r.holdOverOff += n
		r.off += n
		total += n
		dst = dst[n:]
		r.dropDrainedHoldOver()
	}

	for len(dst) > 0 {
		p, err := r.nextPromise()
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("%w reading %d bytes: got %d", ErrInvalidSize, len(b), total)
		}
		n := copy(dst, p.buf[p.readOff:])
		p.readOff += n
		r.off += n
		total += n
		dst = dst[n:]
	}
	return nil
}

// Skip advances the reader by l bytes and copies nothing. A skip to the end of
// the range is valid. A skip past the end returns ErrInvalidSize.
func (r *BucketAsyncBufReader) Skip(l int) error {
	if l > r.Len() {
		return ErrInvalidSize
	}
	return r.skip(l)
}

// skip advances the reader by n bytes and makes no bounds check of its own.
func (r *BucketAsyncBufReader) skip(n int) error {
	if n <= 0 {
		return nil
	}

	if avail := len(r.holdOver) - r.holdOverOff; avail > 0 {
		drop := min(n, avail)
		r.holdOverOff += drop
		r.off += drop
		n -= drop
		r.dropDrainedHoldOver()
	}

	for n > 0 {
		p, err := r.nextPromise()
		if err != nil {
			return err
		}
		if p == nil {
			return fmt.Errorf("%w skipping %d bytes", ErrInvalidSize, n)
		}
		drop := min(n, p.remaining())
		p.readOff += drop
		r.off += drop
		n -= drop
	}
	return nil
}

// dropDrainedHoldOver clears holdOver once every byte in it is consumed.
func (r *BucketAsyncBufReader) dropDrainedHoldOver() {
	if r.holdOverOff >= len(r.holdOver) {
		r.holdOver = r.holdOver[:0]
		r.holdOverOff = 0
	}
}

// Size returns the size of one read-ahead chunk. Decbuf uses Size to decide
// whether a peek of a given length can come from one contiguous slice, so Size
// reports the largest contiguous peek and not the total buffered bytes.
func (r *BucketAsyncBufReader) Size() int {
	return r.chunkSize
}

// Len returns the bytes that remain between the cursor and the end of the range.
func (r *BucketAsyncBufReader) Len() int {
	return r.length - r.off
}

// Offset returns the cursor position, relative to the base offset.
func (r *BucketAsyncBufReader) Offset() int {
	return r.off
}

// Buffered returns the bytes that the reader can serve without a wait on a fetch.
// A chunk that is still filling does not count, because its bytes are not in
// memory yet.
func (r *BucketAsyncBufReader) Buffered() int {
	n := len(r.holdOver) - r.holdOverOff
	if r.curr != nil && r.currReady {
		n += r.curr.remaining()
	}
	return n
}

// Reset moves the reader back to the base offset.
func (r *BucketAsyncBufReader) Reset() error {
	return r.ResetAt(0)
}

// ResetAt moves the reader to off, relative to the base offset. A forward move
// into bytes that are already in memory is a skip. Every other move tears the
// pipeline down and starts a new one at off.
func (r *BucketAsyncBufReader) ResetAt(off int) error {
	if off > r.length {
		return ErrInvalidSize
	}

	if dist := off - r.off; dist > 0 && dist < r.Buffered() {
		return r.skip(dist)
	}

	r.tearDown()
	r.newGeneration()
	r.off = off
	r.queueOff = off
	r.done = false
	r.err = nil
	r.queueReadAhead()
	return nil
}

// Close cancels every fetch that is still in flight and returns the chunk
// buffers to the pool. A second call does nothing.
func (r *BucketAsyncBufReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.tearDown()
	return nil
}

// tearDown cancels the current generation, then waits for and releases every
// promise. The cancel comes first. That is what keeps Close from a block on the
// tail latency of requests whose bytes nobody will read.
func (r *BucketAsyncBufReader) tearDown() {
	r.genCancel(errAsyncReaderTornDown)

	if r.prev != nil {
		r.prev.release()
		r.prev = nil
	}
	if r.curr != nil {
		r.curr.release()
		r.curr = nil
	}
	r.currReady = false

	for p := r.queue.pop(); p != nil; p = r.queue.pop() {
		p.release()
	}
	r.queue.reset()

	r.holdOver = r.holdOver[:0]
	r.holdOverOff = 0
}

// bucketReadPromise is one in-flight GetRange call and the buffer that receives
// it. The fill goroutine writes buf and err, and both are safe to read after wg.
type bucketReadPromise struct {
	base    int // absolute offset in the object of the requested range
	length  int // bytes requested; the one source of truth for the range
	readOff int // bytes already handed to the consumer

	wg  sync.WaitGroup
	buf []byte // the pooled array, re-sliced by fill to the bytes that arrived

	bufPtr  *[]byte
	bufPool *sync.Pool
	err     error
}

func newBucketReadPromise(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
	bufPool *sync.Pool,
) *bucketReadPromise {
	bufPtr := bufPool.Get().(*[]byte)
	if cap(*bufPtr) < length {
		// fill requests exactly length bytes, so the buffer must hold them whatever
		// size the pool gives out. Without this the chunk under-delivers and every
		// later chunk offset is wrong.
		bufPool.Put(bufPtr)
		b := make([]byte, length)
		bufPtr = &b
	}

	p := &bucketReadPromise{
		base:    base,
		length:  length,
		buf:     (*bufPtr)[:length],
		bufPtr:  bufPtr,
		bufPool: bufPool,
	}
	p.wg.Go(func() { p.fill(ctx, bkt, name) })
	return p
}

// fill runs on its own goroutine and makes the one GetRange call of this
// promise. It re-slices buf to the bytes that arrived and records the error.
//
// fill does not map io.ErrUnexpectedEOF to io.EOF. BucketReader.Read can do that
// safely because it advances its offset by the bytes delivered. This reader
// computes chunk offsets in advance, so it needs the distinction to tell a
// truncated chunk from a clean end of range.
func (p *bucketReadPromise) fill(ctx context.Context, bkt objstore.BucketReader, name string) {
	rc, err := bkt.GetRange(ctx, name, int64(p.base), int64(p.length))
	if err != nil {
		p.buf = p.buf[:0]
		p.err = err
		return
	}
	defer func() { _ = rc.Close() }()

	n, err := io.ReadFull(rc, p.buf)
	p.buf = p.buf[:n]
	p.err = err
}

// remaining returns the bytes of this promise that the consumer has not taken.
// It is only valid after wg.
func (p *bucketReadPromise) remaining() int {
	return len(p.buf) - p.readOff
}

// short reports whether the fetch delivered fewer bytes than it requested.
// It is only valid after wg.
func (p *bucketReadPromise) short() bool {
	return len(p.buf) < p.length
}

// release waits for the fill goroutine, then returns the buffer to the pool.
// A second call does nothing.
func (p *bucketReadPromise) release() {
	p.wg.Wait()
	if p.bufPtr == nil {
		return
	}
	// Restore the pooled slice to full capacity, so that the next Get sees a
	// buffer it can use.
	*p.bufPtr = (*p.bufPtr)[:cap(*p.bufPtr)]
	p.bufPool.Put(p.bufPtr)
	p.bufPtr = nil
	p.buf = nil
}

// promiseRing is a FIFO of read promises with a fixed capacity. The capacity is
// set at construction to maxInFlight and never grows, because queueReadAhead
// never pushes past it.
type promiseRing struct {
	buf   []*bucketReadPromise
	head  int
	count int
}

func newPromiseRing(capacity int) promiseRing {
	return promiseRing{buf: make([]*bucketReadPromise, capacity)}
}

func (r *promiseRing) len() int {
	return r.count
}

// push adds a promise at the back. A push on a full ring is a programming error
// in queueReadAhead, so it panics instead of a silent drop.
func (r *promiseRing) push(p *bucketReadPromise) {
	if r.count == len(r.buf) {
		panic("promiseRing: push on a full ring")
	}
	r.buf[(r.head+r.count)%len(r.buf)] = p
	r.count++
}

// pop removes and returns the promise at the front, or nil when the ring is empty.
func (r *promiseRing) pop() *bucketReadPromise {
	if r.count == 0 {
		return nil
	}
	p := r.buf[r.head]
	r.buf[r.head] = nil
	r.head = (r.head + 1) % len(r.buf)
	r.count--
	return p
}

// at returns the promise i places behind the front, or nil when i is out of range.
func (r *promiseRing) at(i int) *bucketReadPromise {
	if i < 0 || i >= r.count {
		return nil
	}
	return r.buf[(r.head+i)%len(r.buf)]
}

// reset empties the ring. The caller must release the promises first.
func (r *promiseRing) reset() {
	clear(r.buf)
	r.head = 0
	r.count = 0
}
