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
	off    int // fetch position: bytes already requested from object storage

	buf       []byte
	chunkSize int

	mu             sync.Mutex
	dataAvailable  *sync.Cond
	spaceAvailable *sync.Cond
	bufR           int    // read position in buf
	bufW           int    // write position in buf
	lastErr        error  // sticky error visible to consumer once buffer drains
	gen            uint64 // bumped on Reset/Seek to invalidate in-flight fetches
	pendingFetch   bool   // goroutine has released mu mid-fetch
	waiters        int    // consumers currently blocked on dataAvailable
	closed         bool

	workerCtx    context.Context
	workerCancel context.CancelFunc
	fetchCtx     context.Context
	fetchCancel  context.CancelFunc
	workerWG     sync.WaitGroup
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

func (r *BucketReader) startPrefetch(chunkSize int) {
	r.chunkSize = chunkSize
	r.dataAvailable = sync.NewCond(&r.mu)
	r.spaceAvailable = sync.NewCond(&r.mu)
	r.workerCtx, r.workerCancel = context.WithCancel(r.ctx)
	r.fetchCtx, r.fetchCancel = context.WithCancel(r.workerCtx)
	r.workerWG.Add(1)
	go r.prefetchLoop()
}

func (r *BucketReader) stopPrefetch() {
	r.mu.Lock()
	r.closed = true
	r.workerCancel()
	r.dataAvailable.Broadcast()
	r.spaceAvailable.Broadcast()
	r.mu.Unlock()
	r.workerWG.Wait()
}

func (r *BucketReader) prefetchLoop() {
	defer r.workerWG.Done()

	for {
		r.mu.Lock()
		for !r.shouldFetch() {
			if r.closed {
				r.mu.Unlock()
				return
			}
			r.spaceAvailable.Wait()
		}

		toFetch := r.chunkSize
		if free := len(r.buf) - r.bufW; toFetch > free {
			toFetch = free
		}
		if remaining := r.length - r.off; toFetch > remaining {
			toFetch = remaining
		}
		sourceOff := int64(r.base + r.off)
		bufStart := r.bufW
		ourGen := r.gen
		fetchCtx := r.fetchCtx
		r.pendingFetch = true
		r.mu.Unlock()

		var n int
		rc, err := r.bkt.GetRange(fetchCtx, r.name, sourceOff, int64(toFetch))
		if err == nil {
			n, err = io.ReadFull(rc, r.buf[bufStart:bufStart+toFetch])
			_ = rc.Close()
			if errors.Is(err, io.ErrUnexpectedEOF) {
				err = io.EOF
			}
		}

		r.mu.Lock()
		r.pendingFetch = false
		r.spaceAvailable.Signal()

		if r.closed {
			r.mu.Unlock()
			return
		}
		if r.gen != ourGen || errors.Is(err, context.Canceled) {
			r.mu.Unlock()
			continue
		}
		// Commit any bytes that were fetched before handling the error.
		if n > 0 {
			r.bufW = bufStart + n
			r.off += n
		}
		if errors.Is(err, io.EOF) {
			r.lastErr = io.EOF
		} else if err != nil {
			r.lastErr = err
		} else if r.off >= r.length {
			r.lastErr = io.EOF
		}
		r.dataAvailable.Broadcast()
		r.mu.Unlock()
	}
}

// shouldFetch must be called with r.mu held.
func (r *BucketReader) shouldFetch() bool {
	if r.lastErr != nil {
		return false
	}
	if r.off >= r.length {
		return false
	}
	free := len(r.buf) - r.bufW
	if free <= 0 {
		return false
	}
	if free >= r.chunkSize {
		return true
	}
	if remaining := r.length - r.off; remaining <= free {
		return true
	}
	// Partial fetch only when a consumer is actively blocked, to avoid
	// fragmenting the prefetch into many small GetRange calls in steady state.
	return r.waiters > 0
}

// maybeCompact must be called with r.mu held. It is a no-op while a fetch is
// in flight so the goroutine's reserved write range buf[bufW:] stays valid.
func (r *BucketReader) maybeCompact() {
	if r.pendingFetch {
		return
	}
	if r.bufR == 0 {
		return
	}
	if len(r.buf)-r.bufW >= r.chunkSize {
		return
	}
	copy(r.buf, r.buf[r.bufR:r.bufW])
	r.bufW -= r.bufR
	r.bufR = 0
	r.spaceAvailable.Signal()
}

func (r *BucketReader) readErr() error {
	err := r.lastErr
	r.lastErr = nil
	return err
}

func (r *BucketReader) readDirect(p []byte) (int, error) {
	if r.off >= r.length {
		return 0, io.EOF
	}
	toRead := len(p)
	if remaining := r.length - r.off; toRead > remaining {
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

func (r *BucketReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.buf == nil {
		return r.readDirect(p)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for r.bufR == r.bufW && r.lastErr == nil && !r.closed {
		if r.isExhaustedLocked() {
			r.lastErr = io.EOF
			break
		}
		r.maybeCompact()
		r.waiters++
		r.dataAvailable.Wait()
		r.waiters--
	}

	if r.bufR == r.bufW {
		if r.lastErr != nil {
			return 0, r.readErr()
		}
		return 0, io.EOF
	}

	n := copy(p, r.buf[r.bufR:r.bufW])
	r.bufR += n
	r.spaceAvailable.Signal()
	return n, nil
}

func (r *BucketReader) Peek(n int) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for r.bufW-r.bufR < n && r.bufW-r.bufR < len(r.buf) && r.lastErr == nil && !r.closed {
		if r.isExhaustedLocked() {
			r.lastErr = io.EOF
			break
		}
		r.maybeCompact()
		r.waiters++
		r.dataAvailable.Wait()
		r.waiters--
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

	r.mu.Lock()
	defer r.mu.Unlock()

	remain := n
	for {
		skip := r.bufW - r.bufR
		if skip == 0 {
			if r.lastErr != nil {
				return n - remain, r.readErr()
			}
			if r.closed {
				return n - remain, io.EOF
			}
			if r.isExhaustedLocked() {
				r.lastErr = io.EOF
				return n - remain, r.readErr()
			}
			r.maybeCompact()
			r.waiters++
			r.dataAvailable.Wait()
			r.waiters--
			continue
		}
		if skip > remain {
			skip = remain
		}
		r.bufR += skip
		remain -= skip
		r.spaceAvailable.Signal()
		if remain == 0 {
			return n, nil
		}
	}
}

// isExhaustedLocked reports whether the source has been fully fetched and no
// more data can arrive without a Reset. Must be called with r.mu held.
func (r *BucketReader) isExhaustedLocked() bool {
	return r.off >= r.length && !r.pendingFetch
}

func (r *BucketReader) Size() int {
	return len(r.buf)
}

func (r *BucketReader) Buffered() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bufW - r.bufR
}

func (r *BucketReader) Reset(off int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resetLocked(off)
}

// resetLocked must be called with r.mu held.
func (r *BucketReader) resetLocked(off int) {
	if r.fetchCancel != nil {
		r.fetchCancel()
		r.fetchCtx, r.fetchCancel = context.WithCancel(r.workerCtx)
	}
	r.gen++
	r.off = off
	r.bufR = 0
	r.bufW = 0
	r.lastErr = nil
	r.spaceAvailable.Broadcast()
	r.dataAvailable.Broadcast()
}

func (r *BucketReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return 0, fmt.Errorf("invalid Seek whence: %d", whence)
	}
	if offset < 0 {
		return 0, fmt.Errorf("seek to negative offset %d", offset)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.dataAvailable != nil {
		r.resetLocked(int(offset))
	} else {
		r.off = int(offset)
	}
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
	return newBucketBufReader(ctx, &bucketBufPool, bkt, name, base, length, 0)
}

func newBucketBufReader(
	ctx context.Context, bufPool *sync.Pool, bkt objstore.BucketReader, name string, base int, length int, chunkSize int,
) *BucketBufReader {
	reader := NewBucketReader(ctx, bkt, name, base, length)
	reader.buf = bufPool.Get().([]byte)
	if chunkSize <= 0 {
		chunkSize = len(reader.buf) / 8
		if chunkSize < 1 {
			chunkSize = 1
		}
	}
	reader.startPrefetch(chunkSize)

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
	bbr.r.stopPrefetch()
	bbr.bufPool.Put(bbr.r.buf)
	bbr.r.buf = nil
	return nil
}
