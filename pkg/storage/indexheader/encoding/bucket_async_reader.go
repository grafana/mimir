// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"io"
	"sync"

	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

const (
	ReadAheadFactor = 4
)

type BufPromise struct {
	bufioReader *bufio.Reader

	cancel context.CancelCauseFunc
	eg     *errgroup.Group
}

func NewBufPromise(
	ctx context.Context,
	bkt objstore.BucketReader,
	name string,
	base int,
	length int,
	bufioReader *bufio.Reader,
) *BufPromise {
	// Create handle to cancel an inflight bucket read
	ctx, cancel := context.WithCancelCause(ctx)
	bucketReader := NewBucketReader(ctx, bkt, name, base, length)
	bufioReader.Reset(bucketReader)

	// Propagate cancellable context to errgroup.
	eg, _ := errgroup.WithContext(ctx)
	bp := &BufPromise{
		cancel:      cancel,
		eg:          eg,
		bufioReader: bufioReader,
	}

	bp.eg.Go(func() error {
		// Send the fill to the background.
		// Peek reads length bytes into the buffer, and does not consume them.
		_, err := bp.bufioReader.Peek(length)
		return err
	})
	return bp
}

func (bp *BufPromise) Read(dst []byte) (n int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		// A Read after a bad Peek overwrites the bucket read error with a generic short read error.
		return 0, err
	}
	return bp.bufioReader.Read(dst)
}

func (bp *BufPromise) Buffered() (n int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		// A Read after a bad Peek overwrites the bucket read error with a generic short read error.
		return 0, err
	}
	return bp.bufioReader.Buffered(), nil
}

// Discard consumes and drops at most n bytes from the promise.
func (bp *BufPromise) Discard(n int) (discarded int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		return 0, err
	}
	return bp.bufioReader.Discard(n)
}

func (bp *BufPromise) Release(bufioPool *sync.Pool, cancelCause error) {
	bp.cancel(cancelCause)
	_ = bp.eg.Wait() // Ensure any write to the buffer completes.
	bufioPool.Put(bp.bufioReader)
	bp.bufioReader = nil
}

type BucketAsyncBufReader struct {
	ctx        context.Context
	bkt        objstore.BucketReader
	name       string
	base       int
	length     int
	readOffset int

	bufSize        int
	peekBuf        []byte
	bufIdx         int
	bufPromises    []*BufPromise
	bufferedOffset int
	bufioPool      *sync.Pool
}

func NewBucketAsyncBufReader(
	ctx context.Context,
	bkt objstore.BucketReader,
	name string, base int, length int,

) *BucketAsyncBufReader {
	return newBucketAsyncBufReader(
		ctx, bkt, name, base, length, 0,
		&bucketBufioPool, ReadBufferSize, ReadAheadFactor,
	)
}

func newBucketAsyncBufReader(
	ctx context.Context,
	bkt objstore.BucketReader,
	name string,
	base int,
	length int,
	startOffset int,
	bufioPool *sync.Pool,
	bufSize int,
	maxBufCount int,
) *BucketAsyncBufReader {
	bufsForLength := (length + bufSize - 1) / bufSize
	numBufs := min(maxBufCount, bufsForLength)
	bufPromises := make([]*BufPromise, numBufs)

	iBase := base + startOffset
	bufferedOffset := startOffset
	for i := range numBufs {
		bufioReader := bufioPool.Get().(*bufio.Reader)
		bufLen := min(length-bufferedOffset, bufSize)
		bufPromises[i] = NewBufPromise(ctx, bkt, name, iBase, bufLen, bufioReader)
		iBase += bufLen
		bufferedOffset += bufLen
	}

	return &BucketAsyncBufReader{
		ctx:            ctx,
		bkt:            bkt,
		name:           name,
		base:           base,
		length:         length,
		readOffset:     startOffset,
		bufSize:        bufSize,
		peekBuf:        make([]byte, 0, bufSize),
		bufPromises:    bufPromises,
		bufferedOffset: bufferedOffset,
		bufioPool:      bufioPool,
	}
}

func (bbar *BucketAsyncBufReader) Reset() error {
	return bbar.ResetAt(0)
}

func (bbar *BucketAsyncBufReader) ResetAt(off int) error {
	if off > bbar.length {
		return ErrInvalidSize
	}

	if dist := off - bbar.readOffset; dist > 0 && dist < bbar.Buffered() {
		// Reset via Skip to avoid discarding all buffered bytes.
		return bbar.Skip(dist)
	}

	bbar.Close()
	newBbar := newBucketAsyncBufReader(
		bbar.ctx, bbar.bkt, bbar.name, bbar.base, bbar.length, off,
		bbar.bufioPool, bbar.bufSize, ReadAheadFactor,
	)
	*bbar = *newBbar
	return nil
}

// rotateHead releases the drained head promise, starts a promise for the next chunk
// of the data segment in the same slot, and advances the head index.
// Peek copies its result into peekBuf, so no slice that the reader returned
// points into the buffer of a promise. The release is safe at once.
func (bbar *BucketAsyncBufReader) rotateHead() {
	// No need to clean up buffer, we reset when we retrieve it from the pool
	bbar.bufPromises[bbar.bufIdx].Release(bbar.bufioPool, nil)
	bufioReader := bbar.bufioPool.Get().(*bufio.Reader)

	bufLen := min(bbar.length-bbar.bufferedOffset, bbar.bufSize)
	bbar.bufPromises[bbar.bufIdx] = NewBufPromise(
		bbar.ctx, bbar.bkt, bbar.name, bbar.base+bbar.bufferedOffset, bufLen, bufioReader,
	)
	bbar.bufferedOffset += bufLen

	// Advance current buffer queue index - modulo wraps around to the front of the slice if at end.
	bbar.bufIdx = (bbar.bufIdx + 1) % len(bbar.bufPromises)
}

func (bbar *BucketAsyncBufReader) Skip(l int) error {
	if l > bbar.Len() {
		return ErrInvalidSize
	}

	// Start with any previously-peeked bytes.
	bytesSkipped := min(len(bbar.peekBuf), l)
	bbar.readOffset += bytesSkipped

	// Slide any unconsumed bytes from peekBuf to the beginning of the slice and truncate.
	n := copy(bbar.peekBuf, bbar.peekBuf[bytesSkipped:])
	bbar.peekBuf = bbar.peekBuf[:n]

	// Move on to the promises if we have not satisfied the skip yet.
	// Promises are consumed to discard the data and rotated if exhausted.
	for bytesSkipped < l {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return err
		}

		toSkip := min(l-bytesSkipped, headPromiseBuffered)
		skipN, err := headPromise.Discard(toSkip)
		if err != nil {
			return err
		}
		bbar.readOffset += skipN
		bytesSkipped += skipN

		headPromiseBuffered, err = headPromise.Buffered()
		if err != nil {
			return err
		}

		if headPromiseBuffered <= 0 {
			bbar.rotateHead()
		}
	}

	return nil
}

func (bbar *BucketAsyncBufReader) Peek(n int) ([]byte, error) {
	// Clamp n to the lesser of the capacity of peekBuf or the length of the section.
	n = min(n, cap(bbar.peekBuf), bbar.Len())

	// Start with any previously-peeked bytes.
	// Any data remaining in peekBuf is assumed to still be the valid start of a Peek.
	// The read operations (Read/ReadInto and Skip) are required to update peekBuf
	// to discard any previously-peeked bytes which were consumed by the read.
	peekBytesAvailable := len(bbar.peekBuf)
	if n > peekBytesAvailable {
		bbar.peekBuf = bbar.peekBuf[:n] // Grow length
	}
	peekBytesWritten := min(n, peekBytesAvailable)

	// Move on to the promises if we have not satisfied the peek yet.
	// Promises are consumed to copy into the peekBuf and rotated if exhausted.
	for peekBytesWritten < n {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return nil, err
		}

		toRead := min(n-peekBytesWritten, headPromiseBuffered)
		readN, err := io.ReadFull(headPromise, bbar.peekBuf[peekBytesWritten:peekBytesWritten+toRead])
		peekBytesWritten += readN
		if err != nil {
			return nil, err
		}

		headPromiseBuffered, err = headPromise.Buffered()
		if err != nil {
			return nil, err
		}

		if headPromiseBuffered <= 0 {
			bbar.rotateHead()
		}
	}

	if peekBytesWritten == 0 {
		return nil, nil
	}
	return bbar.peekBuf[:peekBytesWritten], nil
}

func (bbar *BucketAsyncBufReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := bbar.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (bbar *BucketAsyncBufReader) ReadInto(dst []byte) error {
	if len(dst) > bbar.Len() {
		if err := bbar.Skip(bbar.Len()); err != nil {
			return err
		}
		return ErrInvalidSize
	}

	// Start with any previously-peeked bytes.
	dstBytesWritten := copy(dst, bbar.peekBuf)
	bbar.readOffset += dstBytesWritten
	// Slide any unconsumed bytes from peekBuf to the beginning of the slice and truncate.
	n := copy(bbar.peekBuf, bbar.peekBuf[dstBytesWritten:])
	bbar.peekBuf = bbar.peekBuf[:n]

	// Move on to the promises if we have not satisfied the read yet.
	// Promises are consumed to copy into dst and rotated if exhausted.
	for dstBytesWritten < len(dst) {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return err
		}

		toRead := min(len(dst)-dstBytesWritten, headPromiseBuffered)
		readN, err := io.ReadFull(headPromise, dst[dstBytesWritten:dstBytesWritten+toRead])
		bbar.readOffset += readN
		dstBytesWritten += readN
		if err != nil {
			return err
		}

		headPromiseBuffered, err = headPromise.Buffered()
		if err != nil {
			return err
		}

		if headPromiseBuffered <= 0 {
			bbar.rotateHead()
		}
	}
	return nil
}

// Size returns the largest number of bytes that a single Peek can return.
// Peek assembles its result in peekBuf, so the capacity of peekBuf is the limit.
func (bbar *BucketAsyncBufReader) Size() int {
	return cap(bbar.peekBuf)
}

func (bbar *BucketAsyncBufReader) Len() int {
	return bbar.length - bbar.readOffset
}

func (bbar *BucketAsyncBufReader) Offset() int {
	return bbar.readOffset
}

func (bbar *BucketAsyncBufReader) Buffered() int {
	return bbar.bufferedOffset - bbar.readOffset
}

// errBufPromiseReleased is the cancellation cause when release stops a fill that is still in flight.
var errBufReaderClosed = errors.New("BufReader closed")

// Close cancels all promises and releases buffers back to the pool.
func (bbar *BucketAsyncBufReader) Close() error {
	for i, bufPromise := range bbar.bufPromises {
		// No need to clean up buffer, we reset when we retrieve it from the pool.
		bufPromise.Release(bbar.bufioPool, errBufReaderClosed)
		bbar.bufPromises[i] = nil
	}

	// The BucketReader of a promise does not need a Close call.
	// It closes the reader created by bkt.GetRange on each Read call.
	return nil
}
