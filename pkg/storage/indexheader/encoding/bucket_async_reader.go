// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

const (
	ReadAheadFactor = 4
)

type BufPromise struct {
	//r   *BucketReader
	eg *errgroup.Group
	//bkt objstore.BucketReader

	//ctx    context.Context
	//name   string
	//base   int
	//length int
	//off    int

	//buf   []byte
	bufioReader *bufio.Reader
	//readN       int
}

func NewBufPromise(
	ctx context.Context,
	bkt objstore.BucketReader,
	name string,
	base int,
	length int,
	//buf []byte,
	bufioReader *bufio.Reader,
) *BufPromise {
	bucketReader := NewBucketReader(ctx, bkt, name, base, length)
	bufioReader.Reset(bucketReader)

	bp := &BufPromise{
		eg:          &errgroup.Group{},
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

// Peek returns at most n bytes from the promise, without consuming them.
// The byte slice points into the buffer of the promise.
// It becomes invalid when the promise is released.
func (bp *BufPromise) Peek(n int) ([]byte, error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		return nil, err
	}
	return bp.bufioReader.Peek(n)
}

// Discard consumes and drops at most n bytes from the promise.
func (bp *BufPromise) Discard(n int) (discarded int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		return 0, err
	}
	return bp.bufioReader.Discard(n)
}

// release returns the buffer of the promise to the pool.
// release waits for the fill, because a fill writes to the buffer.
// A buffer in the pool belongs to the next reader that gets it.
// The promise is not usable after release.
func (bp *BufPromise) release(bufioPool *sync.Pool) {
	// The error is not relevant here, because release discards the contents of the buffer.
	_ = bp.eg.Wait()
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

	resetReader func(off int) error

	bufSize int

	bufIdx         int
	bufPromises    []*BufPromise
	bufferedOffset int

	peekBuf []byte

	// pool reference to return to on Close
	bufioPool *sync.Pool
}

func NewBucketAsyncBufReader(
	ctx context.Context, bkt objstore.BucketReader, name string, base int, length int,
) *BucketAsyncBufReader {
	return newBucketAsyncBufReader(
		ctx, bkt, name, base, length,
		&bucketBufioPool, ReadBufferSize, ReadAheadFactor,
	)
}

func newBucketAsyncBufReader(
	ctx context.Context,
	bkt objstore.BucketReader,
	name string,
	base int,
	length int,
	bufioPool *sync.Pool,
	bufSize int,
	maxBufCount int,
) *BucketAsyncBufReader {
	bufsForLength := (length + bufSize - 1) / bufSize
	numBufs := min(maxBufCount, bufsForLength)
	bufPromises := make([]*BufPromise, numBufs)

	iBase := base
	bufferedOffset := 0
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
		bufSize:        bufSize,
		peekBuf:        make([]byte, 0, bufSize),
		bufPromises:    bufPromises,
		bufferedOffset: bufferedOffset,
		bufioPool:      bufioPool,
	}
}

func (bbar *BucketAsyncBufReader) Reset() error {
	//TODO implement me
	panic("implement me")
}

func (bbar *BucketAsyncBufReader) ResetAt(off int) error {
	//TODO implement me
	panic("implement me")
}

// rotateHead releases the drained head promise, starts a promise for the next chunk
// of the data segment in the same slot, and advances the head index.
// Peek copies its result into peekBuf, so no slice that the reader returned
// points into the buffer of a promise. The release is safe at once.
func (bbar *BucketAsyncBufReader) rotateHead() {
	// Note that we don't do anything to clean up the buffer before returning it to the pool here:
	// we reset the buffer when we retrieve it from the pool instead.
	bbar.bufPromises[bbar.bufIdx].release(bbar.bufioPool)
	bufioReader := bbar.bufioPool.Get().(*bufio.Reader)

	// Create a new buffer promise in the same spot in the buffer promise queue.
	// The promise must not reach past the end of the reader.
	// bufferedOffset is relative to the data segment.
	// Add base to get the offset in the object.
	bufLen := min(bbar.length-bbar.bufferedOffset, bbar.bufSize)
	bbar.bufPromises[bbar.bufIdx] = NewBufPromise(
		bbar.ctx, bbar.bkt, bbar.name, bbar.base+bbar.bufferedOffset, bufLen, bufioReader,
	)
	bbar.bufferedOffset += bufLen

	// Advance current buffer queue index - modulo wraps around to the front of the slice if at end.
	bbar.bufIdx = (bbar.bufIdx + 1) % len(bbar.bufPromises)
}

// Skip advances the cursor by l bytes in the data segment and discards those bytes.
// Skip returns ErrInvalidSize if l is greater than the number of bytes that remain.
func (bbar *BucketAsyncBufReader) Skip(l int) error {
	if l > bbar.Len() {
		return ErrInvalidSize
	}

	bytesSkipped := 0
	// First try to complete the skip from previously-peeked bytes.
	// If peekBuf is non-empty, those bytes were not skipped or read yet.
	n := min(len(bbar.peekBuf), l)
	bbar.readOffset += n
	bytesSkipped += n
	// Truncate the peekBuf even if we did not skip all the previously-peeked bytes.
	// Peek interface contract says "byte slice returned becomes invalid at the next read" (which includes Skip).
	bbar.peekBuf = bbar.peekBuf[:0]

	// Move on to skip the data from promises if we have not complete the skip yet.
	for bytesSkipped < l {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return err
		}

		toSkip := min(l-bytesSkipped, headPromiseBuffered)
		n, err := headPromise.Discard(toSkip)
		if err != nil {
			return err
		}
		bbar.readOffset += n
		bytesSkipped += n

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

// Peek returns at most n bytes from the data segment, without consuming them.
// Peek always copies the bytes into peekBuf for now.
// This keeps the logic simple for rotating out exhausted buffer promises
// in the case where a peek crosses the promise boundaries.
// Since peekBuf is pre-allocated, this still avoids the extra slice allocation
// which occurs when callers Read instead of Peek.
func (bbar *BucketAsyncBufReader) Peek(n int) ([]byte, error) {
	// Ensure peekBuf has capacity of n by calling Grow against the truncated slice.
	// This should never trigger a new alloc as peekBuf is pre-allocated to larger than we need -
	// at most it needs to hold the length of one Prometheus label or value.
	// Length must be truncated before return in the case of a short Peek.
	bbar.peekBuf = slices.Grow(bbar.peekBuf[:0], n)[:n]

	peekableBytes := bbar.Size()
	peekBytesWritten := 0
	for peekBytesWritten < n && peekableBytes > 0 {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return nil, err
		}

		toRead := min(n-peekBytesWritten, headPromiseBuffered)
		readN, err := io.ReadFull(headPromise, bbar.peekBuf[peekBytesWritten:peekBytesWritten+toRead])
		peekBytesWritten += readN
		peekableBytes -= readN
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
	// A short Peek is valid; truncate to what was actually read.
	bbar.peekBuf = bbar.peekBuf[:peekBytesWritten]

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
	// First try to satisfy the read from previously-peeked bytes.
	dstBytesWritten := 0
	n := copy(dst, bbar.peekBuf)
	bbar.readOffset += n
	dstBytesWritten += n

	// Truncate the peekBuf even if we did not read all the previously-peeked bytes.
	// Peek interface contract says "byte slice returned becomes invalid at the next read".
	// We do not need to try to serve two subsequent reads from the peekBuf even if they fit.
	bbar.peekBuf = bbar.peekBuf[:0]

	// Move on to read from the promises if we have not satisfied the read yet.
	for dstBytesWritten < len(dst) {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return err
		}

		toRead := min(len(dst)-dstBytesWritten, headPromiseBuffered)
		n, err := io.ReadFull(headPromise, dst[dstBytesWritten:dstBytesWritten+toRead])
		bbar.readOffset += n
		dstBytesWritten += n
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

		// Now we can surface any error
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(dst), err)
		} else if err != nil {
			return err
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
	//TODO implement me
	panic("implement me")
}

// Close releases each promise that the reader still holds,
// and discards the data in the buffers of those promises.
// Close is safe to call more than one time.
func (bbar *BucketAsyncBufReader) Close() error {
	for i, bufPromise := range bbar.bufPromises {
		if bufPromise == nil {
			// An earlier Close released the promise in this slot.
			continue
		}
		// Note that we don't do anything to clean up the buffer before returning it to the pool here:
		// we reset the buffer when we retrieve it from the pool instead.
		bufPromise.release(bbar.bufioPool)
		bbar.bufPromises[i] = nil
	}

	// The BucketReader of a promise does not need a Close call.
	// It closes the reader from bkt.GetRange in each Read call.
	return nil
}
