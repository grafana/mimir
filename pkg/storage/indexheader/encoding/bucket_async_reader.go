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
	"golang.org/x/sync/errgroup"
)

const (
	ReadAheadFactor = 2
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
		b, err := bp.bufioReader.Peek(length)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf(
				"%w reading %d bytes at offset %d of %s (got %d bytes): %s",
				ErrInvalidSize, length, base, name, len(b), err,
			)
		}
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

	bufSize int

	// peekBuf holds peeked bytes until they are read or discarded.
	// Peek is intended to return a slice of bytes without an extra allocation,
	// but we cannot return a slice which spans two underlying promise buffers.
	// We allocate peekBuf with a large capacity and Peek returns subslices of it.
	// peekBuf's slice bounds slides forward through the backing array until it runs out of capacity,
	// then it is compacted by copying remaining elements back to the start of the array.
	peekBuf     []byte
	peekBufBase []byte // Holds the reference to the start of the slice for compaction

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

	peekBufBase := make([]byte, 0, bufSize)
	return &BucketAsyncBufReader{
		ctx:            ctx,
		bkt:            bkt,
		name:           name,
		base:           base,
		length:         length,
		readOffset:     startOffset,
		bufSize:        bufSize,
		peekBufBase:    peekBufBase,
		peekBuf:        peekBufBase[:0],
		bufPromises:    bufPromises,
		bufferedOffset: bufferedOffset,
		bufioPool:      bufioPool,
	}
}

// rotateHead releases the exhausted head promise back to the pool
// and queues a promise to buffer the next read range in its place.
func (bbar *BucketAsyncBufReader) rotateHead() {
	// No need to clean up buffer, we reset when we retrieve it from the pool
	bbar.bufPromises[bbar.bufIdx].Release(bbar.bufioPool, nil)
	bufioReader := bbar.bufioPool.Get().(*bufio.Reader)

	bufLen := min(bbar.length-bbar.bufferedOffset, bbar.bufSize)
	bbar.bufPromises[bbar.bufIdx] = NewBufPromise(
		bbar.ctx, bbar.bkt, bbar.name, bbar.base+bbar.bufferedOffset, bufLen, bufioReader,
	)
	bbar.bufferedOffset += bufLen

	// Advance current buffer queue index - modulo wraps to the front of the slice.
	bbar.bufIdx = (bbar.bufIdx + 1) % len(bbar.bufPromises)
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

func (bbar *BucketAsyncBufReader) Skip(l int) error {
	if l > bbar.Len() {
		return ErrInvalidSize
	}

	// Start with any previously-peeked bytes.
	bytesSkipped := min(len(bbar.peekBuf), l)
	bbar.readOffset += bytesSkipped

	// Advance past the consumed bytes without moving them,
	// so a slice returned by an earlier Peek stays valid.
	bbar.peekBuf = bbar.peekBuf[bytesSkipped:]

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
	// Clamp n to the lesser of the buffer size or the length of the section.
	n = min(n, bbar.bufSize, bbar.Len())
	if n > cap(bbar.peekBuf) {
		// Slide remaining peeked bytes
		bbar.peekBuf = append(bbar.peekBufBase[:0], bbar.peekBuf...)
	}

	// Start with any previously-peeked bytes.
	// Any data remaining in peekBuf is assumed to still be the valid start of a Peek.
	// Read/ReadInto, Reset/ResetAt, and Skip are required to update peekBuf
	// to discard any previously-peeked bytes which were consumed by the read.
	peekBytesAvailable := len(bbar.peekBuf)
	if n > peekBytesAvailable {
		bbar.peekBuf = bbar.peekBuf[:n] // Grow length; will not allocate.
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
	// Advance past the consumed bytes without moving the
	// so a slice returned by an earlier Peek stays valid.
	bbar.peekBuf = bbar.peekBuf[dstBytesWritten:]

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

func (bbar *BucketAsyncBufReader) Size() int {
	// Reported capacity of peekBuf changes as its referenced window slides,
	// but we will compact to make use of its full underlying allocated size if needed.
	return bbar.bufSize
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
