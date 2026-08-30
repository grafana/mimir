// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"io"
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

func (bbar *BucketAsyncBufReader) Peek(n int) ([]byte, error) {
	// Clamp n to the lesser of the capacity of peekBuf or the length of the section.
	n = min(n, cap(bbar.peekBuf), bbar.Len())

	// Start with any previously-peeked bytes - Peek-after-Peek is a valid access pattern.
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
	// TODO consistency in error handling
	//// A read past the end of the data segment is not valid.
	//// The guard in Skip returns ErrInvalidSize for the same condition.
	//// The reader contract also requires this read to consume the bytes that remain,
	//// so move the cursor to the end before the return.
	//// Without this guard the loop below never ends,
	//// because a drained head promise rotates to an empty promise forever.
	//if len(dst) > bbar.Len() {
	//	remaining := bbar.Len()
	//	if err := bbar.Skip(remaining); err != nil {
	//		return err
	//	}
	//	// io.ReadFull reports io.EOF for no bytes and io.ErrUnexpectedEOF for a partial read.
	//	// BucketBufReader passes that error through, so report the same error here.
	//	shortErr := io.ErrUnexpectedEOF
	//	if remaining == 0 {
	//		shortErr = io.EOF
	//	}
	//	return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(dst), shortErr)
	//}

	// Start with any previously-peeked bytes
	dstBytesWritten := copy(dst, bbar.peekBuf)
	bbar.readOffset += dstBytesWritten
	// Slide any unconsumed bytes from peekBuf to the beginning of the slice and truncate.
	bbar.peekBuf = bbar.peekBuf[dstBytesWritten:]

	// Move on to read from the promises if we have not satisfied the read yet.
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

		// TODO consistency in error handling
		//// Now we can surface any error
		//if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		//	return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(dst), err)
		//} else if err != nil {
		//	return err
		//}
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
