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

func (bp *BufPromise) Read(p []byte) (n int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		// A Read after a bad Peek overwrites the bucket read error with a generic short read error.
		return 0, err
	}
	return bp.bufioReader.Read(p)
}

func (bp *BufPromise) Buffered() (n int, err error) {
	if err := bp.eg.Wait(); err != nil {
		// Return any error in the underlying bucket read from the initial fill-via-Peek.
		// A Read after a bad Peek overwrites the bucket read error with a generic short read error.
		return 0, err
	}
	return bp.bufioReader.Buffered(), nil
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

func (bbar *BucketAsyncBufReader) Skip(l int) error {
	//TODO implement me
	panic("implement me")
}

func (bbar *BucketAsyncBufReader) Peek(n int) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (bbar *BucketAsyncBufReader) Read(n int) ([]byte, error) {
	b := make([]byte, n)

	err := bbar.ReadInto(b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (bbar *BucketAsyncBufReader) ReadInto(b []byte) error {
	resultBufWritten := 0
	for resultBufWritten < len(b) {
		headPromise := bbar.bufPromises[bbar.bufIdx]
		headPromiseBuffered, err := headPromise.Buffered()
		if err != nil {
			return err
		}

		toRead := min(len(b)-resultBufWritten, headPromiseBuffered)
		n, err := io.ReadFull(headPromise, b[resultBufWritten:resultBufWritten+toRead])
		bbar.readOffset += n
		resultBufWritten += n

		headPromiseBuffered, err = headPromise.Buffered()
		if err != nil {
			return err
		}

		if headPromiseBuffered <= 0 {
			// Rotate & replace
			headPromise.release(bbar.bufioPool)
			bufioReader := bbar.bufioPool.Get().(*bufio.Reader)
			bufLen := min(bbar.length-bbar.bufferedOffset, bbar.bufSize)
			bbar.bufPromises[bbar.bufIdx] = NewBufPromise(
				bbar.ctx, bbar.bkt, bbar.name, bbar.bufferedOffset, bufLen, bufioReader,
			)
			bbar.bufIdx = (bbar.bufIdx + 1) % len(bbar.bufPromises)
			bbar.bufferedOffset += bufLen
		}

		// Now we can surface any error
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("%w reading %d bytes: %s", ErrInvalidSize, len(b), err)
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (bbar *BucketAsyncBufReader) Size() int {
	return bbar.bufSize
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
			// A rotate or an earlier Close released the promise in this slot.
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
