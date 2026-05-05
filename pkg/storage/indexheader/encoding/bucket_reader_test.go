// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// testBucketContents is the 36-byte payload used throughout bucket reader tests.
// 36 bytes selected to allow two full buffer fills of size 16 and one partial buffer fill to complete.
var testBucketContents = []byte("abcdefghijklmnopqrstuvwxyz1234567890")

const testBucketObjectName = "test-object"

// Equal to minReadBufferSize for bufio.Reader; cannot init with smaller size.
// Small buffer is used to force multiple bufio.Reader fill operations to read full testBucketContents.
const testBufPoolSize = 16

var testBucketBufPool = sync.Pool{
	New: func() any {
		return make([]byte, testBufPoolSize)
	},
}

func newTestBufReader(t *testing.T, base, length int) (*BucketBufReader, *trackingBucket) {
	t.Helper()
	ctx := context.Background()

	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)

	// Use chunkSize == buffer size so existing GetRange-count assertions remain
	// meaningful: each fetch is sized to fully populate the small test buffer
	// in one call, mirroring pre-async behavior.
	return newBucketBufReader(ctx, &testBucketBufPool, bkt, testBucketObjectName, base, length, testBufPoolSize), bkt
}

func newFailingBufReader(t *testing.T, sentinel error) *BucketBufReader {
	t.Helper()
	bkt := &failingBucket{err: sentinel}
	ctx := context.Background()
	return newBucketBufReader(ctx, &testBucketBufPool, bkt, "obj", 0, 10, testBufPoolSize)
}

func TestBucketBufReader_Read_Sequential(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 5, r.Offset())

	b, err = r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:10], b)
	require.Equal(t, 10, r.Offset())
}

func TestBucketBufReader_Read_ExactLength(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b)
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufReader_Read_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	b, err := r.Read(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset())
}

func TestBucketBufReader_Read_BeyondEndAfterPartialConsumption(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	_, err := r.Read(3)
	require.NoError(t, err)

	// 7 remain; attempt to read 8
	b, err := r.Read(8)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketBufReader_ReadInto_Basic(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	buf := make([]byte, 5)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[:5], buf)
	require.Equal(t, 5, r.Offset())
}

func TestBucketBufReader_ReadInto_BeyondEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestBufReader(t, 0, sectionLen)

	buf := make([]byte, sectionLen+1)
	err := r.ReadInto(buf)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketBufReader_Skip_Basic(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:13], b)
}

func TestBucketBufReader_Skip_ToEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	require.NoError(t, r.Skip(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufReader_Skip_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	err := r.Skip(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
}

func TestBucketBufReader_Peek_Basic(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	b, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 0, r.Offset())

	// Read should return the same bytes.
	got, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], got)
}

func TestBucketBufReader_Peek_PastSegmentEnd(t *testing.T) {
	// Peek must return the bytes read and suppress the EOF error
	// when peeking past the configured length or peeking beyond the true end of the file/object.
	const sectionLen = 5
	r, _ := newTestBufReader(t, 0, sectionLen)

	b, err := r.Peek(sectionLen + 5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:sectionLen], b)
	require.Equal(t, 0, r.Offset())
}

func TestBucketBufReader_Peek_AtEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestBufReader(t, 0, sectionLen)
	require.NoError(t, r.Skip(sectionLen))

	b, err := r.Peek(1)
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestBucketBufReader_Reset(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	first, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:10], first)

	require.NoError(t, r.Reset())
	require.Equal(t, 0, r.Offset())
	require.Equal(t, len(testBucketContents), r.Len())

	second, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:10], second)
}

func TestBucketBufReader_ResetAt_Middle(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	require.NoError(t, r.ResetAt(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:15], b)
}

func TestBucketBufReader_ResetAt_End(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	require.NoError(t, r.ResetAt(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufReader_ResetAt_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufReader(t, 0, sectionLen)

	err := r.ResetAt(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
}

func TestBucketBufReader_Len(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	require.Equal(t, len(testBucketContents), r.Len())
	require.NoError(t, r.Skip(10))
	require.Equal(t, len(testBucketContents)-10, r.Len())
	_, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, len(testBucketContents)-15, r.Len())
}

func TestBucketBufReader_Size(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))
	require.Equal(t, testBufPoolSize, r.Size())
}

func TestBucketBufReader_Buffered(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))
	require.Equal(t, 0, r.Buffered())

	// The first read operation causes bufio to fill its buffer.
	// The read bytes will not count towards the buffer length after the call.
	_, err := r.Read(1)
	require.NoError(t, err)
	require.Equal(t, testBufPoolSize-1, r.Buffered())
}

func TestBucketBufReader_Close(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Close())
}

func TestBucketBufReader_GetRangeCalls_Buffering(t *testing.T) {
	r, bkt := newTestBufReader(t, 0, len(testBucketContents))

	// First read forces the prefetch goroutine's first fetch and waits for it.
	// At this point exactly one GetRange has happened: the consumer just unblocked
	// from waiting on data and the buffer still has 15 unread bytes, so the
	// prefetcher has no room for another chunk yet.
	_, err := r.Read(1)
	require.NoError(t, err)
	require.Equal(t, 1, bkt.callCount())

	// Reads 2-16 are served from the same buffered chunk. The 16th read drains
	// the buffer; the prefetcher may or may not have started chunk #2 by the
	// time the loop ends, so we don't assert the count here.
	for i := 1; i < testBufPoolSize; i++ {
		_, err := r.Read(1)
		require.NoError(t, err)
	}

	// Read #17 needs the next chunk; this Read blocks until the second fetch
	// arrives. Once it returns, the buffer holds 15 of the 16 bytes from chunk
	// #2 and the prefetcher again has no room for another chunk, so exactly two
	// GetRange calls have happened.
	_, err = r.Read(1)
	require.NoError(t, err)
	require.Equal(t, 2, bkt.callCount())
}

func TestBucketBufReader_GetRangeCalls_ResetRefetches(t *testing.T) {
	r, bkt := newTestBufReader(t, 0, len(testBucketContents))

	_, err := r.Read(1)
	require.NoError(t, err)
	require.Equal(t, 1, bkt.callCount())

	// After Reset the buffer is discarded; the next read must refetch from the bucket.
	require.NoError(t, r.Reset())
	_, err = r.Read(1)
	require.NoError(t, err)
	require.Equal(t, 2, bkt.callCount())
}

func TestBucketBufReader_Read_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingBufReader(t, sentinel)

	_, err := r.Read(5)
	require.ErrorIs(t, err, sentinel)
}

func TestBucketBufReader_ReadInto_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingBufReader(t, sentinel)

	err := r.ReadInto(make([]byte, 5))
	require.ErrorIs(t, err, sentinel)
}

// trackingBucket wraps an InstrumentedBucketReader and records every GetRange call.
// The async prefetch goroutine calls GetRange from a different goroutine than the
// test, so calls is guarded by mu.
type trackingBucket struct {
	objstore.InstrumentedBucketReader
	mu    sync.Mutex
	calls []rangeCall
}

type rangeCall struct {
	off    int64
	length int64
}

func (b *trackingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.mu.Lock()
	b.calls = append(b.calls, rangeCall{off, length})
	b.mu.Unlock()
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

func (b *trackingBucket) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.calls)
}

func (b *trackingBucket) callsCopy() []rangeCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]rangeCall, len(b.calls))
	copy(out, b.calls)
	return out
}

func newTrackingBucket(t *testing.T, objectData []byte) *trackingBucket {
	t.Helper()
	inmem := objstore.NewInMemBucket()
	require.NoError(t, inmem.Upload(context.Background(), testBucketObjectName, bytes.NewReader(objectData)))
	return &trackingBucket{InstrumentedBucketReader: objstore.WithNoopInstr(inmem)}
}

// failingBucket is an InstrumentedBucketReader whose GetRange always returns an error.
type failingBucket struct {
	err error
}

func (b *failingBucket) GetRange(_ context.Context, _ string, _, _ int64) (io.ReadCloser, error) {
	return nil, b.err
}

func (b *failingBucket) Get(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, b.err
}

func (b *failingBucket) Iter(_ context.Context, _ string, _ func(string) error, _ ...objstore.IterOption) error {
	return b.err
}

func (b *failingBucket) IterWithAttributes(_ context.Context, _ string, _ func(objstore.IterObjectAttributes) error, _ ...objstore.IterOption) error {
	return b.err
}

func (b *failingBucket) SupportedIterOptions() []objstore.IterOptionType { return nil }

func (b *failingBucket) Exists(_ context.Context, _ string) (bool, error) { return false, b.err }

func (b *failingBucket) Attributes(_ context.Context, _ string) (objstore.ObjectAttributes, error) {
	return objstore.ObjectAttributes{}, b.err
}

func (b *failingBucket) IsObjNotFoundErr(_ error) bool  { return false }
func (b *failingBucket) IsAccessDeniedErr(_ error) bool { return false }
func (b *failingBucket) Name() string                   { return "failing" }
func (b *failingBucket) Close() error                   { return nil }

func (b *failingBucket) ReaderWithExpectedErrs(_ objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}

// blockingBucket holds GetRange calls until release is closed. It also
// reports each call on the starts channel so tests can synchronize with the
// async prefetch goroutine.
type blockingBucket struct {
	objstore.InstrumentedBucketReader
	starts  chan rangeCall
	release chan struct{}
}

func newBlockingBucket(t *testing.T, data []byte) *blockingBucket {
	t.Helper()
	inmem := objstore.NewInMemBucket()
	require.NoError(t, inmem.Upload(context.Background(), testBucketObjectName, bytes.NewReader(data)))
	return &blockingBucket{
		InstrumentedBucketReader: objstore.WithNoopInstr(inmem),
		starts:                   make(chan rangeCall, 64),
		release:                  make(chan struct{}),
	}
}

func (b *blockingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.starts <- rangeCall{off, length}
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

func TestBucketReader_AsyncPrefetch_ChunkedFills(t *testing.T) {
	const (
		bufSize   = 32
		chunkSize = 8
	)
	pool := &sync.Pool{
		New: func() any { return make([]byte, bufSize) },
	}

	ctx := context.Background()
	bkt := newTrackingBucket(t, testBucketContents)
	r := newBucketBufReader(ctx, pool, bkt, testBucketObjectName, 0, len(testBucketContents), chunkSize)

	out := make([]byte, len(testBucketContents))
	require.NoError(t, r.ReadInto(out))
	require.Equal(t, testBucketContents, out)
	require.NoError(t, r.Close())

	calls := bkt.callsCopy()
	require.NotEmpty(t, calls)
	for _, c := range calls {
		require.LessOrEqual(t, c.length, int64(chunkSize), "fetch larger than chunkSize")
	}
	var totalFetched int64
	for _, c := range calls {
		totalFetched += c.length
	}
	require.Equal(t, int64(len(testBucketContents)), totalFetched)
	// 36 bytes split into 8-byte chunks requires at least ceil(36/8)=5 fetches.
	require.GreaterOrEqual(t, len(calls), 5)
}

func TestBucketReader_AsyncPrefetch_ResetCancelsInFlight(t *testing.T) {
	ctx := context.Background()
	bkt := newBlockingBucket(t, testBucketContents)
	r := newBucketBufReader(ctx, &testBucketBufPool, bkt, testBucketObjectName, 0, len(testBucketContents), testBufPoolSize)

	select {
	case firstCall := <-bkt.starts:
		require.Equal(t, int64(0), firstCall.off)
	case <-time.After(2 * time.Second):
		t.Fatal("first GetRange did not start in time")
	}

	require.NoError(t, r.ResetAt(8))

	select {
	case secondCall := <-bkt.starts:
		require.Equal(t, int64(8), secondCall.off, "second fetch should be from the reset offset")
	case <-time.After(2 * time.Second):
		t.Fatal("second GetRange did not start after Reset")
	}

	close(bkt.release)
	require.NoError(t, r.Close())
}

func TestBucketReader_AsyncPrefetch_PeekBlocksUntilFetched(t *testing.T) {
	ctx := context.Background()
	bkt := newBlockingBucket(t, testBucketContents)
	r := newBucketBufReader(ctx, &testBucketBufPool, bkt, testBucketObjectName, 0, len(testBucketContents), testBufPoolSize)

	select {
	case <-bkt.starts:
	case <-time.After(2 * time.Second):
		t.Fatal("first GetRange did not start in time")
	}

	peekDone := make(chan []byte, 1)
	peekErr := make(chan error, 1)
	go func() {
		b, err := r.Peek(5)
		peekErr <- err
		peekDone <- b
	}()

	select {
	case <-peekDone:
		t.Fatal("Peek returned before any data was available")
	case <-time.After(50 * time.Millisecond):
	}

	close(bkt.release)

	select {
	case b := <-peekDone:
		require.NoError(t, <-peekErr)
		require.Equal(t, testBucketContents[:5], b)
	case <-time.After(2 * time.Second):
		t.Fatal("Peek did not return after the bucket released")
	}

	require.NoError(t, r.Close())
}

func TestBucketReader_AsyncPrefetch_CloseStopsGoroutine(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	_, err := r.Read(1)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- r.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5 seconds")
	}
}

func TestBucketReader_AsyncPrefetch_CloseUnblocksFetch(t *testing.T) {
	ctx := context.Background()
	bkt := newBlockingBucket(t, testBucketContents)
	r := newBucketBufReader(ctx, &testBucketBufPool, bkt, testBucketObjectName, 0, len(testBucketContents), testBufPoolSize)

	select {
	case <-bkt.starts:
	case <-time.After(2 * time.Second):
		t.Fatal("first GetRange did not start in time")
	}

	done := make(chan error, 1)
	go func() {
		done <- r.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock the in-flight fetch within 5 seconds")
	}

	close(bkt.release)
}
