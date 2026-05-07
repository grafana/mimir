// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestAsyncReader(t *testing.T, base, length int) (*BucketReaderAsyncReadAhead, *trackingBucket) {
	t.Helper()
	ctx := context.Background()

	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)

	return NewBucketReaderAsyncReadAhead(ctx, bkt, testBucketObjectName, base, length), bkt
}

func newFailingAsyncReader(_ *testing.T, sentinel error) *BucketReaderAsyncReadAhead {
	bkt := &failingBucket{err: sentinel}
	return NewBucketReaderAsyncReadAhead(context.Background(), bkt, "obj", 0, 10)
}

// newSizedTrackingBucket builds a bucket whose object is exactly `length` bytes,
// repeating testBucketContents so that GetRange calls on any sub-range succeed.
// Required for queueReadAhead tests that span multiple promise chunks.
func newSizedTrackingBucket(t *testing.T, length int) *trackingBucket {
	t.Helper()
	objectData := make([]byte, length)
	for i := 0; i < length; i++ {
		objectData[i] = testBucketContents[i%len(testBucketContents)]
	}
	return newTrackingBucket(t, objectData)
}

func newSizedAsyncReader(t *testing.T, length int) *BucketReaderAsyncReadAhead {
	t.Helper()
	bkt := newSizedTrackingBucket(t, length)
	return NewBucketReaderAsyncReadAhead(context.Background(), bkt, testBucketObjectName, 0, length)
}

func TestBucketReaderAsyncReadAhead_Read_Sequential(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	buf := make([]byte, 5)
	n, err := r.ReadInto(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, testBucketContents[:5], buf)

	buf = make([]byte, 5)
	n, err = r.ReadInto(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, testBucketContents[5:10], buf)
}

func TestBucketReaderAsyncReadAhead_Read_ExactLength(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	buf := make([]byte, len(testBucketContents))
	n, err := r.ReadInto(buf)
	require.NoError(t, err)
	require.Equal(t, len(testBucketContents), n)
	require.Equal(t, testBucketContents, buf)
}

func TestBucketReaderAsyncReadAhead_Read_PastEnd(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	// Drain the reader.
	_, err := r.ReadInto(make([]byte, len(testBucketContents)))
	require.NoError(t, err)

	// Subsequent reads must report io.EOF without advancing.
	n, err := r.ReadInto(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)
}

func TestBucketReaderAsyncReadAhead_Read_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingAsyncReader(t, sentinel)

	_, err := r.ReadInto(make([]byte, 5))
	require.ErrorIs(t, err, sentinel)
}

func TestBucketReaderAsyncReadAhead_QueueReadAhead(t *testing.T) {
	t.Run("single chunk when length below chunk size", func(t *testing.T) {
		length := asyncReadAheadChunkSize / 4
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 1, r.bufQueue.Len(), "exactly one promise covers a sub-chunk-sized range")
		require.Equal(t, int64(length), r.queuedLen.Load())
		require.Equal(t, length, r.bufQueue.Front().Value.length)
	})

	t.Run("caps at maxInFlight when range exceeds maxInFlight chunks", func(t *testing.T) {
		length := asyncReadAheadChunkSize * (asyncReadAheadMaxInFlight + 2)
		r := newSizedAsyncReader(t, length)

		require.Equal(t, asyncReadAheadMaxInFlight, r.bufQueue.Len())
		require.Equal(t, int64(asyncReadAheadChunkSize*asyncReadAheadMaxInFlight), r.queuedLen.Load())

		// Each queued promise is exactly chunkSize and at increasing base offsets.
		off := 0
		for e := r.bufQueue.Front(); e != nil; e = e.Next() {
			require.Equal(t, asyncReadAheadChunkSize, e.Value.length)
			require.Equal(t, off, e.Value.base)
			off += asyncReadAheadChunkSize
		}
	})

	t.Run("refills queue after a fully consumed promise is discarded", func(t *testing.T) {
		length := asyncReadAheadChunkSize * (asyncReadAheadMaxInFlight + 2)
		r := newSizedAsyncReader(t, length)

		require.Equal(t, asyncReadAheadMaxInFlight, r.bufQueue.Len())
		queuedBefore := r.queuedLen.Load()

		// Simulate Read fully consuming the front promise and dropping it: curr stays
		// nil, bufQueue shrinks by one. queueReadAhead must top the queue back up.
		r.bufQueue.Remove(r.bufQueue.Front())
		require.Equal(t, asyncReadAheadMaxInFlight-1, r.bufQueue.Len())

		r.queueReadAhead()
		require.Equal(t, asyncReadAheadMaxInFlight, r.bufQueue.Len())
		require.Equal(t, queuedBefore+int64(asyncReadAheadChunkSize), r.queuedLen.Load())
	})

	t.Run("does not queue past total length", func(t *testing.T) {
		// length covers fewer than maxInFlight chunks; queueReadAhead must stop on
		// the length boundary even though there is still room in the pipeline.
		length := asyncReadAheadChunkSize * 2
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 2, r.bufQueue.Len())
		require.Equal(t, int64(length), r.queuedLen.Load())
		require.Less(t, r.bufQueue.Len(), asyncReadAheadMaxInFlight, "pipeline has room for more")

		r.queueReadAhead()
		require.Equal(t, 2, r.bufQueue.Len(), "no additional promise queued past length")
		require.Equal(t, int64(length), r.queuedLen.Load())
	})

	t.Run("last chunk shrinks to remaining bytes", func(t *testing.T) {
		// Two chunks total: one full, one half.
		length := asyncReadAheadChunkSize + asyncReadAheadChunkSize/2
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 2, r.bufQueue.Len())
		require.Equal(t, int64(length), r.queuedLen.Load())

		first := r.bufQueue.Front().Value
		last := r.bufQueue.Back().Value
		require.Equal(t, asyncReadAheadChunkSize, first.length)
		require.Equal(t, asyncReadAheadChunkSize/2, last.length)
		require.Equal(t, asyncReadAheadChunkSize, last.base, "last chunk follows directly after the first")
	})
}
