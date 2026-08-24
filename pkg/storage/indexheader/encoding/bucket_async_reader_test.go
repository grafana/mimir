// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

const (
	// testAsyncChunkSize is small enough that the 36-byte testBucketContents spans
	// three chunks of 16, 16 and 4 bytes, so every test crosses chunk boundaries.
	testAsyncChunkSize = 16

	testAsyncMaxInFlight = 3
)

var testAsyncBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, testAsyncChunkSize)
		return &b
	},
}

func newAsyncReaderOver(t *testing.T, bkt objstore.BucketReader, base, length int) *BucketAsyncBufReader {
	t.Helper()
	r := newBucketAsyncBufReader(
		t.Context(), &testAsyncBufPool, testAsyncChunkSize, testAsyncMaxInFlight,
		bkt, testBucketObjectName, base, length,
	)
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func newTestAsyncReader(t *testing.T, base, length int) (*BucketAsyncBufReader, *trackingBucket) {
	t.Helper()
	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)
	return newAsyncReaderOver(t, bkt, base, length), bkt
}

// newSizedTrackingBucket builds an object of exactly length bytes by repeating
// testBucketContents, so that a GetRange on any sub-range succeeds.
func newSizedTrackingBucket(t *testing.T, length int) *trackingBucket {
	t.Helper()
	objectData := make([]byte, length)
	for i := range objectData {
		objectData[i] = testBucketContents[i%len(testBucketContents)]
	}
	return newTrackingBucket(t, objectData)
}

func newSizedAsyncReader(t *testing.T, length int) *BucketAsyncBufReader {
	t.Helper()
	return newAsyncReaderOver(t, newSizedTrackingBucket(t, length), 0, length)
}

func newFailingAsyncReader(t *testing.T, sentinel error) *BucketAsyncBufReader {
	t.Helper()
	return newAsyncReaderOver(t, &failingBucket{err: sentinel}, 0, 10)
}

//
// Reads, peeks and skips.
//

func TestBucketAsyncBufReader_Read_Sequential(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 5, r.Offset())

	b, err = r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:10], b)
	require.Equal(t, 10, r.Offset())
}

func TestBucketAsyncBufReader_Read_ExactLength(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b)
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_Read_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	b, err := r.Read(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketAsyncBufReader_Read_BeyondEndAfterPartialConsumption(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	_, err := r.Read(3)
	require.NoError(t, err)

	b, err := r.Read(8)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketAsyncBufReader_ReadInto_Basic(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	buf := make([]byte, 5)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[:5], buf)
	require.Equal(t, 5, r.Offset())
}

func TestBucketAsyncBufReader_ReadInto_BeyondEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	err := r.ReadInto(make([]byte, sectionLen+1))
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketAsyncBufReader_ReadInto_SpansChunkBoundary(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	// 20 bytes at a chunk size of 16 crosses one boundary.
	buf := make([]byte, 20)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[:20], buf)
	require.Equal(t, 20, r.Offset())

	// The rest still reads contiguously.
	rest := make([]byte, len(testBucketContents)-20)
	require.NoError(t, r.ReadInto(rest))
	require.Equal(t, testBucketContents[20:], rest)
}

func TestBucketAsyncBufReader_ReadInto_SpansAllChunks(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	buf := make([]byte, len(testBucketContents))
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents, buf)
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_ReadInto_SmallStepsAcrossEveryChunk(t *testing.T) {
	for _, step := range []int{1, 3, 5, 7, 16, 17} {
		t.Run("step="+strconv.Itoa(step), func(t *testing.T) {
			r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

			got := make([]byte, 0, len(testBucketContents))
			for len(got) < len(testBucketContents) {
				n := min(step, len(testBucketContents)-len(got))
				buf := make([]byte, n)
				require.NoError(t, r.ReadInto(buf))
				got = append(got, buf...)
			}
			require.Equal(t, testBucketContents, got)
		})
	}
}

func TestBucketAsyncBufReader_Skip_Basic(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:13], b)
}

func TestBucketAsyncBufReader_Skip_AcrossChunkBoundary(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(20))
	require.Equal(t, 20, r.Offset())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[20:23], b)
}

func TestBucketAsyncBufReader_Skip_ToEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	require.NoError(t, r.Skip(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_Skip_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	require.ErrorIs(t, r.Skip(sectionLen+1), ErrInvalidSize)
}

func TestBucketAsyncBufReader_Peek_Basic(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	b, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 0, r.Offset(), "Peek does not advance the reader")

	got, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], got)
}

func TestBucketAsyncBufReader_Peek_PastSegmentEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	b, err := r.Peek(sectionLen + 5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:sectionLen], b)
	require.Equal(t, 0, r.Offset())
}

func TestBucketAsyncBufReader_Peek_AtEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestAsyncReader(t, 0, sectionLen)
	require.NoError(t, r.Skip(sectionLen))

	b, err := r.Peek(1)
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestBucketAsyncBufReader_Peek_SpansChunkBoundary_ThenRead(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	b, err := r.Peek(20)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:20], b)
	require.Equal(t, 0, r.Offset(), "a peek across a boundary still does not advance the reader")

	buf := make([]byte, 20)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[:20], buf)
	require.Equal(t, 20, r.Offset())

	rest := make([]byte, len(testBucketContents)-20)
	require.NoError(t, r.ReadInto(rest))
	require.Equal(t, testBucketContents[20:], rest)
}

func TestBucketAsyncBufReader_Peek_SpansBoundary_ThenSkip_ThenPeek(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	// Fill holdOver across the first boundary.
	b, err := r.Peek(20)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:20], b)

	// Consume part of holdOver, leaving 0 < available < the next peek.
	require.NoError(t, r.Skip(5))
	require.Equal(t, 5, r.Offset())

	// This peek takes the compaction path: it must not repeat the consumed bytes.
	b, err = r.Peek(18)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:23], b)
	require.Equal(t, 5, r.Offset())

	buf := make([]byte, 18)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[5:23], buf)
}

// TestBucketAsyncBufReader_Peek_SliceOutlivesSkipWithinChunk covers the pattern in
// Decbuf.UnsafeUvarintBytes: Peek, then Skip, then use the peeked slice.
func TestBucketAsyncBufReader_Peek_SliceOutlivesSkipWithinChunk(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	b, err := r.Peek(10)
	require.NoError(t, err)
	require.NoError(t, r.Skip(10))
	require.Equal(t, testBucketContents[:10], b, "the peeked slice is still valid after the Skip")
}

// TestBucketAsyncBufReader_Peek_SliceOutlivesChunkRetirement covers the same
// pattern when the Skip drains the chunk the slice points into. The retired chunk
// buffer must not go back to the pool while the slice is live.
func TestBucketAsyncBufReader_Peek_SliceOutlivesChunkRetirement(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(6))

	// A peek of exactly the bytes left in the first chunk takes the zero-copy path.
	b, err := r.Peek(10)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[6:16], b)

	// Drain the first chunk, then force the reader onto the second one.
	require.NoError(t, r.Skip(10))
	next, err := r.Peek(4)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[16:20], next)

	require.Equal(t, testBucketContents[6:16], b, "the retired chunk buffer was recycled under a live peek")
}

//
// Positioning.
//

func TestBucketAsyncBufReader_Reset(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

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

func TestBucketAsyncBufReader_ResetAt_Middle(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	require.NoError(t, r.ResetAt(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:15], b)
}

func TestBucketAsyncBufReader_ResetAt_End(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	require.NoError(t, r.ResetAt(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_ResetAt_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncReader(t, 0, sectionLen)

	require.ErrorIs(t, r.ResetAt(sectionLen+1), ErrInvalidSize)
}

// TestBucketAsyncBufReader_ResetAt_Backwards checks that a reposition builds a
// working pipeline again. tearDown cancels the context that covers the in-flight
// fetches, so the new generation needs a context of its own.
func TestBucketAsyncBufReader_ResetAt_Backwards(t *testing.T) {
	r, bkt := newTestAsyncReader(t, 0, len(testBucketContents))

	buf := make([]byte, len(testBucketContents))
	require.NoError(t, r.ReadInto(buf))
	callsBefore := len(bkt.calls())

	require.NoError(t, r.ResetAt(0))
	require.Equal(t, 0, r.Offset())

	again := make([]byte, len(testBucketContents))
	require.NoError(t, r.ReadInto(again))
	require.Equal(t, testBucketContents, again)
	require.Greater(t, len(bkt.calls()), callsBefore, "a backwards reset refetches")
}

func TestBucketAsyncBufReader_ResetAt_RepeatedGenerations(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	for _, off := range []int{0, 20, 0, 30, 5} {
		require.NoError(t, r.ResetAt(off))
		require.Equal(t, off, r.Offset())

		buf := make([]byte, 5)
		require.NoError(t, r.ReadInto(buf))
		require.Equal(t, testBucketContents[off:off+5], buf)
	}
}

func TestBucketAsyncBufReader_ResetAt_WithinBuffered_DoesNotRefetch(t *testing.T) {
	r, bkt := newTestAsyncReader(t, 0, len(testBucketContents))

	require.NoError(t, r.ReadInto(make([]byte, 4)))
	require.LessOrEqual(t, r.Buffered(), testAsyncChunkSize, "Buffered counts only the bytes in memory")

	callsBefore := len(bkt.calls())
	require.NoError(t, r.ResetAt(10))
	require.Equal(t, 10, r.Offset())
	require.Len(t, bkt.calls(), callsBefore, "a forward reset inside the buffer does not refetch")

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:13], b)
}

func TestBucketAsyncBufReader_Buffered_ExcludesChunksStillFilling(t *testing.T) {
	r := newSizedAsyncReader(t, testAsyncChunkSize*testAsyncMaxInFlight)

	// Nothing has been waited on yet, so nothing is in memory.
	require.Equal(t, 0, r.Buffered())

	require.NoError(t, r.ReadInto(make([]byte, 1)))
	require.Equal(t, testAsyncChunkSize-1, r.Buffered(),
		"only the current chunk counts, never the chunks still in flight")
}

//
// Sizes and lifecycle.
//

func TestBucketAsyncBufReader_Len(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))

	require.Equal(t, len(testBucketContents), r.Len())
	require.NoError(t, r.Skip(10))
	require.Equal(t, len(testBucketContents)-10, r.Len())
	_, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, len(testBucketContents)-15, r.Len())
}

func TestBucketAsyncBufReader_Size(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))
	require.Equal(t, testAsyncChunkSize, r.Size())
}

func TestBucketAsyncBufReader_Close(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Close())
}

func TestBucketAsyncBufReader_Close_Idempotent(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Close())
	require.NoError(t, r.Close())
}

func TestBucketAsyncBufReader_MethodsAfterClose_DoNotPanic(t *testing.T) {
	r, _ := newTestAsyncReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Close())

	require.NotPanics(t, func() {
		_ = r.Len()
		_ = r.Size()
		_ = r.Offset()
		_ = r.Buffered()
	})

	// A read after Close reports a size error rather than a panic or a new fetch.
	require.ErrorIs(t, r.ReadInto(make([]byte, 1)), ErrInvalidSize)
	b, err := r.Peek(1)
	require.NoError(t, err)
	require.Nil(t, b)
}

//
// Pipeline shape.
//

func TestBucketAsyncBufReader_QueueReadAhead(t *testing.T) {
	t.Run("single chunk when length below chunk size", func(t *testing.T) {
		length := testAsyncChunkSize / 4
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 1, r.queue.len(), "exactly one chunk covers a sub-chunk range")
		require.Equal(t, length, r.queueOff)
		require.Equal(t, length, r.queue.at(0).length)
	})

	t.Run("caps at maxInFlight when range exceeds maxInFlight chunks", func(t *testing.T) {
		length := testAsyncChunkSize * (testAsyncMaxInFlight + 2)
		r := newSizedAsyncReader(t, length)

		require.Equal(t, testAsyncMaxInFlight, r.queue.len())
		require.Equal(t, testAsyncChunkSize*testAsyncMaxInFlight, r.queueOff)

		for i := 0; i < r.queue.len(); i++ {
			p := r.queue.at(i)
			require.Equal(t, testAsyncChunkSize, p.length)
			require.Equal(t, i*testAsyncChunkSize, p.base)
		}
	})

	t.Run("refills queue after a drained chunk is retired", func(t *testing.T) {
		length := testAsyncChunkSize * (testAsyncMaxInFlight + 2)
		r := newSizedAsyncReader(t, length)

		require.Equal(t, testAsyncMaxInFlight, r.queue.len())
		queuedBefore := r.queueOff

		// Stand in for a read that drained and retired the front chunk.
		retired := r.queue.pop()
		require.NotNil(t, retired)
		retired.release()
		require.Equal(t, testAsyncMaxInFlight-1, r.queue.len())

		r.queueReadAhead()
		require.Equal(t, testAsyncMaxInFlight, r.queue.len())
		require.Equal(t, queuedBefore+testAsyncChunkSize, r.queueOff)
	})

	t.Run("does not queue past total length", func(t *testing.T) {
		length := testAsyncChunkSize * 2
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 2, r.queue.len())
		require.Equal(t, length, r.queueOff)
		require.Less(t, r.queue.len(), testAsyncMaxInFlight, "the pipeline still has room")

		r.queueReadAhead()
		require.Equal(t, 2, r.queue.len(), "no chunk queued past the end of the range")
		require.Equal(t, length, r.queueOff)
	})

	t.Run("last chunk shrinks to remaining bytes", func(t *testing.T) {
		length := testAsyncChunkSize + testAsyncChunkSize/2
		r := newSizedAsyncReader(t, length)

		require.Equal(t, 2, r.queue.len())
		require.Equal(t, length, r.queueOff)
		require.Equal(t, testAsyncChunkSize, r.queue.at(0).length)
		require.Equal(t, testAsyncChunkSize/2, r.queue.at(1).length)
		require.Equal(t, testAsyncChunkSize, r.queue.at(1).base)
	})
}

func TestBucketAsyncBufReader_Pipelining_IssuesConcurrentGetRange(t *testing.T) {
	length := testAsyncChunkSize * (testAsyncMaxInFlight + 2)
	gated := newGatedBucket(t, newSizedTrackingBucket(t, length))
	r := newAsyncReaderOver(t, gated, 0, length)

	gated.waitEntered(t, testAsyncMaxInFlight)
	require.Equal(t, testAsyncMaxInFlight, gated.peakInFlight(),
		"the pipeline keeps maxInFlight fetches running at once")

	gated.releaseAll()

	buf := make([]byte, length)
	require.NoError(t, r.ReadInto(buf))
	require.LessOrEqual(t, gated.peakInFlight(), testAsyncMaxInFlight,
		"the pipeline never exceeds maxInFlight")
}

// TestBucketAsyncBufReader_Close_WhileFillsInFlight checks that Close cancels the
// fetches instead of waiting for them. Without the cancel it blocks on the tail
// latency of requests whose bytes nobody will read.
func TestBucketAsyncBufReader_Close_WhileFillsInFlight(t *testing.T) {
	length := testAsyncChunkSize * testAsyncMaxInFlight
	gated := newGatedBucket(t, newSizedTrackingBucket(t, length))
	r := newAsyncReaderOver(t, gated, 0, length)

	gated.waitEntered(t, testAsyncMaxInFlight)

	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("Close blocked while fetches were still gated")
	}
}

//
// Failure handling.
//

func TestBucketAsyncBufReader_Read_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingAsyncReader(t, sentinel)

	_, err := r.Read(5)
	require.ErrorIs(t, err, sentinel)
}

func TestBucketAsyncBufReader_ReadInto_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingAsyncReader(t, sentinel)

	require.ErrorIs(t, r.ReadInto(make([]byte, 5)), sentinel)
}

// TestBucketAsyncBufReader_Error_MidPipeline_Propagates fails a chunk that is not
// the first one. The reader must report it rather than move on to the chunk after.
func TestBucketAsyncBufReader_Error_MidPipeline_Propagates(t *testing.T) {
	sentinel := errors.New("storage error")
	bkt := &failingAtOffsetBucket{
		InstrumentedBucketReader: newTrackingBucket(t, testBucketContents),
		failFrom:                 testAsyncChunkSize,
		err:                      sentinel,
	}
	r := newAsyncReaderOver(t, bkt, 0, len(testBucketContents))

	require.NoError(t, r.ReadInto(make([]byte, testAsyncChunkSize)), "the first chunk succeeds")
	require.ErrorIs(t, r.ReadInto(make([]byte, 4)), sentinel)
}

// TestBucketAsyncBufReader_Error_WithPartialData_Propagates covers a transfer that
// breaks part way through a chunk. io.ReadFull returns bytes and an error together,
// and the error must not wait until those bytes are consumed.
func TestBucketAsyncBufReader_Error_WithPartialData_Propagates(t *testing.T) {
	sentinel := errors.New("storage error")
	bkt := &partialThenErrorBucket{
		InstrumentedBucketReader: newTrackingBucket(t, testBucketContents),
		failAt:                   testAsyncChunkSize,
		keep:                     8,
		err:                      sentinel,
	}
	r := newAsyncReaderOver(t, bkt, 0, len(testBucketContents))

	require.NoError(t, r.ReadInto(make([]byte, testAsyncChunkSize)))
	require.ErrorIs(t, r.ReadInto(make([]byte, 4)), sentinel,
		"the partial bytes must not be served ahead of the error")
}

func TestBucketAsyncBufReader_Error_IsSticky(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingAsyncReader(t, sentinel)

	require.ErrorIs(t, r.ReadInto(make([]byte, 5)), sentinel)
	require.ErrorIs(t, r.ReadInto(make([]byte, 5)), sentinel, "the error is reported again")
	_, err := r.Peek(1)
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, r.Skip(1), sentinel)
}

// TestBucketAsyncBufReader_ShortRead_MidPipeline_DoesNotSilentlySkip is the
// regression test for the offset skew. Chunk offsets are computed from requested
// lengths, so a truncated chunk in the middle must end the stream. A move to the
// next chunk would join two ranges that are not adjacent and report no error.
func TestBucketAsyncBufReader_ShortRead_MidPipeline_DoesNotSilentlySkip(t *testing.T) {
	bkt := &truncatingBucket{
		InstrumentedBucketReader: newTrackingBucket(t, testBucketContents),
		truncateAt:               testAsyncChunkSize,
		keep:                     8,
	}
	r := newAsyncReaderOver(t, bkt, 0, len(testBucketContents))

	got := make([]byte, 0, len(testBucketContents))
	var err error
	for {
		buf := make([]byte, 4)
		if err = r.ReadInto(buf); err != nil {
			break
		}
		got = append(got, buf...)
	}

	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, testBucketContents[:testAsyncChunkSize+8], got,
		"the reader stops at the truncation instead of splicing in a later chunk")
}

// TestBucketAsyncBufReader_ShortRead_LastChunk_IsCleanEOF covers the legitimate
// case where the configured length overshoots the object.
func TestBucketAsyncBufReader_ShortRead_LastChunk_IsCleanEOF(t *testing.T) {
	overshoot := len(testBucketContents) + 4
	r, _ := newTestAsyncReader(t, 0, overshoot)

	buf := make([]byte, len(testBucketContents))
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents, buf)

	err := r.ReadInto(make([]byte, 1))
	require.ErrorIs(t, err, ErrInvalidSize, "the end of the object is a size error, not a storage error")
}

// TestBucketAsyncBufReader_Skip_AfterShortRead_ReturnsErrInvalidSize checks that a
// skip which runs dry reports a size error. Callers such as
// index.PostingsOffsetTable match on ErrInvalidSize and miss a raw io.EOF.
func TestBucketAsyncBufReader_Skip_AfterShortRead_ReturnsErrInvalidSize(t *testing.T) {
	bkt := &truncatingBucket{
		InstrumentedBucketReader: newTrackingBucket(t, testBucketContents),
		truncateAt:               testAsyncChunkSize,
		keep:                     8,
	}
	r := newAsyncReaderOver(t, bkt, 0, len(testBucketContents))

	// Len still reports the configured length, so the bounds check passes and the
	// skip loop is what runs dry.
	err := r.Skip(len(testBucketContents))
	require.ErrorIs(t, err, ErrInvalidSize)
	require.NotErrorIs(t, err, io.EOF)
}

//
// Resource accounting.
//

func TestBucketAsyncBufReader_NoLeakedReadClosers(t *testing.T) {
	length := testAsyncChunkSize * (testAsyncMaxInFlight + 2)
	counting := &countingBucket{InstrumentedBucketReader: newSizedTrackingBucket(t, length)}

	t.Run("after a full read", func(t *testing.T) {
		r := newAsyncReaderOver(t, counting, 0, length)
		require.NoError(t, r.ReadInto(make([]byte, length)))
		require.NoError(t, r.Close())
		require.Equal(t, 0, counting.openCount())
	})

	t.Run("after an early close", func(t *testing.T) {
		r := newAsyncReaderOver(t, counting, 0, length)
		require.NoError(t, r.ReadInto(make([]byte, 4)))
		require.NoError(t, r.Close())
		require.Equal(t, 0, counting.openCount())
	})
}

// TestBucketAsyncBufReader_ReleasesEveryPromise checks the structural half of
// buffer accounting: once the reader is torn down it retains no promise, so every
// chunk buffer went back to the pool.
func TestBucketAsyncBufReader_ReleasesEveryPromise(t *testing.T) {
	length := testAsyncChunkSize * (testAsyncMaxInFlight + 3)

	requireNoPromisesRetained := func(t *testing.T, r *BucketAsyncBufReader) {
		t.Helper()
		require.Nil(t, r.curr)
		require.Nil(t, r.prev)
		require.Equal(t, 0, r.queue.len())
	}

	t.Run("after Close following a full read", func(t *testing.T) {
		r := newSizedAsyncReader(t, length)
		require.NoError(t, r.ReadInto(make([]byte, length)))
		require.NoError(t, r.Close())
		requireNoPromisesRetained(t, r)
	})

	t.Run("after Close part way through a read", func(t *testing.T) {
		r := newSizedAsyncReader(t, length)
		require.NoError(t, r.ReadInto(make([]byte, testAsyncChunkSize+4)))
		require.NoError(t, r.Close())
		requireNoPromisesRetained(t, r)
	})

	t.Run("after a failed fetch", func(t *testing.T) {
		sentinel := errors.New("storage error")
		bkt := &failingAtOffsetBucket{
			InstrumentedBucketReader: newSizedTrackingBucket(t, length),
			failFrom:                 testAsyncChunkSize,
			err:                      sentinel,
		}
		r := newAsyncReaderOver(t, bkt, 0, length)
		require.ErrorIs(t, r.ReadInto(make([]byte, length)), sentinel)
		require.NoError(t, r.Close())
		requireNoPromisesRetained(t, r)
	})
}

// TestBucketAsyncBufReader_BufferPool_ReusesBuffers reads far more chunks than the
// pipeline holds. A reader that returns its buffers allocates a number that does
// not scale with the length of the range.
//
// The bound is loose on purpose. A plain build allocates exactly maxInFlight plus
// one buffer here, but sync.Pool keeps a shard per P, and under the race detector
// a Get often misses a buffer that a Put left on another shard. That raises the
// count to about a quarter of the chunks. A reader that leaked its buffers would
// allocate one per chunk, well past this bound.
func TestBucketAsyncBufReader_BufferPool_ReusesBuffers(t *testing.T) {
	const chunks = 200
	length := testAsyncChunkSize * chunks
	bkt := newSizedTrackingBucket(t, length)
	pool := newAllocCountingPool(testAsyncChunkSize)

	newReader := func() *BucketAsyncBufReader {
		r := newBucketAsyncBufReader(
			t.Context(), &pool.pool, testAsyncChunkSize, testAsyncMaxInFlight,
			bkt, testBucketObjectName, 0, length,
		)
		t.Cleanup(func() { _ = r.Close() })
		return r
	}

	r := newReader()
	require.NoError(t, r.ReadInto(make([]byte, length)))
	require.NoError(t, r.Close())

	afterFirstRead := pool.allocs()
	t.Logf("allocations after one full read of %d chunks: %d", chunks, afterFirstRead)
	require.Less(t, afterFirstRead, chunks/2, "a full read must reuse its buffers")

	// A second reader over the same pool must find the buffers that the first one
	// returned, on Close and on the teardown inside ResetAt alike. This phase reads
	// the range twice, so it covers 2*chunks chunks.
	r2 := newReader()
	require.NoError(t, r2.ReadInto(make([]byte, length)))
	require.NoError(t, r2.ResetAt(0))
	require.NoError(t, r2.ReadInto(make([]byte, length)))
	require.NoError(t, r2.Close())

	t.Logf("allocations for two more full reads: %d", pool.allocs()-afterFirstRead)
	require.Less(t, pool.allocs()-afterFirstRead, chunks, "Close and ResetAt return every buffer")
}

//
// End to end through Decbuf.
//

// TestBucketAsyncBufReader_Decbuf_MatchesSyncReader decodes the same payload
// through both bucket readers and compares the results. The payload spans several
// chunks, and it holds one byte slice longer than Size() so that both the Peek and
// the Read branch of Decbuf.UnsafeUvarintBytes run.
func TestBucketAsyncBufReader_Decbuf_MatchesSyncReader(t *testing.T) {
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	strs := []string{"alpha", "beta", "a-symbol-name-that-is-longer-than-one-chunk", "z"}

	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, 0xDEADBEEF)
	payload = binary.BigEndian.AppendUint64(payload, 0x0102030405060708)
	payload = append(payload, 0x7F)
	payload = binary.AppendUvarint(payload, 300)
	for _, s := range strs {
		payload = binary.AppendUvarint(payload, uint64(len(s)))
		payload = append(payload, s...)
	}
	require.Greater(t, len(payload), testAsyncChunkSize*2, "the payload must span several chunks")

	decode := func(t *testing.T, d *Decbuf) []any {
		t.Helper()
		out := []any{d.Be32(), d.Be64(), d.Byte(), d.Uvarint64()}
		for range strs {
			out = append(out, d.UvarintStr())
		}
		require.NoError(t, d.Err())
		return out
	}

	bkt := newTrackingBucket(t, payload)

	syncReader := newBucketBufReader(t.Context(), &testBucketBufPool, bkt, testBucketObjectName, 0, len(payload))
	t.Cleanup(func() { _ = syncReader.Close() })
	syncDecbuf := Decbuf{r: syncReader}
	want := decode(t, &syncDecbuf)

	asyncDecbuf := Decbuf{r: newAsyncReaderOver(t, bkt, 0, len(payload))}
	got := decode(t, &asyncDecbuf)

	require.Equal(t, want, got)
	require.Equal(t, []any{
		uint32(0xDEADBEEF), uint64(0x0102030405060708), byte(0x7F), uint64(300),
		strs[0], strs[1], strs[2], strs[3],
	}, got)

	t.Run("CheckCrc32", func(t *testing.T) {
		checked := binary.BigEndian.AppendUint32(slices.Clone(payload), crc32.Checksum(payload, castagnoli))
		crcBkt := newTrackingBucket(t, checked)

		d := Decbuf{r: newAsyncReaderOver(t, crcBkt, 0, len(checked))}
		d.CheckCrc32(castagnoli)
		require.NoError(t, d.Err())
	})

	t.Run("ResetAt rereads the same values", func(t *testing.T) {
		d := Decbuf{r: newAsyncReaderOver(t, bkt, 0, len(payload))}
		require.Equal(t, want, decode(t, &d))
		d.ResetAt(0)
		require.NoError(t, d.Err())
		require.Equal(t, want, decode(t, &d))
	})
}

//
// Test buckets and pools.
//

// limitedReadCloser applies a Reader over the body of another ReadCloser while it
// still closes the original.
type limitedReadCloser struct {
	io.Reader
	closer io.Closer
}

func (rc limitedReadCloser) Close() error { return rc.closer.Close() }

// truncatingBucket delivers fewer bytes than requested for the range at
// truncateAt. The in-memory bucket clamps at the end of the object, so it cannot
// produce a short read in the middle of one.
type truncatingBucket struct {
	objstore.InstrumentedBucketReader
	truncateAt int64
	keep       int64
}

func (b *truncatingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	rc, err := b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
	if err != nil || off != b.truncateAt {
		return rc, err
	}
	return limitedReadCloser{Reader: io.LimitReader(rc, b.keep), closer: rc}, nil
}

// failingAtOffsetBucket fails every range at or past failFrom, so that a chunk
// other than the first one is the one that fails.
type failingAtOffsetBucket struct {
	objstore.InstrumentedBucketReader
	failFrom int64
	err      error
}

func (b *failingAtOffsetBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if off >= b.failFrom {
		return nil, b.err
	}
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

// partialThenErrorBucket serves the range at failAt as keep bytes followed by an
// error, the shape io.ReadFull produces when a transfer breaks part way through.
type partialThenErrorBucket struct {
	objstore.InstrumentedBucketReader
	failAt int64
	keep   int64
	err    error
}

func (b *partialThenErrorBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	rc, err := b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
	if err != nil || off != b.failAt {
		return rc, err
	}
	body := io.MultiReader(io.LimitReader(rc, b.keep), errReader{err: b.err})
	return limitedReadCloser{Reader: body, closer: rc}, nil
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// gatedBucket holds every GetRange call until the test releases it, and records
// the highest number of calls that waited at the same time.
type gatedBucket struct {
	objstore.InstrumentedBucketReader

	release     chan struct{}
	releaseOnce sync.Once
	entered     chan struct{}

	mtx      sync.Mutex
	inFlight int
	peak     int
}

func newGatedBucket(t *testing.T, inner objstore.InstrumentedBucketReader) *gatedBucket {
	t.Helper()
	b := &gatedBucket{
		InstrumentedBucketReader: inner,
		release:                  make(chan struct{}),
		entered:                  make(chan struct{}, 128),
	}
	t.Cleanup(b.releaseAll)
	return b
}

func (b *gatedBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.enter()
	select {
	case b.entered <- struct{}{}:
	default:
	}

	select {
	case <-b.release:
		b.leave()
	case <-ctx.Done():
		b.leave()
		return nil, context.Cause(ctx)
	}
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

func (b *gatedBucket) enter() {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.inFlight++
	b.peak = max(b.peak, b.inFlight)
}

func (b *gatedBucket) leave() {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.inFlight--
}

func (b *gatedBucket) peakInFlight() int {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.peak
}

func (b *gatedBucket) releaseAll() {
	b.releaseOnce.Do(func() { close(b.release) })
}

// waitEntered blocks until n GetRange calls have reached the gate.
func (b *gatedBucket) waitEntered(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-b.entered:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for GetRange call %d of %d", i+1, n)
		}
	}
}

// countingBucket counts the ReadClosers it handed out that are not closed yet.
type countingBucket struct {
	objstore.InstrumentedBucketReader

	mtx  sync.Mutex
	open int
}

func (b *countingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	rc, err := b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}
	b.mtx.Lock()
	b.open++
	b.mtx.Unlock()
	return countingReadCloser{ReadCloser: rc, bkt: b}, nil
}

func (b *countingBucket) openCount() int {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.open
}

type countingReadCloser struct {
	io.ReadCloser
	bkt *countingBucket
}

func (rc countingReadCloser) Close() error {
	rc.bkt.mtx.Lock()
	rc.bkt.open--
	rc.bkt.mtx.Unlock()
	return rc.ReadCloser.Close()
}

// allocCountingPool is a chunk buffer pool that counts the buffers it allocated.
// A reader that returns its buffers allocates a bounded number of them, whatever
// the length of the range it reads.
type allocCountingPool struct {
	pool sync.Pool

	mtx sync.Mutex
	n   int
}

func newAllocCountingPool(chunkSize int) *allocCountingPool {
	p := &allocCountingPool{}
	p.pool.New = func() any {
		p.mtx.Lock()
		p.n++
		p.mtx.Unlock()
		b := make([]byte, chunkSize)
		return &b
	}
	return p
}

func (p *allocCountingPool) allocs() int {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.n
}
