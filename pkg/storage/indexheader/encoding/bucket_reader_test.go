// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

// testBucketContents is the 20-byte payload used throughout bucket reader tests.
var testBucketContents = []byte("abcdefghij1234567890")

const testBucketObjectName = "test-object"

// trackingBucket wraps an InstrumentedBucketReader and records every GetRange call.
type trackingBucket struct {
	objstore.InstrumentedBucketReader
	calls []rangeCall
}

type rangeCall struct {
	off    int64
	length int64
}

func (b *trackingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.calls = append(b.calls, rangeCall{off, length})
	return b.InstrumentedBucketReader.GetRange(ctx, name, off, length)
}

// newTrackingBucket uploads objectData to an in-memory bucket and returns a
// tracking wrapper around it.
func newTrackingBucket(t *testing.T, objectData []byte) *trackingBucket {
	t.Helper()
	inmem := objstore.NewInMemBucket()
	require.NoError(t, inmem.Upload(context.Background(), testBucketObjectName, bytes.NewReader(objectData)))
	return &trackingBucket{InstrumentedBucketReader: objstore.WithNoopInstr(inmem)}
}

// newTestReader returns a BucketReader whose data segment is testBucketContents,
// starting at base bytes into the object.
func newTestReader(t *testing.T, base int64) (*BucketReader, *trackingBucket) {
	t.Helper()
	padding := make([]byte, base)
	objectData := append(padding, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)
	r := NewBucketReader(context.Background(), bkt, testBucketObjectName, base, len(testBucketContents))
	return r, bkt
}

// newFullyBufferedReader returns a BucketReader with all of testBucketContents
// already loaded into the buffer, starting at base bytes into the object.
func newFullyBufferedReader(t *testing.T, base int64) (*BucketReader, *trackingBucket) {
	t.Helper()
	r, bkt := newTestReader(t, base)
	require.NoError(t, r.BufferTo(len(testBucketContents)))
	return r, bkt
}

// ---- BufferTo ---------------------------------------------------------------

func TestBucketReader_BufferTo_ExtendsBuffer(t *testing.T) {
	r, bkt := newTestReader(t, 0)

	require.NoError(t, r.BufferTo(10))
	require.Len(t, bkt.calls, 1)
	require.Equal(t, rangeCall{off: 0, length: 10}, bkt.calls[0])
	require.Equal(t, 10, r.Size())
}

func TestBucketReader_BufferTo_IdempotentWhenAlreadyBuffered(t *testing.T) {
	r, bkt := newTestReader(t, 0)

	require.NoError(t, r.BufferTo(10))
	require.NoError(t, r.BufferTo(10)) // same offset again
	require.NoError(t, r.BufferTo(5))  // smaller offset
	require.Len(t, bkt.calls, 1, "only the first call should issue a GetRange")
	require.Equal(t, 10, r.Size())
}

func TestBucketReader_BufferTo_IncrementalFetches(t *testing.T) {
	r, bkt := newTestReader(t, 0)

	require.NoError(t, r.BufferTo(5))
	require.NoError(t, r.BufferTo(10))
	require.NoError(t, r.BufferTo(20))

	require.Len(t, bkt.calls, 3)
	require.Equal(t, rangeCall{off: 0, length: 5}, bkt.calls[0], "first fetch: bytes 0–4")
	require.Equal(t, rangeCall{off: 5, length: 5}, bkt.calls[1], "second fetch: bytes 5–9")
	require.Equal(t, rangeCall{off: 10, length: 10}, bkt.calls[2], "third fetch: bytes 10–19")
	require.Equal(t, 20, r.Size())
}

func TestBucketReader_BufferTo_ErrorWhenOffsetBeforeCurrentCursor(t *testing.T) {
	r, _ := newTestReader(t, 0)

	require.NoError(t, r.BufferTo(15))
	require.NoError(t, r.Skip(10))

	err := r.BufferTo(5)
	require.Error(t, err, "buffering to an offset before the cursor must fail")
}

func TestBucketReader_BufferTo_ErrorOnCursorPositionEvenWhenBuffered(t *testing.T) {
	// Bytes 0–14 are already in the buffer, cursor is at 10.
	// BufferTo(8) < cursor(10) must error even though offset 8 is already buffered.
	r, _ := newTestReader(t, 0)

	require.NoError(t, r.BufferTo(15))
	require.NoError(t, r.Skip(10))

	require.Error(t, r.BufferTo(8))
}

func TestBucketReader_BufferTo_AbsoluteOffsetsWithNonZeroBase(t *testing.T) {
	const base = 7
	r, bkt := newTestReader(t, base)

	require.NoError(t, r.BufferTo(8))
	require.NoError(t, r.BufferTo(15))

	require.Len(t, bkt.calls, 2)
	require.Equal(t, rangeCall{off: base + 0, length: 8}, bkt.calls[0], "first fetch absolute offset")
	require.Equal(t, rangeCall{off: base + 8, length: 7}, bkt.calls[1], "second fetch absolute offset")
}

func TestBucketReader_BufferTo_PropagatesBucketError(t *testing.T) {
	sentinel := errors.New("bucket unavailable")
	failing := &failingBucket{err: sentinel}
	r := NewBucketReader(context.Background(), failing, testBucketObjectName, 0, 20)

	require.ErrorIs(t, r.BufferTo(10), sentinel)
}

// ---- Read -------------------------------------------------------------------

func TestBucketReader_Read_SequentialReads(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), b, "first read")

	b, err = r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), b, "second read")

	b, err = r.Read(10)
	require.NoError(t, err)
	require.Equal(t, []byte("1234567890"), b, "third read")
}

func TestBucketReader_Read_BeyondSegmentEnd(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)
	require.NoError(t, r.Skip(12))

	b, err := r.Read(10) // only 8 remain in segment
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Empty(t, b)
	require.Equal(t, 20, r.Offset(), "cursor consumed to end after read beyond segment")

	b, err = r.Read(1) // cursor already at end
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Empty(t, b)
}

func TestBucketReader_Read_DataNotBuffered(t *testing.T) {
	r, _ := newTestReader(t, 0) // buffer is empty

	b, err := r.Read(5)
	require.ErrorIs(t, err, ErrInvalidSize, "read from empty buffer must fail")
	require.Empty(t, b)
	require.Equal(t, 20, r.Offset(), "cursor consumed to end after failed read")
}

func TestBucketReader_Read_PartiallyBuffered(t *testing.T) {
	r, _ := newTestReader(t, 0)
	require.NoError(t, r.BufferTo(5)) // only first 5 bytes buffered

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), b, "read within buffered range")

	b, err = r.Read(1) // byte 5 is not buffered
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Empty(t, b)
}

func TestBucketReader_Read_ExactSegmentLength(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b)
	require.Equal(t, 0, r.Len())
}

func TestBucketReader_Read_ReturnedSliceIsIndependentCopy(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	b, err := r.Read(5)
	require.NoError(t, err)

	// Mutating the returned slice must not affect subsequent reads.
	b[0] = 'Z'

	r2, _ := newFullyBufferedReader(t, 0)
	b2, err := r2.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), b2, "buffer must not be affected by mutation of returned slice")
}

// ---- ReadInto ---------------------------------------------------------------

func TestBucketReader_ReadInto_SequentialReads(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	buf := make([]byte, 5)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, []byte("abcde"), buf)

	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, []byte("fghij"), buf)
}

func TestBucketReader_ReadInto_BeyondSegmentEnd(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)
	require.NoError(t, r.Skip(15))

	buf := make([]byte, 10) // only 5 remain
	require.ErrorIs(t, r.ReadInto(buf), ErrInvalidSize)
	require.Equal(t, 20, r.Offset())

	require.ErrorIs(t, r.ReadInto(make([]byte, 1)), ErrInvalidSize, "subsequent read after cursor at end")
}

func TestBucketReader_ReadInto_DataNotBuffered(t *testing.T) {
	r, _ := newTestReader(t, 0)

	buf := make([]byte, 5)
	require.ErrorIs(t, r.ReadInto(buf), ErrInvalidSize)
	require.Equal(t, 20, r.Offset())
}

// ---- Peek -------------------------------------------------------------------

func TestBucketReader_Peek_DoesNotAdvanceCursor(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	p1, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), p1)

	p2, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), p2, "second peek must return same bytes")

	require.Equal(t, 0, r.Offset(), "cursor must not move")
}

func TestBucketReader_Peek_AfterRead(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	_, err := r.Read(5)
	require.NoError(t, err)

	p, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), p, "peek after read must start from new cursor position")
}

func TestBucketReader_Peek_BeyondBufferedEndReturnsAvailable(t *testing.T) {
	r, _ := newTestReader(t, 0)
	require.NoError(t, r.BufferTo(8)) // only 8 bytes buffered

	p, err := r.Peek(20) // request more than buffered
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:8], p, "peek must return only buffered bytes")
}

func TestBucketReader_Peek_NothingBuffered(t *testing.T) {
	r, _ := newTestReader(t, 0)

	p, err := r.Peek(5)
	require.NoError(t, err)
	require.Empty(t, p)
}

func TestBucketReader_Peek_AtEndOfSegment(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)
	require.NoError(t, r.Skip(len(testBucketContents)))

	p, err := r.Peek(1)
	require.NoError(t, err)
	require.Empty(t, p)
}

func TestBucketReader_Peek_BeyondSegmentEndReturnsAvailable(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	p, err := r.Peek(len(testBucketContents) + 100)
	require.NoError(t, err)
	require.Equal(t, testBucketContents, p, "peek beyond segment end must return all remaining bytes")
}

// ---- Reset ------------------------------------------------------------------

func TestBucketReader_Reset_MovesCursorToBeginning(t *testing.T) {
	r, bkt := newFullyBufferedReader(t, 0)
	callsBefore := len(bkt.calls)

	_, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, 10, r.Offset())

	require.NoError(t, r.Reset())
	require.Equal(t, 0, r.Offset())
	require.Len(t, bkt.calls, callsBefore, "Reset must not issue additional GetRange calls")
}

func TestBucketReader_Reset_BufferRetained(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	_, err := r.Read(10)
	require.NoError(t, err)

	require.NoError(t, r.Reset())

	// Re-reading after Reset should work without calling BufferTo again.
	b, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:10], b)
}

// ---- ResetAt ----------------------------------------------------------------

func TestBucketReader_ResetAt_MovesToArbitraryOffset(t *testing.T) {
	r, bkt := newFullyBufferedReader(t, 0)
	callsBefore := len(bkt.calls)

	require.NoError(t, r.ResetAt(5))
	require.Equal(t, 5, r.Offset())

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), b)
	require.Len(t, bkt.calls, callsBefore, "ResetAt must not issue GetRange calls")
}

func TestBucketReader_ResetAt_ToZero(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	require.NoError(t, r.Skip(10))
	require.NoError(t, r.ResetAt(0))
	require.Equal(t, 0, r.Offset())
}

func TestBucketReader_ResetAt_ToEndOfSegment(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	require.NoError(t, r.ResetAt(len(testBucketContents)))
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketReader_ResetAt_BeyondSegmentLength(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	require.ErrorIs(t, r.ResetAt(len(testBucketContents)+1), ErrInvalidSize)
}

func TestBucketReader_ResetAt_BackwardThenReadBufferedData(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	_, err := r.Read(15)
	require.NoError(t, err)

	// Move cursor backward: data is still in buffer.
	require.NoError(t, r.ResetAt(5))

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), b, "previously buffered data must be readable after backward ResetAt")
}

// ---- Skip -------------------------------------------------------------------

func TestBucketReader_Skip_AdvancesCursor(t *testing.T) {
	r, _ := newTestReader(t, 0) // no buffering needed for Skip

	require.Equal(t, 20, r.Len())
	require.NoError(t, r.Skip(5))
	require.Equal(t, 5, r.Offset())
	require.Equal(t, 15, r.Len())
}

func TestBucketReader_Skip_DoesNotRequireBuffering(t *testing.T) {
	r, bkt := newTestReader(t, 0) // buffer is empty

	require.NoError(t, r.Skip(10))
	require.Empty(t, bkt.calls, "Skip must not trigger any GetRange calls")
	require.Equal(t, 10, r.Offset())
}

func TestBucketReader_Skip_ToExactEnd(t *testing.T) {
	r, _ := newTestReader(t, 0)

	require.NoError(t, r.Skip(len(testBucketContents)))
	require.Equal(t, 0, r.Len())
}

func TestBucketReader_Skip_BeyondEnd(t *testing.T) {
	r, _ := newTestReader(t, 0)

	require.ErrorIs(t, r.Skip(len(testBucketContents)+1), ErrInvalidSize)
}

func TestBucketReader_Skip_InterleaveWithBufferTo(t *testing.T) {
	r, _ := newTestReader(t, 0)
	require.NoError(t, r.BufferTo(10))

	require.NoError(t, r.Skip(5))
	require.Equal(t, 5, r.Offset())

	// Read the next 5 buffered bytes.
	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij"), b)

	// Skip beyond the buffered range: cursor advances, but no GetRange.
	require.NoError(t, r.Skip(5))
	require.Equal(t, 15, r.Offset())
}

// ---- Len --------------------------------------------------------------------

func TestBucketReader_Len_TracksRemainingSegment(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	require.Equal(t, 20, r.Len(), "initial")

	_, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, 15, r.Len(), "after first read")

	_, err = r.Peek(3)
	require.NoError(t, err)
	require.Equal(t, 15, r.Len(), "peek must not change Len")

	_, err = r.Read(100)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, 0, r.Len(), "after read beyond end")

	require.NoError(t, r.Reset())
	require.Equal(t, 20, r.Len(), "after Reset")

	require.NoError(t, r.ResetAt(7))
	require.Equal(t, 13, r.Len(), "after ResetAt")
}

// ---- Offset -----------------------------------------------------------------

func TestBucketReader_Offset_TracksPosition(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)

	require.Equal(t, 0, r.Offset(), "initial")

	_, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, 5, r.Offset(), "after first read")

	_, err = r.Read(2)
	require.NoError(t, err)
	require.Equal(t, 7, r.Offset(), "after second read")

	_, err = r.Peek(3)
	require.NoError(t, err)
	require.Equal(t, 7, r.Offset(), "peek must not change Offset")

	_, err = r.Read(100)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, 20, r.Offset(), "after read beyond end")

	require.NoError(t, r.Reset())
	require.Equal(t, 0, r.Offset(), "after Reset")

	require.NoError(t, r.ResetAt(3))
	require.Equal(t, 3, r.Offset(), "after ResetAt")
}

// ---- Buffered ---------------------------------------------------------------

func TestBucketReader_Buffered_ReflectsInMemoryAvailability(t *testing.T) {
	r, _ := newTestReader(t, 0)

	require.Equal(t, 0, r.Buffered(), "initially nothing is buffered")

	require.NoError(t, r.BufferTo(10))
	require.Equal(t, 10, r.Buffered(), "after BufferTo(10)")

	_, err := r.Read(4)
	require.NoError(t, err)
	require.Equal(t, 6, r.Buffered(), "after reading 4 bytes, 6 buffered bytes remain ahead")

	require.NoError(t, r.BufferTo(20))
	require.Equal(t, 16, r.Buffered(), "after extending buffer to 20, 16 remain ahead of cursor")
}

func TestBucketReader_Buffered_AfterResetBufferIsRetained(t *testing.T) {
	r, _ := newTestReader(t, 0)
	require.NoError(t, r.BufferTo(15))
	_, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, 5, r.Buffered())

	require.NoError(t, r.Reset())
	require.Equal(t, 15, r.Buffered(), "reset to 0: all 15 buffered bytes are ahead of cursor again")
}

func TestBucketReader_Buffered_CursorBeyondBufferIsZero(t *testing.T) {
	r, _ := newTestReader(t, 0)
	require.NoError(t, r.BufferTo(5))

	// Skip past the buffered region without buffering more.
	require.NoError(t, r.Skip(5))
	require.Equal(t, 0, r.Buffered())

	require.NoError(t, r.Skip(5))
	require.Equal(t, 0, r.Buffered(), "cursor beyond buffer must clamp to zero")
}

// ---- Size -------------------------------------------------------------------

func TestBucketReader_Size_GrowsWithBuffer(t *testing.T) {
	r, _ := newTestReader(t, 0)

	require.Equal(t, 0, r.Size(), "initially no bytes buffered")

	require.NoError(t, r.BufferTo(8))
	require.Equal(t, 8, r.Size())

	require.NoError(t, r.BufferTo(20))
	require.Equal(t, 20, r.Size())
}

func TestBucketReader_Size_NotAffectedByCursorMovement(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)
	size := r.Size()

	_, err := r.Read(10)
	require.NoError(t, err)
	require.Equal(t, size, r.Size(), "reading must not shrink the buffer size")

	require.NoError(t, r.Reset())
	require.Equal(t, size, r.Size(), "reset must not shrink the buffer size")
}

// ---- Non-zero base ----------------------------------------------------------

func TestBucketReader_NonZeroBase_ReturnsCorrectBytes(t *testing.T) {
	const base = 5
	r, _ := newFullyBufferedReader(t, base)

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b, "BucketReader must expose the segment bytes, not the prefix padding")
}

func TestBucketReader_NonZeroBase_GetRangeUsesAbsoluteOffsets(t *testing.T) {
	const base = 13
	r, bkt := newTestReader(t, base)

	require.NoError(t, r.BufferTo(7))
	require.NoError(t, r.BufferTo(20))

	require.Len(t, bkt.calls, 2)
	require.Equal(t, rangeCall{off: base, length: 7}, bkt.calls[0])
	require.Equal(t, rangeCall{off: base + 7, length: 13}, bkt.calls[1])
}

// ---- Empty segment ----------------------------------------------------------

func TestBucketReader_EmptySegment(t *testing.T) {
	bkt := newTrackingBucket(t, []byte{})
	r := NewBucketReader(context.Background(), bkt, testBucketObjectName, 0, 0)

	require.Equal(t, 0, r.Len())
	require.Equal(t, 0, r.Offset())
	require.Equal(t, 0, r.Buffered())
	require.Equal(t, 0, r.Size())

	require.ErrorIs(t, r.Skip(1), ErrInvalidSize)
	require.ErrorIs(t, r.ResetAt(1), ErrInvalidSize)
	require.NoError(t, r.ResetAt(0))
	require.NoError(t, r.BufferTo(0), "BufferTo(0) on empty segment must be a no-op")
	require.Empty(t, bkt.calls)
}

// ---- Close ------------------------------------------------------------------

func TestBucketReader_Close_ReturnsNil(t *testing.T) {
	r, _ := newFullyBufferedReader(t, 0)
	require.NoError(t, r.Close())
}

// ---- Combined workflow ------------------------------------------------------

// TestBucketReader_WorkflowIncrementalBuffering simulates a realistic caller
// that progressively extends the buffer as it learns how much data it needs,
// interspersed with resets and re-reads.
func TestBucketReader_WorkflowIncrementalBuffering(t *testing.T) {
	r, bkt := newTestReader(t, 0)

	// Buffer first 5 bytes and read them.
	require.NoError(t, r.BufferTo(5))
	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("abcde"), b)

	// Discover we need more; extend the buffer.
	require.NoError(t, r.BufferTo(15))
	b, err = r.Read(10)
	require.NoError(t, err)
	require.Equal(t, []byte("fghij12345"), b)

	// Reset and re-read from the start without issuing new GetRange calls.
	callsBefore := len(bkt.calls)
	require.NoError(t, r.Reset())
	b, err = r.Read(15)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:15], b)
	require.Len(t, bkt.calls, callsBefore, "re-reading from buffer must not issue new GetRange calls")

	// Extend to fill the remaining segment, then read the rest.
	require.NoError(t, r.BufferTo(20))
	b, err = r.Read(5)
	require.NoError(t, err)
	require.Equal(t, []byte("67890"), b)
	require.Equal(t, 0, r.Len())
}

// ---- failingBucket ----------------------------------------------------------

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

func (b *failingBucket) IsObjNotFoundErr(_ error) bool   { return false }
func (b *failingBucket) IsAccessDeniedErr(_ error) bool  { return false }
func (b *failingBucket) Name() string                    { return "failing" }
func (b *failingBucket) Close() error                    { return nil }

func (b *failingBucket) ReaderWithExpectedErrs(_ objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}
