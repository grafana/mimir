// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

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
		return bufio.NewReaderSize(nil, testBufPoolSize)
	},
}

func newTestBufReader(t *testing.T, base, length int) (*BucketBufReader, *trackingBucket) {
	t.Helper()
	ctx := context.Background()

	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)

	return newBucketBufReader(ctx, &testBucketBufPool, bkt, testBucketObjectName, base, length), bkt
}

func newFailingBufReader(t *testing.T, sentinel error) *BucketBufReader {
	t.Helper()
	bkt := &failingBucket{err: sentinel}
	ctx := context.Background()
	reader := NewBucketReader(ctx, bkt, "obj", 0, 10)
	bufioReader := testBucketBufPool.Get().(*bufio.Reader)
	bufioReader.Reset(reader)
	return &BucketBufReader{
		ctx:    ctx,
		bkt:    bkt,
		name:   "obj",
		base:   0,
		length: 10,
		r:      reader,
		buf:    bufioReader,
	}
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

	peek1, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], peek1)
	require.Equal(t, 0, r.Offset(), "Peek does not consume")

	// Same-size Peek returns the same bytes.
	peek2, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], peek2)

	// Smaller Peek returns the same starting bytes.
	peek3, err := r.Peek(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:3], peek3)

	// Larger Peek returns the original bytes plus more
	peek4, err := r.Peek(10)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:10], peek4)
}

func TestBucketBufReader_Peek_PastSegmentEnd(t *testing.T) {
	// Peek must return the bytes read and suppress the EOF error
	// when peeking beyond the configured length or the true end of the file/object.
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

func TestBucketBufReader_Peek_Skip(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	peek1, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], peek1)
	require.Equal(t, 0, r.Offset())

	// Skip the same bytes.
	require.NoError(t, r.Skip(5))
	require.Equal(t, 5, r.Offset())

	// Another Peek returns the next bytes after Skip consumed previous bytes.
	peek2, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:10], peek2)
	require.Equal(t, 5, r.Offset())

	// A Skip short of the peeked bytes consumes some of them.
	require.NoError(t, r.Skip(3))
	require.Equal(t, 8, r.Offset())

	// A Skip beyond the remaining peeked bytes consumes them and more.
	require.NoError(t, r.Skip(8))
	require.Equal(t, 16, r.Offset())
}

func TestBucketBufReader_Peek_Read(t *testing.T) {
	r, _ := newTestBufReader(t, 0, len(testBucketContents))

	peek1, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], peek1)
	require.Equal(t, 0, r.Offset())

	// Read returns the same bytes.
	read1, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], read1)
	require.Equal(t, 5, r.Offset())

	// Another Peek returns the next bytes after Read consumed previous bytes.
	peek2, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:10], peek2)
	require.Equal(t, 5, r.Offset())

	// A Read short of the peeked bytes consumes some of them.
	read2, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:8], read2)
	require.Equal(t, 8, r.Offset())

	// A Read beyond the remaining peeked bytes consumes them and more.
	read3, err := r.Read(8)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[8:16], read3)
	require.Equal(t, 16, r.Offset())
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

	// The first read operation causes bufio to fill its buffer.
	// The first testBufPoolSize bytes read will be covered by a single GetRange call.
	for i := 0; i < testBufPoolSize; i++ {
		_, err := r.Read(1)
		require.NoError(t, err)
	}
	require.Len(t, bkt.calls, 1)

	// Buffer depleted; next read operation triggers another bufio fill and GetRange call.
	_, err := r.Read(1)
	require.NoError(t, err)
	require.Len(t, bkt.calls, 2)
}

func TestBucketBufReader_GetRangeCalls_ResetRefetches(t *testing.T) {
	r, bkt := newTestBufReader(t, 0, len(testBucketContents))

	_, err := r.Read(1)
	require.NoError(t, err)
	require.Len(t, bkt.calls, 1)

	// After Reset the buffer is discarded; the next read must refetch from the bucket.
	require.NoError(t, r.Reset())
	_, err = r.Read(1)
	require.NoError(t, err)
	require.Len(t, bkt.calls, 2)
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
func (b *failingBucket) IsConditionNotMetErr(_ error) bool {
	return false
}
func (b *failingBucket) Name() string { return "failing" }
func (b *failingBucket) Close() error { return nil }

func (b *failingBucket) ReaderWithExpectedErrs(_ objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b
}
