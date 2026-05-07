// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"bufio"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var testAsyncBufPool = sync.Pool{
	New: func() any {
		return bufio.NewReaderSize(nil, testBufPoolSize)
	},
}

func newTestBufAsyncReader(t *testing.T, base, length int) (*BucketBufAsyncReader, *trackingBucket) {
	t.Helper()
	ctx := context.Background()

	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)

	return newBucketBufAsyncReader(ctx, &testAsyncBufPool, bkt, testBucketObjectName, base, length), bkt
}

func newFailingBufAsyncReader(t *testing.T, sentinel error) *BucketBufAsyncReader {
	t.Helper()
	bkt := &failingBucket{err: sentinel}
	return newBucketBufAsyncReader(context.Background(), &testAsyncBufPool, bkt, "obj", 0, 10)
}

func TestBucketBufAsyncReader_Read_Sequential(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 5, r.Offset())

	b, err = r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[5:10], b)
	require.Equal(t, 10, r.Offset())
}

func TestBucketBufAsyncReader_Read_ExactLength(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b)
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufAsyncReader_Read_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	b, err := r.Read(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset())
}

func TestBucketBufAsyncReader_Read_BeyondEndAfterPartialConsumption(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	_, err := r.Read(3)
	require.NoError(t, err)

	b, err := r.Read(8)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Nil(t, b)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketBufAsyncReader_ReadInto_Basic(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	buf := make([]byte, 5)
	require.NoError(t, r.ReadInto(buf))
	require.Equal(t, testBucketContents[:5], buf)
	require.Equal(t, 5, r.Offset())
}

func TestBucketBufAsyncReader_ReadInto_BeyondEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	buf := make([]byte, sectionLen+1)
	err := r.ReadInto(buf)
	require.ErrorIs(t, err, ErrInvalidSize)
	require.Equal(t, sectionLen, r.Offset(), "cursor advanced to end")
}

func TestBucketBufAsyncReader_Skip_Basic(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	require.NoError(t, r.Skip(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:13], b)
}

func TestBucketBufAsyncReader_Skip_ToEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	require.NoError(t, r.Skip(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufAsyncReader_Skip_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	err := r.Skip(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
}

func TestBucketBufAsyncReader_Peek_Basic(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	b, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 0, r.Offset())

	got, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], got)
}

func TestBucketBufAsyncReader_Peek_PastSegmentEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	b, err := r.Peek(sectionLen + 5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:sectionLen], b)
	require.Equal(t, 0, r.Offset())
}

func TestBucketBufAsyncReader_Peek_AtEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })
	require.NoError(t, r.Skip(sectionLen))

	b, err := r.Peek(1)
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestBucketBufAsyncReader_Reset(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

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

func TestBucketBufAsyncReader_ResetAt_Middle(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	require.NoError(t, r.ResetAt(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:15], b)
}

func TestBucketBufAsyncReader_ResetAt_End(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	require.NoError(t, r.ResetAt(sectionLen))
	require.Equal(t, sectionLen, r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketBufAsyncReader_ResetAt_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestBufAsyncReader(t, 0, sectionLen)
	t.Cleanup(func() { _ = r.Close() })

	err := r.ResetAt(sectionLen + 1)
	require.ErrorIs(t, err, ErrInvalidSize)
}

func TestBucketBufAsyncReader_Len(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })

	require.Equal(t, len(testBucketContents), r.Len())
	require.NoError(t, r.Skip(10))
	require.Equal(t, len(testBucketContents)-10, r.Len())
	_, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, len(testBucketContents)-15, r.Len())
}

func TestBucketBufAsyncReader_Size(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	t.Cleanup(func() { _ = r.Close() })
	require.Equal(t, testBufPoolSize, r.Size())
}

func TestBucketBufAsyncReader_Close(t *testing.T) {
	r, _ := newTestBufAsyncReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Close())
}

func TestBucketBufAsyncReader_Read_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingBufAsyncReader(t, sentinel)
	t.Cleanup(func() { _ = r.Close() })

	_, err := r.Read(5)
	require.ErrorIs(t, err, sentinel)
}

func TestBucketBufAsyncReader_ReadInto_GetRangeError(t *testing.T) {
	sentinel := errors.New("storage error")
	r := newFailingBufAsyncReader(t, sentinel)
	t.Cleanup(func() { _ = r.Close() })

	err := r.ReadInto(make([]byte, 5))
	require.ErrorIs(t, err, sentinel)
}