package encoding

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// testBucketContentsLong is a 64-byte payload for tests that need more data than
// the read-ahead window holds. Every byte is distinct, so a read at a wrong offset
// gives a different result.
var testBucketContentsLong = []byte("abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+/")

func newTestAsyncBufReader(t *testing.T, base, length int) (*BucketAsyncBufReader, *trackingBucket) {
	t.Helper()
	ctx := context.Background()

	objectData := make([]byte, 0, length)
	objectData = append(objectData, testBucketContents...)
	bkt := newTrackingBucket(t, objectData)

	return newBucketAsyncBufReader(
		ctx, bkt, testBucketObjectName, base, length,
		&testBucketBufPool, testBufPoolSize, 4,
	), bkt
}

// newTestAsyncBufReaderWithData builds a reader over the given object data,
// with an explicit read-ahead buffer count.
// A test that needs a rotation of the read-ahead window must use this helper,
// because a rotation needs a length greater than maxBufCount*testBufPoolSize bytes.
func newTestAsyncBufReaderWithData(
	t *testing.T, objectData []byte, base, length, maxBufCount int,
) (*BucketAsyncBufReader, *trackingBucket) {
	t.Helper()

	bkt := newTrackingBucket(t, objectData)

	return newBucketAsyncBufReader(
		t.Context(), bkt, testBucketObjectName, base, length,
		&testBucketBufPool, testBufPoolSize, maxBufCount,
	), bkt
}

func TestBucketAsyncBufReader_Read_Sequential(t *testing.T) {
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

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
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

	b, err := r.Read(len(testBucketContents))
	require.NoError(t, err)
	require.Equal(t, testBucketContents, b)
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_Peek_Basic(t *testing.T) {
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

	b, err := r.Peek(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], b)
	require.Equal(t, 0, r.Offset(), "Peek does not consume")

	// Read returns the same bytes.
	got, err := r.Read(5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:5], got)
}

func TestBucketAsyncBufReader_Peek_AcrossPromiseBoundary(t *testing.T) {
	// Each buffer promise holds testBufPoolSize bytes.
	// A peek of 8 bytes at offset 12 takes 4 bytes from the first promise
	// and 4 bytes from the second promise.
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Skip(testBufPoolSize-4))

	b, err := r.Peek(8)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[testBufPoolSize-4:testBufPoolSize+4], b)
	require.Equal(t, testBufPoolSize-4, r.Offset(), "Peek does not consume")
}

func TestBucketAsyncBufReader_Peek_PastSegmentEnd(t *testing.T) {
	// Peek must return the bytes read and suppress the EOF error
	// when peeking past the configured length or peeking beyond the true end of the object.
	const sectionLen = 5
	r, _ := newTestAsyncBufReader(t, 0, sectionLen)

	b, err := r.Peek(sectionLen + 5)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[:sectionLen], b)
	require.Equal(t, 0, r.Offset())
}

func TestBucketAsyncBufReader_Peek_AtEnd(t *testing.T) {
	const sectionLen = 5
	r, _ := newTestAsyncBufReader(t, 0, sectionLen)
	require.NoError(t, r.Skip(sectionLen))

	b, err := r.Peek(1)
	require.NoError(t, err)
	require.Nil(t, b)
}

//func TestBucketAsyncBufReader_Peek_BeyondPeekBuffer(t *testing.T) {
//	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))
//
//	b, err := r.Peek(r.Size() + 1)
//	require.ErrorIs(t, err, ErrInvalidSize)
//	require.Nil(t, b)
//}

// TestBucketAsyncBufReader_Peek_ThenSkip covers the access pattern of
// Decbuf.UnsafeUvarintBytes, which peeks bytes and then skips exactly those bytes.
// The skip drains the head promise and returns the buffer of that promise to the pool.
// The reader then takes that same buffer back and fills it with the third chunk.
// Peek copies into peekBuf, so the peeked bytes stay valid through that refill.
func TestBucketAsyncBufReader_Peek_ThenSkip(t *testing.T) {
	// The read-ahead window holds maxBufCount*testBufPoolSize = 32 bytes of the 48-byte segment,
	// so the reader must refill a buffer to reach the third chunk.
	const (
		length      = 48
		maxBufCount = 2
	)
	r, _ := newTestAsyncBufReaderWithData(t, testBucketContentsLong, 0, length, maxBufCount)

	// Peek and skip the whole head promise, which drains and releases it.
	b, err := r.Peek(testBufPoolSize)
	require.NoError(t, err)
	require.NoError(t, r.Skip(len(b)))
	require.Equal(t, testBufPoolSize, r.Offset())

	// Read the second chunk, then one byte of the third chunk.
	// The read of the third chunk waits for the refill of the released buffer.
	got, err := r.Read(testBufPoolSize)
	require.NoError(t, err)
	require.Equal(t, testBucketContentsLong[testBufPoolSize:2*testBufPoolSize], got)

	got, err = r.Read(1)
	require.NoError(t, err)
	require.Equal(t, testBucketContentsLong[2*testBufPoolSize:2*testBufPoolSize+1], got)

	require.Equal(t, testBucketContentsLong[:testBufPoolSize], b, "peeked bytes survive the refill")
}

// TestBucketAsyncBufReader_Peek_ThenSkip_AcrossPromiseBoundary makes sure that a skip
// after a straddling peek discards the bytes that the peek returned.
// Peek does not consume, so the cursor still points at the first peeked byte.
func TestBucketAsyncBufReader_Peek_ThenSkip_AcrossPromiseBoundary(t *testing.T) {
	// A peek of 8 bytes at offset 12 takes 4 bytes from the first promise
	// and 4 bytes from the second promise, so it goes through peekBuf.
	const peekAt = testBufPoolSize - 4
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))
	require.NoError(t, r.Skip(peekAt))

	b, err := r.Peek(8)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[peekAt:peekAt+8], b)

	require.NoError(t, r.Skip(len(b)))
	require.Equal(t, peekAt+8, r.Offset(), "the skip consumes the peeked bytes, not the bytes after them")
	require.Equal(t, testBucketContents[peekAt:peekAt+8], b, "the skip does not disturb peekBuf")

	// The next read starts one byte past the peeked bytes.
	got, err := r.Read(4)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[peekAt+8:peekAt+12], got)
}

func TestBucketAsyncBufReader_Skip_Basic(t *testing.T) {
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(10))
	require.Equal(t, 10, r.Offset())
	require.Equal(t, len(testBucketContents)-10, r.Len())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[10:13], b)
}

func TestBucketAsyncBufReader_Skip_AcrossPromises(t *testing.T) {
	// A skip of 20 bytes drains the first promise of testBufPoolSize bytes,
	// then takes the remainder from the second promise.
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(testBufPoolSize+4))
	require.Equal(t, testBufPoolSize+4, r.Offset())

	b, err := r.Read(3)
	require.NoError(t, err)
	require.Equal(t, testBucketContents[testBufPoolSize+4:testBufPoolSize+7], b)
}

func TestBucketAsyncBufReader_Skip_ToEnd(t *testing.T) {
	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))

	require.NoError(t, r.Skip(len(testBucketContents)))
	require.Equal(t, len(testBucketContents), r.Offset())
	require.Equal(t, 0, r.Len())
}

func TestBucketAsyncBufReader_Skip_BeyondEnd(t *testing.T) {
	const sectionLen = 10
	r, _ := newTestAsyncBufReader(t, 0, sectionLen)

	require.ErrorIs(t, r.Skip(sectionLen+1), ErrInvalidSize)
}

//func TestBucketAsyncBufReader_Skip_Negative(t *testing.T) {
//	// Decbuf.SkipUvarintBytes converts a uint64 from the object to an int,
//	// so a corrupt length can arrive as a negative number.
//	r, _ := newTestAsyncBufReader(t, 0, len(testBucketContents))
//
//	require.ErrorIs(t, r.Skip(-1), ErrInvalidSize)
//	require.Equal(t, 0, r.Offset())
//}

func TestBucketAsyncBufReader_Read_ExactLength_NonZeroBaseWithRotation(t *testing.T) {
	// The read-ahead window holds maxBufCount*testBufPoolSize = 32 bytes.
	// A length of 48 forces the reader to release one buffer promise and refill it.
	// The refilled promise must add base to the buffered offset.
	// Without base, the promise reads the object 4 bytes too early.
	const (
		base        = 4
		length      = 48
		maxBufCount = 2
	)
	r, _ := newTestAsyncBufReaderWithData(t, testBucketContentsLong, base, length, maxBufCount)

	b, err := r.Read(length)
	require.NoError(t, err)
	require.Equal(t, testBucketContentsLong[base:base+length], b)
	require.Equal(t, length, r.Offset())
	require.Equal(t, 0, r.Len())
}
