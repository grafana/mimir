package encoding

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
