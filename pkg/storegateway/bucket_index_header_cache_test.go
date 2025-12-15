package storegateway

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func TestIndexHeaderCachedBucketReader(t *testing.T) {
	ctx := t.Context()

	testbkt, err := filesystem.NewBucket("../storage/indexheader/testdata")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, testbkt.Close())
	})

	cache := NewIndexHeaderCache()

	bkt := newIndexHeaderCachedBucketReader(objstore.WithNoopInstr(testbkt), cache)

	// This is a cache miss.
	r1, err := bkt.GetRange(ctx, "index_format_v2/index", 0, PageSize)
	require.NoError(t, err)

	data1, err := io.ReadAll(r1)
	require.NoError(t, err)

	require.NoError(t, r1.Close())

	// Second read - should be cache hit (we can't directly verify this without instrumentation,
	// but we can verify the data is correct)
	r2, err := bkt.GetRange(ctx, "index_format_v2/index", 0, PageSize)
	require.NoError(t, err)

	data2, err := io.ReadAll(r2)
	require.NoError(t, err)

	require.NoError(t, r2.Close())

	require.Equal(t, data1, data2)

	// Read directly from underlying bucket for comparison
	r3, err := testbkt.GetRange(ctx, "index_format_v2/index", 0, PageSize)
	require.NoError(t, err)

	data3, err := io.ReadAll(r3)

	require.NoError(t, r3.Close())

	require.Equal(t, data1, data3)
}
