// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSingleBucketBlockMetadataFetcher_Fetch(t *testing.T) {
	bucketReader := objstore.WithNoopInstr(objstore.NewInMemBucket())
	meta1 := &block.Meta{}
	meta1.ULID = ulid.MustNew(1, nil)
	meta2 := &block.Meta{}
	meta2.ULID = ulid.MustNew(2, nil)
	fetchErr := errors.New("failed to fetch")

	tests := map[string]struct {
		metas          map[ulid.ULID]*block.Meta
		err            error
		expectedBlocks map[ulid.ULID]FetchedBlock
	}{
		"returns each block with the bucket reader": {
			metas: map[ulid.ULID]*block.Meta{meta1.ULID: meta1, meta2.ULID: meta2},
			expectedBlocks: map[ulid.ULID]FetchedBlock{
				meta1.ULID: {Meta: meta1, BucketReader: bucketReader},
				meta2.ULID: {Meta: meta2, BucketReader: bucketReader},
			},
		},
		"returns a partial view together with the error": {
			metas: map[ulid.ULID]*block.Meta{meta1.ULID: meta1},
			err:   fetchErr,
			expectedBlocks: map[ulid.ULID]FetchedBlock{
				meta1.ULID: {Meta: meta1, BucketReader: bucketReader},
			},
		},
		"returns no blocks if the metadata fetcher returns no blocks": {
			err: fetchErr,
		},
	}

	for name, testCase := range tests {
		t.Run(name, func(t *testing.T) {
			fetcher := newSingleBucketBlockMetadataFetcher(staticMetadataFetcher{metas: testCase.metas, err: testCase.err}, bucketReader)

			blocks, err := fetcher.Fetch(t.Context())
			if testCase.err != nil {
				require.ErrorIs(t, err, testCase.err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, testCase.expectedBlocks, blocks)
		})
	}
}

func TestMultiBucketBlockMetadataFetcher_Fetch(t *testing.T) {
	// The buckets have different content, so that the assertions can tell them apart.
	newBucket, oldBucket := objstore.NewInMemBucket(), objstore.NewInMemBucket()
	require.NoError(t, newBucket.Upload(t.Context(), "name", strings.NewReader("new")))
	require.NoError(t, oldBucket.Upload(t.Context(), "name", strings.NewReader("old")))
	newBucketReader := objstore.WithNoopInstr(newBucket)
	oldBucketReader := objstore.WithNoopInstr(oldBucket)
	onlyInNewBucket := &block.Meta{}
	onlyInNewBucket.ULID = ulid.MustNew(1, nil)
	inBothBuckets := &block.Meta{}
	inBothBuckets.ULID = ulid.MustNew(2, nil)
	onlyInOldBucket := &block.Meta{}
	onlyInOldBucket.ULID = ulid.MustNew(3, nil)

	newBucketFetcher := newSingleBucketBlockMetadataFetcher(staticMetadataFetcher{metas: map[ulid.ULID]*block.Meta{
		onlyInNewBucket.ULID: onlyInNewBucket,
		inBothBuckets.ULID:   inBothBuckets,
	}}, newBucketReader)
	oldBucketFetcher := newSingleBucketBlockMetadataFetcher(staticMetadataFetcher{metas: map[ulid.ULID]*block.Meta{
		inBothBuckets.ULID:   inBothBuckets,
		onlyInOldBucket.ULID: onlyInOldBucket,
	}}, oldBucketReader)

	t.Run("returns the union of the blocks of all buckets, with the first bucket winning", func(t *testing.T) {
		fetcher := newMultiBucketBlockMetadataFetcher([]BlockMetadataFetcher{newBucketFetcher, oldBucketFetcher})

		blocks, err := fetcher.Fetch(t.Context())
		require.NoError(t, err)
		require.Equal(t, map[ulid.ULID]FetchedBlock{
			onlyInNewBucket.ULID: {Meta: onlyInNewBucket, BucketReader: newBucketReader},
			inBothBuckets.ULID:   {Meta: inBothBuckets, BucketReader: newBucketReader},
			onlyInOldBucket.ULID: {Meta: onlyInOldBucket, BucketReader: oldBucketReader},
		}, blocks)
	})

	t.Run("returns the blocks of the buckets that have blocks", func(t *testing.T) {
		emptyBucketFetcher := newSingleBucketBlockMetadataFetcher(staticMetadataFetcher{}, newBucketReader)
		fetcher := newMultiBucketBlockMetadataFetcher([]BlockMetadataFetcher{emptyBucketFetcher, oldBucketFetcher})

		blocks, err := fetcher.Fetch(t.Context())
		require.NoError(t, err)
		require.Equal(t, map[ulid.ULID]FetchedBlock{
			inBothBuckets.ULID:   {Meta: inBothBuckets, BucketReader: oldBucketReader},
			onlyInOldBucket.ULID: {Meta: onlyInOldBucket, BucketReader: oldBucketReader},
		}, blocks)
	})

	t.Run("returns no blocks if one bucket fails, also when it returns a partial view", func(t *testing.T) {
		fetchErr := errors.New("failed to fetch")
		failingBucketFetcher := newSingleBucketBlockMetadataFetcher(staticMetadataFetcher{
			metas: map[ulid.ULID]*block.Meta{onlyInOldBucket.ULID: onlyInOldBucket},
			err:   fetchErr,
		}, oldBucketReader)
		fetcher := newMultiBucketBlockMetadataFetcher([]BlockMetadataFetcher{newBucketFetcher, failingBucketFetcher})

		blocks, err := fetcher.Fetch(t.Context())
		require.ErrorIs(t, err, fetchErr)
		require.Nil(t, blocks)
	})
}

type staticMetadataFetcher struct {
	metas map[ulid.ULID]*block.Meta
	err   error
}

func (f staticMetadataFetcher) Fetch(context.Context) (map[ulid.ULID]*block.Meta, map[ulid.ULID]error, error) {
	return f.metas, nil, f.err
}
