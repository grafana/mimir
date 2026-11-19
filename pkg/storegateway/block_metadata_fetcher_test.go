// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
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

type staticMetadataFetcher struct {
	metas map[ulid.ULID]*block.Meta
	err   error
}

func (f staticMetadataFetcher) Fetch(context.Context) (map[ulid.ULID]*block.Meta, map[ulid.ULID]error, error) {
	return f.metas, nil, f.err
}
