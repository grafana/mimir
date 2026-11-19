// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// FetchedBlock is a block that a BlockMetadataFetcher found.
type FetchedBlock struct {
	Meta *block.Meta

	// BucketReader reads the objects of the block.
	BucketReader objstore.InstrumentedBucketReader
}

// BlockMetadataFetcher finds the blocks that a BucketStore loads.
type BlockMetadataFetcher interface {
	// Fetch returns the blocks that it found. It can return blocks together with an error. Then, the
	// returned blocks are a partial view of the blocks.
	// Fetch is not safe for concurrent use.
	Fetch(ctx context.Context) (map[ulid.ULID]FetchedBlock, error)
}

// singleBucketBlockMetadataFetcher is a BlockMetadataFetcher that reads all blocks from one bucket.
type singleBucketBlockMetadataFetcher struct {
	metadataFetcher block.MetadataFetcher
	bucketReader    objstore.InstrumentedBucketReader
}

func newSingleBucketBlockMetadataFetcher(metadataFetcher block.MetadataFetcher, bucketReader objstore.InstrumentedBucketReader) *singleBucketBlockMetadataFetcher {
	return &singleBucketBlockMetadataFetcher{
		metadataFetcher: metadataFetcher,
		bucketReader:    bucketReader,
	}
}

func (f *singleBucketBlockMetadataFetcher) Fetch(ctx context.Context) (map[ulid.ULID]FetchedBlock, error) {
	metas, _, err := f.metadataFetcher.Fetch(ctx)
	if metas == nil {
		return nil, err
	}

	blocks := make(map[ulid.ULID]FetchedBlock, len(metas))
	for id, meta := range metas {
		blocks[id] = FetchedBlock{Meta: meta, BucketReader: f.bucketReader}
	}
	return blocks, err
}
