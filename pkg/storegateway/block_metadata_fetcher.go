// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"

	"github.com/grafana/dskit/concurrency"
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

// multiBucketBlockMetadataFetcher is a BlockMetadataFetcher that finds blocks in more than one bucket. It
// has one BlockMetadataFetcher for each bucket, in bucket read order.
type multiBucketBlockMetadataFetcher struct {
	fetchers []BlockMetadataFetcher
}

func newMultiBucketBlockMetadataFetcher(fetchers []BlockMetadataFetcher) *multiBucketBlockMetadataFetcher {
	return &multiBucketBlockMetadataFetcher{fetchers: fetchers}
}

// Fetch implements BlockMetadataFetcher. It returns the union of the blocks that the fetchers find. If
// more than one fetcher finds the same block, the block of the first fetcher is returned. If a fetcher
// returns an error, Fetch returns the error and no blocks.
func (f *multiBucketBlockMetadataFetcher) Fetch(ctx context.Context) (map[ulid.ULID]FetchedBlock, error) {
	blocksPerFetcher := make([]map[ulid.ULID]FetchedBlock, len(f.fetchers))

	err := concurrency.ForEachJob(ctx, len(f.fetchers), len(f.fetchers), func(ctx context.Context, idx int) error {
		blocks, err := f.fetchers[idx].Fetch(ctx)
		if err != nil {
			return err
		}
		blocksPerFetcher[idx] = blocks
		return nil
	})
	if err != nil {
		return nil, err
	}

	mergedBlocks := map[ulid.ULID]FetchedBlock{}
	for _, blocks := range blocksPerFetcher {
		for id, fetchedBlock := range blocks {
			if _, found := mergedBlocks[id]; !found {
				mergedBlocks[id] = fetchedBlock
			}
		}
	}
	return mergedBlocks, nil
}
