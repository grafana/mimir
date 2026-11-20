// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// blockBuckets gives the bucket of each block of a tenant.
type blockBuckets struct {
	// readBuckets are the tenant's buckets in read order. The first bucket is the write bucket.
	readBuckets []objstore.InstrumentedBucket

	mtx sync.RWMutex
	// bucketIndexes holds the index in readBuckets of each block that is not in the write bucket.
	bucketIndexes map[ulid.ULID]int
}

func newBlockBuckets(readBuckets ...objstore.InstrumentedBucket) *blockBuckets {
	return &blockBuckets{readBuckets: readBuckets}
}

// write returns the bucket that new blocks go to.
func (b *blockBuckets) write() objstore.InstrumentedBucket {
	return b.readBuckets[0]
}

// of returns the bucket of the block. For a block that is not in bucketIndexes, it returns the write bucket.
func (b *blockBuckets) of(id ulid.ULID) objstore.InstrumentedBucket {
	b.mtx.RLock()
	defer b.mtx.RUnlock()
	return b.readBuckets[b.bucketIndexes[id]]
}

// set replaces the bucket indexes of the blocks.
func (b *blockBuckets) set(bucketIndexes map[ulid.ULID]int) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	b.bucketIndexes = bucketIndexes
}

// unionOfBucketMetas returns the union of the metas of the buckets. The buckets are in read order.
// If a block is in more than one bucket, it uses the first bucket.
// It also returns the index of the bucket of each block that is not in the first bucket.
func unionOfBucketMetas(metasPerBucket []map[ulid.ULID]*block.Meta) (map[ulid.ULID]*block.Meta, map[ulid.ULID]int) {
	metas := map[ulid.ULID]*block.Meta{}
	bucketIndexes := map[ulid.ULID]int{}
	for idx, bucketMetas := range metasPerBucket {
		for id, meta := range bucketMetas {
			if _, ok := metas[id]; ok {
				continue
			}
			metas[id] = meta
			if idx > 0 {
				bucketIndexes[id] = idx
			}
		}
	}
	return metas, bucketIndexes
}

// multiBucketMetaFetcher fetches the block metas of a tenant from all read buckets.
type multiBucketMetaFetcher struct {
	buckets *blockBuckets

	// fetchers has one fetcher for each read bucket, in read order.
	fetchers []*block.MetaFetcher

	deduplicateFilter block.MetadataFilter
}

// newMultiBucketMetaFetcher returns a fetcher for the read buckets of buckets. The bucketNames are in read order.
// The fetcher of the write bucket caches the metas in cacheDir. The fetchers of the other buckets cache the metas
// in subdirectories of cacheDir.
func newMultiBucketMetaFetcher(logger log.Logger, concurrency int, buckets *blockBuckets, bucketNames []string, cacheDir string, reg prometheus.Registerer, deduplicateFilter block.MetadataFilter, maxLookback time.Duration) (*multiBucketMetaFetcher, error) {
	fetchers := make([]*block.MetaFetcher, 0, len(buckets.readBuckets))
	for idx, bkt := range buckets.readBuckets {
		bucketCacheDir := cacheDir
		if idx > 0 {
			bucketCacheDir = filepath.Join(cacheDir, "read-bucket-"+strconv.Itoa(idx))
		}

		bucketReg := prometheus.WrapRegistererWith(prometheus.Labels{"bucket": bucketNames[idx]}, reg)
		filters := []block.MetadataFilter{NewLabelRemoverFilter(compactionIgnoredLabels)}
		fetcher, err := block.NewMetaFetcher(log.With(logger, "bucket", bucketNames[idx]), concurrency, bkt, bucketCacheDir, bucketReg, filters, maxLookback)
		if err != nil {
			return nil, err
		}
		fetchers = append(fetchers, fetcher)
	}

	return &multiBucketMetaFetcher{
		buckets:           buckets,
		fetchers:          fetchers,
		deduplicateFilter: deduplicateFilter,
	}, nil
}

// FetchWithoutMarkedForDeletion fetches the metas from each bucket, without the blocks that the bucket marks for deletion.
// Then it takes the union. If a block is in more than one bucket, it uses the first bucket in read order.
// Then it removes the duplicate blocks, and the blocks that their bucket marks for no-compaction.
// It sets the bucket of each block in the union in buckets.
func (f *multiBucketMetaFetcher) FetchWithoutMarkedForDeletion(ctx context.Context) (map[ulid.ULID]*block.Meta, map[ulid.ULID]error, error) {
	metasPerBucket := make([]map[ulid.ULID]*block.Meta, len(f.fetchers))
	partialsPerBucket := make([]map[ulid.ULID]error, len(f.fetchers))
	err := concurrency.ForEachJob(ctx, len(f.fetchers), len(f.fetchers), func(ctx context.Context, idx int) error {
		var err error
		metasPerBucket[idx], partialsPerBucket[idx], err = f.fetchers[idx].FetchWithoutMarkedForDeletion(ctx)
		return err
	})
	if err != nil {
		return nil, nil, err
	}

	metas, bucketIndexes := unionOfBucketMetas(metasPerBucket)
	partials := map[ulid.ULID]error{}
	for _, bucketPartials := range partialsPerBucket {
		for id, partialErr := range bucketPartials {
			_, inMetas := metas[id]
			_, inPartials := partials[id]
			if !inMetas && !inPartials {
				partials[id] = partialErr
			}
		}
	}

	// The deduplication filter must see the blocks that are marked for no-compaction.
	synced := newNoopGaugeVec()
	if err := f.deduplicateFilter.Filter(ctx, metas, synced); err != nil {
		return nil, nil, fmt.Errorf("filter metas: %w", err)
	}
	for idx, bkt := range f.buckets.readBuckets {
		bucketMetas := map[ulid.ULID]*block.Meta{}
		for id, meta := range metas {
			if bucketIndexes[id] == idx {
				bucketMetas[id] = meta
			}
		}
		if err := NewNoCompactionMarkFilter(bkt).Filter(ctx, bucketMetas, synced); err != nil {
			return nil, nil, fmt.Errorf("filter metas: %w", err)
		}
		for id := range metas {
			if _, kept := bucketMetas[id]; bucketIndexes[id] == idx && !kept {
				delete(metas, id)
			}
		}
	}

	f.buckets.set(bucketIndexes)
	return metas, partials, nil
}
