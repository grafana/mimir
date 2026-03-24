// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_index_metadata_fetcher.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

const (
	corruptedBucketIndex = "corrupted-bucket-index"
	noBucketIndex        = "no-bucket-index"
)

// BucketIndexLoader is an in-memory cache, that fetches tenant's bucket index from bucket.
type BucketIndexLoader struct {
	userID      string
	bkt         objstore.Bucket
	cfgProvider bucket.TenantConfigProvider
	logger      log.Logger

	idx atomic.Pointer[bucketindex.Index]
}

func NewBucketIndexLoader(userID string, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *BucketIndexLoader {
	return &BucketIndexLoader{
		userID:      userID,
		bkt:         bkt,
		cfgProvider: cfgProvider,
		logger:      logger,
	}
}

// FetchIndex retrieves the bucket index from bucket, updating the cached instance.
func (l *BucketIndexLoader) FetchIndex(ctx context.Context) (*bucketindex.Index, error) {
	idx, err := bucketindex.ReadIndex(ctx, l.bkt, l.userID, l.cfgProvider, l.logger)
	if err != nil {
		return nil, err
	}

	l.idx.Store(idx)

	return idx, nil
}

// Index returns the last read instance of bucket index. If the bucket index hasn't been read successfully yet,
// the returned instance is nil.
func (l *BucketIndexLoader) Index() *bucketindex.Index {
	return l.idx.Load()
}

type BucketIndexBlockMetadataFetcherMetrics struct {
	*block.FetcherMetrics

	bucketStoreMetrics *BucketStoreMetrics
}

func NewBucketIndexBlockMetadataFetcherMetrics(reg prometheus.Registerer, bucketStoreMetrics *BucketStoreMetrics) *BucketIndexBlockMetadataFetcherMetrics {
	return &BucketIndexBlockMetadataFetcherMetrics{
		FetcherMetrics:     block.NewFetcherMetrics(reg, [][]string{{corruptedBucketIndex}, {noBucketIndex}, {minTimeExcludedMeta}}),
		bucketStoreMetrics: bucketStoreMetrics,
	}
}

// BucketIndexBlockMetadataFetcher is a Thanos block.MetadataFetcher implementation leveraging on the Mimir bucket index.
type BucketIndexBlockMetadataFetcher struct {
	userID  string
	loader  *BucketIndexLoader
	logger  log.Logger
	filters []block.MetadataFilter
	metrics *BucketIndexBlockMetadataFetcherMetrics
}

func NewBucketIndexBlockMetadataFetcher(
	userID string,
	loader *BucketIndexLoader,
	logger log.Logger,
	metrics *BucketIndexBlockMetadataFetcherMetrics,
	filters []block.MetadataFilter,
) *BucketIndexBlockMetadataFetcher {
	return &BucketIndexBlockMetadataFetcher{
		userID:  userID,
		loader:  loader,
		logger:  logger,
		filters: filters,
		metrics: metrics,
	}
}

// Fetch implements block.MetadataFetcher. Not goroutine-safe.
func (f *BucketIndexBlockMetadataFetcher) Fetch(ctx context.Context) (metas map[ulid.ULID]*block.Meta, partial map[ulid.ULID]error, err error) {
	f.metrics.ResetTx()

	start := time.Now()
	defer func() {
		f.metrics.SyncDuration.Observe(time.Since(start).Seconds())
		if err != nil {
			f.metrics.SyncFailures.Inc()
		}
	}()
	f.metrics.Syncs.Inc()

	// Keep reference to previously fetched index to compare which blocks were added below.
	oldIdx := f.loader.Index()

	idx, err := f.loader.FetchIndex(ctx)
	if errors.Is(err, bucketindex.ErrIndexNotFound) {
		// This is a legit case happening when the first blocks of a tenant have recently been uploaded by ingesters
		// and their bucket index has not been created yet.
		f.metrics.Synced.WithLabelValues(noBucketIndex).Set(1)
		f.metrics.Submit()

		return nil, nil, nil
	}
	if errors.Is(err, bucketindex.ErrIndexCorrupted) {
		// In case a single tenant bucket index is corrupted, we don't want the store-gateway to fail at startup
		// because unable to fetch blocks metadata. We'll act as if the tenant has no bucket index, but the query
		// will fail anyway in the querier (the querier fails in the querier if bucket index is corrupted).
		level.Error(f.logger).Log("msg", "corrupted bucket index found", "user", f.userID, "err", err)
		f.metrics.Synced.WithLabelValues(corruptedBucketIndex).Set(1)
		f.metrics.Submit()

		return nil, nil, nil
	}
	if err != nil {
		f.metrics.Synced.WithLabelValues(block.FailedMeta).Set(1)
		f.metrics.Submit()

		return nil, nil, errors.Wrapf(err, "read bucket index")
	}

	level.Info(f.logger).Log("msg", "loaded bucket index", "user", f.userID, "updatedAt", idx.UpdatedAt)

	// If we successfully got a new index, and it is different from what was fetched on a previous sync,
	// collect the set of "known blocks" to track block discovery latency below.
	var knownBlocks map[ulid.ULID]struct{}
	if oldIdx != nil && oldIdx.UpdatedAt != idx.UpdatedAt {
		knownBlocks = make(map[ulid.ULID]struct{}, len(oldIdx.Blocks))
		for _, b := range oldIdx.Blocks {
			knownBlocks[b.ID] = struct{}{}
		}
	}

	// Build block metas out of the index.
	metas = make(map[ulid.ULID]*block.Meta, len(idx.Blocks))
	for _, b := range idx.Blocks {
		metas[b.ID] = b.ThanosMeta()

		if len(knownBlocks) != 0 {
			// If we have blocks from previous fetch, and the block isn't in the set, we track its discovery latency
			// as time from block creation (ULID timestamp) to now.
			_, ok := knownBlocks[b.ID]
			if ok {
				continue
			}
			blockCreationTime := time.UnixMilli(int64(b.ID.Time()))
			f.metrics.bucketStoreMetrics.blockDiscoveryLatency.Observe(time.Since(blockCreationTime).Seconds())
		}
	}

	for _, filter := range f.filters {
		var err error

		// NOTE: filter can update synced metric accordingly to the reason of the exclude.
		if customFilter, ok := filter.(MetadataFilterWithBucketIndex); ok {
			err = customFilter.FilterWithBucketIndex(ctx, metas, idx, f.metrics.Synced)
		} else {
			err = filter.Filter(ctx, metas, f.metrics.Synced)
		}

		if err != nil {
			return nil, nil, errors.Wrap(err, "filter metas")
		}
	}

	f.metrics.Synced.WithLabelValues(block.LoadedMeta).Set(float64(len(metas)))
	f.metrics.Submit()

	return metas, nil, nil
}

type bucketIndexMetadataReader struct {
	indexReader interface {
		Index() *bucketindex.Index
	}
}

// newBucketIndexMetadataReaderFromLoader is an adapter from BucketIndexLoader to BucketIndexMetadataReader.
func newBucketIndexMetadataReaderFromLoader(loader *BucketIndexLoader) BucketIndexMetadataReader {
	return &bucketIndexMetadataReader{
		indexReader: loader,
	}
}

func (r *bucketIndexMetadataReader) Metadata() *bucketindex.Metadata {
	idx := r.indexReader.Index()
	if idx == nil {
		return &bucketindex.Metadata{}
	}
	return idx.Metadata()
}
