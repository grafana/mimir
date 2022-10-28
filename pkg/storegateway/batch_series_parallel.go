package storegateway

import (
	"io"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// batchSeriesSet will implement storepb.SeriesSet
//
// Implementation will be similar to existing batchedSeriesSet
// A batchSeriesSet is created and used in BucketStore.Series
type batchSeriesSet interface {
	storage.SeriesSet

	seriesWatcher
}

// batchSeriesSetFactory is used in BucketStore.Series
type batchSeriesSetFactory func(
// postings can also become a postings provider, but for now we fetch all postings in one go.
// It can also be calculated by the seriesSet itself. For simplicity of this interface it's just provided.
	postings []storage.SeriesRef,
	batcher batcher,
	batchFactory batchFactory, // the batchedSeriesSet creates its own batches and passes them to be populated
) batchSeriesSet

type seriesWatcher interface {
	// seriesDisposed is a callback when a series can be safely discarded.
	// This method will be called after BucketStore.Series sends the series over gRPC.
	//
	// Once the batchSeriesSet receives a seriesDisposed call, it can look up
	// whether the disposed series is beyond an existing batch.
	// If that's the case it can close that batch.
	//
	// This relies on BucketStore.Series iterating through the series in a sorted manner,
	// because batchSeriesSet will infer that if series X is disposed, then so must be series X-1 and X-2
	//
	// The thing that invokes seriesDisposed will be a tracker somewhere in BucketStore.Series
	// or a top-level storepb.SeriesSet implementation, which calls
	seriesDisposed(seriesID storage.SeriesRef)
}

// batcher fetches batches in parallel while respecting the global quotas by its quotaManager
// batcher is created in BucketStore.Series
// batcher can also potentially offload batches to disk as long as batch keeps being an interface
type batcher interface {
	populate(b batch)
}

// batcherFactory is used in BucketStore.Series
type batcherFactory func(
	quotas quotaManager,
	filter shardingFilter, // since the batcher is the first to know the labelset, it's the first to be able to filter out un-owned series
	seriesReader seriesReader,
	chunkReader chunkReader,
	minTime, maxTime int,
	skipChunks bool,
// maybe also ULID and tenant ID
) batcher

type batch interface {
	io.Closer // takes care of freeing resources

	postings() []storage.SeriesRef
	series() []seriesEntry                                           // to be used by the batchedSeriesSet to emulate a storepb.SeriesSet
	saveChunks(series storage.SeriesRef, chunks []storepb.AggrChunk) // can be with SeriesRef or a local index within the batch
	saveLabels(series storage.SeriesRef, lset labels.Labels)         // can be with SeriesRef or a local index within the batch
}

type batchFactory func(pool pool.Bytes) batch

// seriesReader implements a subset of the functionality that the existing bucketIndexReader does
type seriesReader interface {
	// I can't find a reliable way to tell the # of bytes that the index for the series will take
	// before we fetch it from the cache,
	// so we'll account for them after fetching them for each batch
	preload(postings []storage.SeriesRef) (numSeries int, numBytes int)

	// alternatively it can receive a pointer to the destination instead of returning it
	labelSet(series storage.SeriesRef) labels.Labels
	// alternatively it can receive a pointer to the destination instead of returning it
	chunkRefs(series storage.SeriesRef) []chunks.ChunkRef
}

// seriesReaderFactory is used in BucketStore.Series
func seriesReaderFactory(
	partitioner Partitioner,
	indexBytes bytesFetcher,
	seriesCache seriesCacheWithContext,
	indexHeaderReader indexheader.Reader,
) seriesReader {
	return nil
}

// seriesCacheWithContext has similar interface as indexcache.IndexCache, but doesn't need to be fed all the details like ULID & tenantID
type seriesCacheWithContext interface {
	FetchMultiSeriesForRefs(ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef)
	StoreSeriesForRef(id storage.SeriesRef, v []byte)
}

type chunkReader interface {
	prepareToFetch(postings []storage.SeriesRef) (numSeries int, numBytes int)

	fetch(into batch)
}

// chunkReaderFactory is used in BucketStore.Series
func chunkReaderFactory(
	partitioner Partitioner,
	pool pool.Bytes, // this may not be necessary
	chunkBytes bytesFetcher,
) chunkReader {
	return nil
}

// bytesFetcher will be backed by a block's bucket reader or a block's index header reader
type bytesFetcher interface {
	bytes(seq int, start, length int64) (io.ReadCloser, error)
}

type shardingFilter interface {
	isOwned(id storage.SeriesRef, lset labels.Labels) bool
}

// quotaManager takes care of restricting the global maximum inflight bytes
// and of restricting series and chunks limit per tenant
type quotaManager interface {
	reserveSeries(numSeries, numBytes int) error // blocks or errors our
	reserveChunks(numChunks, numBytes int) error // blocks or errors our
}
