// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore/tracing"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// expandedPostingsPromise is the promise returned by bucketIndexReader.expandedPostingsPromise.
// The second return value indicates whether the returned data comes from the cache.
type expandedPostingsPromise func(ctx context.Context) ([]storage.SeriesRef, bool, error)

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that reads index that is stored in
// object storage without having to fully download it.
type bucketIndexReader struct {
	block *bucketBlock
	dec   *index.Decoder
}

func newBucketIndexReader(block *bucketBlock) *bucketIndexReader {
	r := &bucketIndexReader{
		block: block,
		dec: &index.Decoder{
			LookupSymbol: block.indexHeaderReader.LookupSymbol,
		},
	}
	return r
}

// ExpandedPostings returns postings in expanded list instead of index.Postings.
// This is because we need to have them buffered anyway to perform efficient lookup
// on object storage.
// Found posting IDs (ps) are not strictly required to point to a valid Series, e.g. during
// background garbage collections.
//
// Reminder: A posting is a reference (represented as a uint64) to a series reference, which in turn points to the first
// chunk where the series contains the matching label-value pair for a given block of data. Postings can be fetched by
// single label name=value.
func (r *bucketIndexReader) ExpandedPostings(ctx context.Context, ms []*labels.Matcher, stats *safeQueryStats) (returnRefs []storage.SeriesRef, returnErr error) {
	var (
		loaded bool
		cached bool
	)
	span, ctx := tracing.StartSpan(ctx, "ExpandedPostings()")
	defer func() {
		span.LogKV("returned postings", len(returnRefs), "cached", cached, "promise_loaded", loaded)
		if returnErr != nil {
			span.LogFields(otlog.Error(returnErr))
		}
		span.Finish()
	}()
	var promise expandedPostingsPromise
	promise, loaded = r.expandedPostingsPromise(ctx, ms, stats)
	returnRefs, cached, returnErr = promise(ctx)
	return returnRefs, returnErr
}

// expandedPostingsPromise provides a promise for the execution of expandedPostings method.
// First call to this method will be blocking until the expandedPostings are calculated.
// While first call is blocking, concurrent calls with same matchers will return a promise for the same results, without recalculating them.
// The second value returned by this function is set to true when this call just loaded a promise created by another goroutine.
// The promise returned by this function returns a bool value fromCache, set to true when data was loaded from cache.
// TODO: if promise creator's context is canceled, the entire promise will fail, even if there are more callers waiting for the results
// TODO: https://github.com/grafana/mimir/issues/331
func (r *bucketIndexReader) expandedPostingsPromise(ctx context.Context, ms []*labels.Matcher, stats *safeQueryStats) (promise expandedPostingsPromise, loaded bool) {
	var (
		refs   []storage.SeriesRef
		err    error
		done   = make(chan struct{})
		cached bool
	)

	promise = func(ctx context.Context) ([]storage.SeriesRef, bool, error) {
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-done:
		}

		if err != nil {
			return nil, false, err
		}

		// We must make a copy of refs to return, because caller can modify the postings slice in place.
		refsCopy := make([]storage.SeriesRef, len(refs))
		copy(refsCopy, refs)

		return refsCopy, cached, nil
	}

	key := indexcache.CanonicalLabelMatchersKey(ms)

	var loadedPromise interface{}
	loadedPromise, loaded = r.block.expandedPostingsPromises.LoadOrStore(key, promise)
	if loaded {
		return loadedPromise.(expandedPostingsPromise), true
	}
	defer close(done)
	defer r.block.expandedPostingsPromises.Delete(key)

	refs, cached = r.fetchCachedExpandedPostings(ctx, r.block.userID, key, stats)
	if cached {
		return promise, false
	}
	refs, err = r.expandedPostings(ctx, ms, stats)
	if err != nil {
		return promise, false
	}
	r.cacheExpandedPostings(ctx, r.block.userID, key, refs)
	return promise, false
}

func (r *bucketIndexReader) cacheExpandedPostings(ctx context.Context, userID string, key indexcache.LabelMatchersKey, refs []storage.SeriesRef) {
	data, err := diffVarintSnappyEncode(index.NewListPostings(refs), len(refs))
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't encode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return
	}
	r.block.indexCache.StoreExpandedPostings(ctx, userID, r.block.meta.ULID, key, data)
}

func (r *bucketIndexReader) fetchCachedExpandedPostings(ctx context.Context, userID string, key indexcache.LabelMatchersKey, stats *safeQueryStats) ([]storage.SeriesRef, bool) {
	data, ok := r.block.indexCache.FetchExpandedPostings(ctx, userID, r.block.meta.ULID, key)
	if !ok {
		return nil, false
	}

	p, err := r.decodePostings(data, stats)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't decode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, false
	}

	refs, err := index.ExpandPostings(p)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't expand decoded expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, false
	}
	return refs, true
}

// expandedPostings is the main logic of ExpandedPostings, without the promise wrapper.
func (r *bucketIndexReader) expandedPostings(ctx context.Context, ms []*labels.Matcher, stats *safeQueryStats) (returnRefs []storage.SeriesRef, returnErr error) {
	var (
		postingGroups []*postingGroup
		allRequested  = false
		hasAdds       = false
		keys          []labels.Label
	)

	// NOTE: Derived from tsdb.PostingsForMatchers.
	for _, m := range ms {
		// Each group is separate to tell later what postings are intersecting with what.
		pg, err := toPostingGroup(r.block.indexHeaderReader, m)
		if err != nil {
			return nil, errors.Wrap(err, "toPostingGroup")
		}

		// If this groups adds nothing, it's an empty group. We can shortcut this, since intersection with empty
		// postings would return no postings anyway.
		// E.g. label="non-existing-value" returns empty group.
		if !pg.addAll && len(pg.addKeys) == 0 {
			return nil, nil
		}

		postingGroups = append(postingGroups, pg)
		allRequested = allRequested || pg.addAll
		hasAdds = hasAdds || len(pg.addKeys) > 0

		// Postings returned by fetchPostings will be in the same order as keys
		// so it's important that we iterate them in the same order later.
		// We don't have any other way of pairing keys and fetched postings.
		keys = append(keys, pg.addKeys...)
		keys = append(keys, pg.removeKeys...)
	}

	if len(postingGroups) == 0 {
		return nil, nil
	}

	// We only need special All postings if there are no other adds. If there are, we can skip fetching
	// special All postings completely.
	if allRequested && !hasAdds {
		// add group with label to fetch "special All postings".
		name, value := index.AllPostingsKey()
		allPostingsLabel := labels.Label{Name: name, Value: value}

		postingGroups = append(postingGroups, newPostingGroup(true, []labels.Label{allPostingsLabel}, nil))
		keys = append(keys, allPostingsLabel)
	}

	fetchedPostings, err := r.fetchPostings(ctx, keys, stats)
	if err != nil {
		return nil, errors.Wrap(err, "get postings")
	}

	// Get "add" and "remove" postings from groups. We iterate over postingGroups and their keys
	// again, and this is exactly the same order as before (when building the groups), so we can simply
	// use one incrementing index to fetch postings from returned slice.
	postingIndex := 0

	var groupAdds, groupRemovals []index.Postings
	for _, g := range postingGroups {
		// We cannot add empty set to groupAdds, since they are intersected.
		if len(g.addKeys) > 0 {
			toMerge := make([]index.Postings, 0, len(g.addKeys))
			for _, l := range g.addKeys {
				toMerge = append(toMerge, checkNilPosting(l, fetchedPostings[postingIndex]))
				postingIndex++
			}

			groupAdds = append(groupAdds, index.Merge(toMerge...))
		}

		for _, l := range g.removeKeys {
			groupRemovals = append(groupRemovals, checkNilPosting(l, fetchedPostings[postingIndex]))
			postingIndex++
		}
	}

	result := index.Without(index.Intersect(groupAdds...), index.Merge(groupRemovals...))

	ps, err := index.ExpandPostings(result)
	if err != nil {
		return nil, errors.Wrap(err, "expand")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	version, err := r.block.indexHeaderReader.IndexVersion()
	if err != nil {
		return nil, errors.Wrap(err, "get index version")
	}
	if version >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	return ps, nil
}

// FetchPostings fills postings requested by posting groups.
// It returns one postings for each key, in the same order.
// If postings for given key is not fetched, entry at given index will be an ErrPostings
func (r *bucketIndexReader) FetchPostings(ctx context.Context, keys []labels.Label, stats *safeQueryStats) ([]index.Postings, error) {
	ps, err := r.fetchPostings(ctx, keys, stats)
	if err != nil {
		return nil, err
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	version, err := r.block.indexHeaderReader.IndexVersion()
	if err != nil {
		return nil, errors.Wrap(err, "get index version")
	}
	for i := range ps {
		ps[i] = checkNilPosting(keys[i], ps[i])
		if version >= 2 {
			ps[i] = paddedPostings{ps[i]}
		}
	}
	return ps, nil
}

// fetchPostings is the version-unaware private implementation of FetchPostings.
// callers of this method may need to add padding to the results.
// If postings for given key is not fetched, entry at given index will be nil.
func (r *bucketIndexReader) fetchPostings(ctx context.Context, keys []labels.Label, stats *safeQueryStats) ([]index.Postings, error) {
	timer := prometheus.NewTimer(r.block.metrics.postingsFetchDuration)
	defer timer.ObserveDuration()

	var ptrs []postingPtr

	output := make([]index.Postings, len(keys))

	// Fetch postings from the cache with a single call.
	fromCache, _ := r.block.indexCache.FetchMultiPostings(ctx, r.block.userID, r.block.meta.ULID, keys)

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for ix, key := range keys {
		// Get postings for the given key from cache first.
		if b, ok := fromCache[key]; ok {
			stats.update(func(stats *queryStats) {
				stats.postingsTouched++
				stats.postingsTouchedSizeSum += len(b)
			})

			l, err := r.decodePostings(b, stats)
			if err == nil {
				output[ix] = l
				continue
			}

			level.Warn(r.block.logger).Log(
				"msg", "can't decode cached postings",
				"err", err,
				"key", fmt.Sprintf("%+v", key),
				"block", r.block.meta.ULID,
				"bytes_len", len(b),
				"bytes_head_hex", hex.EncodeToString(b[:util_math.Min(8, len(b))]),
			)
		}

		// Cache miss; save pointer for actual posting in index stored in object store.
		ptr, err := r.block.indexHeaderReader.PostingsOffset(key.Name, key.Value)
		if errors.Is(err, indexheader.NotFoundRangeErr) {
			// This block does not have any posting for given key.
			output[ix] = index.EmptyPostings()
			continue
		}

		if err != nil {
			return nil, errors.Wrap(err, "index header PostingsOffset")
		}

		stats.update(func(stats *queryStats) {
			stats.postingsToFetch++
		})

		ptrs = append(ptrs, postingPtr{ptr: ptr, keyID: ix})
	}

	sort.Slice(ptrs, func(i, j int) bool {
		return ptrs[i].ptr.Start < ptrs[j].ptr.Start
	})

	// TODO(bwplotka): Asses how large in worst case scenario this can be. (e.g fetch for AllPostingsKeys)
	// Consider sub split if too big.
	parts := r.block.partitioner.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	g, ctx := errgroup.WithContext(ctx)
	for _, part := range parts {
		i, j := part.ElemRng[0], part.ElemRng[1]

		start := int64(part.Start)
		// We assume index does not have any ptrs that has 0 length.
		length := int64(part.End) - start

		// Fetch from object storage concurrently and update stats and posting list.
		g.Go(func() error {
			begin := time.Now()

			b, err := r.block.readIndexRange(ctx, start, length)
			if err != nil {
				return errors.Wrap(err, "read postings range")
			}
			fetchTime := time.Since(begin)

			stats.update(func(stats *queryStats) {
				stats.postingsFetchCount++
				stats.postingsFetched += j - i
				stats.postingsFetchDurationSum += fetchTime
				stats.postingsFetchedSizeSum += int(length)
			})

			for _, p := range ptrs[i:j] {
				// index-header can estimate endings, which means we need to resize the endings.
				pBytes, err := resizePostings(b[p.ptr.Start-start : p.ptr.End-start])
				if err != nil {
					return err
				}

				dataToCache := pBytes

				compressionTime := time.Duration(0)
				compressions, compressionErrors, compressedSize := 0, 0, 0

				// Reencode postings before storing to cache. If that fails, we store original bytes.
				// This can only fail, if postings data was somehow corrupted,
				// and there is nothing we can do about it.
				// Errors from corrupted postings will be reported when postings are used.
				compressions++
				s := time.Now()
				bep := newBigEndianPostings(pBytes[4:])
				data, err := diffVarintSnappyEncode(bep, bep.length())
				compressionTime = time.Since(s)
				if err == nil {
					dataToCache = data
					compressedSize = len(data)
				} else {
					compressionErrors = 1
				}

				// Return postings. Truncate first 4 bytes which are length of posting.
				// Access to output is not protected by a mutex because each goroutine
				// is expected to handle a different set of keys.
				output[p.keyID] = newBigEndianPostings(pBytes[4:])

				r.block.indexCache.StorePostings(ctx, r.block.userID, r.block.meta.ULID, keys[p.keyID], dataToCache)

				// If we just fetched it we still have to update the stats for touched postings.
				stats.update(func(stats *queryStats) {
					stats.postingsTouched++
					stats.postingsTouchedSizeSum += len(pBytes)
					stats.cachedPostingsCompressions += compressions
					stats.cachedPostingsCompressionErrors += compressionErrors
					stats.cachedPostingsOriginalSizeSum += len(pBytes)
					stats.cachedPostingsCompressedSizeSum += compressedSize
					stats.cachedPostingsCompressionTimeSum += compressionTime
				})
			}
			return nil
		})
	}

	return output, g.Wait()
}

func (r *bucketIndexReader) decodePostings(b []byte, stats *safeQueryStats) (index.Postings, error) {
	// Even if this instance is not using compression, there may be compressed
	// entries in the cache written by other stores.
	var (
		l   index.Postings
		err error
	)
	if isDiffVarintSnappyEncodedPostings(b) {
		s := time.Now()
		l, err = diffVarintSnappyDecode(b)

		stats.update(func(stats *queryStats) {
			stats.cachedPostingsDecompressions++
			stats.cachedPostingsDecompressionTimeSum += time.Since(s)
			if err != nil {
				stats.cachedPostingsDecompressionErrors++
			}
		})
	} else {
		_, l, err = r.dec.Postings(b)
	}
	return l, err
}
func (r *bucketIndexReader) preloadSeries(ctx context.Context, ids []storage.SeriesRef, stats *safeQueryStats) (*bucketIndexLoadedSeries, error) {
	span, ctx := tracing.StartSpan(ctx, "preloadSeries()")
	defer span.Finish()

	timer := prometheus.NewTimer(r.block.metrics.seriesFetchDuration)
	defer timer.ObserveDuration()

	loaded := newBucketIndexLoadedSeries()

	// Load series from cache, overwriting the list of ids to preload
	// with the missing ones.
	fromCache, ids := r.block.indexCache.FetchMultiSeriesForRefs(ctx, r.block.userID, r.block.meta.ULID, ids)
	for id, b := range fromCache {
		loaded.addSeries(id, b)
	}

	parts := r.block.partitioner.Partition(len(ids), func(i int) (start, end uint64) {
		return uint64(ids[i]), uint64(ids[i] + maxSeriesSize)
	})
	g, ctx := errgroup.WithContext(ctx)
	for _, p := range parts {
		s, e := p.Start, p.End
		i, j := p.ElemRng[0], p.ElemRng[1]

		g.Go(func() error {
			return r.loadSeries(ctx, ids[i:j], false, s, e, loaded, stats)
		})
	}
	return loaded, g.Wait()
}

func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []storage.SeriesRef, refetch bool, start, end uint64, loaded *bucketIndexLoadedSeries, stats *safeQueryStats) error {
	begin := time.Now()

	b, err := r.block.readIndexRange(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read series range")
	}

	stats.update(func(stats *queryStats) {
		stats.seriesFetchCount++
		stats.seriesFetched += len(ids)
		stats.seriesFetchDurationSum += time.Since(begin)
		stats.seriesFetchedSizeSum += int(end - start)
	})

	for i, id := range ids {
		c := b[uint64(id)-start:]

		l, n := binary.Uvarint(c)
		if n < 1 {
			return errors.New("reading series length failed")
		}
		if len(c) < n+int(l) {
			if i == 0 && refetch {
				return errors.Errorf("invalid remaining size, even after refetch, remaining: %d, expected %d", len(c), n+int(l))
			}

			// Inefficient, but should be rare.
			r.block.metrics.seriesRefetches.Inc()
			level.Warn(r.block.logger).Log("msg", "series size exceeded expected size; refetching", "id", id, "series length", n+int(l), "maxSeriesSize", maxSeriesSize)

			// Fetch plus to get the size of next one if exists.
			return r.loadSeries(ctx, ids[i:], true, uint64(id), uint64(id)+uint64(n+int(l)+1), loaded, stats)
		}
		c = c[n : n+int(l)]

		loaded.addSeries(id, c)

		r.block.indexCache.StoreSeriesForRef(ctx, r.block.userID, r.block.meta.ULID, id, c)
	}
	return nil
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// LookupLabelsSymbols populates label set strings from symbolized label set.
func (r *bucketIndexReader) LookupLabelsSymbols(symbolized []symbolizedLabel) (labels.Labels, error) {
	lbls := make(labels.Labels, len(symbolized))
	for ix, s := range symbolized {
		ln, err := r.dec.LookupSymbol(s.name)
		if err != nil {
			return nil, errors.Wrap(err, "lookup label name")
		}
		lv, err := r.dec.LookupSymbol(s.value)
		if err != nil {
			return nil, errors.Wrap(err, "lookup label value")
		}
		lbls[ix] = labels.Label{Name: ln, Value: lv}
	}
	return lbls, nil
}

// bucketIndexLoadedSeries holds the result of a series load operation.
type bucketIndexLoadedSeries struct {
	// Keeps the series that have been loaded from the index.
	seriesMx sync.Mutex
	series   map[storage.SeriesRef][]byte
}

func newBucketIndexLoadedSeries() *bucketIndexLoadedSeries {
	return &bucketIndexLoadedSeries{
		series: map[storage.SeriesRef][]byte{},
	}
}

// addSeries stores a series raw data in the data structure. A stored series can be loaded via unsafeLoadSeriesForTime().
// This function is concurrency safe.
func (l *bucketIndexLoadedSeries) addSeries(ref storage.SeriesRef, data []byte) {
	l.seriesMx.Lock()
	l.series[ref] = data
	l.seriesMx.Unlock()
}

// unsafeLoadSeriesForTime populates the given symbolized labels for the series identified by the reference if at least one chunk is within
// time selection.
// unsafeLoadSeriesForTime also populates chunk metas slices if skipChunks is set to false. Chunks are also limited by the given time selection.
// unsafeLoadSeriesForTime returns false, when there are no series data for given time range.
//
// Error is returned on decoding error or if the reference does not resolve to a known series.
//
// It's NOT safe to call this function concurrently with addSeries().
func (l *bucketIndexLoadedSeries) unsafeLoadSeriesForTime(ref storage.SeriesRef, lset *[]symbolizedLabel, chks *[]chunks.Meta, skipChunks bool, mint, maxt int64, stats *queryStats) (ok bool, err error) {
	b, ok := l.series[ref]
	if !ok {
		return false, errors.Errorf("series %d not found", ref)
	}

	stats.seriesTouched++
	stats.seriesTouchedSizeSum += len(b)
	return decodeSeriesForTime(b, lset, chks, skipChunks, mint, maxt)
}
