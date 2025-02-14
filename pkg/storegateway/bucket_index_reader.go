// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/util/indexheader"
	streamindex "github.com/grafana/mimir/pkg/util/indexheader/index"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// expandedPostingsPromise is the promise returned by bucketIndexReader.expandedPostingsPromise.
// The second return value indicates whether the returned data comes from the cache.
type expandedPostingsPromise func(ctx context.Context) ([]storage.SeriesRef, []*labels.Matcher, bool, error)

// bucketIndexReader is a custom index reader (not conforming index.Reader interface) that reads index that is stored in
// object storage without having to fully download it.
type bucketIndexReader struct {
	block             *bucketBlock
	postingsStrategy  postingsSelectionStrategy
	dec               *index.Decoder
	indexHeaderReader indexheader.Reader
}

func newBucketIndexReader(block *bucketBlock, postingsStrategy postingsSelectionStrategy) *bucketIndexReader {
	r := &bucketIndexReader{
		block:            block,
		postingsStrategy: postingsStrategy,
		dec: &index.Decoder{
			LookupSymbol: func(ctx context.Context, o uint32) (string, error) {
				return block.indexHeaderReader.LookupSymbol(ctx, o)
			},
		},
		indexHeaderReader: block.indexHeaderReader,
	}
	return r
}

// ExpandedPostings returns postings in expanded list instead of index.Postings.
// This is because we need to have them buffered anyway to perform efficient lookup
// on object storage. The returned postings are sorted.
//
// Depending on the postingsSelectionStrategy there may be some pendingMatchers returned.
// If pendingMatchers is not empty, then the returned postings may or may not match the pendingMatchers.
// The caller is responsible for filtering the series of the postings with the pendingMatchers.
//
// Reminder: A posting is a reference (represented as a uint64) to a series, which points to the first
// byte of a series in the index for a given block of data. Postings can be fetched by
// single label name=value.
func (r *bucketIndexReader) ExpandedPostings(ctx context.Context, ms []*labels.Matcher, stats *safeQueryStats) (returnRefs []storage.SeriesRef, pendingMatchers []*labels.Matcher, returnErr error) {
	var (
		loaded  bool
		cached  bool
		promise expandedPostingsPromise
	)
	span, ctx := opentracing.StartSpanFromContext(ctx, "ExpandedPostings()")
	defer func() {
		span.LogKV("returned postings", len(returnRefs), "cached", cached, "promise_loaded", loaded, "block_id", r.block.meta.ULID.String())
		if returnErr != nil {
			span.LogFields(otlog.Error(returnErr))
		}
		span.Finish()
	}()
	promise, loaded = r.expandedPostingsPromise(ctx, ms, stats)
	returnRefs, pendingMatchers, cached, returnErr = promise(ctx)
	return returnRefs, pendingMatchers, returnErr
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
		refs            []storage.SeriesRef
		pendingMatchers []*labels.Matcher
		err             error
		done            = make(chan struct{})
		cached          bool
	)

	promise = func(ctx context.Context) ([]storage.SeriesRef, []*labels.Matcher, bool, error) {
		select {
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		case <-done:
		}

		if err != nil {
			return nil, nil, false, err
		}

		// We must make a copy of refs to return, because caller can modify the postings slice in place.
		refsCopy := make([]storage.SeriesRef, len(refs))
		copy(refsCopy, refs)

		return refsCopy, pendingMatchers, cached, nil
	}

	key := indexcache.CanonicalLabelMatchersKey(ms)

	var loadedPromise interface{}
	loadedPromise, loaded = r.block.expandedPostingsPromises.LoadOrStore(key, promise)
	if loaded {
		return loadedPromise.(expandedPostingsPromise), true
	}
	defer close(done)
	defer r.block.expandedPostingsPromises.Delete(key)

	refs, pendingMatchers, cached = r.fetchCachedExpandedPostings(ctx, r.block.userID, key, stats)
	if cached {
		return promise, false
	}
	refs, pendingMatchers, err = r.expandedPostings(ctx, ms, stats)
	if err != nil {
		return promise, false
	}
	r.cacheExpandedPostings(r.block.userID, key, refs, pendingMatchers)
	return promise, false
}

func (r *bucketIndexReader) cacheExpandedPostings(userID string, key indexcache.LabelMatchersKey, refs []storage.SeriesRef, pendingMatchers []*labels.Matcher) {
	data, err := diffVarintSnappyWithMatchersEncode(index.NewListPostings(refs), len(refs), key, pendingMatchers)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't encode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return
	}
	r.block.indexCache.StoreExpandedPostings(userID, r.block.meta.ULID, key, r.postingsStrategy.name(), data)
}

func (r *bucketIndexReader) fetchCachedExpandedPostings(ctx context.Context, userID string, key indexcache.LabelMatchersKey, stats *safeQueryStats) ([]storage.SeriesRef, []*labels.Matcher, bool) {
	data, ok := r.block.indexCache.FetchExpandedPostings(ctx, userID, r.block.meta.ULID, key, r.postingsStrategy.name())
	if !ok {
		return nil, nil, false
	}

	p, requestMatcherKey, pendingMatchers, err := r.decodePostings(data, stats)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't decode expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, nil, false
	}
	if requestMatcherKey != key {
		level.Warn(r.block.logger).Log("msg", "request matchers key from cache item doesn't match request", "matchers_key", key, "block", r.block.meta.ULID, "found_key", requestMatcherKey)
		return nil, nil, false
	}

	refs, err := index.ExpandPostings(p)
	if err != nil {
		level.Warn(r.block.logger).Log("msg", "can't expand decoded expanded postings cache", "err", err, "matchers_key", key, "block", r.block.meta.ULID)
		return nil, nil, false
	}
	return refs, pendingMatchers, true
}

// expandedPostings is the main logic of ExpandedPostings, without the promise wrapper.
func (r *bucketIndexReader) expandedPostings(ctx context.Context, ms []*labels.Matcher, stats *safeQueryStats) (returnRefs []storage.SeriesRef, pendingMatchers []*labels.Matcher, returnErr error) {
	postingGroups, err := toPostingGroups(ctx, ms, r.block.indexHeaderReader)
	if err != nil {
		return nil, nil, errors.Wrap(err, "toPostingGroups")
	}
	if len(postingGroups) == 0 {
		return nil, nil, nil
	}

	postingGroups, omittedPostingGroups := r.postingsStrategy.selectPostings(postingGroups)
	logSelectedPostingGroups(ctx, r.block.logger, r.block.meta.ULID, postingGroups, omittedPostingGroups)

	fetchedPostings, err := r.fetchPostings(ctx, extractLabels(postingGroups), stats)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get postings")
	}

	// The order of the fetched postings is the same as the order of the requested keys.
	// This is guaranteed by extractLabels.
	postingIndex := 0

	var groupAdds, groupRemovals []index.Postings
	for _, g := range postingGroups {
		if g.isSubtract {
			for _, l := range g.keys {
				groupRemovals = append(groupRemovals, checkNilPosting(l, fetchedPostings[postingIndex]))
				postingIndex++
			}
		} else {
			toMerge := make([]index.Postings, 0, len(g.keys))
			for _, l := range g.keys {
				toMerge = append(toMerge, checkNilPosting(l, fetchedPostings[postingIndex]))
				postingIndex++
			}

			groupAdds = append(groupAdds, index.Merge(ctx, toMerge...))
		}
	}

	result := index.Without(index.Intersect(groupAdds...), index.Merge(ctx, groupRemovals...))

	ps, err := index.ExpandPostings(result)
	if err != nil {
		return nil, nil, errors.Wrap(err, "expand")
	}

	// As of version two all series entries are 16 byte padded. All references
	// we get have to account for that to get the correct offset.
	version, err := r.block.indexHeaderReader.IndexVersion(ctx)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get index version")
	}
	if version >= 2 {
		for i, id := range ps {
			ps[i] = id * 16
		}
	}

	return ps, extractLabelMatchers(omittedPostingGroups), nil
}

func logSelectedPostingGroups(ctx context.Context, logger log.Logger, blockID ulid.ULID, selectedGroups, omittedGroups []postingGroup) {
	numKeyvals := 2 /* msg */ + 2 /* ulid */ + 4*(len(selectedGroups)+len(omittedGroups))
	keyvals := make([]any, 0, numKeyvals)
	keyvals = append(keyvals, "msg", "select posting groups")
	keyvals = append(keyvals, "ulid", blockID.String())

	formatGroup := func(g postingGroup) (string, int64) {
		if g.matcher == nil {
			return "ALL_POSTINGS", -1
		}
		return g.matcher.String(), g.totalSize
	}

	for i, g := range selectedGroups {
		matcherStr, groupSize := formatGroup(g)
		keyvals = append(keyvals, fmt.Sprintf("selected_%d", i), matcherStr)
		keyvals = append(keyvals, fmt.Sprintf("selected_%d_size", i), groupSize)
	}
	for i, g := range omittedGroups {
		matcherStr, groupSize := formatGroup(g)
		keyvals = append(keyvals, fmt.Sprintf("omitted_%d", i), matcherStr)
		keyvals = append(keyvals, fmt.Sprintf("omitted_%d_size", i), groupSize)
	}
	spanlogger.FromContext(ctx, logger).DebugLog(keyvals...)
}

func extractLabelMatchers(groups []postingGroup) []*labels.Matcher {
	if len(groups) == 0 {
		return nil
	}
	m := make([]*labels.Matcher, len(groups))
	for i := range groups {
		m[i] = groups[i].matcher
	}
	return m
}

// extractLabels returns the keys of the posting groups in the order that they are found in each posting group.
func extractLabels(groups []postingGroup) []labels.Label {
	numKeys := 0
	for _, pg := range groups {
		numKeys += len(pg.keys)
	}
	keys := make([]labels.Label, 0, numKeys)
	for _, pg := range groups {
		keys = append(keys, pg.keys...)
	}
	return keys
}

func extractLabelValues(offsets []streamindex.PostingListOffset) []string {
	vals := make([]string, len(offsets))
	for i := range offsets {
		vals[i] = offsets[i].LabelValue
	}
	return vals
}

var allPostingsKey = func() labels.Label {
	n, v := index.AllPostingsKey()
	return labels.Label{Name: n, Value: v}
}()

// toPostingGroups returns a set of labels for which to look up postings lists. It guarantees that
// each postingGroup's keys exist in the index.
func toPostingGroups(ctx context.Context, ms []*labels.Matcher, indexhdr indexheader.Reader) ([]postingGroup, error) {
	var (
		rawPostingGroups = make([]rawPostingGroup, 0, len(ms))
		allRequested     = false
		hasAdds          = false
	)

	for _, m := range ms {
		// Each group is separate to tell later what postings are intersecting with what.
		rawPostingGroups = append(rawPostingGroups, toRawPostingGroup(m))
	}

	// We can check whether their requested values exist
	// in the index. We do these checks in a specific order, so we minimize the reads from disk
	// and/or to minimize the number of label values we keep in memory (assumption being that indexhdr.LabelValues
	// is likely to return a lot of values).
	sort.Slice(rawPostingGroups, func(i, j int) bool {
		// First we check the non-lazy groups, since for those we only need to call PostingsOffset, which
		// is less expensive than LabelValues for the lazy groups.
		ri, rj := rawPostingGroups[i], rawPostingGroups[j]
		if ri.isLazy != rj.isLazy {
			return !ri.isLazy
		}

		// Within the lazy/non-lazy groups we sort by the number of keys they have.
		// The idea is that groups with fewer keys are more likely to match no actual series.
		if len(ri.keys) != len(rj.keys) {
			return len(ri.keys) < len(rj.keys)
		}

		// Sort by label name to make this a deterministic-ish sort.
		// We could still have two matchers for the same name. (`foo!="bar", foo!="baz"`)
		return ri.labelName < rj.labelName
	})

	postingGroups := make([]postingGroup, 0, len(rawPostingGroups)+1) // +1 for the AllPostings group we might add
	// Next we check whether the posting groups won't select an empty set of postings.
	// Based on the previous sorting, we start with the ones that have a known set of values because it's less expensive to check them in
	// the index header.
	for _, rawGroup := range rawPostingGroups {
		pg, err := rawGroup.toPostingGroup(ctx, indexhdr)
		if err != nil {
			return nil, errors.Wrap(err, "filtering posting group")
		}

		// If this group has no keys to work though and is not a subtract group, then it's an empty group.
		// We can shortcut this, since intersection with empty postings would return no postings.
		// E.g. `label="non-existent-value"` returns empty group.
		if !pg.isSubtract && len(pg.keys) == 0 {
			return nil, nil
		}

		postingGroups = append(postingGroups, pg)
		// If the group is a subtraction group, we must fetch all postings and remove the ones that this matcher selects.
		allRequested = allRequested || pg.isSubtract
		hasAdds = hasAdds || !pg.isSubtract
	}

	// We only need special All postings if there are no other adds. If there are, we can skip fetching
	// special All postings completely.
	if allRequested && !hasAdds {
		postingGroups = append(postingGroups, postingGroup{isSubtract: false, keys: []labels.Label{allPostingsKey}})
	}

	// If hasAdds is false, then there were no posting lists for any labels that we will intersect.
	// If all postings also weren't requested, then we will only be subtracting from an empty set.
	// Shortcut doing any set operations and just return an empty set here.
	// A query that might end up in this case is `{pod=~"non-existent-value.*"}`.
	if !allRequested && !hasAdds {
		return nil, nil
	}

	return postingGroups, nil
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
	version, err := r.block.indexHeaderReader.IndexVersion(ctx)
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
	fromCache := r.block.indexCache.FetchMultiPostings(ctx, r.block.userID, r.block.meta.ULID, keys)

	// Iterate over all groups and fetch posting from cache.
	// If we have a miss, mark key to be fetched in `ptrs` slice.
	// Overlaps are well handled by partitioner, so we don't need to deduplicate keys.
	for ix, key := range keys {
		// Get postings for the given key from cache first.
		if b, _ := fromCache.Next(); b != nil {
			stats.update(func(stats *queryStats) {
				stats.postingsTouched++
				stats.postingsTouchedSizeSum += len(b)
			})

			l, cachedLabelsKey, pendingMatchers, err := r.decodePostings(b, stats)
			if len(pendingMatchers) > 0 {
				return nil, fmt.Errorf("not expecting matchers on non-expanded postings for %s=%s in block %s, but got %s",
					key.Name, key.Value, r.block.meta.ULID, util.MatchersStringer(pendingMatchers))
			}
			if err == nil && cachedLabelsKey == encodeLabelForPostingsCache(key) {
				output[ix] = l
				continue
			}

			level.Warn(r.block.logger).Log(
				"msg", "can't decode cached postings",
				"err", err,
				"key", fmt.Sprintf("%+v", key),
				"labels_key", cachedLabelsKey,
				"block", r.block.meta.ULID,
				"bytes_len", len(b),
				"bytes_head_hex", hex.EncodeToString(b[:min(8, len(b))]),
			)
		}

		// Cache miss; save pointer for actual posting in index stored in object store.
		ptr, err := r.block.indexHeaderReader.PostingsOffset(ctx, key.Name, key.Value)
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
	parts := r.block.partitioners.postings.Partition(len(ptrs), func(i int) (start, end uint64) {
		return uint64(ptrs[i].ptr.Start), uint64(ptrs[i].ptr.End)
	})

	// Use a different TTL for postings based on the duration of the block.
	postingsTTL := indexcache.BlockTTL(r.block.meta)

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

				compressionTime := time.Duration(0)
				compressions, compressionErrors, compressedSize := 0, 0, 0

				// Reencode postings before storing to cache. If that fails, we store original bytes.
				// This can only fail, if postings data was somehow corrupted,
				// and there is nothing we can do about it.
				// Errors from corrupted postings will be reported when postings are used.
				compressions++
				s := time.Now()
				bep := newBigEndianPostings(pBytes[4:])
				dataToCache, err := diffVarintSnappyWithMatchersEncode(bep, bep.length(), encodeLabelForPostingsCache(keys[p.keyID]), nil)
				compressionTime = time.Since(s)
				if err == nil {
					compressedSize = len(dataToCache)
					r.block.indexCache.StorePostings(r.block.userID, r.block.meta.ULID, keys[p.keyID], dataToCache, postingsTTL)
				} else {
					compressionErrors = 1
					level.Warn(r.block.logger).Log(
						"msg", "couldn't encode postings for cache",
						"err", err,
						"user", r.block.userID,
						"block", r.block.meta.ULID,
						"label", keys[p.keyID],
					)
				}

				// Return postings. Truncate first 4 bytes which are length of posting.
				// Access to output is not protected by a mutex because each goroutine
				// is expected to handle a different set of keys.
				output[p.keyID] = newBigEndianPostings(pBytes[4:])

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

func encodeLabelForPostingsCache(l labels.Label) indexcache.LabelMatchersKey {
	var sb strings.Builder
	sb.Grow(len(l.Name) + 1 + len(l.Value))
	sb.WriteString(l.Name)
	sb.WriteByte('=')
	sb.WriteString(l.Value)
	return indexcache.LabelMatchersKey(sb.String())
}

func (r *bucketIndexReader) decodePostings(b []byte, stats *safeQueryStats) (index.Postings, indexcache.LabelMatchersKey, []*labels.Matcher, error) {
	// Even if this instance is not using compression, there may be compressed
	// entries in the cache written by other stores.
	var (
		l               index.Postings
		key             indexcache.LabelMatchersKey
		pendingMatchers []*labels.Matcher
		err             error
	)
	if !isDiffVarintSnappyWithMatchersEncodedPostings(b) {
		return nil, "", nil, errors.New("didn't find expected prefix for postings key")
	}

	s := time.Now()
	l, key, pendingMatchers, err = diffVarintSnappyMatchersDecode(b)

	stats.update(func(stats *queryStats) {
		stats.cachedPostingsDecompressions++
		stats.cachedPostingsDecompressionTimeSum += time.Since(s)
		if err != nil {
			stats.cachedPostingsDecompressionErrors++
		}
	})

	return l, key, pendingMatchers, err
}

// preloadSeries expects the provided ids to be sorted.
func (r *bucketIndexReader) preloadSeries(ctx context.Context, ids []storage.SeriesRef, stats *safeQueryStats) (loadedSeries *bucketIndexLoadedSeries, err error) {
	defer func(startTime time.Time) {
		spanLog := spanlogger.FromContext(ctx, r.block.logger)
		spanLog.DebugLog(
			"msg", "fetched series and chunk refs from object store",
			"block_id", r.block.meta.ULID.String(),
			"series_count", len(loadedSeries.series),
			"err", err,
			"duration", time.Since(startTime),
		)
	}(time.Now())

	timer := prometheus.NewTimer(r.block.metrics.seriesFetchDuration)
	defer timer.ObserveDuration()

	loaded := newBucketIndexLoadedSeries()

	// Load series from cache, overwriting the list of ids to preload
	// with the missing ones.
	fromCache, ids := r.block.indexCache.FetchMultiSeriesForRefs(ctx, r.block.userID, r.block.meta.ULID, ids)
	for id, b := range fromCache {
		loaded.addSeries(id, b)
	}

	parts := r.block.partitioners.series.Partition(len(ids), func(i int) (start, end uint64) {
		return uint64(ids[i]), uint64(ids[i] + tsdb.MaxSeriesSize)
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

var seriesOffsetReaders = &sync.Pool{New: func() any {
	return &offsetTrackingReader{r: bufio.NewReaderSize(nil, 32*1024)}
}}

// loadSeries expects the provided ids to be sorted.
func (r *bucketIndexReader) loadSeries(ctx context.Context, ids []storage.SeriesRef, refetch bool, start, end uint64, loaded *bucketIndexLoadedSeries, stats *safeQueryStats) error {
	defer r.recordLoadSeriesStats(stats, refetch, len(ids), start, end, time.Now())

	reader, err := r.block.indexRangeReader(ctx, int64(start), int64(end-start))
	if err != nil {
		return errors.Wrap(err, "read series range")
	}
	defer runutil.CloseWithLogOnErr(r.block.logger, reader, "loadSeries close range reader")

	offsetReader := seriesOffsetReaders.Get().(*offsetTrackingReader)
	defer seriesOffsetReaders.Put(offsetReader)

	offsetReader.Reset(start, reader)
	defer offsetReader.Release()

	// Use a slab pool to reduce allocations by sharding one large slice of bytes instead of allocating each series' bytes separately.
	// But in order to avoid a race condition with an async cache, we never release the pool and let the GC collect it.
	bytesPool := pool.NewSlabPool[byte](pool.NoopPool{}, seriesBytesSlabSize)

	// Use a different TTL for these series based on the duration of the block. Use a shorter TTL for blocks that
	// are going to be compacted and deleted shortly anyway.
	cacheTTL := indexcache.BlockTTL(r.block.meta)

	for i, id := range ids {
		// We iterate the series in order assuming they are sorted.
		err := offsetReader.SkipTo(uint64(id))
		if err != nil {
			return err
		}
		seriesSize, err := binary.ReadUvarint(offsetReader)
		if err != nil {
			return err
		}
		seriesBytes := bytesPool.Get(int(seriesSize))
		n, err := io.ReadFull(offsetReader, seriesBytes)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if i == 0 && refetch {
				return errors.Errorf("invalid remaining size, even after refetch, read %d, expected %d", n, seriesSize)
			}

			level.Warn(r.block.logger).Log("msg", "series size exceeded expected size; refetching", "series_id", id, "series_length", seriesSize, "max_series_size", tsdb.MaxSeriesSize)
			// Inefficient, but should be rare.
			// Fetch plus to get the size of next one if exists.
			return r.loadSeries(ctx, ids[i:], true, uint64(id), uint64(id)+binary.MaxVarintLen64+seriesSize+1, loaded, stats)
		} else if err != nil {
			return errors.Wrapf(err, "fetching series %d from block %s", id, r.block.meta.ULID)
		}
		loaded.addSeries(id, seriesBytes)

		r.block.indexCache.StoreSeriesForRef(r.block.userID, r.block.meta.ULID, id, seriesBytes, cacheTTL)
	}
	return nil
}

func (r *bucketIndexReader) recordLoadSeriesStats(stats *safeQueryStats, refetch bool, numSeries int, start, end uint64, loadStartTime time.Time) {
	stats.update(func(stats *queryStats) {
		if !refetch {
			// only the root loadSeries will record the time
			stats.seriesFetchDurationSum += time.Since(loadStartTime)
		} else {
			stats.seriesRefetches++
		}
		stats.seriesFetched += numSeries
		stats.seriesFetchedSizeSum += int(end - start)
	})
}

// Close released the underlying resources of the reader.
func (r *bucketIndexReader) Close() error {
	r.block.pendingReaders.Done()
	return nil
}

// LookupLabelsSymbols populates label set strings from symbolized label set.
func (r *bucketIndexReader) LookupLabelsSymbols(ctx context.Context, symbolized []symbolizedLabel, builder *labels.ScratchBuilder) (labels.Labels, error) {
	builder.Reset()
	for _, s := range symbolized {
		ln, err := r.dec.LookupSymbol(ctx, s.name)
		if err != nil {
			return labels.EmptyLabels(), errors.Wrap(err, "lookup label name")
		}
		lv, err := r.dec.LookupSymbol(ctx, s.value)
		if err != nil {
			return labels.EmptyLabels(), errors.Wrap(err, "lookup label value")
		}
		builder.Add(ln, lv)
	}
	return builder.Labels(), nil
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

// unsafeLoadSeries returns symbolized labels for the series identified by the reference if the series has at least one chunk in the block.
// unsafeLoadSeries also populates the given chunk metas slice if skipChunks is set to false. The returned chunkMetas will be in the same
// order as in the index, which at this point is ordered by minTime and by their ref. The returned chunk metas are all the chunk for the series.
// unsafeLoadSeries returns false, when there are no series data.
//
// Error is returned on decoding error or if the reference does not resolve to a known series.
//
// It's NOT safe to call this function concurrently with addSeries().
func (l *bucketIndexLoadedSeries) unsafeLoadSeries(ref storage.SeriesRef, chks *[]chunks.Meta, skipChunks bool, stats *queryStats, lsetPool *pool.SlabPool[symbolizedLabel]) (ok bool, _ []symbolizedLabel, err error) {
	b, ok := l.series[ref]
	if !ok {
		return false, nil, errors.Errorf("series %d not found", ref)
	}
	stats.seriesProcessed++
	stats.seriesProcessedSizeSum += len(b)
	return decodeSeries(b, lsetPool, chks, skipChunks)
}
