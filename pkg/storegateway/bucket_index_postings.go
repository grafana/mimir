// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// rawPostingGroup keeps posting keys for single matcher. It is raw because there is no guarantee
// that the keys in the group have a corresponding postings list in the index.
// Logical result of the group is:
// If isLazy == true: keys will be empty and lazyMatcher will be non-nil. Call toPostingGroup() to populate the keys.
// If isSubtract == true: special All postings minus postings for keys labels.
// If isSubtract == false: merge of postings for keys labels.
// This computation happens in toPostingGroups.
type rawPostingGroup struct {
	isSubtract bool
	labelName  string
	keys       []labels.Label

	isLazy  bool
	matcher *labels.Matcher
	prefix  string
}

func newRawIntersectingPostingGroup(m *labels.Matcher, keys []labels.Label) rawPostingGroup {
	return rawPostingGroup{
		isSubtract: false,
		labelName:  m.Name,
		keys:       keys,
		matcher:    m,
	}
}

func newRawSubtractingPostingGroup(m *labels.Matcher, keys []labels.Label) rawPostingGroup {
	return rawPostingGroup{
		isSubtract: true,
		labelName:  m.Name,
		keys:       keys,
		matcher:    m,
	}
}

func newLazyIntersectingPostingGroup(m *labels.Matcher) rawPostingGroup {
	return rawPostingGroup{
		isLazy:     true,
		isSubtract: false,
		labelName:  m.Name,
		prefix:     m.Prefix(),
		matcher:    m,
	}
}

func newLazySubtractingPostingGroup(m *labels.Matcher) rawPostingGroup {
	return rawPostingGroup{
		isLazy:     true,
		isSubtract: true,
		labelName:  m.Name,
		prefix:     m.Prefix(),
		matcher:    m,
	}
}

// toPostingGroup returns a postingGroup which shares the underlying keys slice with g.
// This means that after calling toPostingGroup g.keys will be modified.
func (g rawPostingGroup) toPostingGroup(r indexheader.Reader) (postingGroup, error) {
	var (
		keys      []labels.Label
		totalSize int64
	)
	if g.isLazy {
		filter := g.matcher.Matches
		if g.isSubtract {
			filter = not(filter)
		}
		vals, err := r.LabelValuesOffsets(g.labelName, g.prefix, filter)
		if err != nil {
			return postingGroup{}, err
		}
		keys = make([]labels.Label, len(vals))
		for i := range vals {
			keys[i] = labels.Label{Name: g.labelName, Value: vals[i].LabelValue}
			totalSize += vals[i].Off.End - vals[i].Off.Start
		}
	} else {
		var err error
		keys, totalSize, err = g.filterNonExistingKeys(r)
		if err != nil {
			return postingGroup{}, errors.Wrap(err, "filter posting keys")
		}
	}

	return postingGroup{
		isSubtract: g.isSubtract,
		matcher:    g.matcher,
		keys:       keys,
		totalSize:  totalSize,
	}, nil
}

// filterNonExistingKeys uses the indexheader.Reader to filter out any label values that do not exist in this index.
// modifies the underlying keys slice of the group. Do not use the rawPostingGroup after calling toPostingGroup.
func (g rawPostingGroup) filterNonExistingKeys(r indexheader.Reader) ([]labels.Label, int64, error) {
	var (
		writeIdx  int
		totalSize int64
	)
	for _, l := range g.keys {
		offset, err := r.PostingsOffset(l.Name, l.Value)
		if errors.Is(err, indexheader.NotFoundRangeErr) {
			// This label name and value doesn't exist in this block, so there are 0 postings we can match.
			// Try with the rest of the set matchers, maybe they can match some series.
			// Continue so we overwrite it next time there's an existing value.
			continue
		} else if err != nil {
			return nil, 0, err
		}
		g.keys[writeIdx] = l
		writeIdx++
		totalSize += offset.End - offset.Start
	}
	return g.keys[:writeIdx], totalSize, nil
}

func toRawPostingGroup(m *labels.Matcher) rawPostingGroup {
	if setMatches := m.SetMatches(); len(setMatches) > 0 && (m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp) {
		keys := make([]labels.Label, 0, len(setMatches))
		for _, val := range setMatches {
			keys = append(keys, labels.Label{Name: m.Name, Value: val})
		}
		if m.Type == labels.MatchNotRegexp {
			return newRawSubtractingPostingGroup(m, keys)
		}
		return newRawIntersectingPostingGroup(m, keys)
	}

	if m.Value != "" {
		// Fast-path for equal matching.
		// Works for every case except for `foo=""`, which is a special case, see below.
		if m.Type == labels.MatchEqual {
			return newRawIntersectingPostingGroup(m, []labels.Label{{Name: m.Name, Value: m.Value}})
		}

		// If matcher is `label!="foo"`, we select an empty label value too,
		// i.e., series that don't have this label.
		// So this matcher selects all series in the storage,
		// except for the ones that do have `label="foo"`
		if m.Type == labels.MatchNotEqual {
			return newRawSubtractingPostingGroup(m, []labels.Label{{Name: m.Name, Value: m.Value}})
		}
	}

	// This is a more generic approach for the previous case.
	// Here we can enter with `label=""` or regexp matchers that match the empty value,
	// like `=~"|foo" or `error!~"5..".
	// Remember: if the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		return newLazySubtractingPostingGroup(m)
	}

	// Our matcher does not match the empty value, so we just need the postings that correspond
	// to label values matched by the matcher.
	return newLazyIntersectingPostingGroup(m)
}

func not(filter func(string) bool) func(string) bool {
	return func(s string) bool { return !filter(s) }
}

// rawPostingGroup keeps posting keys for single matcher. Logical result of the group is:
// If isSubtract == true: special All postings minus postings for keys labels.
// If isSubtract == false: merge of postings for keys labels.
// All the labels in keys should have a corresponding postings list in the index.
// This computation happens in expandedPostings.
type postingGroup struct {
	isSubtract bool
	matcher    *labels.Matcher
	keys       []labels.Label

	// totalSize is the size in bytes of all the posting lists for keys.
	totalSize int64
}

type postingPtr struct {
	keyID int
	ptr   index.Range
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	i    int
	list []byte
	cur  storage.SeriesRef
}

// TODO(bwplotka): Expose those inside Prometheus.
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{
		list: list,
		i:    -4,
	}
}

func (it *bigEndianPostings) At() storage.SeriesRef {
	return it.cur
}

func (it *bigEndianPostings) Next() bool {
	i := it.i + 4
	if i+4 > len(it.list) {
		return false
	}

	it.i = i
	it.cur = storage.SeriesRef(binary.BigEndian.Uint32(it.list[i:]))
	return true
}

func (it *bigEndianPostings) Seek(x storage.SeriesRef) bool {
	if it.cur >= x {
		return true
	}

	start := it.i
	if start < 0 {
		start = 0
	}

	l := it.list[start:]
	num := len(l) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(l[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = storage.SeriesRef(binary.BigEndian.Uint32(l[j:]))
		it.i = start + j
		return true
	}

	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

func (it *bigEndianPostings) Reset() {
	it.i = -1
	it.cur = 0
}

// Returns number of remaining postings values.
func (it *bigEndianPostings) length() int {
	i := max(it.i, 0)
	return (len(it.list) - i) / 4
}

// filterPostingsByCachedShardHash filters the input postings by the provided shard. It filters only
// postings for which we have their series hash already in the cache; if a series is not in the cache,
// postings will be kept in the output.
func filterPostingsByCachedShardHash(ps []storage.SeriesRef, shard *sharding.ShardSelector, seriesHashCache seriesHasher, stats *queryStats) []storage.SeriesRef {
	writeIdx := 0
	for readIdx := 0; readIdx < len(ps); readIdx++ {
		seriesID := ps[readIdx]
		hash, ok := seriesHashCache.CachedHash(seriesID, stats)
		// Keep the posting if it's not in the cache, or it's in the cache and belongs to our shard.
		if !ok || hash%uint64(shard.ShardCount) == uint64(shard.ShardIndex) {
			ps[writeIdx] = seriesID
			writeIdx++
			continue
		}

		// We can filter out the series because doesn't belong to the requested shard,
		// so we're not going to increase the writeIdx.
	}

	// Shrink the size.
	ps = ps[:writeIdx]
	return ps
}

// paddedPostings adds the v2 index padding to postings without expanding them
type paddedPostings struct {
	index.Postings
}

func (p paddedPostings) Seek(v storage.SeriesRef) bool {
	unpadded := v / 16
	if unpadded*16 != v {
		// if someone is looking for 17 (they shouldn't but who knows)
		// then we don't want stop seeking at 16 ((v/16) * 16), so we'll look for the next number
		// this is semantically correct
		unpadded++
	}
	return p.Postings.Seek(unpadded)
}

func (p paddedPostings) At() storage.SeriesRef {
	return p.Postings.At() * 16
}

func resizePostings(b []byte) ([]byte, error) {
	d := encoding.Decbuf{B: b}
	n := d.Be32int()
	if d.Err() != nil {
		return nil, errors.Wrap(d.Err(), "read postings list")
	}

	// 4 for postings number of entries, then 4, foreach each big endian posting.
	size := 4 + n*4
	if len(b) < size {
		return nil, encoding.ErrInvalidSize
	}
	return b[:size], nil
}

func checkNilPosting(l labels.Label, p index.Postings) index.Postings {
	if p == nil {
		// This should not happen. Debug for https://github.com/thanos-io/thanos/issues/874.
		return index.ErrPostings(errors.Errorf("postings is nil for %s, it was never fetched", l))
	}
	return p
}

type postingsSelectionStrategy interface {
	// name should be a static string which identifies this strategy.
	// The return value can be later used to find the strategy which yielded a partitioning of postingGroups.
	name() string

	// selectPostings can modify the passed slice of posting groups.
	selectPostings([]postingGroup) (selected, omitted []postingGroup)
}

type selectAllStrategy struct{}

func (selectAllStrategy) name() string {
	return tsdb.AllPostingsStrategy
}

func (selectAllStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	return groups, nil
}

// worstCaseFetchedDataStrategy select a few of the posting groups such that their total size
// does not exceed the size of series in the worst case. The worst case is fetching all series
// in the smallest non-subtractive posting group - this is effectively the
// upper bound on how many series all the posting groups can select after being intersected/subtracted.
//
// This strategy is meant to prevent fetching some the largest posting lists in an index. Those are usually
// less selective the smaller ones and only add cost to fetching.
//
// The strategy greedily selects the first N posting groups (sorted in ascending order by their size)
// whose combined size doesn't exceed the size of series in the worst case. The rest of the posting groups
// are omitted.
// worstCaseFetchedDataStrategy uses a fixed estimation about the size of series in the index (tsdb.EstimatedSeriesP99Size).
//
// For example, given the query `cpu_seconds_total{namespace="ns1"}`, if `namespace="ns1"` selects 1M series and
// `__name__="cpu_seconds_total"` selects 500K series, then the strategy calculates that the whole query will
// select no more than 500K series. It uses this to calculate that in the worst case we will fetch 500K * tsdb.EstimatedSeriesP99Size
// bytes for the series = 256 MB. So it will not fetch more than 256 MB of posting lists.
//
// We found that this strategy may cause increased API calls for cases where it omits the __name__ posting group.
// Because of this, the strategy always selects the __name__ posting group regardless of its size.
type worstCaseFetchedDataStrategy struct {
	// postingListActualSizeFactor affects how posting lists are summed together.
	// Postings lists have different sizes in the bucket and the cache.
	// The size in a postingGroup is the size in the block.
	// Since we don't know whether we will fetch the lists from the cache
	// or the bucket we can adjust the size we sum by a factor.
	postingListActualSizeFactor float64
}

func (s worstCaseFetchedDataStrategy) name() string {
	return fmt.Sprintf(tsdb.WorstCasePostingsStrategy+"%0.1f", s.postingListActualSizeFactor)
}

func (s worstCaseFetchedDataStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].totalSize < groups[j].totalSize
	})

	maxSelectedSeriesCount := numSeriesInSmallestIntersectingPostingGroup(groups)
	if maxSelectedSeriesCount == 0 {
		// This should also cover the case of all postings group. all postings is requested only when there is no
		// additive group.
		return groups, nil
	}

	var (
		selectedSize                   int64
		atLeastOneIntersectingSelected bool
		maxSelectedSize                = maxSelectedSeriesCount * tsdb.EstimatedSeriesP99Size
	)
	selected = groups
	for i, g := range groups {
		postingListSize := int64(float64(g.totalSize) * s.postingListActualSizeFactor)
		if atLeastOneIntersectingSelected && selectedSize+postingListSize > maxSelectedSize {
			selected = groups[:i]
			omitted = groups[i:]
			break
		}
		selectedSize += postingListSize
		atLeastOneIntersectingSelected = atLeastOneIntersectingSelected || !g.isSubtract
	}

	// We want to include the __name__ group because excluding it is more likely to make
	// makes the more sparse in the index. Selecting more sparse series results in more API calls
	// to the object store. This is because series are first sorted by their __name__ label
	// (assuming there are no labels starting with uppercase letters).
	for i, g := range omitted {
		if len(g.keys) > 0 && g.keys[0].Name == labels.MetricName {
			// Since the underlying slice for selected and omitted in the same, we need to swap the group so that
			// we don't overwrite the first group when we append to selected.
			omitted[0], omitted[i] = omitted[i], omitted[0]
			omitted = omitted[1:]
			selected = selected[:len(selected)+1]
			break
		}
	}
	return selected, omitted
}

// numSeriesInSmallestIntersectingPostingGroup receives a sorted slice of posting groups by their totalSize.
// It returns the number of postings in the smallest intersecting (non-subtractive) postingGroup.
// It returns 0 if there was no intersecting posting group that also wasn't the all-postings group.
func numSeriesInSmallestIntersectingPostingGroup(groups []postingGroup) int64 {
	var minGroupSize int64
	for _, g := range groups {
		if !g.isSubtract && !(len(g.keys) == 1 && g.keys[0] == allPostingsKey) {
			// The size of each posting list contains 4 bytes with the number of entries.
			// We shouldn't count these as series.
			groupSize := g.totalSize - int64(len(g.keys)*4)
			if minGroupSize == 0 || minGroupSize > groupSize {
				minGroupSize = groupSize
			}
		}
	}
	return minGroupSize / tsdb.BytesPerPostingInAPostingList
}

// speculativeFetchedDataStrategy selects postings lists in a very similar way to worstCaseFetchedDataStrategy,
// except it speculates on the size of the actual series after intersecting the selected posting lists.
// Right now it assumes that each intersecting posting list will halve the number of series selected by the query.
//
// For example, given the query `cpu_seconds_total{namespace="ns1"}`, if `namespace="ns1"` selects 1M series and
// `__name__="cpu_seconds_total"` selects 500K series, then the speculative strategy assumes the whole query will
// select 250K series. It uses this to calculate that in the worst case we will fetch 250K * tsdb.EstimatedSeriesP99Size
// bytes for the series = 128 MB. So it will not fetch more than 128 MB of posting lists.
type speculativeFetchedDataStrategy struct{}

func (s speculativeFetchedDataStrategy) name() string {
	return tsdb.SpeculativePostingsStrategy
}

func (s speculativeFetchedDataStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].totalSize < groups[j].totalSize
	})

	maxSelectedSeriesCount := numSeriesInSmallestIntersectingPostingGroup(groups)
	if maxSelectedSeriesCount == 0 {
		// This should also cover the case of all postings group. all postings is requested only when there is no
		// additive group.
		return groups, nil
	}

	var (
		selectedSize                   int64
		atLeastOneIntersectingSelected bool
		maxSelectedSize                = maxSelectedSeriesCount * tsdb.EstimatedSeriesP99Size
	)
	for i, g := range groups {
		if atLeastOneIntersectingSelected && selectedSize+g.totalSize > maxSelectedSize {
			return groups[:i], groups[i:]
		}
		selectedSize += g.totalSize
		atLeastOneIntersectingSelected = atLeastOneIntersectingSelected || !g.isSubtract

		// We assume that every intersecting posting list after the first one will
		// filter out half of the postings.
		if i > 0 && !g.isSubtract {
			maxSelectedSize /= 2
		}
	}
	return groups, nil
}

// labelValuesPostingsStrategy works in a similar way to worstCaseFetchedDataStrategy.
// The differences are:
//   - it doesn't a factor for the posting list size
//   - as the bounded maximum for fetched data it also takes into account the provided allLabelValues;
//     this is useful in LabelValues calls where we have to decide between fetching (some expanded postings + series),
//     (expanded postings + series), or (expanded postings + postings for each label value).
type labelValuesPostingsStrategy struct {
	matchersStrategy postingsSelectionStrategy
	allLabelValues   []streamindex.PostingListOffset
}

func (w labelValuesPostingsStrategy) name() string {
	return "lv-" + w.matchersStrategy.name()
}

func (w labelValuesPostingsStrategy) selectPostings(matchersGroups []postingGroup) (partialMatchersGroups, omittedMatchersGroups []postingGroup) {
	partialMatchersGroups, omittedMatchersGroups = w.matchersStrategy.selectPostings(matchersGroups)

	maxPossibleSeriesSize := numSeriesInSmallestIntersectingPostingGroup(partialMatchersGroups) * tsdb.EstimatedSeriesP99Size
	completeMatchersSize := postingGroupsTotalSize(matchersGroups)

	completeMatchersPlusLabelValuesSize := completeMatchersSize + postingsListsTotalSize(w.allLabelValues)
	completeMatchersPlusSeriesSize := completeMatchersSize + maxPossibleSeriesSize
	partialMatchersPlusSeriesSize := postingGroupsTotalSize(partialMatchersGroups) + maxPossibleSeriesSize

	if util_math.Min(completeMatchersPlusSeriesSize, completeMatchersPlusLabelValuesSize) < partialMatchersPlusSeriesSize {
		return matchersGroups, nil
	}
	return partialMatchersGroups, omittedMatchersGroups
}

func (w labelValuesPostingsStrategy) preferSeriesToPostings(postings []storage.SeriesRef) bool {
	return int64(len(postings)*tsdb.EstimatedSeriesP99Size) < postingsListsTotalSize(w.allLabelValues)
}

func postingGroupsTotalSize(groups []postingGroup) (n int64) {
	for _, g := range groups {
		n += g.totalSize
	}
	return
}

func postingsListsTotalSize(postingLists []streamindex.PostingListOffset) (n int64) {
	for _, l := range postingLists {
		n += l.Off.End - l.Off.Start
	}
	return
}
