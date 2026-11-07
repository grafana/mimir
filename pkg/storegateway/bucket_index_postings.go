// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

// lazyGroupInitialCapacity is the initial capacity of the keys and offsets of a lazy posting group
// when the number of matching label values is unknown.
const lazyGroupInitialCapacity = 1024

// rawPostingGroup keeps posting keys for single matcher. It is raw because there is no guarantee
// that the keys in the group have a corresponding postings list in the index.
// Logical result of the group is:
// If isLazy == true: keys will be empty and lazyMatcher will be non-nil. Call toPostingGroup() to populate the keys.
// The scan of a lazy group can stop early, and then toPostingGroup() leaves the keys empty.
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

// toPostingGroup returns the posting group of the raw group, with the keys and the offsets of its label values.
// For a group that is not lazy, the posting group shares the underlying keys slice with g.
// This means that after calling toPostingGroup g.keys will be modified.
// The scan of a lazy group stops when the total size of the posting lists passes omitThreshold.
// The group then has no keys and no offsets, and its total size and its count of label values are lower bounds.
// Pass math.MaxInt64 to keep the keys of every label value.
func (g rawPostingGroup) toPostingGroup(ctx context.Context, r indexheader.Reader, omitThreshold int64) (postingGroup, error) {
	if g.isLazy {
		filter := g.matcher.Matches
		if g.isSubtract {
			filter = not(filter)
		}

		// A matcher that selects every label value keeps all of them, so the slices can start with the estimate from the scan.
		// For any other matcher the number of matches is unknown, so the slices start small and grow.
		selectsEveryLabelValue := g.selectsEveryLabelValue()

		// The visitor captures these variables, so they live on the heap.
		// They are declared in this block, so that a group that is not lazy does not allocate them.
		var (
			keys            []labels.Label
			offsets         []index.Range
			totalSize       int64
			estimatedValues int
			visitedValues   int
			stoppedEarly    bool
		)
		err := r.ForEachLabelValueOffset(ctx, g.labelName, g.prefix, filter, streamindex.LabelValueVisitor{
			SizeHint: func(estimate int) {
				estimatedValues = estimate
				capacity := estimate
				if !selectsEveryLabelValue {
					capacity = min(estimate, lazyGroupInitialCapacity)
				}
				keys = make([]labels.Label, 0, capacity)
				offsets = make([]index.Range, 0, capacity)
			},
			Visit: func(value []byte, off index.Range) bool {
				visitedValues++
				totalSize += off.End - off.Start
				if totalSize > omitThreshold {
					// Selection certainly omits this group, so it never reads the keys.
					// Stop the scan. The group that toPostingGroup returns has no keys, so the collected keys are garbage.
					stoppedEarly = true
					return false
				}

				if len(keys) == cap(keys) {
					// Double the capacity, but stop at the estimate from SizeHint.
					// All allocations together then stay below twice the final size, where append grows by a quarter.
					newCapacity := 2 * cap(keys)
					if estimatedValues > len(keys) {
						newCapacity = min(newCapacity, estimatedValues)
					}
					keys = append(make([]labels.Label, 0, newCapacity), keys...)
					offsets = append(make([]index.Range, 0, newCapacity), offsets...)
				}
				keys = append(keys, labels.Label{Name: g.labelName, Value: string(value)})
				offsets = append(offsets, off)
				return true
			},
		})
		if err != nil {
			return postingGroup{}, err
		}
		if stoppedEarly {
			return postingGroup{
				isSubtract:   g.isSubtract,
				matcher:      g.matcher,
				totalSize:    totalSize,
				droppedKeys:  visitedValues,
				stoppedEarly: true,
			}, nil
		}

		// Release the unused capacity of a small result, because the group keeps the slices until the request ends.
		if len(keys)*4 < cap(keys) {
			keys = slices.Clone(keys)
			offsets = slices.Clone(offsets)
		}

		return postingGroup{
			isSubtract: g.isSubtract,
			matcher:    g.matcher,
			keys:       keys,
			offsets:    offsets,
			totalSize:  totalSize,
		}, nil
	}

	keys, offsets, totalSize, err := g.filterNonExistingKeys(ctx, r)
	if err != nil {
		return postingGroup{}, errors.Wrap(err, "filter posting keys")
	}
	return postingGroup{
		isSubtract: g.isSubtract,
		matcher:    g.matcher,
		keys:       keys,
		offsets:    offsets,
		totalSize:  totalSize,
	}, nil
}

// selectsEveryLabelValue returns true when a scan of the group keeps every label value of the label.
// An intersecting group keeps every label value for the matchers !="" and =~".+", which accept every non-empty label value.
// A subtracting group keeps the label values that its matcher does not match.
// So it keeps every label value for the matchers ="" and !~".+", which accept no non-empty label value.
func (g rawPostingGroup) selectsEveryLabelValue() bool {
	if !g.isLazy || g.matcher == nil || g.prefix != "" {
		return false
	}
	if g.isSubtract {
		return (g.matcher.Type == labels.MatchEqual && g.matcher.Value == "") ||
			(g.matcher.Type == labels.MatchNotRegexp && g.matcher.Value == ".+")
	}
	return (g.matcher.Type == labels.MatchNotEqual && g.matcher.Value == "") ||
		(g.matcher.Type == labels.MatchRegexp && g.matcher.Value == ".+")
}

// filterNonExistingKeys uses the indexheader.Reader to filter out any label values that do not exist in this index.
// modifies the underlying keys slice of the group. Do not use the rawPostingGroup after calling toPostingGroup.
func (g rawPostingGroup) filterNonExistingKeys(ctx context.Context, r indexheader.Reader) ([]labels.Label, []index.Range, int64, error) {
	var (
		writeIdx  int
		totalSize int64
		offsets   []index.Range
	)

	offsets = make([]index.Range, len(g.keys))
	for _, l := range g.keys {
		offset, err := r.PostingsOffset(ctx, l.Name, l.Value)
		if errors.Is(err, indexheader.NotFoundRangeErr) {
			// This label name and value doesn't exist in this block, so there are 0 postings we can match.
			// Try with the rest of the set matchers, maybe they can match some series.
			// Continue so we overwrite it next time there's an existing value.
			continue
		} else if err != nil {
			return nil, nil, 0, err
		}
		g.keys[writeIdx] = l
		offsets[writeIdx] = offset
		writeIdx++
		totalSize += offset.End - offset.Start
	}
	return g.keys[:writeIdx], offsets[:writeIdx], totalSize, nil
}

func toRawPostingGroup(m *labels.Matcher) rawPostingGroup {
	if setMatches := m.SetMatches(); len(setMatches) > 0 && (m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp) {
		keys := make([]labels.Label, 0, len(setMatches))
		for _, val := range setMatches {
			keys = append(keys, labels.Label{Name: m.Name, Value: val})
		}
		if m.Type == labels.MatchNotRegexp {
			if m.Matches("") { // e.g. !~"foo|bar"
				return newRawSubtractingPostingGroup(m, keys)
			}
			// e.g. !~"|foo": matcher excludes absent-label series, which a
			// subtracting group can't express — fall through to the lazy path.
		} else if !m.Matches("") { // MatchRegexp not matching ""
			return newRawIntersectingPostingGroup(m, keys)
		}
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
// A group with droppedKeys > 0 has no keys and no offsets, only a matcher, a total size and a count of label values.
// expandedPostings scans the label of such a group when selection selects it.
// This computation happens in expandedPostings.
type postingGroup struct {
	isSubtract bool
	matcher    *labels.Matcher
	keys       []labels.Label
	// offsets must match 1:1 with keys
	offsets []index.Range

	// totalSize is the size in bytes of all the posting lists for keys.
	totalSize int64

	// droppedKeys counts the label values that the group represents without keeping them in keys and offsets.
	// expandedPostings scans the group again when selection selects it.
	droppedKeys int
	// sizeEstimated is true when totalSize and droppedKeys are estimates from the sparse index-header.
	sizeEstimated bool
	// stoppedEarly is true when toPostingGroup stopped the scan after the omit threshold.
	// totalSize and droppedKeys are then lower bounds, and totalSize is above the omit threshold.
	stoppedEarly bool
}

// keyCount returns the number of label values that the group represents.
func (g postingGroup) keyCount() int { return len(g.keys) + g.droppedKeys }

type postingPtr struct {
	keyID int
	ptr   index.Range
}

// bigEndianPostings implements the Postings interface over a byte stream of
// big endian numbers.
type bigEndianPostings struct {
	list []byte
	cur  storage.SeriesRef
}

// TODO(bwplotka): Expose those inside Prometheus.
func newBigEndianPostings(list []byte) *bigEndianPostings {
	return &bigEndianPostings{list: list}
}

func (it *bigEndianPostings) At() storage.SeriesRef {
	return it.cur
}

func (it *bigEndianPostings) Next() bool {
	if len(it.list) >= 4 {
		it.cur = storage.SeriesRef(binary.BigEndian.Uint32(it.list))
		it.list = it.list[4:]
		return true
	}
	return false
}

func (it *bigEndianPostings) Seek(x storage.SeriesRef) bool {
	if it.cur >= x {
		return true
	}

	num := len(it.list) / 4
	// Do binary search between current position and end.
	i := sort.Search(num, func(i int) bool {
		return binary.BigEndian.Uint32(it.list[i*4:]) >= uint32(x)
	})
	if i < num {
		j := i * 4
		it.cur = storage.SeriesRef(binary.BigEndian.Uint32(it.list[j:]))
		it.list = it.list[j+4:]
		return true
	}
	it.list = nil
	return false
}

func (it *bigEndianPostings) Err() error {
	return nil
}

// Returns number of remaining postings values.
func (it *bigEndianPostings) length() int {
	return len(it.list) / 4
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

// omitThresholdStrategy is implemented by a postingsSelectionStrategy that can report, before toPostingGroups builds a group,
// a total size above which selectPostings certainly omits the group.
type omitThresholdStrategy interface {
	// omitThreshold returns a total size in bytes. selectPostings omits a group of the label named labelName when the total size
	// of the group is above this size, whichever groups toPostingGroups builds after it.
	// built holds the groups that toPostingGroups built before.
	// It returns math.MaxInt64 when the group must keep its keys.
	omitThreshold(built []postingGroup, labelName string) int64
}

type postingsSelectionStrategy interface {
	// name should be a static string which identifies this strategy.
	// The return value can be later used to find the strategy which yielded a partitioning of postingGroups.
	name() string

	// selectPostings can modify the passed slice of posting groups.
	// A group can have droppedKeys > 0. It has a matcher, a total size and a count of label values, but no keys and no offsets.
	// For a group with stoppedEarly, the total size and the count of label values are lower bounds.
	// A strategy must decide with keyCount, totalSize and the matcher, and it must not expect keys or offsets in such a group.
	selectPostings([]postingGroup) (selected, omitted []postingGroup)
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
	return fmt.Sprintf("worst-case%0.1f", s.postingListActualSizeFactor)
}

// scaledSize returns the size of a posting group that selectPostings sums, after it applies postingListActualSizeFactor.
// It returns math.MaxInt64 when the product does not fit in an int64.
func (s worstCaseFetchedDataStrategy) scaledSize(size int64) int64 {
	scaled := float64(size) * s.postingListActualSizeFactor
	if scaled >= math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(scaled)
}

// omitThreshold implements omitThresholdStrategy.
//
// selectPostings omits a group when the scaled size of the group alone is above the budget.
// The budget is the number of series in the smallest intersecting group, times tsdb.EstimatedSeriesP99Size.
// A group that toPostingGroups builds later can only lower the smallest intersecting group, so it can only lower the budget.
// So a group above the threshold stays omitted.
// Every size between the stopped size and the real size of a group is above the threshold too.
// So selectPostings selects and omits the same groups with the real size and with the stopped size.
// Only the order of the omitted groups can differ.
func (s worstCaseFetchedDataStrategy) omitThreshold(built []postingGroup, labelName string) int64 {
	if labelName == model.MetricNameLabel {
		// selectPostings selects the first omitted group of __name__ again, and it needs the keys of that group.
		return math.MaxInt64
	}

	// smallestBytes is the size of the series in the smallest intersecting group, with 4 bytes for each series.
	smallestBytes := smallestIntersectingGroupBytes(built)
	numSeries := smallestBytes / tsdb.BytesPerPostingInAPostingList
	if numSeries == 0 {
		// selectPostings selects every group when there is no intersecting group, or when the smallest group has no series.
		return math.MaxInt64
	}

	// Every posting list holds at least one series, and a posting list with n series takes 4+4n bytes, which is at most 8n bytes.
	// So the series of a group take at least half of its total size, and the total size of a group is at most twice its series bytes.
	// A group with a total size above 2*smallestBytes has more series than the smallest intersecting group,
	// so it cannot lower the budget, and it sorts after the smallest intersecting group.
	minThreshold := 2 * smallestBytes

	smallestOmittedSize := s.smallestSizeAbove(numSeries * tsdb.EstimatedSeriesP99Size)
	if smallestOmittedSize == math.MaxInt64 {
		return math.MaxInt64
	}
	return max(smallestOmittedSize-1, minThreshold)
}

// smallestSizeAbove returns the smallest size whose scaled size is above the budget. The budget must be positive.
// It returns math.MaxInt64 when no size that a posting group can have is above the budget, and when the factor is not usable.
// A factor that is not positive, that is NaN, or that is above maxFactor is not usable.
// Above maxFactor, selectPostings overflows for the size of a large posting group.
func (s worstCaseFetchedDataStrategy) smallestSizeAbove(budget int64) int64 {
	const (
		maxFactor = 1 << 20
		// maxSize is 4 TiB, which is more than the size of any posting group.
		maxSize = 1 << 42
	)
	// The negation also rejects NaN, because every comparison with NaN is false.
	factor := s.postingListActualSizeFactor
	if !(factor > 0) || factor > maxFactor || s.scaledSize(maxSize) <= budget {
		return math.MaxInt64
	}

	// scaledSize does not decrease when the size grows, so a binary search finds the smallest size above the budget.
	// The search keeps scaledSize(low) <= budget and scaledSize(high) > budget.
	low, high := int64(0), int64(maxSize)
	for high-low > 1 {
		mid := low + (high-low)/2
		if s.scaledSize(mid) > budget {
			high = mid
		} else {
			low = mid
		}
	}
	return high
}

func (s worstCaseFetchedDataStrategy) selectPostings(groups []postingGroup) (selected, omitted []postingGroup) {
	// A stable sort keeps the order of groups with equal sizes, which is the order in which toPostingGroups built them.
	// A group that stopped early has a smaller size than the real group, and the stable order keeps the selection the same.
	slices.SortStableFunc(groups, func(a, b postingGroup) int {
		return cmp.Compare(a.totalSize, b.totalSize)
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
		postingListSize := s.scaledSize(g.totalSize)
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
		labelName := ""
		if g.matcher != nil {
			labelName = g.matcher.Name
		} else if len(g.keys) > 0 {
			labelName = g.keys[0].Name
		}
		if g.keyCount() > 0 && labelName == model.MetricNameLabel {
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
	return smallestIntersectingGroupBytes(groups) / tsdb.BytesPerPostingInAPostingList
}

// smallestIntersectingGroupBytes returns the size in bytes of the series in the smallest intersecting group.
// It returns 0 when there is no intersecting group.
func smallestIntersectingGroupBytes(groups []postingGroup) int64 {
	var minGroupSize int64
	for _, g := range groups {
		isAllPostings := len(g.keys) == 1 && g.keys[0] == allPostingsKey
		if !g.isSubtract && !isAllPostings {
			// The size of each posting list contains 4 bytes with the number of entries.
			// We shouldn't count these as series.
			groupSize := g.totalSize - int64(g.keyCount()*4)
			if minGroupSize == 0 || minGroupSize > groupSize {
				minGroupSize = groupSize
			}
		}
	}
	return minGroupSize
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

	if min(completeMatchersPlusSeriesSize, completeMatchersPlusLabelValuesSize) < partialMatchersPlusSeriesSize {
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
