// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"encoding/binary"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
)

// postingGroup keeps posting keys for single matcher. Logical result of the group is:
// If addAll is set: special All postings minus postings for removeKeys labels. No need to merge postings for addKeys in this case.
// If addAll is not set: Merge of postings for "addKeys" labels minus postings for removeKeys labels
// This computation happens in ExpandedPostings.
type postingGroup struct {
	addAll     bool
	addKeys    []labels.Label
	removeKeys []labels.Label
}

func newPostingGroup(addAll bool, addKeys, removeKeys []labels.Label) postingGroup {
	return postingGroup{
		addAll:     addAll,
		addKeys:    addKeys,
		removeKeys: removeKeys,
	}
}

func (g postingGroup) filterNonExistentKeys(preader postingsOffsetReader) (_ postingGroup, retErr error) {
	filterKeys := func(keys []labels.Label) []labels.Label {
		existingKeys := 0
		for i, l := range keys {
			if _, err := preader.PostingsOffset(l.Name, l.Value); errors.Is(err, indexheader.NotFoundRangeErr) {
				// This label name and value doesn't exist in this block, so there are 0 postings we can match.
				// Set it to an empty value, so we can filter it out later.
				keys[i] = labels.Label{}
				// Try with the rest of the set matchers, maybe they can match some series.
				continue
			} else if err != nil {
				retErr = err
				return nil
			}
			existingKeys++
		}
		if existingKeys == len(keys) {
			return keys
		}
		if existingKeys == 0 {
			return nil
		}

		filtered := make([]labels.Label, 0, existingKeys)
		for _, k := range keys {
			if k == (labels.Label{}) {
				continue
			}
			filtered = append(filtered, k)
		}
		return filtered
	}

	return postingGroup{
		addAll:     g.addAll,
		addKeys:    filterKeys(g.addKeys),
		removeKeys: filterKeys(g.removeKeys),
	}, retErr
}

type lazyPostingGroup struct {
	isRemoveGroup bool
	labelName     string
	matcher       func(string) bool
	addAll        bool
}

func newLazyPostingGroup(addAll bool, isRemoveGroup bool, labelName string, matcher func(string) bool) lazyPostingGroup {
	return lazyPostingGroup{
		isRemoveGroup: isRemoveGroup,
		labelName:     labelName,
		matcher:       matcher,
		addAll:        addAll,
	}
}

// toPostingGroup guarantees that all the keys in the returned postingGroup exist in the index
func (g lazyPostingGroup) toPostingGroup(lvr labelValuesReader) (postingGroup, error) {
	vals, err := lvr.LabelValues(g.labelName, g.matcher)
	if err != nil {
		return postingGroup{}, err
	}
	keys := make([]labels.Label, len(vals))
	for i := range vals {
		keys[i] = labels.Label{Name: g.labelName, Value: vals[i]}
	}

	if g.isRemoveGroup {
		return postingGroup{
			addAll:     g.addAll,
			removeKeys: keys,
		}, nil
	}
	return postingGroup{
		addAll:  g.addAll,
		addKeys: keys,
	}, nil
}

type postingsOffsetReader interface {
	PostingsOffset(name string, value string) (index.Range, error)
}

type labelValuesReader interface {
	LabelValues(name string, filter func(string) bool) ([]string, error)
}

// toPostingGroup returns either a postingGroup or a lazyPostingGroup. It returns a postingGroup when it can
// derive all the possible values that would satisfy the matcher without looking up the index
// (e.g. pod="compactor=1"` or pod=~"compactor-(0|1|2|3)")
// If the matcher needs to use the index, then toPostingGroup returns a lazyPostingGroup (e.g. pod=~"compactor.*").
// NOTE: Derived from tsdb.postingsForMatcher
func toPostingGroup(m *labels.Matcher) (_ postingGroup, _ lazyPostingGroup, isLazy bool) {
	if setMatches := m.SetMatches(); len(setMatches) > 0 && (m.Type == labels.MatchRegexp || m.Type == labels.MatchNotRegexp) {
		keys := make([]labels.Label, 0, len(setMatches))
		for _, val := range setMatches {
			keys = append(keys, labels.Label{Name: m.Name, Value: val})
		}
		if m.Type == labels.MatchNotRegexp {
			return newPostingGroup(true, nil, keys), lazyPostingGroup{}, false
		}
		return newPostingGroup(false, keys, nil), lazyPostingGroup{}, false
	}

	if m.Value != "" {
		// Fast-path for equal matching.
		// Works for every case except for `foo=""`, which is a special case, see below.
		if m.Type == labels.MatchEqual {
			return newPostingGroup(false, []labels.Label{{Name: m.Name, Value: m.Value}}, nil), lazyPostingGroup{}, false
		}

		// If matcher is `label!="foo"`, we select an empty label value too,
		// i.e., series that don't have this label.
		// So this matcher selects all series in the storage,
		// except for the ones that do have `label="foo"`
		if m.Type == labels.MatchNotEqual {
			return newPostingGroup(true, nil, []labels.Label{{Name: m.Name, Value: m.Value}}), lazyPostingGroup{}, false
		}
	}

	// This is a more generic approach for the previous case.
	// Here we can enter with `label=""` or regexp matchers that match the empty value,
	// like `=~"|foo" or `error!~"5..".
	// Remember: if the matcher selects an empty value, it selects all the series which don't
	// have the label name set too. See: https://github.com/prometheus/prometheus/issues/3575
	// and https://github.com/prometheus/prometheus/pull/3578#issuecomment-351653555.
	if m.Matches("") {
		return postingGroup{}, newLazyPostingGroup(true, true, m.Name, not(m.Matches)), true
	}

	// Our matcher does not match the empty value, so we just need the postings that correspond
	// to label values matched by the matcher.
	return postingGroup{}, newLazyPostingGroup(false, false, m.Name, m.Matches), true
}

func not(filter func(string) bool) func(string) bool {
	return func(s string) bool { return !filter(s) }
}

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
