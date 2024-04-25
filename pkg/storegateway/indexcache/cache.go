// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexcache

import (
	"context"
	"encoding/base64"
	"reflect"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/crypto/blake2b"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const (
	cacheTypePostings          = "Postings"
	cacheTypeSeriesForRef      = "SeriesForRef"
	cacheTypeExpandedPostings  = "ExpandedPostings"
	cacheTypeSeriesForPostings = "SeriesForPostings"
	cacheTypeLabelNames        = "LabelNames"
	cacheTypeLabelValues       = "LabelValues"

	defaultTTL = 7 * 24 * time.Hour
)

var (
	allCacheTypes = []string{
		cacheTypePostings,
		cacheTypeSeriesForRef,
		cacheTypeExpandedPostings,
		cacheTypeSeriesForPostings,
		cacheTypeLabelNames,
		cacheTypeLabelValues,
	}
)

type BytesResult interface {
	// Next should return a byte slice if there was a cache hit for the current key; otherwise Next should return nil.
	// Next should return false when there are no more keys in the result.
	Next() ([]byte, bool)

	// Remaining should return the number of keys left in the result.
	// There may or may not be an item for each key.
	Remaining() int

	// Size should return the size in bytes of the result.
	Size() int
}

type MapIterator[T comparable] struct {
	M    map[T][]byte
	Keys []T
}

func (l *MapIterator[T]) Next() ([]byte, bool) {
	if len(l.Keys) == 0 {
		return nil, false
	}
	b := l.M[l.Keys[0]]
	l.Keys = l.Keys[1:]
	return b, true
}

func (l *MapIterator[T]) Remaining() int {
	return len(l.Keys)
}

func (l *MapIterator[T]) Size() int {
	return sumBytes[T](l.M)
}

// IndexCache is the interface exported by index cache backends.
type IndexCache interface {
	// StorePostings stores postings for a single series.
	StorePostings(userID string, blockID ulid.ULID, l labels.Label, v []byte, ttl time.Duration)

	// FetchMultiPostings fetches multiple postings - each identified by a label.
	// The returned result should contain one item for each requested key.
	FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, keys []labels.Label) (result BytesResult)

	// StoreSeriesForRef stores a single series.
	StoreSeriesForRef(userID string, blockID ulid.ULID, id storage.SeriesRef, v []byte, ttl time.Duration)

	// FetchMultiSeriesForRefs fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	// The order of the returned misses should be the same as their relative order in the provided ids.
	FetchMultiSeriesForRefs(ctx context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef)

	// StoreExpandedPostings stores the result of ExpandedPostings, encoded with an unspecified codec.
	StoreExpandedPostings(userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string, v []byte)

	// FetchExpandedPostings fetches the result of ExpandedPostings, encoded with an unspecified codec.
	FetchExpandedPostings(ctx context.Context, userID string, blockID ulid.ULID, key LabelMatchersKey, postingsSelectionStrategy string) ([]byte, bool)

	// StoreSeriesForPostings stores a series set for the provided postings.
	StoreSeriesForPostings(userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey, v []byte)
	// FetchSeriesForPostings fetches a series set for the provided postings.
	FetchSeriesForPostings(ctx context.Context, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey PostingsKey) ([]byte, bool)

	// StoreLabelNames stores the result of a LabelNames() call.
	StoreLabelNames(userID string, blockID ulid.ULID, matchersKey LabelMatchersKey, v []byte)
	// FetchLabelNames fetches the result of a LabelNames() call.
	FetchLabelNames(ctx context.Context, userID string, blockID ulid.ULID, matchersKey LabelMatchersKey) ([]byte, bool)

	// StoreLabelValues stores the result of a LabelValues() call.
	StoreLabelValues(userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey, v []byte)
	// FetchLabelValues fetches the result of a LabelValues() call.
	FetchLabelValues(ctx context.Context, userID string, blockID ulid.ULID, labelName string, matchersKey LabelMatchersKey) ([]byte, bool)
}

// BlockTTL determines an appropriate TTL for cache entries related to a particular block
// based on metadata about the block. Use a shorter TTL for temporary blocks that will be
// compacted and deleted soon and a longer TTL for larger blocks that won't be deleted.
func BlockTTL(meta *block.Meta) time.Duration {
	duration := time.Duration(meta.MaxTime-meta.MinTime) * time.Millisecond
	if duration%time.Hour != 0 {
		duration += time.Hour - duration%time.Hour
	}

	// Pick the max of one hour or duration adjusted up to the next hour
	duration = max(time.Hour, duration)

	// Anything less than 24h is a temporary block that will eventually be compacted into
	// a 24h block. Use a shorter TTL since otherwise these will stay in the cache long
	// after they're no longer relevant.
	if duration < 24*time.Hour {
		return duration
	}

	return defaultTTL
}

// PostingsKey represents a canonical key for a []storage.SeriesRef slice
type PostingsKey string

// CanonicalPostingsKey creates a canonical version of PostingsKey
func CanonicalPostingsKey(postings []storage.SeriesRef) PostingsKey {
	hashable := unsafeCastPostingsToBytes(postings)
	checksum := blake2b.Sum256(hashable)
	return PostingsKey(base64.RawURLEncoding.EncodeToString(checksum[:]))
}

const bytesPerPosting = int(unsafe.Sizeof(storage.SeriesRef(0)))

// unsafeCastPostingsToBytes returns the postings as a slice of bytes with minimal allocations.
// It casts the memory region of the underlying array to a slice of bytes. The resulting byte slice is only valid as long as the postings slice exists and is unmodified.
func unsafeCastPostingsToBytes(postings []storage.SeriesRef) []byte {
	byteSlice := make([]byte, 0)
	slicePtr := (*reflect.SliceHeader)(unsafe.Pointer(&byteSlice))
	slicePtr.Data = (*reflect.SliceHeader)(unsafe.Pointer(&postings)).Data
	slicePtr.Len = len(postings) * bytesPerPosting
	slicePtr.Cap = slicePtr.Len
	return byteSlice
}

// LabelMatchersKey represents a canonical key for a []*matchers.Matchers slice
type LabelMatchersKey string

// CanonicalLabelMatchersKey creates a canonical version of LabelMatchersKey
func CanonicalLabelMatchersKey(ms []*labels.Matcher) LabelMatchersKey {
	sorted := make([]labels.Matcher, len(ms))
	for i := range ms {
		sorted[i] = labels.Matcher{Type: ms[i].Type, Name: ms[i].Name, Value: ms[i].Value}
	}
	sort.Sort(sortedLabelMatchers(sorted))

	const (
		typeLen = 2
		sepLen  = 1
	)
	var size int
	for _, m := range sorted {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	for _, m := range sorted {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte(0)
	}
	return LabelMatchersKey(sb.String())
}

type sortedLabelMatchers []labels.Matcher

func (c sortedLabelMatchers) Less(i, j int) bool {
	if c[i].Name != c[j].Name {
		return c[i].Name < c[j].Name
	}
	if c[i].Type != c[j].Type {
		return c[i].Type < c[j].Type
	}
	return c[i].Value < c[j].Value
}

func (c sortedLabelMatchers) Len() int      { return len(c) }
func (c sortedLabelMatchers) Swap(i, j int) { c[i], c[j] = c[j], c[i] }

func initLabelValuesForAllCacheTypes(vec *prometheus.MetricVec) {
	for _, typ := range allCacheTypes {
		_, err := vec.GetMetricWithLabelValues(typ)
		if err != nil {
			panic(err)
		}
	}
}

func shardKey(shard *sharding.ShardSelector) string {
	if shard == nil {
		return "all"
	}
	return shard.LabelValue()
}
