// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/cache.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"encoding/base64"
	"sort"
	"strconv"
	"strings"

	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"golang.org/x/crypto/blake2b"
)

const (
	cacheTypePostings         string = "Postings"
	cacheTypeSeriesForRef     string = "SeriesForRef"
	cacheTypeExpandedPostings string = "ExpandedPostings"

	sliceHeaderSize = 16
)

var (
	ulidSize      = uint64(len(ulid.ULID{}))
	allCacheTypes = []string{
		cacheTypePostings,
		cacheTypeSeriesForRef,
		cacheTypeExpandedPostings,
	}
)

// IndexCache is the interface exported by index cache backends.
type IndexCache interface {
	// StorePostings stores postings for a single series.
	StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte)

	// FetchMultiPostings fetches multiple postings - each identified by a label -
	// and returns a map containing cache hits, along with a list of missing keys.
	FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label)

	// StoreSeriesForRef stores a single series.
	StoreSeriesForRef(ctx context.Context, blockID ulid.ULID, id storage.SeriesRef, v []byte)

	// FetchMultiSeriesForRefs fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	FetchMultiSeriesForRefs(ctx context.Context, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef)

	// StoreExpandedPostings stores the result of ExpandedPostings, encoded with an unspecified codec.
	StoreExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey, v []byte)

	// FetchExpandedPostings fetches the result of ExpandedPostings, encoded with an unspecified codec.
	FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey) ([]byte, bool)
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

type cacheKey interface {
	string() string
	typ() string
	size() uint64
}

type cacheKeyPostings struct {
	block ulid.ULID
	label labels.Label
}

func (c cacheKeyPostings) string() string {
	// Use cryptographically hash functions to avoid hash collisions
	// which would end up in wrong query results.
	lblHash := blake2b.Sum256([]byte(c.label.Name + ":" + c.label.Value))
	return "P:" + c.block.String() + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])
}

func (c cacheKeyPostings) typ() string {
	return cacheTypePostings
}

func (c cacheKeyPostings) size() uint64 {
	// ULID + 2 slice headers + number of chars in value and name.
	return ulidSize + 2*sliceHeaderSize + uint64(len(c.label.Value)+len(c.label.Name))
}

type cacheKeySeriesForRef struct {
	block ulid.ULID
	ref   storage.SeriesRef
}

func (c cacheKeySeriesForRef) string() string {
	return "S:" + c.block.String() + ":" + strconv.FormatUint(uint64(c.ref), 10)
}

func (c cacheKeySeriesForRef) typ() string {
	return cacheTypeSeriesForRef
}

func (c cacheKeySeriesForRef) size() uint64 {
	return ulidSize + 8 // ULID + uint64.
}

type cacheKeyExpandedPostings struct {
	block       ulid.ULID
	matchersKey LabelMatchersKey
}

func (c cacheKeyExpandedPostings) string() string {
	hash := blake2b.Sum256([]byte(c.matchersKey))
	return "E:" + c.block.String() + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
}

func (c cacheKeyExpandedPostings) typ() string {
	return cacheTypeExpandedPostings
}

func (c cacheKeyExpandedPostings) size() uint64 {
	return ulidSize + sliceHeaderSize + uint64(len(c.matchersKey))
}
