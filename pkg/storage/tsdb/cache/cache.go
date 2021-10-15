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
	"github.com/prometheus/prometheus/pkg/labels"
	"golang.org/x/crypto/blake2b"
)

const (
	cacheTypePostings         string = "Postings"
	cacheTypeSeries           string = "Series"
	cacheTypeExpandedPostings string = "ExpandedPostings"

	sliceHeaderSize = 16
)

var ulidSize = uint64(len(ulid.ULID{}))

// IndexCache is the interface exported by index cache backends.
type IndexCache interface {
	// StorePostings stores postings for a single series.
	StorePostings(ctx context.Context, blockID ulid.ULID, l labels.Label, v []byte)

	// FetchMultiPostings fetches multiple postings - each identified by a label -
	// and returns a map containing cache hits, along with a list of missing keys.
	FetchMultiPostings(ctx context.Context, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label)

	// StoreSeries stores a single series.
	StoreSeries(ctx context.Context, blockID ulid.ULID, id uint64, v []byte)

	// FetchMultiSeries fetches multiple series - each identified by ID - from the cache
	// and returns a map containing cache hits, along with a list of missing IDs.
	FetchMultiSeries(ctx context.Context, blockID ulid.ULID, ids []uint64) (hits map[uint64][]byte, misses []uint64)

	// StoreExpandedPostings stores the result of ExpandedPostings, encoded with an unspecified codec
	StoreExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey, v []byte)

	// FetchExpandedPostings fetches the result of ExpandedPostings, encoded with an unspecified codec
	FetchExpandedPostings(ctx context.Context, blockID ulid.ULID, key LabelMatchersKey) ([]byte, bool)
}

type cacheKey struct {
	block ulid.ULID
	key   interface{}
}

func (c cacheKey) keyType() string {
	switch c.key.(type) {
	case cacheKeyPostings:
		return cacheTypePostings
	case cacheKeySeries:
		return cacheTypeSeries
	case cacheKeyExpandedPostings:
		return cacheTypeExpandedPostings
	}
	return "<unknown>"
}

func (c cacheKey) size() uint64 {
	switch k := c.key.(type) {
	case cacheKeyPostings:
		// ULID + 2 slice headers + number of chars in value and name.
		return ulidSize + 2*sliceHeaderSize + uint64(len(k.Value)+len(k.Name))
	case cacheKeyExpandedPostings:
		// ULID + string header + number of key.
		return ulidSize + sliceHeaderSize + uint64(len(k))
	case cacheKeySeries:
		return ulidSize + 8 // ULID + uint64.
	}
	return 0
}

func (c cacheKey) string() string {
	switch key := c.key.(type) {
	case cacheKeyPostings:
		// Use cryptographically hash functions to avoid hash collisions
		// which would end up in wrong query results.
		lblHash := blake2b.Sum256([]byte(key.Name + ":" + key.Value))
		return "P:" + c.block.String() + ":" + base64.RawURLEncoding.EncodeToString(lblHash[0:])

	case cacheKeyExpandedPostings:
		hash := blake2b.Sum256([]byte(key))
		return "E:" + c.block.String() + ":" + base64.RawURLEncoding.EncodeToString(hash[0:])
	case cacheKeySeries:
		return "S:" + c.block.String() + ":" + strconv.FormatUint(uint64(key), 10)
	default:
		return ""
	}
}

type (
	cacheKeyPostings         labels.Label
	cacheKeySeries           uint64
	cacheKeyExpandedPostings LabelMatchersKey
)

// LabelMatchersKey represents a canonical key for a []*matchers.Matchers slice
type LabelMatchersKey string

// CanonicalLabelMatchersKey creates a canonical version of LabelMatchersKey
func CanonicalLabelMatchersKey(ms []*labels.Matcher) LabelMatchersKey {
	// TODO consider building a []int, sort that based on the ms order, and then build the string.
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
