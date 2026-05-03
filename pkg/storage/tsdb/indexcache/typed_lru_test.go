// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// TestTypedLRU_RoundTripPerItemType drives one Store→Fetch round-trip per item type
// through a RemoteIndexCache backed by TypedLRUCache, and asserts that the L1's
// per-item-type hit counter increments for the matching label. This catches any
// drift between the prefixes routed by typed_lru.go and the prefixes emitted by
// remote.go's *CacheKey functions — the exact class of bug that previously caused
// Postings ("P2:") and LabelValues ("LV2:") keys to silently bypass the L1.
func TestTypedLRU_RoundTripPerItemType(t *testing.T) {
	// Generous per-type budgets so nothing gets evicted during the test.
	sizes := PerTypeLRUSizes{
		Postings:          100,
		SeriesForRef:      100,
		ExpandedPostings:  100,
		SeriesForPostings: 100,
		LabelNames:        100,
		LabelValues:       100,
	}

	user := "tenant1"
	blockID := ulid.MustNew(1, nil)
	value := []byte("payload")
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	matchersKey := CanonicalLabelMatchersKey(matchers)

	type tc struct {
		name     string
		itemType string
		exec     func(t *testing.T, rc *RemoteIndexCache)
	}

	cases := []tc{
		{
			name:     "Postings",
			itemType: cacheTypePostings,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				lbl := labels.Label{Name: "k", Value: "v"}
				rc.StorePostings(user, blockID, lbl, value, time.Hour)
				res := rc.FetchMultiPostings(context.Background(), user, blockID, []labels.Label{lbl})
				v, ok := res.Next()
				require.True(t, ok)
				require.Equal(t, value, v)
			},
		},
		{
			name:     "SeriesForRef",
			itemType: cacheTypeSeriesForRef,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				id := storage.SeriesRef(42)
				rc.StoreSeriesForRef(user, blockID, id, value, time.Hour)
				hits, misses := rc.FetchMultiSeriesForRefs(context.Background(), user, blockID, []storage.SeriesRef{id})
				require.Empty(t, misses)
				require.Equal(t, value, hits[id])
			},
		},
		{
			name:     "ExpandedPostings",
			itemType: cacheTypeExpandedPostings,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				rc.StoreExpandedPostings(user, blockID, matchersKey, "strategy", value)
				v, ok := rc.FetchExpandedPostings(context.Background(), user, blockID, matchersKey, "strategy")
				require.True(t, ok)
				require.Equal(t, value, v)
			},
		},
		{
			name:     "SeriesForPostings",
			itemType: cacheTypeSeriesForPostings,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				shard := &sharding.ShardSelector{ShardIndex: 0, ShardCount: 1}
				postingsKey := PostingsKey("k")
				rc.StoreSeriesForPostings(user, blockID, shard, postingsKey, value)
				v, ok := rc.FetchSeriesForPostings(context.Background(), user, blockID, shard, postingsKey)
				require.True(t, ok)
				require.Equal(t, value, v)
			},
		},
		{
			name:     "LabelNames",
			itemType: cacheTypeLabelNames,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				rc.StoreLabelNames(user, blockID, matchersKey, value)
				v, ok := rc.FetchLabelNames(context.Background(), user, blockID, matchersKey)
				require.True(t, ok)
				require.Equal(t, value, v)
			},
		},
		{
			name:     "LabelValues",
			itemType: cacheTypeLabelValues,
			exec: func(t *testing.T, rc *RemoteIndexCache) {
				rc.StoreLabelValues(user, blockID, "label", matchersKey, value)
				v, ok := rc.FetchLabelValues(context.Background(), user, blockID, "label", matchersKey)
				require.True(t, ok)
				require.Equal(t, value, v)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Fresh L1 + RemoteIndexCache per case so per-type metrics are isolated.
			l1Reg := prometheus.NewPedanticRegistry()
			inner := newMockedRemoteCacheClient(nil)
			wrapped, err := NewTypedLRUCache(inner, "index-cache", l1Reg, sizes, time.Hour, log.NewNopLogger())
			require.NoError(t, err)
			rc, err := newRemoteIndexCacheForTest(log.NewNopLogger(), wrapped, prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			c.exec(t, rc)

			// Assertion: the L1 saw exactly one request and one hit, both labelled
			// with the expected item_type. The bug caught here is "wrong prefix in
			// itemTypePrefixes" — under that bug the typed L1 routes the key to the
			// fallback (no LRU slot) and neither the request nor the hit counter
			// increment for c.itemType.
			assert.Equal(t, 1.0, gatherCounterByLabel(t, l1Reg, "cache_memory_requests_total", "item_type", c.itemType),
				"requests counter for item_type=%s did not increment — likely a prefix-routing bug in typed_lru.go", c.itemType)
			assert.Equal(t, 1.0, gatherCounterByLabel(t, l1Reg, "cache_memory_hits_total", "item_type", c.itemType),
				"hits counter for item_type=%s did not increment — likely a prefix-routing bug in typed_lru.go", c.itemType)
		})
	}
}

// gatherCounterByLabel returns the value of the counter `name` whose label `labelName`
// equals `labelValue`. Returns 0 if not present.
func gatherCounterByLabel(t *testing.T, g prometheus.Gatherer, name, labelName, labelValue string) float64 {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == labelName && lp.GetValue() == labelValue {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestTypedLRU_PrefixesMatchRemote sanity-checks that every prefix in the routing
// table actually corresponds to a real cache-key emitted by remote.go's *CacheKey
// functions. Catches static drift even before the runtime round-trip would.
func TestTypedLRU_PrefixesMatchRemote(t *testing.T) {
	user := "u"
	blockID := ulid.MustNew(1, nil)
	mk := CanonicalLabelMatchersKey([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "x", "y")})

	keys := map[string]string{
		"Postings":          postingsCacheKey(user, blockID.String(), labels.Label{Name: "n", Value: "v"}),
		"SeriesForRef":      seriesForRefCacheKey(user, blockID, storage.SeriesRef(1)),
		"ExpandedPostings":  expandedPostingsCacheKey(user, blockID, mk, "strategy"),
		"SeriesForPostings": seriesForPostingsCacheKey(user, blockID, &sharding.ShardSelector{ShardIndex: 0, ShardCount: 1}, PostingsKey("k")),
		"LabelNames":        labelNamesCacheKey(user, blockID, mk),
		"LabelValues":       labelValuesCacheKey(user, blockID, "n", mk),
	}

	// Each generated key must classify into the matching item-type slot.
	for typeName, k := range keys {
		idx := keyTypeIdx(k)
		require.NotEqualf(t, -1, idx,
			"key for %s (%q) classified as -1 — prefix table drift in typed_lru.go", typeName, k)
		require.Equalf(t, typeName, itemTypeNames[idx],
			"key for %s (%q) routed to %q instead — prefix table drift in typed_lru.go",
			typeName, k, itemTypeNames[idx])

		// Belt and braces: the prefix in the table should actually be a prefix of the key.
		require.Truef(t, strings.HasPrefix(k, itemTypePrefixes[idx]),
			"item type %s at idx %d: key %q does not start with prefix %q", typeName, idx, k, itemTypePrefixes[idx])
	}
}
