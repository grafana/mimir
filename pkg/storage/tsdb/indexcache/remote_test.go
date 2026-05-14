// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexcache

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"

	"github.com/grafana/mimir/pkg/storage/fixtures"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/sharding"
)

type mockedPostingsOffset struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
	rng      index.Range
}

type mockedPostingsOffsetsForMatcher struct {
	tenantID   string
	blockID    ulid.ULID
	m          *labels.Matcher
	isSubtract bool
	//rngs       []index.Range
}

func TestRemotePostingsOffsetTableCache_FetchPostingsOffset(t *testing.T) {
	t.Parallel()

	tenant0 := "tenant-0"
	tenant1 := "tenant1"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	label0 := labels.Label{Name: "instance", Value: "a"}
	label1 := labels.Label{Name: "instance", Value: "b"}
	label2 := labels.Label{Name: "instance", Value: "c"}
	rng0 := index.Range{Start: 4, End: 16}
	rng1 := index.Range{Start: 20, End: 32}
	rng2 := index.Range{Start: 36, End: 60}

	tests := map[string]struct {
		setup         []mockedPostingsOffset
		mockedErr     error
		fetchTenantID string
		fetchBlockID  ulid.ULID
		fetchLabels   []labels.Label
		expectHits    map[labels.Label]index.Range
		expectMisses  []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:         []mockedPostingsOffset{},
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectHits:    nil,
			expectMisses:  []labels.Label{label0, label1},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant1, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant1, blockID: block2, lbl: label1, rng: rng1},
			},
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectHits: map[labels.Label]index.Range{
				label0: rng0,
				label1: rng1,
			},
			expectMisses: []labels.Label{},
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block2, lbl: label2, rng: rng2},
			},
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectHits:    map[labels.Label]index.Range{label0: rng0},
			expectMisses:  []labels.Label{label1},
		},
		"should return no hits on remote cache error": {
			setup: []mockedPostingsOffset{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng1},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant0, blockID: block2, lbl: label0, rng: rng2},
			},
			mockedErr:     errors.New("mocked error"),
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectHits:    nil,
			expectMisses:  []labels.Label{label0, label1},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{CachePostingsOffsets: true}, log.NewNopLogger(), client, nil)
			require.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StorePostingsOffset(p.tenantID, p.blockID, p.lbl, p.rng, time.Hour)
			}

			// Fetch postings from cached and assert on it.
			testFetchPostingsOffsets(
				ctx, t, c,
				testData.fetchTenantID, testData.fetchBlockID, testData.fetchLabels,
				testData.expectHits,
			)
		})
	}
}

func TestRemotePostingsOffsetTableCache_FetchPostingsOffsetsForMatcher(t *testing.T) {
	t.Parallel()

	tenant0 := "tenant-0"
	tenant1 := "tenant1"
	block0 := ulid.MustNew(0, nil)
	block1 := ulid.MustNew(1, nil)

	matchAnyPlus := labels.MustNewMatcher(labels.MatchRegexp, "instance", ".+")
	matchEqual := labels.MustNewMatcher(labels.MatchEqual, "instance", fixtures.LabelLongSuffix)

	label0 := labels.Label{Name: "instance", Value: "a"}
	label1 := labels.Label{Name: "instance", Value: "b"}
	label2 := labels.Label{Name: "instance", Value: "c"}
	rng0 := index.Range{Start: 4, End: 16}
	rng1 := index.Range{Start: 20, End: 32}
	rng2 := index.Range{Start: 36, End: 60}

	offset0 := streamindex.PostingListOffset{LabelValue: label0.Value, Off: rng0}
	offset1 := streamindex.PostingListOffset{LabelValue: label1.Value, Off: rng1}
	offset2 := streamindex.PostingListOffset{LabelValue: label2.Value, Off: rng2}

	allKeyValues := map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset{
		{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {offset0, offset1, offset2},
		{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {offset0, offset1},
		{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: false}:   {offset0},
		{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: false}:   {offset1},
		{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: true}:    {offset1, offset2},
		{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: true}:    {offset0},

		{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: false}: {offset0, offset1},
		{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: false}: {offset1, offset2},
		{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
		{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}:   {offset1},
		{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}:   {offset0},
		{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:    {offset0},
		{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:    {offset1},
	}
	allKeys := make([]mockedPostingsOffsetsForMatcher, 0, len(allKeyValues))
	for k := range allKeyValues {
		allKeys = append(allKeys, k)
	}

	tests := map[string]struct {
		setupSetVals   map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset
		mockedErr      error
		fetches        []mockedPostingsOffsetsForMatcher // no need to set rngs values
		expectedHits   map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset
		expectedMisses []mockedPostingsOffsetsForMatcher
	}{
		"should return no hits on empty cache": {
			setupSetVals:   map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset{},
			fetches:        allKeys,
			expectedHits:   nil,
			expectedMisses: allKeys,
		},
		"should return no misses on 100% hit ratio": {
			setupSetVals:   allKeyValues,
			fetches:        allKeys,
			expectedHits:   allKeyValues,
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setupSetVals: map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset{
				// Store first 4 values for tenant0
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {offset0, offset1, offset2},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {offset0, offset1},
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},

				// Store last 4 values for tenant1
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}: {offset1},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}: {offset0},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:  {offset0},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:  {offset1},
			},
			fetches: allKeys,
			expectedHits: map[mockedPostingsOffsetsForMatcher][]streamindex.PostingListOffset{
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: false}: {offset0, offset1, offset2},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: false}: {offset0, offset1},
				{tenantID: tenant0, blockID: block0, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant0, blockID: block1, m: matchAnyPlus, isSubtract: true}:  {},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: false}:   {offset1},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: false}:   {offset0},
				{tenantID: tenant1, blockID: block0, m: matchEqual, isSubtract: true}:    {offset0},
				{tenantID: tenant1, blockID: block1, m: matchEqual, isSubtract: true}:    {offset1},
			},
			expectedMisses: []mockedPostingsOffsetsForMatcher{
				// Miss last 4 values for tenant0
				{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: false},
				{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: false},
				{tenantID: tenant0, blockID: block0, m: matchEqual, isSubtract: true},
				{tenantID: tenant0, blockID: block1, m: matchEqual, isSubtract: true},
				// Miss first 4 values for tenant1
				{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: false},
				{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: false},
				{tenantID: tenant1, blockID: block0, m: matchAnyPlus, isSubtract: true},
				{tenantID: tenant1, blockID: block1, m: matchAnyPlus, isSubtract: true},
			},
		},
		"should return no hits on remote cache error": {
			setupSetVals:   allKeyValues,
			mockedErr:      errors.New("mocked error"),
			fetches:        allKeys,
			expectedHits:   nil,
			expectedMisses: allKeys,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{CachePostingsOffsets: true}, log.NewNopLogger(), client, nil)
			require.NoError(t, err)

			ctx := context.Background()
			for k, v := range testData.setupSetVals {
				c.StorePostingsOffsetsForMatcher(k.tenantID, k.blockID, k.m, k.isSubtract, v, time.Hour)
			}

			testFetchPostingsOffsetsForMatcher(
				ctx, t, c,
				testData.fetches,
				testData.expectedHits,
				testData.expectedMisses,
			)
		})
	}
}

func TestRemoteIndexCache_PostingsOffsetsDisabled(t *testing.T) {
	t.Parallel()

	tenant := "tenant-0"
	block := ulid.MustNew(1, nil)
	lbl := labels.Label{Name: "instance", Value: "a"}
	matcher := labels.MustNewMatcher(labels.MatchEqual, "instance", "a")
	offsets := []streamindex.PostingListOffset{
		{LabelValue: "a", Off: index.Range{Start: 1, End: 2}},
	}

	client := newMockedRemoteCacheClient(nil)
	c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
	require.NoError(t, err)

	ctx := context.Background()

	// Stores must no-op: nothing should land in the underlying client.
	c.StorePostingsOffset(tenant, block, lbl, index.Range{Start: 4, End: 16}, time.Hour)
	c.StorePostingsOffsetsForMatcher(tenant, block, matcher, false, offsets, time.Hour)
	assert.Empty(t, client.cache)

	// Fetches must return zero values without consulting the client.
	rng, ok := c.FetchPostingsOffset(ctx, tenant, block, lbl)
	assert.False(t, ok)
	assert.Equal(t, index.Range{}, rng)

	gotOffsets, ok := c.FetchPostingsOffsetsForMatcher(ctx, tenant, block, matcher, false)
	assert.False(t, ok)
	assert.Nil(t, gotOffsets)
}

func TestRemoteIndexCache_FetchMultiPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	label1 := labels.Label{Name: "instance", Value: "a"}
	label2 := labels.Label{Name: "instance", Value: "b"}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedPostings
		mockedErr      error
		fetchUserID    string
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label][]byte
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostings{},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostings{
				{userID: user1, block: block1, label: label1, value: value1},
				{userID: user2, block: block1, label: label1, value: value2},
				{userID: user1, block: block1, label: label2, value: value2},
				{userID: user1, block: block2, label: label1, value: value3},
			},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchLabels:  []labels.Label{label1, label2},
			expectedHits: map[labels.Label][]byte{
				label1: value1,
				label2: value2,
			},
			expectedMisses: []labels.Label{},
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostings{
				{userID: user1, block: block1, label: label1, value: value1},
				{userID: user1, block: block2, label: label1, value: value3},
			},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   map[labels.Label][]byte{label1: value1},
			expectedMisses: []labels.Label{label2},
		},
		"should return no hits on remote cache error": {
			setup: []mockedPostings{
				{userID: user1, block: block1, label: label1, value: value1},
				{userID: user1, block: block1, label: label2, value: value2},
				{userID: user1, block: block2, label: label1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StorePostings(p.userID, p.block, p.label, p.value, time.Hour)
			}

			// Fetch postings from cached and assert on it.
			testFetchMultiPostings(ctx, t, c, testData.fetchUserID, testData.fetchBlockID, testData.fetchLabels, testData.expectedHits)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchLabels)), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypePostings)))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypePostings)))
			for _, typ := range remove(allCacheTypes, cacheTypePostings) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func BenchmarkRemoteIndexCache_FetchMultiPostings(b *testing.B) {
	const (
		numHits   = 10000
		numMisses = 10000

		numKeys = numHits + numMisses
	)

	var (
		ctx     = context.Background()
		userID  = "user-1"
		blockID = ulid.MustNew(1, nil)
	)

	benchCases := map[string]struct {
		fetchLabels []labels.Label
	}{
		"short labels": {
			fetchLabels: func() []labels.Label {
				fetchLabels := make([]labels.Label, 0, numKeys)
				for i := 0; i < numKeys; i++ {
					fetchLabels = append(fetchLabels, labels.Label{Name: model.MetricNameLabel, Value: fmt.Sprintf("series_%d", i)})
				}
				return fetchLabels
			}(),
		},
		"long labels": { // this should trigger hashing the labels instead of embedding them in the cache key
			fetchLabels: func() []labels.Label {
				fetchLabels := make([]labels.Label, 0, numKeys)
				for i := 0; i < numKeys; i++ {
					fetchLabels = append(fetchLabels, labels.Label{Name: model.MetricNameLabel, Value: "series_" + strings.Repeat(strconv.Itoa(i), 100)})
				}
				return fetchLabels
			}(),
		},
	}

	for name, benchCase := range benchCases {
		fetchLabels := benchCase.fetchLabels
		b.Run(name, func(b *testing.B) {
			client := newMockedRemoteCacheClient(nil)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(b, err)

			// Store the postings expected before running the benchmark.
			for i := 0; i < numHits; i++ {
				c.StorePostings(userID, blockID, fetchLabels[i], []byte{1}, time.Hour)
			}

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				results := c.FetchMultiPostings(ctx, userID, blockID, fetchLabels)
				assert.Equal(b, numKeys, results.Remaining())
				actualHits := 0
				// iterate over the returned map to account for cost of access
				for i := 0; i < numKeys; i++ {
					bytes, ok := results.Next()
					assert.True(b, ok)
					if bytes != nil {
						actualHits++
					}
				}
				assert.Equal(b, numHits, actualHits)
			}
		})
	}
}

func TestRemoteIndexCache_FetchMultiSeriesForRef(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedSeriesForRef
		mockedErr      error
		fetchUserID    string
		fetchBlockID   ulid.ULID
		fetchIDs       []storage.SeriesRef
		expectedHits   map[storage.SeriesRef][]byte
		expectedMisses []storage.SeriesRef
	}{
		"should return no hits on empty cache": {
			setup:          []mockedSeriesForRef{},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchIDs:       []storage.SeriesRef{1, 2},
			expectedHits:   nil,
			expectedMisses: []storage.SeriesRef{1, 2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedSeriesForRef{
				{userID: user1, block: block1, id: 1, value: value1},
				{userID: user2, block: block1, id: 1, value: value2},
				{userID: user1, block: block1, id: 1, value: value1},
				{userID: user1, block: block1, id: 2, value: value2},
				{userID: user1, block: block2, id: 1, value: value3},
			},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchIDs:     []storage.SeriesRef{1, 2},
			expectedHits: map[storage.SeriesRef][]byte{
				1: value1,
				2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedSeriesForRef{
				{userID: user1, block: block1, id: 1, value: value1},
				{userID: user1, block: block2, id: 1, value: value3},
			},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchIDs:       []storage.SeriesRef{1, 2},
			expectedHits:   map[storage.SeriesRef][]byte{1: value1},
			expectedMisses: []storage.SeriesRef{2},
		},
		"should return no hits on remote cache error": {
			setup: []mockedSeriesForRef{
				{userID: user1, block: block1, id: 1, value: value1},
				{userID: user1, block: block1, id: 2, value: value2},
				{userID: user1, block: block2, id: 1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchIDs:       []storage.SeriesRef{1, 2},
			expectedHits:   nil,
			expectedMisses: []storage.SeriesRef{1, 2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the series expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeriesForRef(p.userID, p.block, p.id, p.value, time.Hour)
			}

			// Fetch series from cached and assert on it.
			hits, misses := c.FetchMultiSeriesForRefs(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchIDs)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchIDs)), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeriesForRef)))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeriesForRef)))
			for _, typ := range remove(allCacheTypes, cacheTypeSeriesForRef) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func TestRemoteIndexCache_FetchExpandedPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	matchers1 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	matchers2 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "baz", "boo")}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}
	postingsStrategy1 := "s1"
	postingsStrategy2 := "s2"

	tests := map[string]struct {
		setup         []mockedExpandedPostings
		mockedErr     error
		fetchUserID   string
		fetchBlockID  ulid.ULID
		fetchKey      LabelMatchersKey
		fetchStrategy string
		expectedData  []byte
		expectedOk    bool
	}{
		"should return no hit on empty cache": {
			setup:        []mockedExpandedPostings{},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
		"should return no miss on hit": {
			setup: []mockedExpandedPostings{
				{userID: user1, block: block1, matchers: matchers1, value: value1, postingsStrategy: postingsStrategy1},
				{userID: user2, block: block1, matchers: matchers1, value: value2, postingsStrategy: postingsStrategy1},
				{userID: user1, block: block1, matchers: matchers2, value: value2, postingsStrategy: postingsStrategy1},
				{userID: user1, block: block2, matchers: matchers1, value: value3, postingsStrategy: postingsStrategy1},
				{userID: user1, block: block1, matchers: matchers1, value: value1, postingsStrategy: postingsStrategy2},
			},
			fetchUserID:   user1,
			fetchBlockID:  block1,
			fetchKey:      CanonicalLabelMatchersKey(matchers1),
			fetchStrategy: postingsStrategy1,
			expectedData:  value1,
			expectedOk:    true,
		},
		"should return no hit on remote cache error": {
			setup: []mockedExpandedPostings{
				{userID: user1, block: block1, matchers: matchers1, value: value1},
				{userID: user1, block: block1, matchers: matchers2, value: value2},
				{userID: user1, block: block2, matchers: matchers1, value: value3},
			},
			mockedErr:    context.DeadlineExceeded,
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreExpandedPostings(p.userID, p.block, CanonicalLabelMatchersKey(p.matchers), testData.fetchStrategy, p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchExpandedPostings(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchKey, testData.fetchStrategy)
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeExpandedPostings)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeExpandedPostings)))
			for _, typ := range remove(allCacheTypes, cacheTypeExpandedPostings) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func TestRemoteIndexCache_FetchSeriesForPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	matchers1 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	matchers2 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "baz", "boo")}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}
	shard1 := (*sharding.ShardSelector)(nil)
	shard2 := &sharding.ShardSelector{ShardIndex: 1, ShardCount: 16}
	postings1 := []storage.SeriesRef{1, 2}
	postings2 := []storage.SeriesRef{2, 3}

	tests := map[string]struct {
		setup        []mockedSeries
		mockedErr    error
		fetchUserID  string
		fetchBlockID ulid.ULID
		fetchKey     LabelMatchersKey
		fetchShard   *sharding.ShardSelector
		postings     []storage.SeriesRef
		expectedData []byte
		expectedOk   bool
	}{
		"should return no hit on empty cache": {
			setup:        []mockedSeries{},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			postings:     postings1,
			expectedData: nil,
			expectedOk:   false,
		},
		"should return no miss on hit": {
			setup: []mockedSeries{
				{userID: user1, block: block1, shard: shard1, postings: postings1, value: value1},
				{userID: user2, block: block1, shard: shard1, postings: postings1, value: value2}, // different user
				{userID: user1, block: block1, shard: shard2, postings: postings1, value: value2}, // different shard
				{userID: user1, block: block2, shard: shard1, postings: postings1, value: value3}, // different block
				{userID: user1, block: block2, shard: shard1, postings: postings2, value: value3}, // different postings
			},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			postings:     postings1,
			expectedData: value1,
			expectedOk:   true,
		},
		"should return no hit on remote cache error": {
			setup: []mockedSeries{
				{userID: user1, block: block1, matchers: matchers1, shard: shard1, postings: postings1, value: value1},
				{userID: user1, block: block1, matchers: matchers2, shard: shard1, postings: postings1, value: value2},
				{userID: user1, block: block2, matchers: matchers1, shard: shard1, postings: postings1, value: value3},
			},
			mockedErr:    context.DeadlineExceeded,
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			postings:     postings1,
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeriesForPostings(p.userID, p.block, p.shard, CanonicalPostingsKey(p.postings), p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchSeriesForPostings(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchShard, CanonicalPostingsKey(testData.postings))
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeriesForPostings)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeriesForPostings)))
			for _, typ := range remove(allCacheTypes, cacheTypeSeriesForPostings) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func TestRemoteIndexCache_FetchLabelNames(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	matchers1 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	matchers2 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "baz", "boo")}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup        []mockedLabelNames
		mockedErr    error
		fetchUserID  string
		fetchBlockID ulid.ULID
		fetchKey     LabelMatchersKey
		expectedData []byte
		expectedOk   bool
	}{
		"should return no hit on empty cache": {
			setup:        []mockedLabelNames{},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
		"should return no miss on hit": {
			setup: []mockedLabelNames{
				{userID: user1, block: block1, matchers: matchers1, value: value1},
				{userID: user2, block: block1, matchers: matchers1, value: value2},
				{userID: user1, block: block1, matchers: matchers2, value: value2},
				{userID: user1, block: block2, matchers: matchers1, value: value3},
			},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: value1,
			expectedOk:   true,
		},
		"should return no hit on remote cache error": {
			setup: []mockedLabelNames{
				{userID: user1, block: block1, matchers: matchers1, value: value1},
				{userID: user1, block: block1, matchers: matchers2, value: value2},
				{userID: user1, block: block2, matchers: matchers1, value: value3},
			},
			mockedErr:    context.DeadlineExceeded,
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreLabelNames(p.userID, p.block, CanonicalLabelMatchersKey(p.matchers), p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchLabelNames(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchKey)
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeLabelNames)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeLabelNames)))
			for _, typ := range remove(allCacheTypes, cacheTypeLabelNames) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func TestRemoteIndexCache_FetchLabelValues(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	user1 := "tenant1"
	user2 := "tenant2"
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	labelName1 := "one"
	labelName2 := "two"
	matchers1 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")}
	matchers2 := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "baz", "boo")}
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedLabelValues
		mockedErr      error
		fetchUserID    string
		fetchBlockID   ulid.ULID
		fetchLabelName string
		fetchKey       LabelMatchersKey
		expectedData   []byte
		expectedOk     bool
	}{
		"should return no hit on empty cache": {
			setup:          []mockedLabelValues{},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabelName: labelName1,
			fetchKey:       CanonicalLabelMatchersKey(matchers1),
			expectedData:   nil,
			expectedOk:     false,
		},
		"should return no miss on hit": {
			setup: []mockedLabelValues{
				{userID: user1, block: block1, labelName: labelName1, matchers: matchers1, value: value1},
				{userID: user2, block: block1, labelName: labelName1, matchers: matchers1, value: value2},
				{userID: user1, block: block1, labelName: labelName2, matchers: matchers2, value: value2},
				{userID: user1, block: block2, labelName: labelName1, matchers: matchers1, value: value3},
				{userID: user1, block: block2, labelName: labelName1, matchers: matchers2, value: value3},
			},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabelName: labelName1,
			fetchKey:       CanonicalLabelMatchersKey(matchers1),
			expectedData:   value1,
			expectedOk:     true,
		},
		"should return no hit on remote cache error": {
			setup: []mockedLabelValues{
				{userID: user1, block: block1, labelName: labelName1, matchers: matchers1, value: value1},
				{userID: user1, block: block1, labelName: labelName2, matchers: matchers2, value: value2},
				{userID: user1, block: block2, labelName: labelName1, matchers: matchers1, value: value3},
				{userID: user1, block: block2, labelName: labelName1, matchers: matchers2, value: value3},
			},
			mockedErr:      context.DeadlineExceeded,
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchLabelName: labelName1,
			fetchKey:       CanonicalLabelMatchersKey(matchers1),
			expectedData:   nil,
			expectedOk:     false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(IndexCacheConfig{}, log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreLabelValues(p.userID, p.block, p.labelName, CanonicalLabelMatchersKey(p.matchers), p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchLabelValues(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchLabelName, testData.fetchKey)
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeLabelValues)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeLabelValues)))
			for _, typ := range remove(allCacheTypes, cacheTypeLabelValues) {
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(typ)))
				assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(typ)))
			}
		})
	}
}

func TestStringCacheKeys_Values(t *testing.T) {
	t.Parallel()

	user := "tenant"
	uid := ulid.MustNew(1, nil)

	tests := map[string]struct {
		key      string
		expected string
	}{
		"should stringify postings cache key": {
			key: postingsCacheKey(user, uid.String(), labels.Label{Name: "foo", Value: "bar"}),
			expected: func() string {
				encodedLabel := base64.RawURLEncoding.EncodeToString([]byte("foo:bar"))
				return fmt.Sprintf("P2:%s:%s:%s", user, uid.String(), encodedLabel)
			}(),
		},
		"should hash long postings cache key": {
			key: postingsCacheKey(user, uid.String(), labels.Label{Name: "foo", Value: strings.Repeat("bar", 11)}),
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:" + strings.Repeat("bar", 11)))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P2:%s:%s:%s", user, uid.String(), encodedHash)
			}(),
		},
		"should stringify series cache key": {
			key:      seriesForRefCacheKey(user, uid, 12345),
			expected: fmt.Sprintf("S:%s:%s:12345", user, uid.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := testData.key
			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestStringCacheKeys_ShouldGuaranteeReasonablyShortKeysLength(t *testing.T) {
	t.Parallel()

	user := "tenant"
	uid := ulid.MustNew(1, nil)

	tests := map[string]struct {
		keys        []string
		expectedLen int
	}{
		"should guarantee reasonably short key length for postings": {
			expectedLen: 80,
			keys: []string{
				postingsCacheKey(user, uid.String(), labels.Label{Name: "a", Value: "b"}),
				postingsCacheKey(user, uid.String(), labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}),
			},
		},
		"should guarantee reasonably short key length for series": {
			expectedLen: 56,
			keys: []string{
				seriesForRefCacheKey(user, uid, math.MaxUint64),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, key := range testData.keys {
				assert.LessOrEqual(t, len(key), testData.expectedLen)
			}
		})
	}
}

func BenchmarkStringCacheKeys(b *testing.B) {
	userID := "tenant"
	uid := ulid.MustNew(1, nil)
	lbl := labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}
	lmKey := CanonicalLabelMatchersKey([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "foo", "bar")})

	b.Run("postings", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			postingsCacheKey(userID, uid.String(), lbl)
		}
	})

	b.Run("series ref", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seriesForRefCacheKey(userID, uid, math.MaxUint64)
		}
	})

	b.Run("expanded postings", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			expandedPostingsCacheKey(userID, uid, lmKey, "strategy")
		}
	})
}

func TestPostingsCacheKey_ShouldOnlyAllocateOncePerCall(t *testing.T) {
	const numRuns = 1000

	blockID := ulid.MustNew(1, nil)
	lbl := labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}

	actualAllocs := testing.AllocsPerRun(numRuns, func() {
		postingsCacheKey("user-1", blockID.String(), lbl)
	})

	// Allow for 1 extra allocation here, reported when running the test with -race.
	assert.LessOrEqual(t, actualAllocs, 2.0)
}

func TestPostingsCacheKeyLabelHash_ShouldNotAllocateMemory(t *testing.T) {
	const numRuns = 1000

	lbl := labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}

	actualAllocs := testing.AllocsPerRun(numRuns, func() {
		cacheKeyLabelID(lbl)
	})

	// Allow for 1 extra allocation here, reported when running the test with -race.
	assert.LessOrEqual(t, actualAllocs, 1.0)
}

func TestPostingsCacheKeyLabelHash_ShouldBeConcurrencySafe(t *testing.T) {
	const (
		numWorkers       = 10
		numRunsPerWorker = 10000
	)

	// Generate a different label per worker, and their expected hash.
	inputPerWorker := make([]labels.Label, 0, numWorkers)
	expectedPerWorker := make([][]byte, 0, numWorkers)

	for w := 0; w < numWorkers; w++ {
		inputPerWorker = append(inputPerWorker, labels.Label{Name: model.MetricNameLabel, Value: fmt.Sprintf("series_%d", w)})

		hash, hashLen := cacheKeyLabelID(inputPerWorker[w])
		expectedPerWorker = append(expectedPerWorker, hash[0:hashLen])
	}

	// Sanity check: ensure expected hashes are different for each worker.
	for w := 0; w < numWorkers; w++ {
		for c := 0; c < numWorkers; c++ {
			if w == c {
				continue
			}

			require.NotEqual(t, expectedPerWorker[w], expectedPerWorker[c])
		}
	}

	// Run workers, each generating the hash for their own label.
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()

			for r := 0; r < numRunsPerWorker; r++ {
				actual, hashLen := cacheKeyLabelID(inputPerWorker[workerID])
				assert.Equal(t, expectedPerWorker[workerID], actual[0:hashLen])
			}
		}(w)
	}

	wg.Wait()
}

type mockedPostings struct {
	userID string
	block  ulid.ULID
	label  labels.Label
	value  []byte
}

type mockedSeriesForRef struct {
	userID string
	block  ulid.ULID
	id     storage.SeriesRef
	value  []byte
}

type mockedExpandedPostings struct {
	userID           string
	block            ulid.ULID
	matchers         []*labels.Matcher
	postingsStrategy string
	value            []byte
}

type mockedLabelNames struct {
	userID   string
	block    ulid.ULID
	matchers []*labels.Matcher
	value    []byte
}

type mockedSeries struct {
	userID   string
	block    ulid.ULID
	matchers []*labels.Matcher
	shard    *sharding.ShardSelector
	postings []storage.SeriesRef
	value    []byte
}

type mockedLabelValues struct {
	userID    string
	block     ulid.ULID
	labelName string
	matchers  []*labels.Matcher
	value     []byte
}

type mockedRemoteCacheClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedRemoteCacheClient(mockedGetMultiErr error) *mockedRemoteCacheClient {
	return &mockedRemoteCacheClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedRemoteCacheClient) GetMulti(_ context.Context, keys []string, _ ...cache.Option) map[string][]byte {
	if c.mockedGetMultiErr != nil {
		return nil
	}

	hits := map[string][]byte{}

	for _, key := range keys {
		if value, ok := c.cache[key]; ok {
			hits[key] = value
		}
	}

	return hits
}

func (c *mockedRemoteCacheClient) GetMultiWithError(ctx context.Context, keys []string, opts ...cache.Option) (map[string][]byte, error) {
	return c.GetMulti(ctx, keys, opts...), c.mockedGetMultiErr
}

func (c *mockedRemoteCacheClient) SetAsync(key string, value []byte, _ time.Duration) {
	c.cache[key] = value
}

func (c *mockedRemoteCacheClient) SetMultiAsync(data map[string][]byte, _ time.Duration) {
	for key, value := range data {
		c.cache[key] = value
	}
}

func (c *mockedRemoteCacheClient) Set(_ context.Context, key string, value []byte, _ time.Duration) error {
	c.cache[key] = value
	return nil
}

func (c *mockedRemoteCacheClient) Add(_ context.Context, key string, value []byte, _ time.Duration) error {
	if _, ok := c.cache[key]; ok {
		return cache.ErrNotStored
	}

	c.cache[key] = value
	return nil
}

func (c *mockedRemoteCacheClient) Delete(_ context.Context, key string) error {
	delete(c.cache, key)

	return nil
}

func (c *mockedRemoteCacheClient) Stop() {
	// Nothing to do.
}

func (c *mockedRemoteCacheClient) Name() string {
	return "mock"
}

// remove a string from a slice of strings
func remove(slice []string, needle string) []string {
	res := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != needle {
			res = append(res, s)
		}
	}
	return res
}
