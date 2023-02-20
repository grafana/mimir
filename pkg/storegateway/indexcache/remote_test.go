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
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/blake2b"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

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
			expectedMisses: nil,
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
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StorePostings(ctx, p.userID, p.block, p.label, p.value)
			}

			// Fetch postings from cached and assert on it.
			hits, misses := c.FetchMultiPostings(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchLabels)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

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
		fetchIds       []storage.SeriesRef
		expectedHits   map[storage.SeriesRef][]byte
		expectedMisses []storage.SeriesRef
	}{
		"should return no hits on empty cache": {
			setup:          []mockedSeriesForRef{},
			fetchUserID:    user1,
			fetchBlockID:   block1,
			fetchIds:       []storage.SeriesRef{1, 2},
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
			fetchIds:     []storage.SeriesRef{1, 2},
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
			fetchIds:       []storage.SeriesRef{1, 2},
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
			fetchIds:       []storage.SeriesRef{1, 2},
			expectedHits:   nil,
			expectedMisses: []storage.SeriesRef{1, 2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the series expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeriesForRef(ctx, p.userID, p.block, p.id, p.value)
			}

			// Fetch series from cached and assert on it.
			hits, misses := c.FetchMultiSeriesForRefs(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchIds)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchIds)), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeriesForRef)))
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

	tests := map[string]struct {
		setup        []mockedExpandedPostings
		mockedErr    error
		fetchUserID  string
		fetchBlockID ulid.ULID
		fetchKey     LabelMatchersKey
		expectedData []byte
		expectedOk   bool
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
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreExpandedPostings(ctx, p.userID, p.block, CanonicalLabelMatchersKey(p.matchers), p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchExpandedPostings(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchKey)
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
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeriesForPostings(ctx, p.userID, p.block, p.shard, CanonicalPostingsKey(p.postings), p.value)
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

func TestRemoteIndexCache_FetchSeries(t *testing.T) {
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

	tests := map[string]struct {
		setup        []mockedSeries
		mockedErr    error
		fetchUserID  string
		fetchBlockID ulid.ULID
		fetchKey     LabelMatchersKey
		fetchShard   *sharding.ShardSelector
		expectedData []byte
		expectedOk   bool
	}{
		"should return no hit on empty cache": {
			setup:        []mockedSeries{},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			expectedData: nil,
			expectedOk:   false,
		},
		"should return no miss on hit": {
			setup: []mockedSeries{
				{userID: user1, block: block1, matchers: matchers1, shard: shard1, value: value1},
				{userID: user2, block: block1, matchers: matchers1, shard: shard1, value: value2}, // different user
				{userID: user1, block: block1, matchers: matchers1, shard: shard2, value: value2}, // different shard
				{userID: user1, block: block1, matchers: matchers2, shard: shard1, value: value2}, // different matchers
				{userID: user1, block: block2, matchers: matchers1, shard: shard1, value: value3}, // different block
			},
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			expectedData: value1,
			expectedOk:   true,
		},
		"should return no hit on remote cache error": {
			setup: []mockedSeries{
				{userID: user1, block: block1, matchers: matchers1, shard: shard1, value: value1},
				{userID: user1, block: block1, matchers: matchers2, shard: shard1, value: value2},
				{userID: user1, block: block2, matchers: matchers1, shard: shard1, value: value3},
			},
			mockedErr:    context.DeadlineExceeded,
			fetchUserID:  user1,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			fetchShard:   shard1,
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			client := newMockedRemoteCacheClient(testData.mockedErr)
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeries(ctx, p.userID, p.block, CanonicalLabelMatchersKey(p.matchers), p.shard, p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchSeries(ctx, testData.fetchUserID, testData.fetchBlockID, testData.fetchKey, testData.fetchShard)
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeries)))
			for _, typ := range remove(allCacheTypes, cacheTypeSeries) {
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
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreLabelNames(ctx, p.userID, p.block, CanonicalLabelMatchersKey(p.matchers), p.value)
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
			c, err := NewRemoteIndexCache(log.NewNopLogger(), client, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreLabelValues(ctx, p.userID, p.block, p.labelName, CanonicalLabelMatchersKey(p.matchers), p.value)
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
			key: postingsCacheKey(user, uid, labels.Label{Name: "foo", Value: "bar"}),
			expected: func() string {
				hash := blake2b.Sum256([]byte("foo:bar"))
				encodedHash := base64.RawURLEncoding.EncodeToString(hash[0:])

				return fmt.Sprintf("P:%s:%s:%s", user, uid.String(), encodedHash)
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
			expectedLen: 79,
			keys: []string{
				postingsCacheKey(user, uid, labels.Label{Name: "a", Value: "b"}),
				postingsCacheKey(user, uid, labels.Label{Name: strings.Repeat("a", 100), Value: strings.Repeat("a", 1000)}),
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
				assert.Equal(t, testData.expectedLen, len(key))
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
			postingsCacheKey(userID, uid, lbl)
		}
	})

	b.Run("series ref", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seriesForRefCacheKey(userID, uid, math.MaxUint64)
		}
	})

	b.Run("expanded postings", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			expandedPostingsCacheKey(userID, uid, lmKey)
		}
	})
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
	userID   string
	block    ulid.ULID
	matchers []*labels.Matcher
	value    []byte
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

func (c *mockedRemoteCacheClient) SetAsync(_ context.Context, key string, value []byte, _ time.Duration) error {
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
