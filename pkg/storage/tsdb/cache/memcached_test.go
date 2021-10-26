// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestMemcachedIndexCache_FetchMultiPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
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
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label][]byte
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostings{},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
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
				{block: block1, label: label1, value: value1},
				{block: block2, label: label1, value: value3},
			},
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   map[labels.Label][]byte{label1: value1},
			expectedMisses: []labels.Label{label2},
		},
		"should return no hits on memcached error": {
			setup: []mockedPostings{
				{block: block1, label: label1, value: value1},
				{block: block1, label: label2, value: value2},
				{block: block2, label: label1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label1, label2},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label1, label2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedIndexCache(log.NewNopLogger(), memcached, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StorePostings(ctx, p.block, p.label, p.value)
			}

			// Fetch postings from cached and assert on it.
			hits, misses := c.FetchMultiPostings(ctx, testData.fetchBlockID, testData.fetchLabels)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchLabels)), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypePostings)))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypePostings)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeries)))
		})
	}
}

func TestMemcachedIndexCache_FetchMultiSeries(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	tests := map[string]struct {
		setup          []mockedSeries
		mockedErr      error
		fetchBlockID   ulid.ULID
		fetchIds       []uint64
		expectedHits   map[uint64][]byte
		expectedMisses []uint64
	}{
		"should return no hits on empty cache": {
			setup:          []mockedSeries{},
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   nil,
			expectedMisses: []uint64{1, 2},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID: block1,
			fetchIds:     []uint64{1, 2},
			expectedHits: map[uint64][]byte{
				1: value1,
				2: value2,
			},
			expectedMisses: nil,
		},
		"should return hits and misses on partial hits": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block2, id: 1, value: value3},
			},
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   map[uint64][]byte{1: value1},
			expectedMisses: []uint64{2},
		},
		"should return no hits on memcached error": {
			setup: []mockedSeries{
				{block: block1, id: 1, value: value1},
				{block: block1, id: 2, value: value2},
				{block: block2, id: 1, value: value3},
			},
			mockedErr:      errors.New("mocked error"),
			fetchBlockID:   block1,
			fetchIds:       []uint64{1, 2},
			expectedHits:   nil,
			expectedMisses: []uint64{1, 2},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedIndexCache(log.NewNopLogger(), memcached, nil)
			assert.NoError(t, err)

			// Store the series expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreSeries(ctx, p.block, p.id, p.value)
			}

			// Fetch series from cached and assert on it.
			hits, misses := c.FetchMultiSeries(ctx, testData.fetchBlockID, testData.fetchIds)
			assert.Equal(t, testData.expectedHits, hits)
			assert.Equal(t, testData.expectedMisses, misses)

			// Assert on metrics.
			assert.Equal(t, float64(len(testData.fetchIds)), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, float64(len(testData.expectedHits)), prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypePostings)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypePostings)))
		})
	}
}

func TestMemcachedIndexCache_FetchExpandedPostings(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
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
		fetchBlockID ulid.ULID
		fetchKey     LabelMatchersKey
		expectedData []byte
		expectedOk   bool
	}{
		"should return no hit on empty cache": {
			setup:        []mockedExpandedPostings{},
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
		"should return no miss on hit": {
			setup: []mockedExpandedPostings{
				{block: block1, matchers: matchers1, value: value1},
				{block: block1, matchers: matchers2, value: value2},
				{block: block2, matchers: matchers1, value: value3},
			},
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: value1,
			expectedOk:   true,
		},
		"should return no hit on memcached error": {
			setup: []mockedExpandedPostings{
				{block: block1, matchers: matchers1, value: value1},
				{block: block1, matchers: matchers2, value: value2},
				{block: block2, matchers: matchers1, value: value3},
			},
			mockedErr:    context.DeadlineExceeded,
			fetchBlockID: block1,
			fetchKey:     CanonicalLabelMatchersKey(matchers1),
			expectedData: nil,
			expectedOk:   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memcached := newMockedMemcachedClient(testData.mockedErr)
			c, err := NewMemcachedIndexCache(log.NewNopLogger(), memcached, nil)
			assert.NoError(t, err)

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				c.StoreExpandedPostings(ctx, p.block, CanonicalLabelMatchersKey(p.matchers), p.value)
			}

			// Fetch postings from cached and assert on it.
			data, ok := c.FetchExpandedPostings(ctx, testData.fetchBlockID, testData.fetchKey)
			assert.Equal(t, testData.expectedData, data)
			assert.Equal(t, testData.expectedOk, ok)

			// Assert on metrics.
			expectedHits := 0.0
			if testData.expectedOk {
				expectedHits = 1.0
			}
			assert.Equal(t, float64(1), prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeExpandedPostings)))
			assert.Equal(t, expectedHits, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeExpandedPostings)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypeSeries)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.requests.WithLabelValues(cacheTypePostings)))
			assert.Equal(t, 0.0, prom_testutil.ToFloat64(c.hits.WithLabelValues(cacheTypePostings)))
		})
	}
}

type mockedPostings struct {
	block ulid.ULID
	label labels.Label
	value []byte
}

type mockedSeries struct {
	block ulid.ULID
	id    uint64
	value []byte
}

type mockedExpandedPostings struct {
	block    ulid.ULID
	matchers []*labels.Matcher
	value    []byte
}

type mockedMemcachedClient struct {
	cache             map[string][]byte
	mockedGetMultiErr error
}

func newMockedMemcachedClient(mockedGetMultiErr error) *mockedMemcachedClient {
	return &mockedMemcachedClient{
		cache:             map[string][]byte{},
		mockedGetMultiErr: mockedGetMultiErr,
	}
}

func (c *mockedMemcachedClient) GetMulti(ctx context.Context, keys []string) map[string][]byte {
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

func (c *mockedMemcachedClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	c.cache[key] = value

	return nil
}

func (c *mockedMemcachedClient) Stop() {
	// Nothing to do.
}
