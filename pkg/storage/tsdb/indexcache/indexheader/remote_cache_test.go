package indexheader

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
)

func TestRemotePostingsOffsetTableCache_FetchPostingsOffset(t *testing.T) {
	t.Parallel()

	// Init some data to conveniently define test cases later one.
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
		setup          []mockedPostings
		mockedErr      error
		fetchTenantID  string
		fetchBlockID   ulid.ULID
		fetchLabels    []labels.Label
		expectedHits   map[labels.Label]index.Range
		expectedMisses []labels.Label
	}{
		"should return no hits on empty cache": {
			setup:          []mockedPostings{},
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label0, label1},
		},
		"should return no misses on 100% hit ratio": {
			setup: []mockedPostings{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant1, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant1, blockID: block2, lbl: label1, rng: rng1},
			},
			fetchTenantID: tenant0,
			fetchBlockID:  block1,
			fetchLabels:   []labels.Label{label0, label1},
			expectedHits: map[labels.Label]index.Range{
				label0: rng0,
				label1: rng1,
			},
			expectedMisses: []labels.Label{},
		},
		"should return hits and misses on partial hits": {
			setup: []mockedPostings{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng0},
				{tenantID: tenant0, blockID: block2, lbl: label2, rng: rng2},
			},
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   map[labels.Label]index.Range{label0: rng0},
			expectedMisses: []labels.Label{label1},
		},
		"should return no hits on remote cache error": {
			setup: []mockedPostings{
				{tenantID: tenant0, blockID: block1, lbl: label0, rng: rng1},
				{tenantID: tenant0, blockID: block1, lbl: label1, rng: rng1},
				{tenantID: tenant0, blockID: block2, lbl: label0, rng: rng2},
			},
			mockedErr:      errors.New("mocked error"),
			fetchTenantID:  tenant0,
			fetchBlockID:   block1,
			fetchLabels:    []labels.Label{label0, label1},
			expectedHits:   nil,
			expectedMisses: []labels.Label{label0, label1},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cacheClient := newMockedRemoteCacheClient(testData.mockedErr)
			cache := NewRemotePostingsOffsetTableCache(cacheClient, log.NewNopLogger())

			// Store the postings expected before running the test.
			ctx := context.Background()
			for _, p := range testData.setup {
				cache.StorePostingsOffset(p.tenantID, p.blockID, p.lbl, p.rng, time.Hour)
			}

			// Fetch postings from cached and assert on it.
			testFetchFetchPostingsOffsets(
				ctx, t, cache,
				testData.fetchTenantID, testData.fetchBlockID, testData.fetchLabels,
				testData.expectedHits,
			)
		})
	}
}

func testFetchFetchPostingsOffsets(
	ctx context.Context,
	t *testing.T,
	cache PostingsOffsetTableCache,
	tenantID string,
	blockID ulid.ULID,
	keys []labels.Label,
	expectedHits map[labels.Label]index.Range,
) {
	t.Helper()

	hits := make(map[labels.Label]index.Range)
	for _, key := range keys {
		if rng, ok := cache.FetchPostingsOffset(ctx, tenantID, blockID, key); ok {
			hits[key] = rng
		}
	}

	if len(expectedHits) == 0 {
		assert.Equal(t, len(hits), 0)
	} else {
		assert.Equal(t, expectedHits, hits)
	}
}

type mockedPostings struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
	rng      index.Range
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
