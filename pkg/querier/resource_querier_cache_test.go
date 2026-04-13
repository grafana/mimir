// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/seriesmetadata"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopQuerier is a minimal implementation of storage.Querier for testing.
type noopQuerier struct{}

func (m *noopQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return storage.EmptySeriesSet()
}

func (m *noopQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *noopQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (m *noopQuerier) Close() error {
	return nil
}

// mockResourceFetcher implements ResourceAttributesFetcher for testing.
type mockResourceFetcher struct {
	data []*ResourceAttributesData
	err  error
}

func (m *mockResourceFetcher) FetchResourceAttributes(ctx context.Context, minT, maxT int64) ([]*ResourceAttributesData, error) {
	return m.data, m.err
}

func TestResourceQuerierCache_GetResourceAt(t *testing.T) {
	testCases := []struct {
		name          string
		fetcherData   []*ResourceAttributesData
		fetcherErr    error
		labelsHash    uint64
		timestamp     int64
		expectFound   bool
		expectVersion *seriesmetadata.ResourceVersion
	}{
		{
			name:        "no fetcher returns false",
			labelsHash:  12345,
			timestamp:   1000,
			expectFound: false,
		},
		{
			name:        "fetcher error returns false",
			fetcherErr:  errors.New("fetch error"),
			labelsHash:  12345,
			timestamp:   1000,
			expectFound: false,
		},
		{
			name: "cache miss returns false",
			fetcherData: []*ResourceAttributesData{
				{
					LabelsHash: 11111,
					Versions: []*seriesmetadata.ResourceVersion{
						{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "test"}},
					},
				},
			},
			labelsHash:  99999, // Different hash
			timestamp:   1500,
			expectFound: false,
		},
		{
			name: "cache hit returns version",
			fetcherData: []*ResourceAttributesData{
				{
					LabelsHash: 12345,
					Versions: []*seriesmetadata.ResourceVersion{
						{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "test"}},
					},
				},
			},
			labelsHash:  12345,
			timestamp:   1500,
			expectFound: true,
			expectVersion: &seriesmetadata.ResourceVersion{
				MinTime:     1000,
				MaxTime:     2000,
				Identifying: map[string]string{"service.name": "test"},
			},
		},
		{
			name: "timestamp before MinTime returns not found",
			fetcherData: []*ResourceAttributesData{
				{
					LabelsHash: 12345,
					Versions: []*seriesmetadata.ResourceVersion{
						{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "test"}},
					},
				},
			},
			labelsHash:  12345,
			timestamp:   500, // Before MinTime - VersionAt returns nil
			expectFound: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var fetcher ResourceAttributesFetcher
			if tc.fetcherData != nil || tc.fetcherErr != nil {
				fetcher = &mockResourceFetcher{data: tc.fetcherData, err: tc.fetcherErr}
			}

			cache := NewResourceQuerierCache(
				&noopQuerier{},
				fetcher,
				0,     // minT
				10000, // maxT
				nil,   // maxCacheBytesProvider
				log.NewNopLogger(),
			)

			// Simulate a Select call to set the stored context (required for cache init).
			cache.Select(user.InjectOrgID(context.Background(), "test-tenant"), false, nil)

			rv, found := cache.(*resourceQuerierCache).GetResourceAt(tc.labelsHash, tc.timestamp)

			assert.Equal(t, tc.expectFound, found)
			if tc.expectFound {
				require.NotNil(t, rv)
				assert.Equal(t, tc.expectVersion.MinTime, rv.MinTime)
				assert.Equal(t, tc.expectVersion.MaxTime, rv.MaxTime)
				assert.Equal(t, tc.expectVersion.Identifying, rv.Identifying)
			}
		})
	}
}

func TestResourceQuerierCache_IterUniqueAttributeNames(t *testing.T) {
	fetcher := &mockResourceFetcher{
		data: []*ResourceAttributesData{
			{
				LabelsHash: 12345,
				Versions: []*seriesmetadata.ResourceVersion{
					{
						Identifying: map[string]string{"service.name": "test", "service.namespace": "prod"},
						Descriptive: map[string]string{"service.version": "1.0.0"},
					},
				},
			},
			{
				LabelsHash: 67890,
				Versions: []*seriesmetadata.ResourceVersion{
					{
						Identifying: map[string]string{"service.name": "other"},
						Descriptive: map[string]string{"host.name": "localhost"},
					},
				},
			},
		},
	}

	cache := NewResourceQuerierCache(
		&noopQuerier{},
		fetcher,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	// Simulate a Select call to set the stored context (required for cache init).
	cache.Select(user.InjectOrgID(context.Background(), "test-tenant"), false, nil)

	var names []string
	err := cache.(*resourceQuerierCache).IterUniqueAttributeNames(func(name string) {
		names = append(names, name)
	})

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"service.name",
		"service.namespace",
		"service.version",
		"host.name",
	}, names)
}

func TestResourceQuerierCache_MultipleSelectCalls(t *testing.T) {
	// Regression test: the cache fetches ALL resource attributes for the time range
	// (not scoped to any particular Select's matchers), so data for any series is
	// available regardless of which Select() call initialized the cache.
	fetcher := &mockResourceFetcher{
		data: []*ResourceAttributesData{
			{
				LabelsHash: 11111,
				Versions: []*seriesmetadata.ResourceVersion{
					{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "svc-a"}},
				},
			},
			{
				LabelsHash: 22222,
				Versions: []*seriesmetadata.ResourceVersion{
					{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "svc-b"}},
				},
			},
		},
	}

	cache := NewResourceQuerierCache(
		&noopQuerier{},
		fetcher,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")

	// Simulate two Select() calls for different metrics (like info(metric_a) + info(metric_b)).
	matcherA, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "metric_a")
	matcherB, _ := labels.NewMatcher(labels.MatchEqual, "__name__", "metric_b")
	cache.Select(ctx, false, nil, matcherA)
	cache.Select(ctx, false, nil, matcherB)

	rc := cache.(*resourceQuerierCache)

	// Both series should be found regardless of which Select() initialized the cache.
	rv1, found1 := rc.GetResourceAt(11111, 1500)
	assert.True(t, found1, "series 11111 should be found")
	require.NotNil(t, rv1)
	assert.Equal(t, "svc-a", rv1.Identifying["service.name"])

	rv2, found2 := rc.GetResourceAt(22222, 1500)
	assert.True(t, found2, "series 22222 should be found")
	require.NotNil(t, rv2)
	assert.Equal(t, "svc-b", rv2.Identifying["service.name"])
}

func TestResourceQuerierCache_MergeDuplicateSeries(t *testing.T) {
	// Test that when the fetcher returns duplicate entries for the same labels hash
	// (e.g., from different store-gateways), versions are merged and deduplicated.
	fetcher := &mockResourceFetcher{
		data: []*ResourceAttributesData{
			{
				LabelsHash: 12345,
				Versions: []*seriesmetadata.ResourceVersion{
					{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "test"}},
				},
			},
			{
				LabelsHash: 12345,
				Versions: []*seriesmetadata.ResourceVersion{
					// Duplicate version — should be deduped.
					{MinTime: 1000, MaxTime: 2000, Identifying: map[string]string{"service.name": "test"}},
					// New version — should be kept.
					{MinTime: 3000, MaxTime: 4000, Identifying: map[string]string{"service.name": "test"}},
				},
			},
		},
	}

	cache := NewResourceQuerierCache(
		&noopQuerier{},
		fetcher,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	cache.Select(ctx, false, nil)

	rc := cache.(*resourceQuerierCache)

	// Should find the series.
	rv, found := rc.GetResourceAt(12345, 1500)
	assert.True(t, found)
	require.NotNil(t, rv)

	// The cache should have merged and deduped to 2 versions.
	vr := rc.cache[uint64(12345)]
	require.NotNil(t, vr)
	assert.Equal(t, 2, len(vr.Versions), "duplicate version should be deduped")

	// Versions should be sorted by MinTime.
	assert.True(t, vr.Versions[0].MinTime <= vr.Versions[1].MinTime,
		"versions should be sorted by MinTime")
}

func TestResourceQuerierCache_ErrorMemoization(t *testing.T) {
	// Verify that after a fetch failure, the error is memoized: the fetcher
	// is called exactly once, and subsequent GetResourceAt calls return false
	// without re-invoking the fetcher.
	callCount := 0
	fetcher := &countingResourceFetcher{
		err:       errors.New("fetch error"),
		callCount: &callCount,
	}

	cache := NewResourceQuerierCache(
		&noopQuerier{},
		fetcher,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	// Simulate a Select call to set the stored context (required for cache init).
	cache.Select(user.InjectOrgID(context.Background(), "test-tenant"), false, nil)

	rc := cache.(*resourceQuerierCache)

	// First call triggers the fetch and gets the memoized error.
	_, found1 := rc.GetResourceAt(12345, 1500)
	assert.False(t, found1, "first call should return false on error")
	assert.Equal(t, 1, callCount, "fetcher should be called once")

	// Second call should NOT re-invoke the fetcher — error is memoized.
	_, found2 := rc.GetResourceAt(12345, 1500)
	assert.False(t, found2, "second call should return false (memoized error)")
	assert.Equal(t, 1, callCount, "fetcher should still have been called exactly once (error memoized)")
}

// countingResourceFetcher is a ResourceAttributesFetcher that counts calls.
type countingResourceFetcher struct {
	data      []*ResourceAttributesData
	err       error
	callCount *int
}

func (f *countingResourceFetcher) FetchResourceAttributes(ctx context.Context, minT, maxT int64) ([]*ResourceAttributesData, error) {
	*f.callCount++
	return f.data, f.err
}

func TestResourceQuerierCache_ContextHasTimeout(t *testing.T) {
	// Verify that the context passed to FetchResourceAttributes has a deadline,
	// preventing indefinite blocking if the store-gateway RPC hangs.
	var capturedCtx context.Context
	fetcher := &contextCapturingFetcher{captureCtx: &capturedCtx}

	cache := NewResourceQuerierCache(
		&noopQuerier{},
		fetcher,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	// Simulate Select() being called first to set the tenant ID.
	ctx := user.InjectOrgID(context.Background(), "test-tenant")
	cache.Select(ctx, false, nil)

	// Trigger lazy initialization via GetResourceAt.
	rq, ok := cache.(storage.ResourceQuerier)
	require.True(t, ok, "cache should implement ResourceQuerier")
	rq.GetResourceAt(123, 1000)

	require.NotNil(t, capturedCtx, "FetchResourceAttributes should have been called")
	deadline, ok := capturedCtx.Deadline()
	assert.True(t, ok, "context should have a deadline")
	assert.False(t, deadline.IsZero(), "deadline should not be zero")
}

// contextCapturingFetcher captures the context passed to FetchResourceAttributes.
type contextCapturingFetcher struct {
	captureCtx *context.Context
}

func (f *contextCapturingFetcher) FetchResourceAttributes(ctx context.Context, _, _ int64) ([]*ResourceAttributesData, error) {
	*f.captureCtx = ctx
	return nil, nil
}

func TestResourceQuerierCache_Close(t *testing.T) {
	cache := NewResourceQuerierCache(
		&noopQuerier{},
		nil,
		0,
		10000,
		nil, // maxCacheBytesProvider
		log.NewNopLogger(),
	)

	err := cache.Close()
	assert.NoError(t, err)
}

func TestResourceQuerierCache_CacheSizeLimit(t *testing.T) {
	// Create many items with non-trivial size.
	var data []*ResourceAttributesData
	for i := uint64(0); i < 100; i++ {
		data = append(data, &ResourceAttributesData{
			LabelsHash: i,
			Versions: []*seriesmetadata.ResourceVersion{
				{
					MinTime:     1000,
					MaxTime:     2000,
					Identifying: map[string]string{"service.name": "service-with-a-long-name-to-consume-bytes"},
					Descriptive: map[string]string{"service.version": "1.0.0", "host.name": "host-with-a-long-name"},
				},
			},
		})
	}

	t.Run("limit set truncates cache", func(t *testing.T) {
		fetcher := &mockResourceFetcher{data: data}

		// Set a very small limit so only a few items fit.
		cache := NewResourceQuerierCache(
			&noopQuerier{},
			fetcher,
			0,
			10000,
			func(_ string) int { return 500 }, // ~500 bytes limit
			log.NewNopLogger(),
		)

		ctx := user.InjectOrgID(context.Background(), "test-tenant")
		cache.Select(ctx, false, nil)

		rc := cache.(*resourceQuerierCache)
		// Trigger initialization.
		rc.GetResourceAt(0, 1500)

		assert.Less(t, len(rc.cache), 100, "cache should be truncated due to size limit")
		assert.Greater(t, len(rc.cache), 0, "cache should have at least some items")
	})

	t.Run("limit 0 caches all", func(t *testing.T) {
		fetcher := &mockResourceFetcher{data: data}

		cache := NewResourceQuerierCache(
			&noopQuerier{},
			fetcher,
			0,
			10000,
			func(_ string) int { return 0 }, // disabled
			log.NewNopLogger(),
		)

		ctx := user.InjectOrgID(context.Background(), "test-tenant")
		cache.Select(ctx, false, nil)

		rc := cache.(*resourceQuerierCache)
		rc.GetResourceAt(0, 1500)

		assert.Equal(t, 100, len(rc.cache), "all items should be cached when limit is 0")
	})

	t.Run("nil provider caches all", func(t *testing.T) {
		fetcher := &mockResourceFetcher{data: data}

		cache := NewResourceQuerierCache(
			&noopQuerier{},
			fetcher,
			0,
			10000,
			nil, // no provider
			log.NewNopLogger(),
		)

		ctx := user.InjectOrgID(context.Background(), "test-tenant")
		cache.Select(ctx, false, nil)

		rc := cache.(*resourceQuerierCache)
		rc.GetResourceAt(0, 1500)

		assert.Equal(t, 100, len(rc.cache), "all items should be cached when provider is nil")
	})
}

func TestEstimateResourceDataSize(t *testing.T) {
	item := &ResourceAttributesData{
		LabelsHash: 12345,
		Versions: []*seriesmetadata.ResourceVersion{
			{
				Identifying: map[string]string{"service.name": "test"},
				Descriptive: map[string]string{"host.name": "localhost"},
			},
		},
	}

	size := estimateResourceDataSize(item)
	assert.Greater(t, size, int64(0), "size should be positive")
	// Rough expected: 32 (base) + 64 (version overhead) + (12+4+16) + (9+9+16) = 162 bytes approx
	assert.Greater(t, size, int64(100), "size should be at least 100 bytes for this item")
	assert.Less(t, size, int64(500), "size should not be unreasonably large")
}
