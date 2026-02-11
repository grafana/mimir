// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
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
				log.NewNopLogger(),
			)

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
		log.NewNopLogger(),
	)

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

func TestResourceQuerierCache_Close(t *testing.T) {
	cache := NewResourceQuerierCache(
		&noopQuerier{},
		nil,
		0,
		10000,
		log.NewNopLogger(),
	)

	err := cache.Close()
	assert.NoError(t, err)
}
