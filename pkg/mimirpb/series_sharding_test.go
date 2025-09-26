// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimirpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardBySeriesLabelAdaptersReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := ShardBySeriesLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	}, ShardingConfig{})

	val2 := ShardBySeriesLabelAdapters("test", []LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	}, ShardingConfig{})

	assert.NotEqual(t, val1, val2)
}

func TestShardBySeriesLabelsAndShardBySeriesLabelAdapters(t *testing.T) {
	userID := "test-user"

	// Same series with different "le" label values
	series1 := labels.FromStrings(
		"__name__", "http_request_duration_seconds_bucket",
		"job", "prometheus",
		"le", "0.1",
	)
	series2 := labels.FromStrings(
		"__name__", "http_request_duration_seconds_bucket",
		"job", "prometheus",
		"le", "0.5",
	)

	series1Adapter := FromLabelsToLabelAdapters(series1)
	series2Adapter := FromLabelsToLabelAdapters(series2)

	t.Run("ExcludeClassicHistogramBucketLabel is disabled", func(t *testing.T) {
		cfg := ShardingConfig{ExcludeClassicHistogramBucketLabel: false}

		series1Hash := ShardBySeriesLabels(userID, series1, cfg)
		series2Hash := ShardBySeriesLabels(userID, series2, cfg)
		assert.NotEqual(t, series1Hash, series2Hash)

		series1AdapterHash := ShardBySeriesLabelAdapters(userID, series1Adapter, cfg)
		series2AdapterHash := ShardBySeriesLabelAdapters(userID, series2Adapter, cfg)
		assert.NotEqual(t, series1AdapterHash, series2AdapterHash)

		assert.Equal(t, series1Hash, series1AdapterHash)
		assert.Equal(t, series2Hash, series2AdapterHash)
	})

	t.Run("ExcludeClassicHistogramBucketLabel is enabled", func(t *testing.T) {
		cfg := ShardingConfig{ExcludeClassicHistogramBucketLabel: true}

		series1Hash := ShardBySeriesLabels(userID, series1, cfg)
		series2Hash := ShardBySeriesLabels(userID, series2, cfg)
		assert.Equal(t, series1Hash, series2Hash)

		series1AdapterHash := ShardBySeriesLabelAdapters(userID, series1Adapter, cfg)
		series2AdapterHash := ShardBySeriesLabelAdapters(userID, series2Adapter, cfg)
		assert.Equal(t, series1AdapterHash, series2AdapterHash)

		assert.Equal(t, series1Hash, series1AdapterHash)
		assert.Equal(t, series2Hash, series2AdapterHash)
	})
}

func BenchmarkShardBySeriesLabels(b *testing.B) {
	userID := "test-user"
	series := labels.FromStrings(
		"__name__", "http_requests_total",
		"job", "prometheus",
		"instance", "localhost:9090",
		"method", "GET",
		"handler", "/api/v1/query",
		"status", "200",
		"cluster", "prod",
		"environment", "production",
		"region", "us-west-2",
		"az", "us-west-2a",
	)

	scenarios := map[string]ShardingConfig{
		"ExcludeClassicHistogramBucketLabel is disabled": {
			ExcludeClassicHistogramBucketLabel: false,
		},
		"ExcludeClassicHistogramBucketLabel is enabled": {
			ExcludeClassicHistogramBucketLabel: true,
		},
	}

	for scenarioName, cfg := range scenarios {
		b.Run(scenarioName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ShardBySeriesLabels(userID, series, cfg)
			}
		})
	}
}

func BenchmarkShardBySeriesLabelAdapters(b *testing.B) {
	userID := "test-user"
	series := []LabelAdapter{
		{Name: "__name__", Value: "http_requests_total"},
		{Name: "job", Value: "prometheus"},
		{Name: "instance", Value: "localhost:9090"},
		{Name: "method", Value: "GET"},
		{Name: "handler", Value: "/api/v1/query"},
		{Name: "status", Value: "200"},
		{Name: "cluster", Value: "prod"},
		{Name: "environment", Value: "production"},
		{Name: "region", Value: "us-west-2"},
		{Name: "az", Value: "us-west-2a"},
	}

	scenarios := map[string]ShardingConfig{
		"ExcludeClassicHistogramBucketLabel is disabled": {
			ExcludeClassicHistogramBucketLabel: false,
		},
		"ExcludeClassicHistogramBucketLabel is enabled": {
			ExcludeClassicHistogramBucketLabel: true,
		},
	}

	for scenarioName, cfg := range scenarios {
		b.Run(scenarioName, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ShardBySeriesLabelAdapters(userID, series, cfg)
			}
		})
	}
}
