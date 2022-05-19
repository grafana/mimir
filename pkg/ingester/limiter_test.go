// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestLimiter_maxSeriesPerMetric(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalSeriesPerMetric = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxSeriesPerMetric("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn)
}

func TestLimiter_maxMetadataPerMetric(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalMetadataPerMetric = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerMetric("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn)
}

func TestLimiter_maxSeriesPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalSeriesPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxSeriesPerUser("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn)
}

func TestLimiter_maxMetadataPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalMetricsWithMetadataPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerUser("test")
	}

	runLimiterMaxFunctionTest(t, applyLimits, runMaxFn)
}

func runLimiterMaxFunctionTest(
	t *testing.T,
	applyLimits func(limits *validation.Limits, globalLimit int),
	runMaxFn func(limiter *Limiter) int,
) {
	tests := map[string]struct {
		globalLimit              int
		ringReplicationFactor    int
		ringZoneAwarenessEnabled bool
		ringIngesterCount        int
		ringZonesCount           int
		shardSize                int
		expectedValue            int
	}{
		"limit is disabled": {
			globalLimit:           0,
			ringReplicationFactor: 1,
			ringIngesterCount:     1,
			ringZonesCount:        1,
			expectedValue:         math.MaxInt32,
		},
		"limit is enabled with replication-factor=1, shard size 0": {
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     10,
			ringZonesCount:        1,
			shardSize:             0,
			expectedValue:         100,
		},
		"limit is enabled with replication-factor=1, shard size 5": {
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     10,
			ringZonesCount:        1,
			shardSize:             5,
			expectedValue:         200,
		},
		"limit is enabled with replication-factor=3, shard size 0": {
			globalLimit:           1000,
			ringReplicationFactor: 3,
			ringIngesterCount:     10,
			ringZonesCount:        1,
			shardSize:             0,
			expectedValue:         300,
		},
		"limit is enabled with replication-factor=3, shard size 5": {
			globalLimit:           1000,
			ringReplicationFactor: 3,
			ringIngesterCount:     10,
			ringZonesCount:        1,
			shardSize:             5,
			expectedValue:         600,
		},
		"zone-awareness enabled, limit enabled and the shard size is 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardSize:                0,
			expectedValue:            300,
		},
		"zone-awareness enabled, limit enabled and the shard size is NOT divisible by number of zones": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardSize:                5,   // Not divisible by number of zones.
			expectedValue:            450, // (900 / 6) * 3
		},
		"zone-awareness enabled, limit enabled and the shard size is divisible by number of zones": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardSize:                6,   // Divisible by number of zones.
			expectedValue:            450, // (900 / 6) * 3
		},
		"zone-awareness enabled, limit enabled and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringZoneAwarenessEnabled: true,
			ringIngesterCount:        9,
			ringZonesCount:           3,
			shardSize:                20, // Greater than number of ingesters.
			expectedValue:            300,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(testData.ringZonesCount)

			// Mock limits
			limits := validation.Limits{IngestionTenantShardSize: testData.shardSize}
			applyLimits(&limits, testData.globalLimit)

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			limiter := NewLimiter(overrides, ring, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled)
			actual := runMaxFn(limiter)
			assert.Equal(t, testData.expectedValue, actual)
		})
	}
}

func TestLimiter_AssertMaxSeriesPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxGlobalSeriesPerMetric int
		ringReplicationFactor    int
		ringIngesterCount        int
		series                   int
		expected                 error
	}{
		"limit is disabled": {
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			series:                   100,
			expected:                 nil,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			series:                   299,
			expected:                 nil,
		},
		"current number of series is above the limit": {
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			series:                   300,
			expected:                 errMaxSeriesPerMetricLimitExceeded,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxSeriesPerMetric("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}
func TestLimiter_AssertMaxMetadataPerMetric(t *testing.T) {
	tests := map[string]struct {
		maxGlobalMetadataPerMetric int
		ringReplicationFactor      int
		ringIngesterCount          int
		metadata                   int
		expected                   error
	}{
		"limit is disabled": {
			maxGlobalMetadataPerMetric: 0,
			ringReplicationFactor:      1,
			ringIngesterCount:          1,
			metadata:                   100,
			expected:                   nil,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			metadata:                   299,
			expected:                   nil,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			metadata:                   300,
			expected:                   errMaxMetadataPerMetricLimitExceeded,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxMetadataPerMetric("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_AssertMaxSeriesPerUser(t *testing.T) {
	tests := map[string]struct {
		maxGlobalSeriesPerUser int
		ringReplicationFactor  int
		ringIngesterCount      int
		series                 int
		expected               error
	}{
		"limit is disabled": {
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			series:                 100,
			expected:               nil,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 299,
			expected:               nil,
		},
		"current number of series is above the limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 300,
			expected:               errMaxSeriesPerUserLimitExceeded,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxSeriesPerUser("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_AssertMaxMetricsWithMetadataPerUser(t *testing.T) {
	tests := map[string]struct {
		maxGlobalMetadataPerUser int
		ringReplicationFactor    int
		ringIngesterCount        int
		metadata                 int
		expected                 error
	}{
		"limit is disabled": {
			maxGlobalMetadataPerUser: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			metadata:                 100,
			expected:                 nil,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			metadata:                 299,
			expected:                 nil,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			metadata:                 300,
			expected:                 errMaxMetadataPerUserLimitExceeded,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("HealthyInstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.AssertMaxMetricsWithMetadataPerUser("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_FormatError(t *testing.T) {
	// Mock the ring
	ring := &ringCountMock{}
	ring.On("HealthyInstancesCount").Return(3)
	ring.On("ZonesCount").Return(1)

	// Mock limits
	limits, err := validation.NewOverrides(validation.Limits{
		MaxGlobalSeriesPerUser:              100,
		MaxGlobalSeriesPerMetric:            20,
		MaxGlobalMetricsWithMetadataPerUser: 10,
		MaxGlobalMetadataPerMetric:          3,
	}, nil)
	require.NoError(t, err)

	limiter := NewLimiter(limits, ring, 3, false)

	actual := limiter.FormatError("user-1", errMaxSeriesPerUserLimitExceeded)
	assert.EqualError(t, actual, "per-user series limit of 100 exceeded, contact an administrator to raise it (per-ingester local limit: 100)")

	actual = limiter.FormatError("user-1", errMaxSeriesPerMetricLimitExceeded)
	assert.EqualError(t, actual, "per-metric series limit of 20 exceeded, contact an administrator to raise it (per-ingester local limit: 20)")

	actual = limiter.FormatError("user-1", errMaxMetadataPerUserLimitExceeded)
	assert.EqualError(t, actual, "per-user metric metadata limit of 10 exceeded, contact an administrator to raise it (per-ingester local limit: 10)")

	actual = limiter.FormatError("user-1", errMaxMetadataPerMetricLimitExceeded)
	assert.EqualError(t, actual, "per-metric metadata limit of 3 exceeded, contact an administrator to raise it (per-ingester local limit: 3)")

	input := errors.New("unknown error")
	actual = limiter.FormatError("user-1", input)
	assert.Equal(t, input, actual)
}

type ringCountMock struct {
	mock.Mock
}

func (m *ringCountMock) HealthyInstancesCount() int {
	args := m.Called()
	return args.Int(0)
}

func (m *ringCountMock) ZonesCount() int {
	args := m.Called()
	return args.Int(0)
}
