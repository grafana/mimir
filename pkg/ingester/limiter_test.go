// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
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
		ingestersInZoneCount     int
		shardSize                int
		expectedValue            int
	}{
		"zone-awareness disabled, limit is disabled": {
			globalLimit:           0,
			ringReplicationFactor: 1,
			ringIngesterCount:     1,
			expectedValue:         math.MaxInt32,
		},
		"zone-awareness disabled, limit is enabled with replication-factor=1, shard size 0": {
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     10,
			shardSize:             0,
			expectedValue:         100, // (1000 / 10 ingesters)
		},
		"zone-awareness disabled, limit is enabled with replication-factor=1, shard size 5": {
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     10,
			shardSize:             5,
			expectedValue:         200, // (1000 / 5 ingesters from the shard)
		},
		"zone-awareness disabled, limit is enabled with replication-factor=3, shard size 0": {
			globalLimit:           1000,
			ringReplicationFactor: 3,
			ringIngesterCount:     10,
			shardSize:             0,
			expectedValue:         300, // (1000 / 10 ingesters) * 3 replication factor
		},
		"zone-awareness disabled, limit is enabled with replication-factor=3, shard size 5": {
			globalLimit:           1000,
			ringReplicationFactor: 3,
			ringIngesterCount:     10,
			shardSize:             5,
			expectedValue:         600, // (1000 / 5 ingesters from the shard) * 3 replication factor
		},
		"zone-awareness disabled, limit is enabled with replication-factor=3 and the shard size > number of ingesters": {
			globalLimit:           1000,
			ringReplicationFactor: 3,
			ringIngesterCount:     10,
			shardSize:             20,  // Greater than number of ingesters.
			expectedValue:         300, // (1000 / 10 ingesters) * 3 replication factor
		},

		"zone-awareness enabled, limit is disabled": {
			globalLimit:              0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     1,
			expectedValue:            math.MaxInt32,
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, all ingesters up and running, and shard size 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        9,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3, // 9 ingesters / 3 zones
			shardSize:                0,
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in this zone, and shard size 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        8, // ingesters are scaled down from 9 to 8
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     2, // in this zone ingesters are scaled down from 3 to 2
			shardSize:                0,
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in another zone, and shard size 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        8, // ingesters are scaled down from 9 to 8
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3, // in this zone ingesters are NOT scaled down
			shardSize:                0,
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in this zone, and shard size 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        10, // ingesters are scaled up from 9 to 10
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     4, // in this zone ingesters are scaled up from 3 to 4
			shardSize:                0,
			expectedValue:            225, // (900 / 3 zones / 4 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in another zone, and shard size 0": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        10, // ingesters are scaled up from 9 to 10
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3, // in this zone ingesters are NOT scaled up
			shardSize:                0,
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, all ingesters up and running, and shard size 5": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        9,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // 9 ingesters / 3 zones
			shardSize:                5,   // the expected number of ingesters per zone is (ceil(5/3) = 2
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in this zone, and shard size 5": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        7, // ingesters are scaled down from 9 to 7
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     1,   // in this zone ingesters are scaled down from 3 to 1
			shardSize:                5,   // the expected number of ingesters per zone is (ceil(5/3) = 2
			expectedValue:            900, // (900 / 3 zones / 1 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in another zone, and shard size 5": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        7, // ingesters are scaled down from 9 to 7
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // in this zone ingesters are NOT scaled down (ignored because of shardSize)
			shardSize:                5,   // the expected number of ingesters per zone is (ceil(5/3) = 2
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in this zone, and shard size 5": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        11, // ingesters are scaled up from 9 to 11
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     5,   // in this zone ingesters are scaled up from 3 to 5 (ignored because of shardSize)
			shardSize:                5,   // the expected number of ingesters per zone is (ceil(5/3) = 2
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in another zone, and shard size 5": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        11, // ingesters are scaled up from 9 to 11
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // in this zone ingesters are NOT scaled up (ignored because of shardSize)
			shardSize:                5,   // the expected number of ingesters per zone is (ceil(5/3) = 2
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, all ingesters up and running, and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        9,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // 9 ingesters / 3 zones
			shardSize:                20,  // Greater than number of ingesters.
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in this zone, and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        8, // ingesters are scaled down from 9 to 8
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     2,   // in this zone ingesters are scaled down from 3 to 2
			shardSize:                20,  // Greater than number of ingesters.
			expectedValue:            450, // (900 / 3 zones / 2 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled down in another zone, and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        8, // ingesters are scaled down from 9 to 8
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // in this zone ingesters are NOT scaled down
			shardSize:                20,  // Greater than number of ingesters.
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in this zone, and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        11, // ingesters are scaled up from 9 to 11
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     5,   // in this zone ingesters are scaled up from 3 to 5
			shardSize:                20,  // Greater than number of ingesters.
			expectedValue:            180, // (900 / 3 zones / 5 ingesters per zone) * 3 replication factor
		},
		"zone-awareness enabled, limit is enabled with replication-factor=3, some ingesters scaled up in another zone, and the shard size > number of ingesters": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        11, // ingesters are scaled up from 9 to 11
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     3,   // in this zone ingesters are NOT scaled up
			shardSize:                20,  // Greater than number of ingesters.
			expectedValue:            300, // (900 / 3 zones / 3 ingesters per zone) * 3 replication factor
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("InstancesCount").Return(testData.ringIngesterCount)
			ring.On("InstancesInZoneCount").Return(testData.ingestersInZoneCount)
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
		expected                 bool
	}{
		"limit is disabled": {
			maxGlobalSeriesPerMetric: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			series:                   100,
			expected:                 true,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			series:                   299,
			expected:                 true,
		},
		"current number of series is above the limit": {
			maxGlobalSeriesPerMetric: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			series:                   300,
			expected:                 false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("InstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.IsWithinMaxSeriesPerMetric("test", testData.series)

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
		expected                   bool
	}{
		"limit is disabled": {
			maxGlobalMetadataPerMetric: 0,
			ringReplicationFactor:      1,
			ringIngesterCount:          1,
			metadata:                   100,
			expected:                   true,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			metadata:                   299,
			expected:                   true,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerMetric: 1000,
			ringReplicationFactor:      3,
			ringIngesterCount:          10,
			metadata:                   300,
			expected:                   false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("InstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.IsWithinMaxMetadataPerMetric("test", testData.metadata)

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
		expected               bool
	}{
		"limit is disabled": {
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			series:                 100,
			expected:               true,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 299,
			expected:               true,
		},
		"current number of series is above the limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 300,
			expected:               false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("InstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.IsWithinMaxSeriesPerUser("test", testData.series)

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
		expected                 bool
	}{
		"limit is disabled": {
			maxGlobalMetadataPerUser: 0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			metadata:                 100,
			expected:                 true,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			metadata:                 299,
			expected:                 true,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerUser: 1000,
			ringReplicationFactor:    3,
			ringIngesterCount:        10,
			metadata:                 300,
			expected:                 false,
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{}
			ring.On("InstancesCount").Return(testData.ringIngesterCount)
			ring.On("ZonesCount").Return(1)

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser,
			}, nil)
			require.NoError(t, err)

			limiter := NewLimiter(limits, ring, testData.ringReplicationFactor, false)
			actual := limiter.IsWithinMaxMetricsWithMetadataPerUser("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

type ringCountMock struct {
	mock.Mock
}

func (m *ringCountMock) InstancesCount() int {
	args := m.Called()
	return args.Int(0)
}

func (m *ringCountMock) InstancesInZoneCount() int {
	args := m.Called()
	return args.Int(0)
}

func (m *ringCountMock) ZonesCount() int {
	args := m.Called()
	return args.Int(0)
}
