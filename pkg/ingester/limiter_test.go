// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
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

	t.Run("ingester ring", func(t *testing.T) {
		runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
	t.Run("partitions ring", func(t *testing.T) {
		runLimiterMaxFunctionTestWithPartitionsRing(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
}

func TestLimiter_maxMetadataPerMetric(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalMetadataPerMetric = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerMetric("test")
	}

	t.Run("ingester ring", func(t *testing.T) {
		runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
	t.Run("partitions ring", func(t *testing.T) {
		runLimiterMaxFunctionTestWithPartitionsRing(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
}

func TestLimiter_maxSeriesPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalSeriesPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxSeriesPerUser("test", 0)
	}

	t.Run("ingester ring", func(t *testing.T) {
		runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
	t.Run("partitions ring", func(t *testing.T) {
		runLimiterMaxFunctionTestWithPartitionsRing(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
}

func TestLimiter_maxMetadataPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalMetricsWithMetadataPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxMetadataPerUser("test")
	}

	t.Run("ingester ring", func(t *testing.T) {
		runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
	t.Run("partitions ring", func(t *testing.T) {
		runLimiterMaxFunctionTestWithPartitionsRing(t, applyLimits, runMaxFn, math.MaxInt32, false)
	})
}

func TestLimiter_maxExemplarsPerUser(t *testing.T) {
	applyLimits := func(limits *validation.Limits, globalLimit int) {
		limits.MaxGlobalExemplarsPerUser = globalLimit
	}

	runMaxFn := func(limiter *Limiter) int {
		return limiter.maxExemplarsPerUser("test")
	}

	t.Run("ingester ring", func(t *testing.T) {
		runLimiterMaxFunctionTest(t, applyLimits, runMaxFn, 0, true)
	})
	t.Run("partitions ring", func(t *testing.T) {
		runLimiterMaxFunctionTestWithPartitionsRing(t, applyLimits, runMaxFn, 0, true)
	})
}

func runLimiterMaxFunctionTest(
	t *testing.T,
	applyLimits func(limits *validation.Limits, globalLimit int),
	runMaxFn func(limiter *Limiter) int,
	expectedLimitForZeroGlobalLimit int,
	expectedUseGlobalLimitIfUnableToComputeLocalLimit bool,
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
		"zone-awareness disabled, global limit is 0": {
			globalLimit:           0,
			ringReplicationFactor: 1,
			ringIngesterCount:     1,
			expectedValue:         expectedLimitForZeroGlobalLimit,
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
		"zone-awareness disabled, limit is enabled with replication-factor=1, shard size 0, no ingesters seen by the ring client": {
			globalLimit:           1000,
			ringReplicationFactor: 1,
			ringIngesterCount:     0, // No ingesters seen by the ring client.
			shardSize:             0,
			expectedValue: func() int {
				if expectedUseGlobalLimitIfUnableToComputeLocalLimit {
					return 1000
				}

				return expectedLimitForZeroGlobalLimit
			}(),
		},

		"zone-awareness enabled, global limit is 0": {
			globalLimit:              0,
			ringReplicationFactor:    1,
			ringIngesterCount:        1,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     1,
			expectedValue:            expectedLimitForZeroGlobalLimit,
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
		"zone-awareness enabled, limit is enabled with replication-factor=3, no ingesters seen by the ring client in this zone": {
			globalLimit:              900,
			ringReplicationFactor:    3,
			ringIngesterCount:        6, // Ingesters only running on the other zones.,
			ringZoneAwarenessEnabled: true,
			ringZonesCount:           3,
			ingestersInZoneCount:     0,  // No ingesters seen in the zone.
			shardSize:                20, // Greater than number of ingesters.
			expectedValue: func() int {
				if expectedUseGlobalLimitIfUnableToComputeLocalLimit {
					return 900
				}

				return expectedLimitForZeroGlobalLimit
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: testData.ringIngesterCount, zonesCount: testData.ringZonesCount, instancesInZoneCount: testData.ingestersInZoneCount}

			// Mock limits
			limits := validation.Limits{IngestionTenantShardSize: testData.shardSize}
			applyLimits(&limits, testData.globalLimit)

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, testData.ringReplicationFactor, testData.ringZoneAwarenessEnabled, "zone", overrides.IngestionTenantShardSize)
			limiter := NewLimiter(overrides, strategy)
			actual := runMaxFn(limiter)
			assert.Equal(t, testData.expectedValue, actual)
		})
	}
}

func runLimiterMaxFunctionTestWithPartitionsRing(
	t *testing.T,
	applyLimits func(limits *validation.Limits, globalLimit int),
	runMaxFn func(limiter *Limiter) int,
	expectedLimitForZeroGlobalLimit int,
	expectedUseGlobalLimitIfUnableToComputeLocalLimit bool,
) {
	tests := map[string]struct {
		globalLimit               int
		partitionStates           []ring.PartitionState
		tenantPartitionsShardSize int
		expectedValue             int
	}{
		"limit is disabled": {
			globalLimit:               0,
			partitionStates:           []ring.PartitionState{ring.PartitionActive, ring.PartitionInactive},
			tenantPartitionsShardSize: 0,
			expectedValue:             expectedLimitForZeroGlobalLimit,
		},
		"limit is enabled, using all active partitions, only 1 is active": {
			globalLimit:               1000,
			partitionStates:           []ring.PartitionState{ring.PartitionActive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive},
			tenantPartitionsShardSize: 0,
			expectedValue:             1000 / 1,
		},
		"limit is enabled, using all active partitions, only 2 are active": {
			globalLimit:               1000,
			partitionStates:           []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive},
			tenantPartitionsShardSize: 0,
			expectedValue:             1000 / 2,
		},
		"limit is enabled, using 2 partitions out of 5, all active": {
			globalLimit:               1000,
			partitionStates:           []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			tenantPartitionsShardSize: 2,
			expectedValue:             1000 / 2,
		},
		"limit is enabled, using 5 partitions, but only 2 are active": {
			globalLimit:               1000,
			partitionStates:           []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive},
			tenantPartitionsShardSize: 5,
			expectedValue:             1000 / 2,
		},
		"limit is enabled, using 2 partitions, but 0 are active": {
			globalLimit:               1000,
			partitionStates:           []ring.PartitionState{ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive, ring.PartitionInactive},
			tenantPartitionsShardSize: 2,
			expectedValue: func() int {
				if expectedUseGlobalLimitIfUnableToComputeLocalLimit {
					return 1000
				}

				return expectedLimitForZeroGlobalLimit
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			h := &partitionRingHolder{pr: buildPartitionRingFromPartitionStates(testData.partitionStates...)}

			// Mock limits
			limits := validation.Limits{IngestionPartitionsTenantShardSize: testData.tenantPartitionsShardSize}
			applyLimits(&limits, testData.globalLimit)

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			strategy := newPartitionRingLimiterStrategy(h, overrides.IngestionPartitionsTenantShardSize)
			limiter := NewLimiter(overrides, strategy)
			actual := runMaxFn(limiter)
			assert.Equal(t, testData.expectedValue, actual)
		})
	}
}

func buildPartitionRingFromPartitionStates(states ...ring.PartitionState) *ring.PartitionRing {
	pr := ring.NewPartitionRingDesc()
	for ix, s := range states {
		if s == ring.PartitionUnknown {
			continue
		}

		pr.AddPartition(int32(ix), s, time.Time{})
	}
	return ring.NewPartitionRing(*pr)
}

func TestLimiter_IsWithinMaxSeriesPerMetric(t *testing.T) {
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
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: testData.ringIngesterCount, zonesCount: 1}

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric,
			}, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, testData.ringReplicationFactor, false, "", limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxSeriesPerMetric("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxSeriesPerMetric_WithPartitionsRing(t *testing.T) {
	tests := map[string]struct {
		maxGlobalSeriesPerMetric int
		partitionStates          []ring.PartitionState
		series                   int
		expected                 bool
	}{
		"limit is disabled": {
			maxGlobalSeriesPerMetric: 0,
			partitionStates:          []ring.PartitionState{ring.PartitionActive},
			series:                   100,
			expected:                 true,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerMetric: 900,
			partitionStates:          []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                   299,
			expected:                 true,
		},
		"current number of series is above the limit": {
			maxGlobalSeriesPerMetric: 900,
			partitionStates:          []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                   300,
			expected:                 false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pr := buildPartitionRingFromPartitionStates(testData.partitionStates...)

			limits, err := validation.NewOverrides(validation.Limits{MaxGlobalSeriesPerMetric: testData.maxGlobalSeriesPerMetric}, nil)
			require.NoError(t, err)

			strategy := newPartitionRingLimiterStrategy(&partitionRingHolder{pr: pr}, limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxSeriesPerMetric("test", testData.series)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxMetadataPerMetric(t *testing.T) {
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
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: testData.ringIngesterCount, zonesCount: 1}

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric,
			}, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, testData.ringReplicationFactor, false, "", limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxMetadataPerMetric("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxMetadataPerMetric_WithPartitionsRing(t *testing.T) {
	tests := map[string]struct {
		maxGlobalMetadataPerMetric int
		partitionStates            []ring.PartitionState
		metadata                   int
		expected                   bool
	}{
		"limit is disabled": {
			maxGlobalMetadataPerMetric: 0,
			partitionStates:            []ring.PartitionState{ring.PartitionActive},
			metadata:                   100,
			expected:                   true,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerMetric: 900,
			partitionStates:            []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			metadata:                   299,
			expected:                   true,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerMetric: 900,
			partitionStates:            []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			metadata:                   300,
			expected:                   false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pr := buildPartitionRingFromPartitionStates(testData.partitionStates...)

			limits, err := validation.NewOverrides(validation.Limits{MaxGlobalMetadataPerMetric: testData.maxGlobalMetadataPerMetric}, nil)
			require.NoError(t, err)

			strategy := newPartitionRingLimiterStrategy(&partitionRingHolder{pr: pr}, limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxMetadataPerMetric("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxSeriesPerUser(t *testing.T) {
	tests := map[string]struct {
		maxGlobalSeriesPerUser int
		ringReplicationFactor  int
		ringIngesterCount      int
		series                 int
		tenantShardSize        int
		minLocalLimit          int
		expected               bool
	}{
		"limit is disabled": {
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			series:                 100,
			expected:               true,
		},
		"limit is disabled, series is above min limit": {
			maxGlobalSeriesPerUser: 0,
			ringReplicationFactor:  1,
			ringIngesterCount:      1,
			series:                 100,
			minLocalLimit:          50,
			expected:               true,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 299,
			expected:               true,
		},
		"current number of series is above the limit, min limit not set": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 300,
			expected:               false,
		},
		"current number of series is above the limit, but below min limit": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 300,
			minLocalLimit:          400,
			expected:               true,
		},
		"current number of series below the limit with non-zero shard size to the check": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 999,
			tenantShardSize:        3,
			expected:               true,
		},
		"current number of series is at the limit with non-zero shard size to the check": {
			maxGlobalSeriesPerUser: 1000,
			ringReplicationFactor:  3,
			ringIngesterCount:      10,
			series:                 1000,
			tenantShardSize:        3,
			expected:               false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: testData.ringIngesterCount, zonesCount: 1}

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalSeriesPerUser:   testData.maxGlobalSeriesPerUser,
				IngestionTenantShardSize: testData.tenantShardSize,
			}, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, testData.ringReplicationFactor, false, "", limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxSeriesPerUser("test", testData.series, testData.minLocalLimit)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxSeriesPerUser_WithPartitionsRing(t *testing.T) {
	tests := map[string]struct {
		maxGlobalSeriesPerUser int
		partitionStates        []ring.PartitionState
		series                 int
		tenantShardSize        int
		expected               bool
		minLocalLimit          int
	}{
		"limit is disabled": {
			maxGlobalSeriesPerUser: 0,
			partitionStates:        []ring.PartitionState{ring.PartitionActive},
			series:                 100,
			expected:               true,
		},
		"limit is disabled, series is above min limit": {
			maxGlobalSeriesPerUser: 0,
			partitionStates:        []ring.PartitionState{ring.PartitionActive},
			series:                 100,
			minLocalLimit:          50,
			expected:               true,
		},
		"current number of series is below the limit": {
			maxGlobalSeriesPerUser: 900,
			partitionStates:        []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                 299,
			expected:               true,
		},
		"current number of series is above the limit, min limit not set": {
			maxGlobalSeriesPerUser: 900,
			partitionStates:        []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                 300,
			expected:               false,
		},
		"current number of series is above the limit, but below min limit": {
			maxGlobalSeriesPerUser: 900,
			partitionStates:        []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                 350,
			expected:               true,
			minLocalLimit:          400,
		},
		"current number of series below the limit with non-zero shard size to the check": {
			maxGlobalSeriesPerUser: 1000,
			partitionStates:        []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                 499,
			tenantShardSize:        2,
			expected:               true,
		},
		"current number of series is at the limit with non-zero shard size to the check": {
			maxGlobalSeriesPerUser: 1000,
			partitionStates:        []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			series:                 500,
			tenantShardSize:        2,
			expected:               false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pr := buildPartitionRingFromPartitionStates(testData.partitionStates...)

			limits, err := validation.NewOverrides(validation.Limits{MaxGlobalSeriesPerUser: testData.maxGlobalSeriesPerUser, IngestionPartitionsTenantShardSize: testData.tenantShardSize}, nil)
			require.NoError(t, err)

			strategy := newPartitionRingLimiterStrategy(&partitionRingHolder{pr: pr}, limits.IngestionPartitionsTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxSeriesPerUser("test", testData.series, testData.minLocalLimit)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxMetricsWithMetadataPerUser(t *testing.T) {
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
		t.Run(testName, func(t *testing.T) {
			// Mock the ring
			ring := &ringCountMock{instancesCount: testData.ringIngesterCount, zonesCount: 1}

			// Mock limits
			limits, err := validation.NewOverrides(validation.Limits{
				MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser,
			}, nil)
			require.NoError(t, err)

			strategy := newIngesterRingLimiterStrategy(ring, testData.ringReplicationFactor, false, "", limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxMetricsWithMetadataPerUser("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

func TestLimiter_IsWithinMaxMetricsWithMetadataPerUser_WithPartitionsRing(t *testing.T) {
	tests := map[string]struct {
		maxGlobalMetadataPerUser int
		partitionStates          []ring.PartitionState
		metadata                 int
		expected                 bool
	}{
		"limit is disabled": {
			maxGlobalMetadataPerUser: 0,
			partitionStates:          []ring.PartitionState{ring.PartitionActive},
			metadata:                 100,
			expected:                 true,
		},
		"current number of metadata is below the limit": {
			maxGlobalMetadataPerUser: 900,
			partitionStates:          []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			metadata:                 299,
			expected:                 true,
		},
		"current number of metadata is above the limit": {
			maxGlobalMetadataPerUser: 900,
			partitionStates:          []ring.PartitionState{ring.PartitionActive, ring.PartitionActive, ring.PartitionActive},
			metadata:                 300,
			expected:                 false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pr := buildPartitionRingFromPartitionStates(testData.partitionStates...)

			limits, err := validation.NewOverrides(validation.Limits{MaxGlobalMetricsWithMetadataPerUser: testData.maxGlobalMetadataPerUser}, nil)
			require.NoError(t, err)

			strategy := newPartitionRingLimiterStrategy(&partitionRingHolder{pr: pr}, limits.IngestionTenantShardSize)
			limiter := NewLimiter(limits, strategy)
			actual := limiter.IsWithinMaxMetricsWithMetadataPerUser("test", testData.metadata)

			assert.Equal(t, testData.expected, actual)
		})
	}
}

type ringCountMock struct {
	instancesCount       int
	instancesInZoneCount int
	zonesCount           int
}

func (m *ringCountMock) InstancesCount() int {
	return m.instancesCount
}

func (m *ringCountMock) InstancesInZoneCount(_ string) int {
	return m.instancesInZoneCount
}

func (m *ringCountMock) WritableInstancesWithTokensCount() int { return m.instancesCount }

func (m *ringCountMock) WritableInstancesWithTokensInZoneCount(_ string) int {
	return m.instancesInZoneCount
}

func (m *ringCountMock) ZonesCount() int {
	return m.zonesCount
}

type partitionRingHolder struct {
	pr *ring.PartitionRing
}

func (prh *partitionRingHolder) PartitionRing() *ring.PartitionRing {
	return prh.pr
}
