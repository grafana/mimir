// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"math"

	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

// RingCount is the interface exposed by a ring implementation which allows
// to count members
type RingCount interface {
	InstancesCount() int
	InstancesInZoneCount() int
	ZonesCount() int
}

// Limiter implements primitives to get the maximum number of series
// an ingester can handle for a specific tenant
type Limiter struct {
	limits               *validation.Overrides
	ring                 RingCount
	replicationFactor    int
	zoneAwarenessEnabled bool
}

// NewLimiter makes a new in-memory series limiter
func NewLimiter(
	limits *validation.Overrides,
	ring RingCount,
	replicationFactor int,
	zoneAwarenessEnabled bool,
) *Limiter {
	return &Limiter{
		limits:               limits,
		ring:                 ring,
		replicationFactor:    replicationFactor,
		zoneAwarenessEnabled: zoneAwarenessEnabled,
	}
}

// IsWithinMaxSeriesPerMetric returns true if limit has not been reached compared to the current
// number of series in input; otherwise returns false.
func (l *Limiter) IsWithinMaxSeriesPerMetric(userID string, series int) bool {
	actualLimit := l.maxSeriesPerMetric(userID)
	return series < actualLimit
}

// IsWithinMaxMetadataPerMetric returns true if limit has not been reached compared to the current
// number of metadata per metric in input; otherwise returns false.
func (l *Limiter) IsWithinMaxMetadataPerMetric(userID string, metadata int) bool {
	actualLimit := l.maxMetadataPerMetric(userID)
	return metadata < actualLimit
}

// IsWithinMaxSeriesPerUser returns true if limit has not been reached compared to the current
// number of series in input; otherwise returns false.
func (l *Limiter) IsWithinMaxSeriesPerUser(userID string, series int) bool {
	actualLimit := l.maxSeriesPerUser(userID)
	return series < actualLimit
}

// IsWithinMaxMetricsWithMetadataPerUser returns true if limit has not been reached compared to the current
// number of metrics with metadata in input; otherwise returns false.
func (l *Limiter) IsWithinMaxMetricsWithMetadataPerUser(userID string, metrics int) bool {
	actualLimit := l.maxMetadataPerUser(userID)
	return metrics < actualLimit
}

func (l *Limiter) maxSeriesPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalSeriesPerMetric)
}

func (l *Limiter) maxMetadataPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalMetadataPerMetric)
}

func (l *Limiter) maxSeriesPerUser(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalSeriesPerUser)
}

func (l *Limiter) maxMetadataPerUser(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalMetricsWithMetadataPerUser)
}

func (l *Limiter) convertGlobalToLocalLimitOrUnlimited(userID string, globalLimitFn func(string) int) int {
	// We can assume that series/metadata are evenly distributed across ingesters
	globalLimit := globalLimitFn(userID)
	localLimit := l.convertGlobalToLocalLimit(userID, globalLimit)

	// If the limit is disabled
	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return localLimit
}

func (l *Limiter) convertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	zonesCount := l.getZonesCount()
	var ingestersInZoneCount int
	if zonesCount > 1 {
		// In this case zone-aware replication is enabled, and ingestersInZoneCount is initially set to
		// the total number of ingesters in the corresponding zone
		ingestersInZoneCount = l.ring.InstancesInZoneCount()
	} else {
		// In this case zone-aware replication is disabled, and ingestersInZoneCount is initially set to
		// the total number of ingesters
		ingestersInZoneCount = l.ring.InstancesCount()
	}
	shardSize := l.getShardSize(userID)
	// If shuffle sharding is enabled and the total number of ingesters in the zone is greater than the
	// expected number of ingesters per sharded zone, then we should honor the latter because series/metadata
	// cannot be written to more ingesters than that.
	if shardSize > 0 {
		ingestersInZoneCount = util_math.Min(ingestersInZoneCount, util.ShuffleShardExpectedInstancesPerZone(shardSize, zonesCount))
	}

	// This may happen, for example when the total number of ingesters is asynchronously updated, or
	// when zone-aware replication is enabled but ingesters in a zone have been scaled down.
	// In those cases we ignore the global limit.
	if ingestersInZoneCount == 0 {
		return 0
	}

	// Global limit is equally distributed among all the active zones.
	// The portion of global limit related to each zone is then equally distributed
	// among all the ingesters belonging to that zone.
	return int((float64(globalLimit*l.replicationFactor) / float64(zonesCount)) / float64(ingestersInZoneCount))
}

func (l *Limiter) getShardSize(userID string) int {
	return l.limits.IngestionTenantShardSize(userID)
}

func (l *Limiter) getZonesCount() int {
	if l.zoneAwarenessEnabled {
		return util_math.Max(l.ring.ZonesCount(), 1)
	}
	return 1
}
