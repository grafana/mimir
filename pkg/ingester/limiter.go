// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"math"

	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

// limiterTenantLimits provides access to limits used by Limiter.
type limiterTenantLimits interface {
	MaxGlobalSeriesPerUser(userID string) int
	MaxGlobalSeriesPerMetric(userID string) int
	MaxGlobalMetadataPerMetric(userID string) int
	MaxGlobalMetricsWithMetadataPerUser(userID string) int
	MaxGlobalExemplarsPerUser(userID string) int
}

// Limiter implements primitives to get the maximum number of series, exemplars, metadata, etc.
// that an ingester can handle for a specific tenant
type Limiter struct {
	limits       limiterTenantLimits
	ringStrategy limiterRingStrategy
}

// NewLimiter makes a new in-memory series limiter
func NewLimiter(limits limiterTenantLimits, limiterRingSupport limiterRingStrategy) *Limiter {
	return &Limiter{
		limits:       limits,
		ringStrategy: limiterRingSupport,
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
func (l *Limiter) IsWithinMaxSeriesPerUser(userID string, series int, minLocalLimit int) bool {
	actualLimit := l.maxSeriesPerUser(userID, minLocalLimit)
	return series < actualLimit
}

// IsWithinMaxMetricsWithMetadataPerUser returns true if limit has not been reached compared to the current
// number of metrics with metadata in input; otherwise returns false.
func (l *Limiter) IsWithinMaxMetricsWithMetadataPerUser(userID string, metrics int) bool {
	actualLimit := l.maxMetadataPerUser(userID)
	return metrics < actualLimit
}

func (l *Limiter) maxSeriesPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalSeriesPerMetric, 0)
}

func (l *Limiter) maxMetadataPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalMetadataPerMetric, 0)
}

func (l *Limiter) maxSeriesPerUser(userID string, minLocalLimit int) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalSeriesPerUser, minLocalLimit)
}

func (l *Limiter) maxMetadataPerUser(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.limits.MaxGlobalMetricsWithMetadataPerUser, 0)
}

func (l *Limiter) maxExemplarsPerUser(userID string) int {
	globalLimit := l.limits.MaxGlobalExemplarsPerUser(userID)

	// We don't use `convertGlobalToLocalLimitOrUnlimited`, because we don't want "unlimited" part. 0 means disabled.
	localLimit := l.ringStrategy.convertGlobalToLocalLimit(userID, globalLimit)
	if localLimit > 0 {
		return localLimit
	}

	// The local limit could be 0 either because the global limit is 0 or because we haven't been able to compute
	// the local limit (e.g. the ring client sees no ingesters or partitions). In this case we fallback to the
	// global limit, which could either be 0 (disabled) or greater than 0 (enabled).
	return globalLimit
}

func (l *Limiter) convertGlobalToLocalLimitOrUnlimited(userID string, globalLimitFn func(string) int, minLocalLimit int) int {
	// We can assume that series/metadata are evenly distributed across ingesters
	globalLimit := globalLimitFn(userID)
	localLimit := l.ringStrategy.convertGlobalToLocalLimit(userID, globalLimit)

	// If the limit is disabled
	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return max(minLocalLimit, localLimit)
}

// limiterRingStrategy provides computations based on ingester or partitions ring.
type limiterRingStrategy interface {
	// convertGlobalToLocalLimit converts global limit to local, per-ingester limit, using given user's shard size (ingesters or partitions).
	convertGlobalToLocalLimit(userID string, globalLimit int) int

	// getShardSize returns shard size applicable for given ring.
	getShardSize(userID string) int
}

// ingesterRingLimiterRingCount is the interface exposed by a ring implementation which allows
// to count members
type ingesterRingLimiterRingCount interface {
	WritableInstancesWithTokensCount() int
	WritableInstancesWithTokensInZoneCount(zone string) int
	ZonesCount() int
}

type ingesterRingLimiterStrategy struct {
	ring                 ingesterRingLimiterRingCount
	replicationFactor    int
	zoneAwarenessEnabled bool
	ingesterZone         string

	getIngestionTenantShardSize func(userID string) int
}

func newIngesterRingLimiterStrategy(ring ingesterRingLimiterRingCount, replicationFactor int, zoneAwarenessEnabled bool, ingesterZone string, getIngestionTenantShardSize func(userID string) int) *ingesterRingLimiterStrategy {
	return &ingesterRingLimiterStrategy{
		ring:                        ring,
		replicationFactor:           replicationFactor,
		zoneAwarenessEnabled:        zoneAwarenessEnabled,
		ingesterZone:                ingesterZone,
		getIngestionTenantShardSize: getIngestionTenantShardSize,
	}
}

func (is *ingesterRingLimiterStrategy) convertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	zonesCount := is.getZonesCount()
	userShardSize := is.getShardSize(userID)

	var ingestersInZoneCount int
	if zonesCount > 1 {
		// In this case zone-aware replication is enabled, and ingestersInZoneCount is initially set to
		// the total number of writable ingesters with tokens in the corresponding zone
		ingestersInZoneCount = is.ring.WritableInstancesWithTokensInZoneCount(is.ingesterZone)
	} else {
		// In this case zone-aware replication is disabled, and ingestersInZoneCount is initially set to
		// the total number of writable ingesters with tokens
		ingestersInZoneCount = is.ring.WritableInstancesWithTokensCount()
	}
	// If shuffle sharding is enabled and the total number of ingesters in the zone is greater than the
	// expected number of ingesters per sharded zone, then we should honor the latter because series/metadata
	// cannot be written to more ingesters than that.
	if userShardSize > 0 {
		ingestersInZoneCount = min(ingestersInZoneCount, util.ShuffleShardExpectedInstancesPerZone(userShardSize, zonesCount))
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
	return int((float64(globalLimit*is.replicationFactor) / float64(zonesCount)) / float64(ingestersInZoneCount))
}

func (is *ingesterRingLimiterStrategy) getShardSize(userID string) int {
	return is.getIngestionTenantShardSize(userID)
}

func (is *ingesterRingLimiterStrategy) getZonesCount() int {
	if is.zoneAwarenessEnabled {
		return max(is.ring.ZonesCount(), 1)
	}
	return 1
}

// Interface for mocking.
type partitionRingWatcher interface {
	PartitionRing() *ring.PartitionRing
}

type partitionRingLimiterStrategy struct {
	partitionRingWatcher        partitionRingWatcher
	getPartitionTenantShardSize func(userID string) int
}

func newPartitionRingLimiterStrategy(watcher partitionRingWatcher, getPartitionTenantShardSize func(userID string) int) *partitionRingLimiterStrategy {
	return &partitionRingLimiterStrategy{
		partitionRingWatcher:        watcher,
		getPartitionTenantShardSize: getPartitionTenantShardSize,
	}
}

func (ps *partitionRingLimiterStrategy) convertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	userShardSize := ps.getShardSize(userID)

	pr := ps.partitionRingWatcher.PartitionRing()
	// ShuffleShardSize correctly handles cases when user has 0 or negative number of shards,
	// or more shards than number of active partitions in the ring.
	activePartitionsCount := pr.ShuffleShardSize(userShardSize)

	// If we haven't found any active partitions (e.g. partition was just added but this ingester hasn't seen it yet),
	// ignore global limit.
	if activePartitionsCount == 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return int(float64(globalLimit) / float64(activePartitionsCount))
}

func (ps *partitionRingLimiterStrategy) getShardSize(userID string) int {
	return ps.getPartitionTenantShardSize(userID)
}

type flusherLimiterStrategy struct{}

func (f flusherLimiterStrategy) convertGlobalToLocalLimit(_ string, _ int) int {
	return 0
}

func (f flusherLimiterStrategy) getShardSize(_ string) int {
	return 0
}
