// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingesterlimiter

import (
	"math"

	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
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
	// TODO @oleg: Limits shouldn't be exported, whoever has built the Limiter, can pass those Limits to the same entity.
	Limits       limiterTenantLimits
	ringStrategy LimiterRingStrategy
}

// NewLimiter makes a new in-memory series limiter
func NewLimiter(limits limiterTenantLimits, limiterRingSupport LimiterRingStrategy) *Limiter {
	return &Limiter{
		Limits:       limits,
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
	actualLimit := l.MaxSeriesPerUser(userID, minLocalLimit)
	return series < actualLimit
}

// IsWithinMaxMetricsWithMetadataPerUser returns true if limit has not been reached compared to the current
// number of metrics with metadata in input; otherwise returns false.
func (l *Limiter) IsWithinMaxMetricsWithMetadataPerUser(userID string, metrics int) bool {
	actualLimit := l.maxMetadataPerUser(userID)
	return metrics < actualLimit
}

func (l *Limiter) maxSeriesPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.Limits.MaxGlobalSeriesPerMetric, 0)
}

func (l *Limiter) maxMetadataPerMetric(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.Limits.MaxGlobalMetadataPerMetric, 0)
}

func (l *Limiter) MaxSeriesPerUser(userID string, minLocalLimit int) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.Limits.MaxGlobalSeriesPerUser, minLocalLimit)
}

func (l *Limiter) maxMetadataPerUser(userID string) int {
	return l.convertGlobalToLocalLimitOrUnlimited(userID, l.Limits.MaxGlobalMetricsWithMetadataPerUser, 0)
}

func (l *Limiter) MaxExemplarsPerUser(userID string) int {
	globalLimit := l.Limits.MaxGlobalExemplarsPerUser(userID)

	// We don't use `convertGlobalToLocalLimitOrUnlimited`, because we don't want "unlimited" part. 0 means disabled.
	localLimit := l.ringStrategy.ConvertGlobalToLocalLimit(userID, globalLimit)
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
	localLimit := l.ringStrategy.ConvertGlobalToLocalLimit(userID, globalLimit)

	// If the limit is disabled
	if localLimit == 0 {
		localLimit = math.MaxInt32
	}

	return max(minLocalLimit, localLimit)
}

// LimiterRingStrategy provides computations based on ingester or partitions ring.
type LimiterRingStrategy interface {
	// ConvertGlobalToLocalLimit converts global limit to local, per-ingester limit, using given user's shard size (ingesters or partitions).
	ConvertGlobalToLocalLimit(userID string, globalLimit int) int

	// GetShardSize returns shard size applicable for given ring.
	GetShardSize(userID string) int
}

// ingesterRingLimiterRingCount is the interface exposed by a ring implementation which allows
// to count members
type ingesterRingLimiterRingCount interface {
	InstancesWithTokensCount() int
	InstancesWithTokensInZoneCount(zone string) int
	ZonesCount() int
}

type IngesterRingLimiterStrategy struct {
	ring                 ingesterRingLimiterRingCount
	replicationFactor    int
	zoneAwarenessEnabled bool
	ingesterZone         string

	getIngestionTenantShardSize func(userID string) int
}

func NewIngesterRingLimiterStrategy(ring ingesterRingLimiterRingCount, replicationFactor int, zoneAwarenessEnabled bool, ingesterZone string, getIngestionTenantShardSize func(userID string) int) *IngesterRingLimiterStrategy {
	return &IngesterRingLimiterStrategy{
		ring:                        ring,
		replicationFactor:           replicationFactor,
		zoneAwarenessEnabled:        zoneAwarenessEnabled,
		ingesterZone:                ingesterZone,
		getIngestionTenantShardSize: getIngestionTenantShardSize,
	}
}

func (is *IngesterRingLimiterStrategy) ConvertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	zonesCount := is.getZonesCount()
	userShardSize := is.GetShardSize(userID)

	var ingestersInZoneCount int
	if zonesCount > 1 {
		// In this case zone-aware replication is enabled, and ingestersInZoneCount is initially set to
		// the total number of ingesters with tokens in the corresponding zone
		ingestersInZoneCount = is.ring.InstancesWithTokensInZoneCount(is.ingesterZone)
	} else {
		// In this case zone-aware replication is disabled, and ingestersInZoneCount is initially set to
		// the total number of ingesters with tokens
		ingestersInZoneCount = is.ring.InstancesWithTokensCount()
	}
	// If shuffle sharding is enabled and the total number of ingesters in the zone is greater than the
	// expected number of ingesters per sharded zone, then we should honor the latter because series/metadata
	// cannot be written to more ingesters than that.
	if userShardSize > 0 {
		ingestersInZoneCount = util_math.Min(ingestersInZoneCount, util.ShuffleShardExpectedInstancesPerZone(userShardSize, zonesCount))
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

func (is *IngesterRingLimiterStrategy) GetShardSize(userID string) int {
	return is.getIngestionTenantShardSize(userID)
}

func (is *IngesterRingLimiterStrategy) getZonesCount() int {
	if is.zoneAwarenessEnabled {
		return util_math.Max(is.ring.ZonesCount(), 1)
	}
	return 1
}

// Interface for mocking.
type partitionRingWatcher interface {
	PartitionRing() *ring.PartitionRing
}

type PartitionRingLimiterStrategy struct {
	partitionRingWatcher        partitionRingWatcher
	getPartitionTenantShardSize func(userID string) int
}

func NewPartitionRingLimiterStrategy(watcher partitionRingWatcher, getPartitionTenantShardSize func(userID string) int) *PartitionRingLimiterStrategy {
	return &PartitionRingLimiterStrategy{
		partitionRingWatcher:        watcher,
		getPartitionTenantShardSize: getPartitionTenantShardSize,
	}
}

func (ps *PartitionRingLimiterStrategy) ConvertGlobalToLocalLimit(userID string, globalLimit int) int {
	if globalLimit == 0 {
		return 0
	}

	userShardSize := ps.GetShardSize(userID)

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

func (ps *PartitionRingLimiterStrategy) GetShardSize(userID string) int {
	return ps.getPartitionTenantShardSize(userID)
}

type FlusherLimiterStrategy struct{}

func (f FlusherLimiterStrategy) ConvertGlobalToLocalLimit(_ string, _ int) int { return 0 }
func (f FlusherLimiterStrategy) GetShardSize(_ string) int                     { return 0 }
