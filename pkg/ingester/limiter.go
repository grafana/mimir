// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"fmt"
	"math"

	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	// These errors are only internal, to change the API error messages, see Limiter's methods below.
	errMaxSeriesPerMetricLimitExceeded   = errors.New("per-metric series limit exceeded")
	errMaxMetadataPerMetricLimitExceeded = errors.New("per-metric metadata limit exceeded")
	errMaxSeriesPerUserLimitExceeded     = errors.New("per-user series limit exceeded")
	errMaxMetadataPerUserLimitExceeded   = errors.New("per-user metric metadata limit exceeded")
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
	ingestionShardsForTenant map[string]int
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
		ingestionShardsForTenant: make(map[string]int),
	}
}

// AssertMaxSeriesPerMetric limit has not been reached compared to the current
// number of series in input and returns an error if so.
func (l *Limiter) AssertMaxSeriesPerMetric(userID string, series int) error {
	if actualLimit := l.maxSeriesPerMetric(userID); series < actualLimit {
		return nil
	}

	return errMaxSeriesPerMetricLimitExceeded
}

// AssertMaxMetadataPerMetric limit has not been reached compared to the current
// number of metadata per metric in input and returns an error if so.
func (l *Limiter) AssertMaxMetadataPerMetric(userID string, metadata int) error {
	if actualLimit := l.maxMetadataPerMetric(userID); metadata < actualLimit {
		return nil
	}

	return errMaxMetadataPerMetricLimitExceeded
}

// AssertMaxSeriesPerUser limit has not been reached compared to the current
// number of series in input and returns an error if so.
func (l *Limiter) AssertMaxSeriesPerUser(userID string, series int) error {
	if actualLimit := l.maxSeriesPerUser(userID); series < actualLimit {
		return nil
	}

	return errMaxSeriesPerUserLimitExceeded
}

// AssertMaxMetricsWithMetadataPerUser limit has not been reached compared to the current
// number of metrics with metadata in input and returns an error if so.
func (l *Limiter) AssertMaxMetricsWithMetadataPerUser(userID string, metrics int) error {
	if actualLimit := l.maxMetadataPerUser(userID); metrics < actualLimit {
		return nil
	}

	return errMaxMetadataPerUserLimitExceeded
}

// FormatError returns the input error enriched with the actual limits for the given user.
// It acts as pass-through if the input error is unknown.
func (l *Limiter) FormatError(userID string, err error) error {
	//nolint:errorlint // We don't expect wrapped errors.
	switch err {
	case errMaxSeriesPerUserLimitExceeded:
		return l.formatMaxSeriesPerUserError(userID)
	case errMaxSeriesPerMetricLimitExceeded:
		return l.formatMaxSeriesPerMetricError(userID)
	case errMaxMetadataPerUserLimitExceeded:
		return l.formatMaxMetadataPerUserError(userID)
	case errMaxMetadataPerMetricLimitExceeded:
		return l.formatMaxMetadataPerMetricError(userID)
	default:
		return err
	}
}

func (l *Limiter) formatMaxSeriesPerUserError(userID string) error {
	globalLimit := l.limits.MaxGlobalSeriesPerUser(userID)

	return errors.New(globalerror.MaxSeriesPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user series limit of %d exceeded", globalLimit),
		validation.MaxSeriesPerUserFlag,
	))
}

func (l *Limiter) formatMaxSeriesPerMetricError(userID string) error {
	globalLimit := l.limits.MaxGlobalSeriesPerMetric(userID)

	return errors.New(globalerror.MaxSeriesPerMetric.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-metric series limit of %d exceeded", globalLimit),
		validation.MaxSeriesPerMetricFlag,
	))
}

func (l *Limiter) formatMaxMetadataPerUserError(userID string) error {
	globalLimit := l.limits.MaxGlobalMetricsWithMetadataPerUser(userID)

	return errors.New(globalerror.MaxMetadataPerUser.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-user metric metadata limit of %d exceeded", globalLimit),
		validation.MaxMetadataPerUserFlag,
	))
}

func (l *Limiter) formatMaxMetadataPerMetricError(userID string) error {
	globalLimit := l.limits.MaxGlobalMetadataPerMetric(userID)

	return errors.New(globalerror.MaxMetadataPerMetric.MessageWithPerTenantLimitConfig(
		fmt.Sprintf("per-metric metadata limit of %d exceeded", globalLimit),
		validation.MaxMetadataPerMetricFlag,
	))
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
	if shards, ok := l.ingestionShardsForTenant[userID] ; ok {
		return shards
	} else {
		configShards := l.limits.IngestionTenantShardSize(userID)
		l.ingestionShardsForTenant[userID] = configShards
		return configShards
	}
}

func (l *Limiter) getZonesCount() int {
	if l.zoneAwarenessEnabled {
		return util_math.Max(l.ring.ZonesCount(), 1)
	}
	return 1
}

func (l *Limiter) ClearShardsForTenant(){
	l.ingestionShardsForTenant = make(map[string]int)
}
