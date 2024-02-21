// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Distributor uses WriteNoExtend, but we include all instance states in our operation. Reason is that we want to detect changes to the ring
// to see if we need to recompute token ranges for this instance. Token ranges computation is independent of ring states. (It should depend on
// "replica extension", but WriteNoExtend doesn't use that.)
var ownedSeriesRingOp = ring.NewOp([]ring.InstanceState{ring.PENDING, ring.JOINING, ring.ACTIVE, ring.LEAVING}, nil)

const (
	recomputeOwnedSeriesReasonEarlyCompaction      = "early compaction"
	recomputeOwnedSeriesReasonCompaction           = "compaction"
	recomputeOwnedSeriesReasonNewUser              = "new user"
	recomputeOwnedSeriesReasonGetTokenRangesFailed = "token ranges check failed"
	recomputeOwnedSeriesReasonRingChanged          = "ring changed"
	recomputeOwnedSeriesReasonShardSizeChanged     = "shard size changed"
	recomputeOwnedSeriesReasonLocalLimitChanged    = "local series limit changed"
)

type ownedSeriesService struct {
	services.Service

	instanceID    string
	ingestersRing ring.ReadRing

	logger log.Logger

	getIngesterShardSize func(user string) int
	getLocalSeriesLimit  func(user string, minLocalLimit int) int
	getTSDBUsers         func() []string
	getTSDB              func(user string) *userTSDB

	ownedSeriesCheckDuration prometheus.Histogram

	previousRing ring.ReplicationSet
}

func newOwnedSeriesService(
	interval time.Duration,
	instanceID string,
	ingesterRing ring.ReadRing,
	logger log.Logger,
	reg prometheus.Registerer,
	getIngesterShardSize func(user string) int,
	getLocalSeriesLimit func(user string, minLocalLimit int) int,
	getTSDBUsers func() []string,
	getTSDB func(user string) *userTSDB,
) *ownedSeriesService {
	oss := &ownedSeriesService{
		instanceID:           instanceID,
		ingestersRing:        ingesterRing,
		logger:               logger,
		getIngesterShardSize: getIngesterShardSize,
		getLocalSeriesLimit:  getLocalSeriesLimit,
		getTSDBUsers:         getTSDBUsers,
		getTSDB:              getTSDB,
		ownedSeriesCheckDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_owned_series_check_duration_seconds",
			Help:    "How long does it take to check for owned series for all users.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	oss.Service = services.NewTimerService(interval, nil, oss.onPeriodicCheck, nil)
	return oss
}

// This function runs periodically. It checks if ring has changed, and updates number of owned series for any
// user that requires it (due to ring change, compaction, shard size change, ...).
func (oss *ownedSeriesService) onPeriodicCheck(ctx context.Context) error {
	ringChanged, err := oss.checkRingForChanges()
	if err != nil {
		level.Error(oss.logger).Log("msg", "can't check ring for updates", "err", err)
		return nil // If we returned error, service would stop.
	}

	oss.updateAllTenants(ctx, ringChanged)
	return nil
}

// Reads current ring, stores it, and returns bool indicating whether ring has changed since last call of this method.
func (oss *ownedSeriesService) checkRingForChanges() (bool, error) {
	rs, err := oss.ingestersRing.GetAllHealthy(ownedSeriesRingOp)
	if err != nil {
		return false, err
	}

	// Ignore state and IP address changes, since they have no impact on token distribution
	ringChanged := ring.HasReplicationSetChangedWithoutStateOrAddr(oss.previousRing, rs)
	oss.previousRing = rs
	return ringChanged, nil
}

// updateAllTenants iterates over all open TSDBs and updates owned series for all users that need it, either
// because of external trigger (new user, compaction), or because of changed token ranges.
func (oss *ownedSeriesService) updateAllTenants(ctx context.Context, ringChanged bool) int {
	updatedUsers := 0

	start := time.Now()
	for _, userID := range oss.getTSDBUsers() {
		if ctx.Err() != nil {
			return updatedUsers
		}

		db := oss.getTSDB(userID)
		if db == nil {
			continue
		}

		if oss.updateTenant(userID, db, ringChanged) {
			updatedUsers++
		}
	}
	elapsed := time.Since(start)

	if updatedUsers > 0 {
		level.Info(oss.logger).Log("msg", "updated owned series for users", "updatedUsers", updatedUsers, "duration", elapsed, "ringChanged", ringChanged)
	}
	oss.ownedSeriesCheckDuration.Observe(elapsed.Seconds())

	return updatedUsers
}

// Updates token ranges and recomputes owned series for user, if necessary. If recomputation happened, true is returned.
//
// This method is complicated, because it takes many possible scenarios into consideration:
// 1. Ring changed
// 2. Shard size changed
// 3. Local limit changed
// 4. Previous ring check failed [stored as reason]
// 5. Previous computation of owned series failed [stored as reason]
// 6. Other reasons for check and recomputation (new TSDB, compaction)
//
// Ring and shard size changes require new check of the ring to see if token ranges for this ingester have changed. We also need to check ring if previous ring check has failed.
// When doing computation of owned series, we make sure to pass up-to-date number of shards.
func (oss *ownedSeriesService) updateTenant(userID string, db *userTSDB, ringChanged bool) bool {
	shardSize := oss.getIngesterShardSize(userID)
	localLimit := oss.getLocalSeriesLimit(userID, 0)

	reason := db.getAndClearReasonForRecomputeOwnedSeries() // Clear reason, so that other reasons can be set while we run update here.

	if reason == "" {
		os := db.ownedSeriesState()

		// Check if shard size or local limit has changed
		if shardSize != os.shardSize {
			reason = recomputeOwnedSeriesReasonShardSizeChanged
		} else if localLimit != os.localLimit {
			reason = recomputeOwnedSeriesReasonLocalLimitChanged
		}
	}

	if !ringChanged && reason == "" {
		// Nothing to do for this tenant.
		return false
	}

	// We need to check for tokens even if ringChanged is false, because previous ring check may have failed.
	subring := oss.ingestersRing.ShuffleShard(userID, shardSize)

	ranges, err := subring.GetTokenRangesForInstance(oss.instanceID)
	if err != nil {
		if errors.Is(err, ring.ErrInstanceNotFound) {
			// This ingester doesn't own the tenant anymore, so there will be no "owned" series.
			ranges = nil
		} else {
			level.Error(oss.logger).Log("msg", "failed to get token ranges from user's subring", "user", userID, "ingester", oss.instanceID, "err", err)

			// If we failed to get token ranges, set the new reason, to make sure we do the check in next iteration.
			if reason == "" {
				reason = recomputeOwnedSeriesReasonGetTokenRangesFailed
			}
			db.triggerRecomputeOwnedSeries(reason)
			return false
		}
	}

	if db.updateTokenRanges(ranges) && reason == "" {
		reason = recomputeOwnedSeriesReasonRingChanged
	}

	if reason != "" {
		if !db.recomputeOwnedSeries(shardSize, reason, oss.logger) {
			db.triggerRecomputeOwnedSeries(reason)
		}
		return true
	}
	return false
}

func secondaryTSDBHashFunctionForUser(userID string) func(labels.Labels) uint32 {
	return func(ls labels.Labels) uint32 {
		return mimirpb.ShardByAllLabels(userID, ls)
	}
}
