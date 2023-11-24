// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
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
	recomputeOwnedSeriesReasonGetTokenRangesFailed = "token ranges failed"
	recomputeOwnedSeriesReasonUpdateFailed         = "update failed"
	recomputeOwnedSeriesReasonRingChanged          = "ring changed"
	recomputeOwnedSeriesReasonShardSizeChanged     = "shard size changed"
)

type ownedSeriesService struct {
	services.Service

	instanceID    string
	ingestersRing ring.ReadRing

	logger log.Logger

	getIngesterShardSize func(user string) int
	getTSDBUsers         func() []string
	getTSDB              func(user string) *userTSDB

	previousRing ring.ReplicationSet
}

func newOwnedSeriesService(interval time.Duration, instanceID string, ingesterRing ring.ReadRing, logger log.Logger, getIngesterShardSize func(user string) int, getTSDBUsers func() []string, getTSDB func(user string) *userTSDB) *ownedSeriesService {
	oss := &ownedSeriesService{
		instanceID:           instanceID,
		ingestersRing:        ingesterRing,
		logger:               logger,
		getIngesterShardSize: getIngesterShardSize,
		getTSDBUsers:         getTSDBUsers,
		getTSDB:              getTSDB,
	}

	oss.Service = services.NewTimerService(interval, oss.starting, oss.iter, nil)
	return oss
}

// This is Starting function for ownedSeries service. Service is only started after all TSDBs are opened.
// Pushes are not allowed yet when this function runs.
func (oss *ownedSeriesService) starting(ctx context.Context) error {
	err := ring.WaitInstanceState(ctx, oss.ingestersRing, oss.instanceID, ring.ACTIVE)
	if err != nil {
		return err
	}

	if _, err := oss.checkRingForChanges(); err != nil {
		return fmt.Errorf("can't read ring: %v", err)
	}

	// We pass ringChanged=true, but all TSDBs at this point (after opening TSDBs, but before ingester switched to Running state) also have "new user" trigger set anyway.
	oss.updateAll(ctx, true)
	return nil
}

// This function runs periodically. It checks if ring has changed, and updates number of owned series for any
// user that requires it (due to ring change, compaction, shard size change, ...).
func (oss *ownedSeriesService) iter(ctx context.Context) error {
	ringChanged, err := oss.checkRingForChanges()
	if err != nil {
		level.Error(oss.logger).Log("msg", "can't check ring for updates", "err", err)
		return nil // If we returned error, service would stop.
	}

	start := time.Now()
	updatedUsers := oss.updateAll(ctx, ringChanged)
	if updatedUsers > 0 {
		level.Info(oss.logger).Log("msg", "updated owned series for users", "updatedUsers", updatedUsers, "duration", time.Since(start), "ringChanged", ringChanged)
	}
	return nil
}

// Reads current ring, stores it, and returns bool indicating whether ring has changed since last call of this method.
func (oss *ownedSeriesService) checkRingForChanges() (bool, error) {
	rs, err := oss.ingestersRing.GetAllHealthy(ownedSeriesRingOp)
	if err != nil {
		return false, err
	}

	// Since token ranges computation doesn't care about state, we don't need to either.
	ringChanged := ring.HasReplicationSetChangedWithoutState(oss.previousRing, rs)
	oss.previousRing = rs
	return ringChanged, nil
}

// updateAll iterates over all open TSDBs and updates owned series for all users that need it, either
// because of external trigger (new user, compaction), or because of changed token ranges.
func (oss *ownedSeriesService) updateAll(ctx context.Context, ringChanged bool) int {
	updatedUsers := 0
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
	return updatedUsers
}

// Updates token ranges and recomputes owned series for user, if necessary. If recomputation happened, true is returned.
func (oss *ownedSeriesService) updateTenant(userID string, db *userTSDB, ringChanged bool) bool {
	shardSize := oss.getIngesterShardSize(userID)

	reason := db.requiresOwnedSeriesUpdate.Swap("") // Clear reason, so that other reasons can be set while we run update here.
	if reason == "" {
		_, ownedShardSize := db.OwnedSeriesAndShards()
		if shardSize != ownedShardSize {
			reason = recomputeOwnedSeriesReasonShardSizeChanged
		}
	}

	if !ringChanged && reason == "" {
		// Nothing to do for this tenant.
		return false
	}

	// We need to check for tokens even if ringChanged is false, because we may be here because of previous ring check failure.
	subring := oss.ingestersRing.ShuffleShard(userID, shardSize)

	ranges, err := subring.GetTokenRangesForInstance(oss.instanceID)
	if err != nil {
		if errors.Is(err, ring.ErrInstanceNotFound) {
			// This ingester doesn't own the tenant anymore, so there will be no "owned" series.
			ranges = nil
		} else {
			level.Error(oss.logger).Log("msg", "failed to get token ranges from user's subring", "user", userID, "ingester", oss.instanceID, "err", err)

			// If we failed to get token ranges, set the new reason, to make sure we do the check in next iteration.
			db.TriggerRecomputeOwnedSeries(recomputeOwnedSeriesReasonGetTokenRangesFailed)
			return false
		}
	}

	if db.UpdateTokenRanges(ranges) && reason == "" {
		reason = recomputeOwnedSeriesReasonRingChanged
	}

	if reason != "" {
		if db.RecomputeOwnedSeries(shardSize, reason, oss.logger) {
			db.TriggerRecomputeOwnedSeries(recomputeOwnedSeriesReasonUpdateFailed)
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
