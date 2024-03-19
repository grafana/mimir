// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
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

// ownedSeriesRingStrategy wraps access to the ring, to allow owned series service to be ignorant to whether it uses ingester ring or partitions ring.
type ownedSeriesRingStrategy interface {
	// checkRingForChanges reads current ring, stores it, and returns bool indicating whether ring has changed since last call
	// of this method in such a way that new recomputation of token ranges is needed.
	checkRingForChanges() (bool, error)

	// shardSizeForUser returns shard size used by tenant. Size can be number of ingesters or partitions.
	shardSizeForUser(tenant string) int

	// tokenRangesForUser returns token ranges owned by this ingester for given user.
	// If ingester doesn't own the tenant anymore, it should return nil tokens and no error.
	tokenRangesForUser(userID string, shardSize int) (ring.TokenRanges, error)

	// ownerKeyAndValue returns key and value used in log message to indicate "object" that owned series operates on.
	ownerKeyAndValue() (string, string)
}

type ownedSeriesService struct {
	services.Service

	logger       log.Logger
	ringStrategy ownedSeriesRingStrategy

	getLocalSeriesLimit func(user string, minLocalLimit int) int
	getTSDBUsers        func() []string
	getTSDB             func(user string) *userTSDB

	ownedSeriesCheckDuration prometheus.Histogram

	interval                  time.Duration
	initialRingCheckSucceeded bool
}

func newOwnedSeriesService(
	interval time.Duration,
	ringStrategy ownedSeriesRingStrategy,
	logger log.Logger,
	reg prometheus.Registerer,
	getLocalSeriesLimit func(user string, minLocalLimit int) int,
	getTSDBUsers func() []string,
	getTSDB func(user string) *userTSDB,
) *ownedSeriesService {
	oss := &ownedSeriesService{
		logger:              logger,
		ringStrategy:        ringStrategy,
		getLocalSeriesLimit: getLocalSeriesLimit,
		getTSDBUsers:        getTSDBUsers,
		getTSDB:             getTSDB,
		interval:            interval,
		ownedSeriesCheckDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ingester_owned_series_check_duration_seconds",
			Help:    "How long does it take to check for owned series for all users.",
			Buckets: prometheus.DefBuckets,
		}),
	}

	oss.Service = services.NewBasicService(oss.starting, oss.running, nil)
	return oss
}

// This method should run as fast as possible and avoid blocking on external conditions
// (e.g. whether lifecycler added instance to the ring or not),
// because it is started before lifecyclers.
func (oss *ownedSeriesService) starting(ctx context.Context) error {
	// Fetch and cache current state of the ring.
	_, err := oss.ringStrategy.checkRingForChanges()
	if err != nil {
		if errors.Is(err, ring.ErrEmptyRing) {
			level.Warn(oss.logger).Log("msg", "skipped initial owned series computation, ring is empty")
			oss.initialRingCheckSucceeded = false
			// Service will continue in this case.
		} else {
			return fmt.Errorf("can't read ring: %v", err)
		}
	} else {
		oss.initialRingCheckSucceeded = true

		// Check all tenants, regardless of whether instance exists in the ring or not.
		// This runs after TSDBs are open, all tenants should have "new user" reason set.
		_ = oss.updateAllTenants(ctx, true)
	}
	return nil
}

// Running function of owned series service. It regularly checks if ring has changed, and updates number of owned series for any
// user that requires it (due to ring change, compaction, shard size change, ...).
func (oss *ownedSeriesService) running(ctx context.Context) error {
	tickerInterval := oss.interval
	if !oss.initialRingCheckSucceeded {
		tickerInterval = 100 * time.Millisecond // Use short interval until we find non-empty ring.
	}

	t := time.NewTicker(tickerInterval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			ringChanged, err := oss.ringStrategy.checkRingForChanges()
			if err != nil {
				level.Error(oss.logger).Log("msg", "can't check ring for updates", "err", err)
				continue
			}

			// Ring check succeeded. If we still use short interval for ticker, reset it to regular interval.
			if tickerInterval != oss.interval {
				tickerInterval = oss.interval
				t.Reset(tickerInterval)
			}

			oss.updateAllTenants(ctx, ringChanged)

		case <-ctx.Done():
			return nil
		}
	}
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
	shardSize := oss.ringStrategy.shardSizeForUser(userID)
	localLimit := oss.getLocalSeriesLimit(userID, 0)

	reason := db.getAndClearReasonForRecomputeOwnedSeries() // Clear reason, so that other reasons can be set while we run update here.

	if reason == "" {
		os := db.ownedSeriesState()

		// Check if shard size or local limit has changed
		if shardSize != os.shardSize {
			reason = recomputeOwnedSeriesReasonShardSizeChanged
		} else if localLimit != os.localSeriesLimit {
			reason = recomputeOwnedSeriesReasonLocalLimitChanged
		}
	}

	if !ringChanged && reason == "" {
		// Nothing to do for this tenant.
		return false
	}

	// We need to check for tokens even if ringChanged is false, because previous ring check may have failed.
	// If this ingester doesn't own the tenant anymore, ringStrategy is expected to return nil ranges. In that case there will be no "owned" series.
	ranges, err := oss.ringStrategy.tokenRangesForUser(userID, shardSize)
	if err != nil {
		ownerKey, ownerValue := oss.ringStrategy.ownerKeyAndValue()
		level.Error(oss.logger).Log("msg", "failed to get token ranges from user's subring", "user", userID, ownerKey, ownerValue, "err", err)

		// If we failed to get token ranges, set the new reason, to make sure we do the check in next iteration.
		if reason == "" {
			reason = recomputeOwnedSeriesReasonGetTokenRangesFailed
		}
		db.triggerRecomputeOwnedSeries(reason)
		return false
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

type ownedSeriesIngesterRingStrategy struct {
	instanceID    string
	ingestersRing ring.ReadRing

	getIngesterShardSize func(user string) int

	previousRing ring.ReplicationSet
}

func newOwnedSeriesIngesterRingStrategy(instanceID string, ingesterRing ring.ReadRing, getIngesterShardSize func(user string) int) *ownedSeriesIngesterRingStrategy {
	return &ownedSeriesIngesterRingStrategy{
		instanceID:           instanceID,
		ingestersRing:        ingesterRing,
		getIngesterShardSize: getIngesterShardSize,
	}
}

func (ir *ownedSeriesIngesterRingStrategy) ownerKeyAndValue() (string, string) {
	return "ingester", ir.instanceID
}

func (ir *ownedSeriesIngesterRingStrategy) checkRingForChanges() (bool, error) {
	rs, err := ir.ingestersRing.GetAllHealthy(ownedSeriesRingOp)
	if err != nil {
		return false, err
	}

	// Ignore state and IP address changes, since they have no impact on token distribution
	ringChanged := ring.HasReplicationSetChangedWithoutStateOrAddr(ir.previousRing, rs)
	ir.previousRing = rs
	return ringChanged, nil
}

func (ir *ownedSeriesIngesterRingStrategy) shardSizeForUser(userID string) int {
	return ir.getIngesterShardSize(userID)
}

func (ir *ownedSeriesIngesterRingStrategy) tokenRangesForUser(userID string, shardSize int) (ring.TokenRanges, error) {
	subring := ir.ingestersRing.ShuffleShard(userID, shardSize)

	ranges, err := subring.GetTokenRangesForInstance(ir.instanceID)
	if errors.Is(err, ring.ErrInstanceNotFound) {
		// Not an error because it means the ingester doesn't own the tenant.
		return nil, nil
	}

	return ranges, err
}

type ownedSeriesPartitionRingStrategy struct {
	partitionID          int32
	partitionRingWatcher *ring.PartitionRingWatcher

	getPartitionShardSize func(user string) int

	previousActivePartitions []int32
}

func newOwnedSeriesPartitionRingStrategy(partitionID int32, partitionRing *ring.PartitionRingWatcher, getPartitionShardSize func(user string) int) *ownedSeriesPartitionRingStrategy {
	return &ownedSeriesPartitionRingStrategy{
		partitionID:           partitionID,
		partitionRingWatcher:  partitionRing,
		getPartitionShardSize: getPartitionShardSize,
	}
}

func (pr *ownedSeriesPartitionRingStrategy) checkRingForChanges() (bool, error) {
	// When using partitions ring, we consider ring to be changed if active partitions have changed.
	r := pr.partitionRingWatcher.PartitionRing()
	if r.PartitionsCount() == 0 {
		return false, ring.ErrEmptyRing
	}

	activePartitions := r.ActivePartitionIDs()
	ringChanged := !slices.Equal(pr.previousActivePartitions, activePartitions)
	pr.previousActivePartitions = activePartitions
	return ringChanged, nil
}

func (pr *ownedSeriesPartitionRingStrategy) shardSizeForUser(userID string) int {
	return pr.getPartitionShardSize(userID)
}

func (pr *ownedSeriesPartitionRingStrategy) tokenRangesForUser(userID string, shardSize int) (ring.TokenRanges, error) {
	r := pr.partitionRingWatcher.PartitionRing()
	sr, err := r.ShuffleShard(userID, shardSize)
	if err != nil {
		return nil, err
	}

	ranges, err := sr.GetTokenRangesForPartition(pr.partitionID)
	if errors.Is(err, ring.ErrPartitionDoesNotExist) {
		// Tenant doesn't use this partition.
		return nil, nil
	}
	return ranges, err
}

func (pr *ownedSeriesPartitionRingStrategy) ownerKeyAndValue() (string, string) {
	return "partition", strconv.Itoa(int(pr.partitionID))
}
