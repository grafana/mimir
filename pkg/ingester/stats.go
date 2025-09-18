// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/util"
)

// PlannerFactoryInterface defines the interface for creating planners
type PlannerFactoryInterface interface {
	CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

// TSDBInterface defines the interface for TSDB operations we need
type TSDBInterface interface {
	Head() HeadInterface
}

// tsdbAdapter adapts a Prometheus TSDB to our TSDBInterface
type tsdbAdapter struct {
	db *tsdb.DB
}

func (t *tsdbAdapter) Head() HeadInterface {
	return &headAdapter{head: t.db.Head()}
}

// headAdapter adapts a Prometheus TSDB Head to our HeadInterface
type headAdapter struct {
	head *tsdb.Head
}

func (h *headAdapter) ULID() ulid.ULID {
	// For head blocks, we get ULID from Meta()
	return h.head.Meta().ULID
}

func (h *headAdapter) IndexReader() (tsdb.IndexReader, error) {
	return h.head.Index()
}

func (h *headAdapter) BlockMeta() tsdb.BlockMeta {
	return h.head.Meta()
}

// HeadInterface defines the interface for TSDB head operations we need
type HeadInterface interface {
	ULID() ulid.ULID
	IndexReader() (tsdb.IndexReader, error)
	BlockMeta() tsdb.BlockMeta
}

// StatisticsService manages the background generation of statistics for head blocks.
// It periodically iterates through all user TSDBs and generates statistics for their head blocks,
// storing them in per-tenant repositories for later use during query planning.
type StatisticsService struct {
	logger         log.Logger
	plannerFactory PlannerFactoryInterface
	statsFrequency time.Duration
}

// NewStatisticsService creates a new StatisticsService.
func NewStatisticsService(logger log.Logger, plannerFactory PlannerFactoryInterface, statsFrequency time.Duration) *StatisticsService {
	return &StatisticsService{
		logger:         logger,
		plannerFactory: plannerFactory,
		statsFrequency: statsFrequency,
	}
}

// statsLoop runs the main statistics generation loop.
// It periodically calls getUserTSDBs to get all user TSDBs and processes each one sequentially.
func (s *StatisticsService) statsLoop(ctx context.Context, getUserTSDBs func() map[string]*userTSDB) error {
	// Skip if statistics frequency is not configured (0 duration)
	if s.statsFrequency <= 0 {
		level.Debug(s.logger).Log("msg", "statistics collection disabled (frequency not configured)")
		<-ctx.Done()
		return nil
	}

	statsTimer := time.NewTicker(util.DurationWithJitter(s.statsFrequency, 0.01))
	defer statsTimer.Stop()

	for {
		select {
		case <-statsTimer.C:
			s.generateStats(ctx, getUserTSDBs())

		case <-ctx.Done():
			return nil
		}
	}
}

// generateStats processes all user TSDBs and generates statistics for their head blocks.
func (s *StatisticsService) generateStats(ctx context.Context, userTSDBs map[string]*userTSDB) {
	if ctx.Err() != nil {
		return
	}

	for userID, userTSDB := range userTSDBs {
		if ctx.Err() != nil {
			return
		}

		if userTSDB == nil || userTSDB.db == nil || userTSDB.plannerRepo == nil {
			continue
		}

		s.generateStatsForUser(userID, &tsdbAdapter{db: userTSDB.db}, userTSDB.plannerRepo)
	}
}

// generateStatsForUser generates statistics for a single user's TSDB head block.
func (s *StatisticsService) generateStatsForUser(userID string, db TSDBInterface, repo lookupplan.PlannerRepository) {
	logger := log.With(s.logger, "user", userID)

	head := db.Head()
	blockULID := head.ULID()

	// Check if we already have a planner for this block
	if existingPlanner := repo.GetPlanner(blockULID); existingPlanner != nil {
		level.Debug(logger).Log("msg", "planner already exists for block", "block", blockULID.String())
		return
	}

	// Get index reader and block metadata
	indexReader, err := head.IndexReader()
	if err != nil {
		level.Warn(logger).Log("msg", "failed to get index reader for head block", "block", blockULID.String(), "err", err)
		return
	}
	defer func() {
		if closeErr := indexReader.Close(); closeErr != nil {
			level.Warn(logger).Log("msg", "failed to close index reader", "err", closeErr)
		}
	}()

	blockMeta := head.BlockMeta()

	// Generate planner using the factory
	level.Info(logger).Log("msg", "generating statistics for head block", "block", blockULID.String(), "series_count", blockMeta.Stats.NumSeries)
	planner := s.plannerFactory.CreatePlanner(blockMeta, indexReader)

	// Store the planner in the repository
	repo.StorePlanner(blockULID, planner)

	level.Info(logger).Log("msg", "stored planner for head block", "block", blockULID.String())
}
