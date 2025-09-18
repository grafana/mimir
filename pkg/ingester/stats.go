// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/lookupplan"
	"github.com/grafana/mimir/pkg/util"
)

// PlannerFactoryInterface defines the interface for creating planners
type PlannerFactoryInterface interface {
	CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

// TSDBProvider defines the interface for providing TSDB operations
type TSDBProvider interface {
	getTSDBUsers() []string
	openHeadBlock(userID string) (tsdb.BlockMeta, tsdb.IndexReader, lookupplan.PlannerRepository, error)
}

// StatisticsService manages the background generation of statistics for head blocks.
// It periodically iterates through all user TSDBs and generates statistics for their head blocks,
// storing them in per-tenant repositories for later use during query planning.
type StatisticsService struct {
	services.Service

	logger         log.Logger
	plannerFactory PlannerFactoryInterface
	tsdbProvider   TSDBProvider
}

// NewStatisticsService creates a new StatisticsService.
func NewStatisticsService(logger log.Logger, plannerFactory PlannerFactoryInterface, statsFrequency time.Duration, tsdbProvider TSDBProvider) *StatisticsService {
	s := &StatisticsService{
		logger:         logger,
		plannerFactory: plannerFactory,
		tsdbProvider:   tsdbProvider,
	}

	// Skip if statistics frequency is not configured (0 duration)
	if statsFrequency <= 0 {
		level.Debug(logger).Log("msg", "statistics collection disabled (frequency not configured)")
		s.Service = services.NewIdleService(nil, nil)
	} else {
		// Add jitter to avoid all ingesters running at the same time
		interval := util.DurationWithJitter(statsFrequency, 0.01)
		s.Service = services.NewTimerService(interval, nil, s.iteration, nil).WithName("statistics service")
	}

	return s
}

// iteration is called periodically by the timer service to generate statistics.
func (s *StatisticsService) iteration(ctx context.Context) error {
	s.generateStats(ctx)
	return nil
}

// generateStats processes all user TSDBs and generates statistics for their head blocks.
func (s *StatisticsService) generateStats(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	userIDs := s.tsdbProvider.getTSDBUsers()
	for _, userID := range userIDs {
		if ctx.Err() != nil {
			return
		}

		s.generateStatsForUser(userID)
	}
}

// generateStatsForUser generates statistics for a single user's TSDB head block.
func (s *StatisticsService) generateStatsForUser(userID string) {
	logger := log.With(s.logger, "user", userID)

	// Get block metadata, index reader, and repository from the provider
	blockMeta, indexReader, repo, err := s.tsdbProvider.openHeadBlock(userID)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to open head block", "err", err)
		return
	}
	defer func() {
		if closeErr := indexReader.Close(); closeErr != nil {
			level.Warn(logger).Log("msg", "failed to close index reader", "err", closeErr)
		}
	}()

	blockULID := blockMeta.ULID

	// Check if we already have a planner for this block
	if existingPlanner := repo.GetPlanner(blockULID); existingPlanner != nil {
		level.Debug(logger).Log("msg", "planner already exists for block", "block", blockULID.String())
		return
	}

	// Generate planner using the factory
	level.Info(logger).Log("msg", "generating statistics for head block", "block", blockULID.String(), "series_count", blockMeta.Stats.NumSeries)
	planner := s.plannerFactory.CreatePlanner(blockMeta, indexReader)

	// Store the planner in the repository
	repo.StorePlanner(blockULID, planner)

	level.Info(logger).Log("msg", "stored planner for head block", "block", blockULID.String())
}
