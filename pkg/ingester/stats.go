// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// IPlannerFactory defines the interface for creating planners
type IPlannerFactory interface {
	CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

// StatisticsService manages the generation of statistics for a single tenant's head blocks.
// It stores pre-computed planners for later use during query planning.
type StatisticsService struct {
	logger         log.Logger
	plannerFactory IPlannerFactory

	// In-memory planner storage by block ULID
	mtx      sync.RWMutex
	planners map[ulid.ULID]index.LookupPlanner // map[blockULID] -> planner
}

// NewStatisticsService creates a new StatisticsService for a single tenant.
func NewStatisticsService(logger log.Logger, plannerFactory IPlannerFactory) *StatisticsService {
	return &StatisticsService{
		logger:         logger,
		plannerFactory: plannerFactory,
		planners:       make(map[ulid.ULID]index.LookupPlanner),
	}
}

// getPlanner returns a cached planner for the given block ULID.
// Returns nil if no planner is cached for this block.
func (s *StatisticsService) getPlanner(blockULID ulid.ULID) index.LookupPlanner {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.planners[blockULID]
}

// storePlanner stores a planner for the given block ULID.
func (s *StatisticsService) storePlanner(blockULID ulid.ULID, planner index.LookupPlanner) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.planners[blockULID] = planner
}

// generateStats generates statistics for the given head block metadata and index reader.
func (s *StatisticsService) generateStats(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) {
	blockULID := blockMeta.ULID

	// Generate planner using the factory
	level.Info(s.logger).Log("msg", "generating statistics for head block", "block", blockULID.String(), "series_count", blockMeta.Stats.NumSeries)
	planner := s.plannerFactory.CreatePlanner(blockMeta, indexReader)

	// Store the planner in our internal storage
	s.storePlanner(blockULID, planner)
}
