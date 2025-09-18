// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sync"

	"github.com/go-kit/log"
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

	plannersMtx sync.RWMutex
	planners    map[ulid.ULID]index.LookupPlanner
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
func (s *StatisticsService) getPlanner(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) index.LookupPlanner {
	s.plannersMtx.RLock()
	planner, ok := s.planners[blockMeta.ULID]
	s.plannersMtx.RUnlock()
	if ok {
		return planner
	}
	s.generateStats(blockMeta, indexReader)

	s.plannersMtx.RLock()
	planner, ok = s.planners[blockMeta.ULID]
	s.plannersMtx.RUnlock()
	if !ok {
		panic("planner should have been created after stats generation")
	}
	return planner

}

// storePlanner stores a planner for the given block ULID.
func (s *StatisticsService) storePlanner(blockULID ulid.ULID, planner index.LookupPlanner) {
	s.plannersMtx.Lock()
	defer s.plannersMtx.Unlock()

	s.planners[blockULID] = planner
}

// generateStats generates statistics for the given head block metadata and index reader.
func (s *StatisticsService) generateStats(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) {
	planner := s.plannerFactory.CreatePlanner(blockMeta, indexReader)
	s.storePlanner(blockMeta.ULID, planner)
}
