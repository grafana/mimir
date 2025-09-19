// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// iPlannerFactory defines the interface for creating planners
type iPlannerFactory interface {
	CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

// plannerProvider manages the generation of statistics for a single tenant's head blocks.
// It stores pre-computed planners for later use during query planning.
type plannerProvider struct {
	plannerFactory iPlannerFactory

	plannersMtx sync.RWMutex
	planners    map[ulid.ULID]index.LookupPlanner
}

// newPlannerProvider creates a new plannerProvider for a single tenant.
func newPlannerProvider(plannerFactory iPlannerFactory) *plannerProvider {
	return &plannerProvider{
		plannerFactory: plannerFactory,
		planners:       make(map[ulid.ULID]index.LookupPlanner),
	}
}

// getPlanner retrieves a planner for the given block metadata and index reader.
// If a planner is not found in the cache, it creates a new one using the planner factory.
// Note that it does not store the newly created planner in the cache; the caller is responsible for that if needed.
func (s *plannerProvider) getPlanner(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) index.LookupPlanner {
	s.plannersMtx.RLock()
	planner, ok := s.planners[blockMeta.ULID]
	s.plannersMtx.RUnlock()
	if ok {
		return planner
	}
	// Planner not found in cache, create a new one.
	// We do not store it here because sometimes the block it refers to may be deleted, and we don't want to retain planners for deleted blocks.
	// We rely on the caller of generateAndStorePlanner to create a planner and store it when needed.
	return s.plannerFactory.CreatePlanner(blockMeta, indexReader)
}

// storePlanner stores a planner for the given block ULID.
func (s *plannerProvider) storePlanner(blockULID ulid.ULID, planner index.LookupPlanner) {
	s.plannersMtx.Lock()
	defer s.plannersMtx.Unlock()

	s.planners[blockULID] = planner
}

// generateAndStorePlanner generates a planner for the given block metadata and index reader, and stores it in the cache.
func (s *plannerProvider) generateAndStorePlanner(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) {
	planner := s.plannerFactory.CreatePlanner(blockMeta, indexReader)
	s.storePlanner(blockMeta.ULID, planner)
}
