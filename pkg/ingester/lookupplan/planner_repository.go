// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
)

// PlannerRepositoryReader provides read-only access to cached planners.
type PlannerRepositoryReader interface {
	// GetPlanner returns a cached planner for the given block ULID.
	// Returns nil if no planner is cached for this block.
	GetPlanner(blockULID ulid.ULID) index.LookupPlanner
}

// PlannerRepository extends PlannerRepositoryReader with write capabilities.
type PlannerRepository interface {
	PlannerRepositoryReader
	// StorePlanner stores a planner for the given block ULID.
	StorePlanner(blockULID ulid.ULID, planner index.LookupPlanner)
}

// inMemoryPlannerRepository is a simple in-memory implementation of PlannerRepository.
// It is thread-safe and stores planners in a map keyed by block ULID.
type inMemoryPlannerRepository struct {
	mtx      sync.RWMutex
	planners map[ulid.ULID]index.LookupPlanner
}

// NewInMemoryPlannerRepository creates a new in-memory planner repository.
func NewInMemoryPlannerRepository() PlannerRepository {
	return &inMemoryPlannerRepository{
		planners: make(map[ulid.ULID]index.LookupPlanner),
	}
}

// GetPlanner returns a cached planner for the given block ULID.
// Returns nil if no planner is cached for this block.
func (r *inMemoryPlannerRepository) GetPlanner(blockULID ulid.ULID) index.LookupPlanner {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return r.planners[blockULID]
}

// StorePlanner stores a planner for the given block ULID.
func (r *inMemoryPlannerRepository) StorePlanner(blockULID ulid.ULID, planner index.LookupPlanner) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	r.planners[blockULID] = planner
}
