// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

// PlannerCreator creates a lookup planner for a TSDB block.
type PlannerCreator interface {
	CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner
}

// PlannerProvider manages planners for a single TSDB.
//
// Planners explicitly generated for mutable blocks, such as the Head, are
// cached and replaced as their statistics are refreshed. Planners for
// immutable blocks are created on demand and cached by Prometheus on the
// block's index reader.
type PlannerProvider struct {
	plannerFactory PlannerCreator

	plannersMtx sync.RWMutex
	planners    map[ulid.ULID]index.LookupPlanner
}

// NewPlannerProvider creates a PlannerProvider.
func NewPlannerProvider(plannerFactory PlannerCreator) *PlannerProvider {
	return &PlannerProvider{
		plannerFactory: plannerFactory,
		planners:       make(map[ulid.ULID]index.LookupPlanner),
	}
}

// GetPlanner returns a cached planner or creates one on demand.
//
// A planner created on demand is not stored because the block may be deleted.
// GenerateAndStorePlanner is used for mutable blocks whose planners need
// periodic refreshes.
func (p *PlannerProvider) GetPlanner(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) index.LookupPlanner {
	p.plannersMtx.RLock()
	planner, ok := p.planners[blockMeta.ULID]
	p.plannersMtx.RUnlock()
	if ok {
		return planner
	}

	return p.plannerFactory.CreatePlanner(blockMeta, indexReader)
}

// GenerateAndStorePlanner generates and stores a planner for a block.
func (p *PlannerProvider) GenerateAndStorePlanner(blockMeta tsdb.BlockMeta, indexReader tsdb.IndexReader) {
	planner := p.plannerFactory.CreatePlanner(blockMeta, indexReader)

	p.plannersMtx.Lock()
	defer p.plannersMtx.Unlock()
	p.planners[blockMeta.ULID] = planner
}
