// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
)

const minSeriesPerBlockForQueryPlanning = 10_000

// PlannerFactory provides index lookup planners for TSDB blocks.
type PlannerFactory struct {
	metrics        Metrics
	logger         log.Logger
	statsGenerator *StatisticsGenerator
}

func NewPlannerFactory(metrics Metrics, logger log.Logger, statsGenerator *StatisticsGenerator) *PlannerFactory {
	return &PlannerFactory{
		metrics:        metrics,
		logger:         logger,
		statsGenerator: statsGenerator,
	}
}

// CreatePlanner returns an appropriate index.LookupPlanner for the given block metadata and reader.
// For very small blocks (< 10,000 series), it returns NoopPlanner to avoid planning overhead.
// For larger blocks, it generates statistics and returns a CostBasedPlanner.
// If statistics generation fails, it falls back to NoopPlanner.
func (p *PlannerFactory) CreatePlanner(meta tsdb.BlockMeta, reader tsdb.IndexReader) index.LookupPlanner {
	logger := log.With(p.logger, "block", meta.ULID.String(), "block_series", meta.Stats.NumSeries)

	if meta.Stats.NumSeries < minSeriesPerBlockForQueryPlanning {
		// For very small blocks, the planning overhead is likely not worth it.
		// This also prevents problems when we've gathered stats on an empty head block,
		// but then the ingester starts receiving series for that tenant.
		level.Debug(logger).Log("msg", "skipping query planning for small block", "planning_threshold_series", minSeriesPerBlockForQueryPlanning)
		return NoopPlanner{}
	}
	stats, err := p.statsGenerator.Stats(meta, reader)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to generate statistics; queries for this block won't use query planning", "err", err)
		return NoopPlanner{}
	}
	return NewCostBasedPlanner(p.metrics, stats)
}
