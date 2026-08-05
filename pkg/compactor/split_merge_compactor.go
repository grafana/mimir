// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/storage/seriesratestats"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func splitAndMergeGrouperFactory(_ context.Context, cfg Config, cfgProvider ConfigProvider, userID string, logger log.Logger, _ prometheus.Registerer) Grouper {
	return NewSplitAndMergeGrouper(
		userID,
		cfg.BlockRanges.ToMilliseconds(),
		cfgProvider,
		logger)
}

func splitAndMergeCompactorFactory(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (Compactor, Planner, error) {
	compactorOpts := tsdb.LeveledCompactorOptions{
		// Match the tsdb.NewLeveledCompactor default: the compactor merges overlapping blocks (vertical compaction).
		EnableOverlappingCompaction: true,
	}
	if cfg.GenerateSeriesRateStats {
		compactorOpts.SeriesStatsObserverFactory = seriesratestats.NewObserverFactory(seriesratestats.DefaultConfig(), logger)
	}

	compactor, err := tsdb.NewLeveledCompactorWithOptions(ctx, reg, util_log.SlogFromGoKit(logger), cfg.BlockRanges.ToMilliseconds(), nil, compactorOpts)
	if err != nil {
		return nil, nil, err
	}

	opts := tsdb.DefaultLeveledCompactorConcurrencyOptions()
	opts.MaxOpeningBlocks = cfg.MaxOpeningBlocksConcurrency
	opts.MaxClosingBlocks = cfg.MaxClosingBlocksConcurrency
	opts.SymbolsFlushersCount = cfg.SymbolsFlushersConcurrency

	compactor.SetConcurrencyOptions(opts)

	planner := NewSplitAndMergePlanner(cfg.BlockRanges.ToMilliseconds())
	return compactor, planner, nil
}

// configureSplitAndMergeCompactor updates the provided configuration injecting the split-and-merge compactor.
func configureSplitAndMergeCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = splitAndMergeGrouperFactory
	cfg.BlocksCompactorFactory = splitAndMergeCompactorFactory
}
