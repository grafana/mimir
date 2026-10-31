// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

func splitAndMergeGrouperFactory(_ context.Context, cfg Config, cfgProvider ConfigProvider, userID string, logger log.Logger, _ prometheus.Registerer) Grouper {
	return NewSplitAndMergeGrouper(
		userID,
		cfg.BlockRanges.ToMilliseconds(),
		cfgProvider,
		logger)
}

func splitAndMergeCompactorFactory(ctx context.Context, cfg Config, cfgProvider ConfigProvider, logger log.Logger, reg prometheus.Registerer) (BlocksCompactorProvider, Planner, error) {
	blockRanges := cfg.BlockRanges.ToMilliseconds()

	concurrencyOpts := tsdb.DefaultLeveledCompactorConcurrencyOptions()
	concurrencyOpts.MaxOpeningBlocks = cfg.MaxOpeningBlocksConcurrency
	concurrencyOpts.MaxClosingBlocks = cfg.MaxClosingBlocksConcurrency
	concurrencyOpts.SymbolsFlushersCount = cfg.SymbolsFlushersConcurrency

	// The metrics are built once and shared by every compactor: registering them twice would panic,
	// and they are aggregated across tenants anyway.
	metrics := tsdb.NewCompactorMetrics(reg)

	// The encoding reaches the merge function through a callback taking no tenant, and jobs for
	// different tenants compact concurrently, so we build one compactor per encoding.
	compactors := make(map[chunkenc.Encoding]Compactor, len(validation.FloatChunkEncodingValues))
	for _, value := range validation.FloatChunkEncodingValues {
		enc := validation.ParseFloatChunkEncoding(value)
		compactor, err := tsdb.NewLeveledCompactorWithOptions(ctx, nil, util_log.SlogFromGoKit(logger), blockRanges, nil, tsdb.LeveledCompactorOptions{
			Metrics:            metrics,
			FloatChunkEncoding: func() chunkenc.Encoding { return enc },
			// Inert here, since Mimir plans compaction itself, but NewLeveledCompactor() set it
			// and this keeps the switch to NewLeveledCompactorWithOptions() behaviour-preserving.
			EnableOverlappingCompaction: true,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("creating compactor for float chunk encoding %s: %w", value, err)
		}

		compactor.SetConcurrencyOptions(concurrencyOpts)
		compactors[enc] = compactor
	}

	// A downstream ConfigProvider may return an encoding the limit cannot select.
	defaultCompactor := compactors[validation.ParseFloatChunkEncoding(validation.DefaultFloatChunkEncodingValue)]
	provider := func(userID string) Compactor {
		if compactor, ok := compactors[cfgProvider.FloatChunkEncoding(userID)]; ok {
			return compactor
		}
		return defaultCompactor
	}

	return provider, NewSplitAndMergePlanner(blockRanges), nil
}

// configureSplitAndMergeCompactor updates the provided configuration injecting the split-and-merge compactor.
func configureSplitAndMergeCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = splitAndMergeGrouperFactory
	cfg.BlocksCompactorFactory = splitAndMergeCompactorFactory
}
