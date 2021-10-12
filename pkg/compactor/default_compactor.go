// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func defaultBlocksGrouperFactory(ctx context.Context, cfg Config, cfgProvider ConfigProvider, userID string, logger log.Logger, reg prometheus.Registerer) Grouper {
	return NewDefaultGrouper(userID)
}

func defaultBlocksCompactorFactory(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (Compactor, Planner, error) {
	compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, cfg.BlockRanges.ToMilliseconds(), downsample.NewPool(), nil)
	if err != nil {
		return nil, nil, err
	}

	planner := NewTSDBBasedPlanner(logger, cfg.BlockRanges.ToMilliseconds())
	return compactor, planner, nil
}

func configureDefaultCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = defaultBlocksGrouperFactory
	cfg.BlocksCompactorFactory = defaultBlocksCompactorFactory
}
