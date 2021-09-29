// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/mimir/pkg/ring"
)

// ConfigureSplitAndMergeCompactor updates the provided configuration injecting the split-and-merge compactor.
func ConfigureSplitAndMergeCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = func(ctx context.Context, cfg Config, cfgProvider ConfigProvider, bkt objstore.Bucket, userID string, ring *ring.Ring, instanceAddr string, logger log.Logger, reg prometheus.Registerer) Grouper {
		return NewSplitAndMergeGrouper(
			userID,
			bkt,
			cfg.BlockRanges.ToMilliseconds(),
			uint32(cfgProvider.CompactorSplitAndMergeShards(userID)),
			createOwnJobFunc(userID, ring, instanceAddr),
			logger)
	}

	cfg.BlocksCompactorFactory = func(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (Compactor, Planner, error) {
		// We don't need to customise the TSDB compactor so we're just using the Prometheus one.
		compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, cfg.BlockRanges.ToMilliseconds(), downsample.NewPool(), nil)
		if err != nil {
			return nil, nil, err
		}

		planner := NewSplitAndMergePlanner(cfg.BlockRanges.ToMilliseconds())
		return compactor, planner, nil
	}
}

func createOwnJobFunc(userID string, ring *ring.Ring, instanceAddr string) ownJobFunc {
	return func(job *job) (bool, error) {
		// If sharding is disabled it means we're expected to run only 1 replica of the compactor
		// and so this compactor instance should own all jobs.
		if ring == nil {
			return true, nil
		}

		// Check whether this compactor instance owns the job.
		rs, err := ring.Get(job.hash(userID), RingOp, nil, nil, nil)
		if err != nil {
			return false, err
		}

		if len(rs.Instances) != 1 {
			return false, fmt.Errorf("unexpected number of compactors in the shard (expected 1, got %d)", len(rs.Instances))
		}

		return rs.Instances[0].Addr == instanceAddr, nil
	}
}
