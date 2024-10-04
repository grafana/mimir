// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
)

type config struct {
	bucket            bucket.Config
	outputDir         string
	tenantConcurrency int
	blockConcurrency  int
	includeTenants    flagext.StringSliceCSV
	excludeTenants    flagext.StringSliceCSV
	dryRun            bool
	maxDuration       time.Duration
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.bucket.RegisterFlags(flag.CommandLine)
	f.StringVar(&c.outputDir, "output-dir", "", "The output directory where split blocks will be written")
	f.IntVar(&c.blockConcurrency, "block-concurrency", 5, "How many blocks can be split at once per tenant")
	f.IntVar(&c.tenantConcurrency, "tenant-concurrency", 1, "How many tenants can be processed concurrently")
	f.Var(&c.includeTenants, "include-tenants", "A comma separated list of what tenants to target")
	f.Var(&c.excludeTenants, "exclude-tenants", "A comma separated list of what tenants to ignore. Has precedence over included tenants")
	f.BoolVar(&c.dryRun, "dry-run", false, "If set blocks are not downloaded and splits are not performed; only what would happen is logged")
	f.DurationVar(&c.maxDuration, "max-block-duration", 24*time.Hour, "Max block duration, blocks larger than this or crossing duration boundary are split.")
}

func (c *config) validate() error {
	if c.maxDuration < 2*time.Hour {
		return fmt.Errorf("max-block-duration must be at least 2 hours")
	}
	if c.maxDuration.Truncate(time.Hour) != c.maxDuration {
		return fmt.Errorf("max-block-duration should be aligned to hours")
	}
	if 24*time.Hour%c.maxDuration.Truncate(time.Hour) != 0 {
		return fmt.Errorf("max-block-duration should divide 24h without remainder")
	}
	if c.outputDir == "" {
		return fmt.Errorf("output-dir is required")
	}
	if c.tenantConcurrency < 1 {
		return fmt.Errorf("tenant-concurrency must be positive")
	}
	if c.blockConcurrency < 1 {
		return fmt.Errorf("block-concurrency must be positive")
	}
	return nil
}

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.registerFlags(flag.CommandLine)

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	logger := log.NewLogfmtLogger(os.Stdout)
	logger = log.With(logger, "ts", log.DefaultTimestampUTC)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := splitBlocks(ctx, cfg, logger); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func splitBlocks(ctx context.Context, cfg config, logger log.Logger) error {
	allowedTenants := util.NewAllowedTenants(cfg.includeTenants, cfg.excludeTenants)

	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket")
	}
	tenants, err := tsdb.ListUsers(ctx, bkt)
	if err != nil {
		return errors.Wrap(err, "failed to list tenants")
	}

	return concurrency.ForEachUser(ctx, tenants, cfg.tenantConcurrency, func(ctx context.Context, tenantID string) error {
		if !allowedTenants.IsAllowed(tenantID) {
			return nil
		}
		logger := log.With(logger, "tenantID", tenantID)

		blocks, err := listBlocksForTenant(ctx, bkt, tenantID)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks for tenant", "err", err)
			return errors.Wrapf(err, "failed to list blocks for tenant %s", tenantID)
		}

		bkt := objstore.NewPrefixedBucket(bkt, tenantID)

		return concurrency.ForEachUser(ctx, blocks, cfg.blockConcurrency, func(ctx context.Context, blockString string) error {
			blockID, err := ulid.Parse(blockString)
			if err != nil {
				return err
			}

			logger := log.With(logger, "block", blockString)
			blockMeta, err := block.DownloadMeta(ctx, logger, bkt, blockID)
			if err != nil {
				level.Error(logger).Log("msg", "failed to read block's meta.json file", "err", err)
				return err
			}

			blockMinTime := timestamp.Time(blockMeta.MinTime)
			blockMaxTime := timestamp.Time(blockMeta.MaxTime)
			level.Info(logger).Log("block_min_time", blockMinTime, "block_max_time", blockMaxTime)

			if blockMinTime.After(blockMaxTime) {
				level.Error(logger).Log("msg", "block has an invalid minTime greater than maxTime")
				return fmt.Errorf("block has an invalid minTime greater than maxTime")
			}

			allowedMaxTime := blockMinTime.Truncate(cfg.maxDuration).Add(cfg.maxDuration)
			if !blockMaxTime.After(allowedMaxTime) {
				level.Info(logger).Log("msg", "block does not need to be split")
				return nil
			}

			if cfg.dryRun {
				level.Info(logger).Log("msg", "dry run: would split block")
				return nil
			}

			if err := splitBlock(ctx, cfg, bkt, tenantID, blockMeta, logger); err != nil {
				level.Error(logger).Log("msg", "failed to split block", "err", err)
				return err
			}

			level.Info(logger).Log("msg", "block split successfully", "dryRun", cfg.dryRun)
			return nil
		})
	})
}

func listBlocksForTenant(ctx context.Context, bkt objstore.Bucket, tenantID string) ([]string, error) {
	var blocks []string
	err := bkt.Iter(ctx, tenantID+objstore.DirDelim, func(name string) error {
		if block, ok := block.IsBlockDir(name); ok {
			blocks = append(blocks, block.String())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// splitBlock downloads the source block to the output directory, then generates new blocks that are within cfg.blockRanges durations.
// After all the splits succeed the original source block is removed from the output directory.
func splitBlock(ctx context.Context, cfg config, bkt objstore.Bucket, tenantID string, meta block.Meta, logger log.Logger) error {
	tenantDir := filepath.Join(cfg.outputDir, tenantID)

	originalBlockDir := filepath.Join(tenantDir, meta.ULID.String())
	if err := block.Download(ctx, logger, bkt, meta.ULID, originalBlockDir); err != nil {
		return err
	}

	return splitLocalBlock(ctx, tenantDir, originalBlockDir, meta, cfg.maxDuration, logger)
}

func splitLocalBlock(ctx context.Context, parentDir, blockDir string, meta block.Meta, maxDuration time.Duration, logger log.Logger) error {
	origBlockMaxTime := meta.MaxTime
	minTime := meta.MinTime
	for minTime < origBlockMaxTime {
		// Max time cannot cross maxDuration boundary, but also should not be greater than original max time.
		maxTime := min(timestamp.Time(minTime).Truncate(maxDuration).Add(maxDuration).UnixMilli(), origBlockMaxTime)

		level.Info(logger).Log("msg", "splitting block", "minTime", timestamp.Time(minTime), "maxTime", timestamp.Time(maxTime))

		// Inject a modified meta and abuse the repair into removing the now "outside" chunks.
		// Chunks that cross boundaries are included in multiple blocks.
		meta.MinTime = minTime
		meta.MaxTime = maxTime
		if err := meta.WriteToDir(logger, blockDir); err != nil {
			return errors.Wrap(err, "failed injecting meta for split")
		}
		splitID, err := block.Repair(ctx, logger, parentDir, meta.ULID, block.SplitBlocksSource, block.IgnoreCompleteOutsideChunk, block.IgnoreIssue347OutsideChunk)
		if err != nil {
			return errors.Wrap(err, "failed while splitting block")
		}

		splitMeta, err := block.ReadMetaFromDir(path.Join(parentDir, splitID.String()))
		if err != nil {
			return errors.Wrap(err, "failed while reading meta.json from split block")
		}

		level.Info(logger).Log("msg", "created block from split", "minTime", timestamp.Time(minTime), "maxTime", timestamp.Time(maxTime), "splitID", splitID, "series", splitMeta.Stats.NumSeries, "chunks", splitMeta.Stats.NumChunks, "samples", splitMeta.Stats.NumSamples)
		minTime = maxTime
	}

	if err := os.RemoveAll(blockDir); err != nil {
		return errors.Wrap(err, "failed to clean up original block directory after splitting block")
	}

	return nil
}
