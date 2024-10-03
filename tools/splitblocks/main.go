// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"slices"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
)

type config struct {
	bucket            bucket.Config
	blockRanges       tsdb.DurationList
	outputDir         string
	tenantConcurrency int
	blockConcurrency  int
	includeTenants    flagext.StringSliceCSV
	excludeTenants    flagext.StringSliceCSV
	dryRun            bool
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.bucket.RegisterFlags(flag.CommandLine)
	c.blockRanges = tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	flag.Var(&c.blockRanges, "block-ranges", "List of compaction time ranges")
	f.StringVar(&c.outputDir, "output-dir", "", "The output directory where split blocks will be written")
	f.IntVar(&c.blockConcurrency, "block-concurrency", 5, "How many blocks can be split at once per tenant")
	f.IntVar(&c.tenantConcurrency, "tenant-concurrency", 1, "How many tenants can be processed concurrently")
	f.Var(&c.includeTenants, "include-tenants", "A comma separated list of what tenants to target")
	f.Var(&c.excludeTenants, "exclude-tenants", "A comma separated list of what tenants to ignore. Has precedence over included tenants")
	f.BoolVar(&c.dryRun, "dry-run", false, "If set blocks are not downloaded and splits are not performed; only what would happen is logged")
}

func (c *config) validate() error {
	if len(c.blockRanges) == 0 {
		return fmt.Errorf("block-ranges must not be empty")
	}
	for _, blockRange := range c.blockRanges {
		if blockRange <= 0 {
			return fmt.Errorf("block-ranges must only contain positive durations")
		}
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
	slices.Sort(cfg.blockRanges)
	maxDuration := cfg.blockRanges[len(cfg.blockRanges)-1]

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

			blockMinTime := time.Unix(0, blockMeta.MinTime*int64(time.Millisecond)).UTC()
			blockMaxTime := time.Unix(0, blockMeta.MaxTime*int64(time.Millisecond)).UTC()
			logger = log.With(logger, "block_min_time", blockMinTime, "block_max_time", blockMaxTime)

			blockDuration := blockMaxTime.Sub(blockMinTime)
			if blockDuration < 0 {
				level.Error(logger).Log("msg", "block has an invalid minTime greater than maxTime")
				return fmt.Errorf("block has an invalid minTime greater than maxTime")
			}
			if blockDuration <= maxDuration {
				level.Debug(logger).Log("msg", "block does not need to be split")
				return nil
			}

			splits := splitDuration(blockDuration, cfg.blockRanges)

			if cfg.dryRun {
				level.Info(logger).Log("msg", "dry run: would split block", "splits", splits)
				return nil
			}

			if err := splitBlock(ctx, cfg, bkt, tenantID, blockMeta, splits, logger); err != nil {
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

// TODO: Boundary alignment? Confusing with configurable block ranges that may differ
func splitDuration(blockDuration time.Duration, ranges []time.Duration) []time.Duration {
	var durations []time.Duration
	rangeIndex := len(ranges) - 1
	for blockDuration > 0 {
		for ranges[rangeIndex] > blockDuration && rangeIndex > 0 {
			rangeIndex--
		}
		duration := min(blockDuration, ranges[rangeIndex])
		durations = append(durations, duration)
		blockDuration -= duration
	}
	return durations
}

// splitBlock downloads the source block to the output directory, then generates new blocks that are within cfg.blockRanges durations.
// After all the splits succeed the original source block is removed from the output directory.
func splitBlock(ctx context.Context, cfg config, bkt objstore.Bucket, tenantID string, meta block.Meta, splits []time.Duration, logger log.Logger) error {
	tenantDir := path.Join(cfg.outputDir, tenantID)

	originalBlockDir := path.Join(tenantDir, meta.ULID.String())
	if err := block.Download(ctx, logger, bkt, meta.ULID, originalBlockDir); err != nil {
		return err
	}

	minTime := meta.MinTime
	for _, split := range splits {
		maxTime := minTime + split.Milliseconds()

		// TODO: I'm not sure if this works because chunks can cross block min/max boundaries
		// The idea was to inject a modified meta to try to abuse the repair into removing the now "outside" chunks
		meta.MinTime = minTime
		meta.MaxTime = maxTime
		if err := meta.WriteToDir(logger, originalBlockDir); err != nil {
			return errors.Wrap(err, "failed injecting meta for split")
		}
		splitID, err := block.Repair(ctx, logger, tenantDir, meta.ULID, block.SplitBlocksSource, block.IgnoreCompleteOutsideChunk, block.IgnoreIssue347OutsideChunk)
		if err != nil {
			return errors.Wrap(err, "failed while splitting block")
		}

		level.Info(logger).Log("msg", "created block from split", "splitID", splitID)
		minTime = maxTime + 1
	}

	if err := os.RemoveAll(originalBlockDir); err != nil {
		return errors.Wrap(err, "failed to clean up original block directory after splitting block")
	}

	return nil
}
