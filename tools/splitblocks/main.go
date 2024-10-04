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
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type config struct {
	bucket           bucket.Config
	blocks           flagext.StringSliceCSV
	bucketPrefix     string
	outputDir        string
	blockConcurrency int
	dryRun           bool
	maxBlockDuration time.Duration
}

func (c *config) registerFlags(f *flag.FlagSet) {
	c.bucket.RegisterFlags(f)
	f.Var(&c.blocks, "blocks", "An optional comma separated list of blocks to target. If not provided, or empty, all blocks are considered.")
	f.StringVar(&c.bucketPrefix, "bucket-prefix", "", "An optional prefix applied to the bucket path")
	f.StringVar(&c.outputDir, "output.dir", "", "The output directory where split blocks will be written")
	f.IntVar(&c.blockConcurrency, "block-concurrency", 5, "How many blocks can be split at once")
	f.BoolVar(&c.dryRun, "dry-run", false, "If set blocks are not downloaded (except metadata) and splits are not performed; only what would happen is logged")
	f.DurationVar(&c.maxBlockDuration, "max-block-duration", 24*time.Hour, "Max block duration, blocks larger than this or crossing a duration boundary are split.")
}

func (c *config) validate() error {
	for _, blockID := range c.blocks {
		if _, err := ulid.Parse(blockID); err != nil {
			return errors.Wrap(err, "blocks contained an invalid block ID")
		}
	}
	if c.maxBlockDuration < 2*time.Hour {
		return fmt.Errorf("max-block-duration must be at least 2 hours")
	}
	if c.maxBlockDuration.Truncate(time.Hour) != c.maxBlockDuration {
		return fmt.Errorf("max-block-duration should be aligned to hours")
	}
	if 24*time.Hour%c.maxBlockDuration.Truncate(time.Hour) != 0 {
		return fmt.Errorf("max-block-duration should divide 24h without remainder")
	}
	if c.outputDir == "" {
		return fmt.Errorf("output-dir is required")
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
	bkt, err := bucket.NewClient(ctx, cfg.bucket, "bucket", logger, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket")
	}

	// Helpful when not specifying -backend=filesystem (since it can use -filesystem.dir)
	if cfg.bucketPrefix != "" {
		bkt = bucket.NewPrefixedBucketClient(bkt, cfg.bucketPrefix)
	}

	var blocks []string
	if len(cfg.blocks) > 0 {
		blocks = cfg.blocks
	} else {
		blocks, err = listBlocks(ctx, bkt)
		if err != nil {
			level.Error(logger).Log("msg", "failed to list blocks", "err", err)
			return errors.Wrapf(err, "failed to list blocks")
		}
	}

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

		allowedMaxTime := blockMinTime.Truncate(cfg.maxBlockDuration).Add(cfg.maxBlockDuration)
		if !blockMaxTime.After(allowedMaxTime) {
			level.Info(logger).Log("msg", "block does not need to be split")
			return nil
		}

		if cfg.dryRun {
			level.Info(logger).Log("msg", "dry run: would split block")
			return nil
		}

		if err := splitBlock(ctx, cfg, bkt, blockMeta, logger); err != nil {
			level.Error(logger).Log("msg", "failed to split block", "err", err)
			return err
		}

		level.Info(logger).Log("msg", "block split successfully")
		return nil
	})
}

func listBlocks(ctx context.Context, bkt objstore.Bucket) ([]string, error) {
	var blocks []string
	err := bkt.Iter(ctx, "", func(name string) error {
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
func splitBlock(ctx context.Context, cfg config, bkt objstore.Bucket, meta block.Meta, logger log.Logger) error {
	originalBlockDir := filepath.Join(cfg.outputDir, meta.ULID.String())
	if err := block.Download(ctx, logger, bkt, meta.ULID, originalBlockDir); err != nil {
		return err
	}

	_, err := splitLocalBlock(ctx, cfg.outputDir, originalBlockDir, meta, cfg.maxBlockDuration, logger)
	return err
}

func splitLocalBlock(ctx context.Context, parentDir, blockDir string, meta block.Meta, maxDuration time.Duration, logger log.Logger) ([]ulid.ULID, error) {
	origBlockMaxTime := meta.MaxTime
	minTime := meta.MinTime
	result := []ulid.ULID(nil)
	for minTime < origBlockMaxTime {
		// Max time cannot cross maxDuration boundary, but also should not be greater than original max time.
		maxTime := min(timestamp.Time(minTime).Truncate(maxDuration).Add(maxDuration).UnixMilli(), origBlockMaxTime)

		level.Info(logger).Log("msg", "splitting block", "minTime", timestamp.Time(minTime), "maxTime", timestamp.Time(maxTime))

		// Inject a modified meta and abuse the repair into removing the now "outside" chunks.
		// Chunks that cross boundaries are included in multiple blocks.
		meta.MinTime = minTime
		meta.MaxTime = maxTime
		if err := meta.WriteToDir(logger, blockDir); err != nil {
			return nil, errors.Wrap(err, "failed injecting meta for split")
		}
		splitID, err := block.Repair(ctx, logger, parentDir, meta.ULID, block.SplitBlocksSource, block.IgnoreCompleteOutsideChunk, block.IgnoreIssue347OutsideChunk)
		if err != nil {
			return nil, errors.Wrap(err, "failed while splitting block")
		}

		splitMeta, err := block.ReadMetaFromDir(path.Join(parentDir, splitID.String()))
		if err != nil {
			return nil, errors.Wrap(err, "failed while reading meta.json from split block")
		}

		level.Info(logger).Log("msg", "created block from split", "minTime", timestamp.Time(minTime), "maxTime", timestamp.Time(maxTime), "splitID", splitID, "series", splitMeta.Stats.NumSeries, "chunks", splitMeta.Stats.NumChunks, "samples", splitMeta.Stats.NumSamples)
		result = append(result, splitID)
		minTime = maxTime
	}

	if err := os.RemoveAll(blockDir); err != nil {
		return nil, errors.Wrap(err, "failed to clean up original block directory after splitting block")
	}

	return result, nil
}
