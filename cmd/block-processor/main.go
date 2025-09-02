// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runutil"
	dslog "github.com/grafana/dskit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/indexheader"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	mimirlog "github.com/grafana/mimir/pkg/util/log"
)

type config struct {
	LogLevel        dslog.Level
	LogFormat       string
	BlockDir        string
	SamplingRate    int
	VerifyOnLoad    bool
	MaxIdleHandles  int
	ConvertToParquet bool
}

func main() {
	cfg := config{}

	cfg.LogLevel.RegisterFlags(flag.CommandLine)

	flag.StringVar(&cfg.LogFormat, "log.format", dslog.LogfmtFormat, "Output log messages in the given format. Valid formats: [logfmt, json]")
	flag.StringVar(&cfg.BlockDir, "block.dir", "", "Path to the local block directory containing the TSDB block")
	flag.IntVar(&cfg.SamplingRate, "sampling.rate", 32, "Posting offsets in-memory sampling rate")
	flag.BoolVar(&cfg.VerifyOnLoad, "verify.on-load", false, "Verify index-header data on load")
	flag.IntVar(&cfg.MaxIdleHandles, "max.idle-handles", 8, "Maximum number of idle file handles")
	flag.BoolVar(&cfg.ConvertToParquet, "convert.to-parquet", false, "Convert TSDB block to Parquet format")

	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), os.Args[0], "is a tool to generate sparse index headers or convert TSDB blocks to Parquet format.")
		fmt.Fprintln(flag.CommandLine.Output(), "\nModes:")
		fmt.Fprintln(flag.CommandLine.Output(), "  Default: Generate sparse index header in-place")
		fmt.Fprintln(flag.CommandLine.Output(), "  -convert.to-parquet: Convert TSDB block to Parquet format in-place")
		fmt.Fprintln(flag.CommandLine.Output(), "\nFlags:")
		flag.PrintDefaults()
	}

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		exitWithMessage(err.Error())
	}

	logger := mimirlog.InitLogger(cfg.LogFormat, cfg.LogLevel, false, mimirlog.RateLimitedLoggerCfg{})

	if cfg.BlockDir == "" {
		exitWithMessage("Use -block.dir parameter to specify the block directory, or -h to get list of available options.")
	}


	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	if cfg.ConvertToParquet {
		if err := convertBlockToParquet(ctx, logger, cfg); err != nil {
			exitWithMessage("Failed to convert block to Parquet: %v", err)
		}
		level.Info(logger).Log("msg", "block converted to Parquet successfully")
	} else {
		if err := generateSparseIndexHeader(ctx, logger, cfg); err != nil {
			exitWithMessage("Failed to generate sparse index header: %v", err)
		}
		level.Info(logger).Log("msg", "sparse index header generated successfully")
	}
}

func exitWithMessage(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func generateSparseIndexHeader(ctx context.Context, logger log.Logger, cfg config) error {
	// Extract block ID from directory name
	blockIDStr := filepath.Base(cfg.BlockDir)
	blockID, err := ulid.Parse(blockIDStr)
	if err != nil {
		return fmt.Errorf("invalid block ID in directory name %s: %w", blockIDStr, err)
	}

	// Check if the block directory exists and contains necessary files
	metaPath := filepath.Join(cfg.BlockDir, block.MetaFilename)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return fmt.Errorf("block directory %s does not contain %s", cfg.BlockDir, block.MetaFilename)
	}

	indexPath := filepath.Join(cfg.BlockDir, block.IndexFilename)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("block directory %s does not contain %s", cfg.BlockDir, block.IndexFilename)
	}

	level.Info(logger).Log("msg", "generating sparse index header", "block_id", blockID.String(), "block_dir", cfg.BlockDir)

	// Configure indexheader settings
	indexHeaderCfg := indexheader.Config{
		VerifyOnLoad:        cfg.VerifyOnLoad,
		MaxIdleFileHandles:  uint(cfg.MaxIdleHandles),
	}

	// Create metrics (no registry since this is a standalone tool)
	metrics := indexheader.NewStreamBinaryReaderMetrics(nil)

	// Create a local filesystem bucket to read the block
	parentDir := filepath.Dir(cfg.BlockDir)
	localBkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: parentDir})
	if err != nil {
		return fmt.Errorf("failed to create local bucket client: %w", err)
	}

	// Instrument the bucket for use with indexheader
	instrBkt := objstore.WithNoopInstr(localBkt)

	// Call NewStreamBinaryReader which will create the sparse index header
	br, err := indexheader.NewStreamBinaryReader(ctx, logger, instrBkt, parentDir, blockID, cfg.SamplingRate, metrics, indexHeaderCfg)
	if err != nil {
		return fmt.Errorf("failed to create sparse index header: %w", err)
	}

	defer br.Close()

	sparseHeaderPath := filepath.Join(cfg.BlockDir, block.SparseIndexHeaderFilename)
	if _, err := os.Stat(sparseHeaderPath); err == nil {
		level.Info(logger).Log("msg", "sparse index header created", "path", sparseHeaderPath)
	} else {
		return fmt.Errorf("sparse index header was not created at expected path %s", sparseHeaderPath)
	}

	return nil
}

func convertBlockToParquet(ctx context.Context, logger log.Logger, cfg config) error {
	// Extract block ID from directory name
	blockIDStr := filepath.Base(cfg.BlockDir)
	blockID, err := ulid.Parse(blockIDStr)
	if err != nil {
		return fmt.Errorf("invalid block ID in directory name %s: %w", blockIDStr, err)
	}

	// Check if the block directory exists and contains necessary files
	metaPath := filepath.Join(cfg.BlockDir, block.MetaFilename)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return fmt.Errorf("block directory %s does not contain %s", cfg.BlockDir, block.MetaFilename)
	}

	indexPath := filepath.Join(cfg.BlockDir, block.IndexFilename)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("block directory %s does not contain %s", cfg.BlockDir, block.IndexFilename)
	}

	chunksPath := filepath.Join(cfg.BlockDir, "chunks")
	if _, err := os.Stat(chunksPath); os.IsNotExist(err) {
		return fmt.Errorf("block directory %s does not contain chunks directory", cfg.BlockDir)
	}

	level.Info(logger).Log("msg", "converting block to Parquet", "block_id", blockID.String(), "block_dir", cfg.BlockDir)

	// Create output bucket (filesystem-based) - use block directory directly
	outputBkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: cfg.BlockDir})
	if err != nil {
		return fmt.Errorf("failed to create output bucket: %w", err)
	}

	// Open the TSDB block
	tsdbBlock, err := tsdb.OpenBlock(
		mimirlog.SlogFromGoKit(logger), cfg.BlockDir, nil, tsdb.DefaultPostingsDecoderFactory,
	)
	if err != nil {
		return fmt.Errorf("failed to open TSDB block: %w", err)
	}
	defer runutil.CloseWithErrCapture(&err, tsdbBlock, "close tsdb block")

	// Read block metadata to get time range
	meta, err := block.ReadMetaFromDir(cfg.BlockDir)
	if err != nil {
		return fmt.Errorf("failed to read block metadata: %w", err)
	}

	// Convert TSDB block to Parquet
	_, err = convert.ConvertTSDBBlock(
		ctx,
		outputBkt,
		meta.MinTime,
		meta.MaxTime,
		[]convert.Convertible{tsdbBlock},
		convert.WithName("."),
	)
	if err != nil {
		return fmt.Errorf("failed to convert block to Parquet: %w", err)
	}

	return nil
}