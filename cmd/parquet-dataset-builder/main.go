package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/thanos-io/objstore"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [flags]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  convert         Convert existing TSDB blocks to parquet\n")
		fmt.Fprintf(os.Stderr, "  generate        Generate TSDB blocks along with their parquet versions\n")
		fmt.Fprintf(os.Stderr, "  promoter        Promote labels from target_info to other series in blocks\n")
		fmt.Fprintf(os.Stderr, "  promote-stream  Promote labels using optimized approach\n")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "convert":
		runConvert()
	case "generate":
		runGenerate()
	case "promoter":
		runPromoter()
	case "promote-stream":
		runPromoterStreaming()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func runConvert() {
	fs := flag.NewFlagSet("convert", flag.ExitOnError)

	cfg := &ConvertConfig{}
	cfg.RegisterFlags(fs)

	fs.Parse(os.Args[2:])

	if err := cfg.Validate(); err != nil {
		fmt.Printf("Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger := log.NewNopLogger()
	if cfg.Verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	bkt, err := bucket.NewClient(ctx, cfg.Storage.Bucket, "parquet-dataset-builder", logger, nil)
	if err != nil {
		fmt.Printf("Failed to create bucket client: %v\n", err)
		os.Exit(1)
	}

	if _, err := bkt.Exists(ctx, "test"); err != nil {
		fmt.Printf("Failed to connect to bucket: %v\n", err)
		os.Exit(1)
	}

	converter := NewConverter(bkt, logger)
	if err := converter.ConvertAll(ctx, cfg.Users); err != nil {
		fmt.Printf("Failed to convert blocks: %v\n", err)
		os.Exit(1)
	}

	// Create bucket indexes for all users
	var usersToIndex []string
	if len(cfg.Users) == 0 {
		// Get all users from the converter
		allUsers, err := converter.ListAllUsers(ctx)
		if err != nil {
			fmt.Printf("Failed to list users for index creation: %v\n", err)
			os.Exit(1)
		}
		usersToIndex = allUsers
	} else {
		usersToIndex = cfg.Users
	}

	for _, userID := range usersToIndex {
		if err := createBucketIndex(ctx, bkt, userID, logger); err != nil {
			fmt.Printf("Failed to create bucket index for user %s: %v\n", userID, err)
			os.Exit(1)
		}
	}

	fmt.Println("Successfully converted blocks to parquet format and created bucket indexes")
}

func runGenerate() {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)

	cfg := &GenerateConfig{}
	cfg.RegisterFlags(fs)

	fs.Parse(os.Args[2:])

	if err := cfg.Validate(); err != nil {
		fmt.Printf("Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger := log.NewNopLogger()
	if cfg.Verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	bkt, err := bucket.NewClient(ctx, cfg.Storage.Bucket, "parquet-dataset-builder", logger, nil)
	if err != nil {
		fmt.Printf("Failed to create bucket client: %v\n", err)
		os.Exit(1)
	}

	if _, err := bkt.Exists(ctx, "test"); err != nil {
		fmt.Printf("Failed to connect to bucket: %v\n", err)
		os.Exit(1)
	}

	generator := NewDatasetGenerator(bkt, logger)
	seriesCount, err := generator.Generate(ctx, cfg)
	if err != nil {
		fmt.Printf("Failed to generate dataset: %v\n", err)
		os.Exit(1)
	}

	// Create bucket index for the user
	if err := createBucketIndex(ctx, bkt, cfg.UserID, logger); err != nil {
		fmt.Printf("Failed to create bucket index for user %s: %v\n", cfg.UserID, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully generated parquet dataset with %d series and created bucket index\n", seriesCount)
}

func runPromoter() {
	fs := flag.NewFlagSet("promoter", flag.ExitOnError)

	cfg := &PromoterConfig{}
	cfg.RegisterFlags(fs)

	fs.Parse(os.Args[2:])

	if err := cfg.Validate(); err != nil {
		fmt.Printf("Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger := log.NewNopLogger()
	if cfg.Verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	promoter := NewPromoter(cfg.BlocksDirectory, logger)
	if err := promoter.PromoteLabels(ctx); err != nil {
		fmt.Printf("Failed to promote labels: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Successfully promoted labels from target_info to series")
}

func runPromoterStreaming() {
	fs := flag.NewFlagSet("promote-stream", flag.ExitOnError)

	cfg := &PromoterConfig{}
	cfg.RegisterFlags(fs)

	fs.Parse(os.Args[2:])

	if err := cfg.Validate(); err != nil {
		fmt.Printf("Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	logger := log.NewNopLogger()
	if cfg.Verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	// Use streaming promoter for better memory efficiency
	promoter := NewStreamingPromoter(cfg.BlocksDirectory, logger)
	if err := promoter.PromoteLabels(ctx); err != nil {
		fmt.Printf("Failed to promote labels: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Successfully promoted labels from target_info to series using optimized approach")
}

func createBucketIndex(ctx context.Context, bkt objstore.Bucket, userID string, logger log.Logger) error {
	level.Info(logger).Log("msg", "Creating bucket index", "user", userID)

	updater := bucketindex.NewUpdater(bkt, userID, nil, 16, logger)
	idx, partials, err := updater.UpdateIndex(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to update bucket index: %w", err)
	}

	if len(partials) > 0 {
		level.Warn(logger).Log("msg", "Found partial blocks during index update", "user", userID, "count", len(partials))
	}

	if err := bucketindex.WriteIndex(ctx, bkt, userID, nil, idx); err != nil {
		return fmt.Errorf("failed to write bucket index: %w", err)
	}
	level.Info(logger).Log("msg", "Successfully created bucket index", "user", userID, "blocks", len(idx.Blocks))
	return nil
}
