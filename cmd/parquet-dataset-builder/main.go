package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/thanos-io/objstore"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <command> [flags]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  convert    Convert existing TSDB blocks to parquet\n")
		fmt.Fprintf(os.Stderr, "  generate   Generate TSDB blocks along with their parquet versions\n")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "convert":
		runConvert()
	case "generate":
		runGenerate()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func runConvert() {
	fs := flag.NewFlagSet("convert", flag.ExitOnError)

	// Use BlocksStorageConfig for consistent bucket configuration
	storageCfg := &tsdb.BlocksStorageConfig{}
	storageCfg.RegisterFlags(fs)

	users := fs.String("users", "", "Comma-separated list of users to convert (empty = all users)")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")

	fs.Parse(os.Args[2:])

	logger := log.NewNopLogger()
	if *verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	bkt, err := bucket.NewClient(ctx, storageCfg.Bucket, "parquet-dataset-builder", logger, nil)
	if err != nil {
		fmt.Printf("Failed to create bucket client: %v\n", err)
		os.Exit(1)
	}

	if _, err := bkt.Exists(ctx, "test"); err != nil {
		fmt.Printf("Failed to connect to bucket: %v\n", err)
		os.Exit(1)
	}

	var userList []string
	if *users != "" {
		userList = strings.Split(*users, ",")
		for i, user := range userList {
			userList[i] = strings.TrimSpace(user)
		}
	}

	converter := NewConverter(bkt, logger)
	if err := converter.ConvertAll(ctx, userList); err != nil {
		fmt.Printf("Failed to convert blocks: %v\n", err)
		os.Exit(1)
	}

	// Create bucket indexes for all users
	var usersToIndex []string
	if len(userList) == 0 {
		// Get all users from the converter
		allUsers, err := converter.ListAllUsers(ctx)
		if err != nil {
			fmt.Printf("Failed to list users for index creation: %v\n", err)
			os.Exit(1)
		}
		usersToIndex = allUsers
	} else {
		usersToIndex = userList
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

	// Use BlocksStorageConfig for consistent bucket configuration
	storageCfg := &tsdb.BlocksStorageConfig{}
	storageCfg.RegisterFlags(fs)

	userID := fs.String("user", "user-1", "User ID for dataset")
	seriesCount := fs.Int("series-count", 10000, "Number of series to generate")
	dpm := fs.Int("dpm", 1, "Datapoints per minute")
	metricNames := fs.String("metric-names", "cpu_usage,memory_usage,disk_io", "Comma-separated metric names")
	labelNames := fs.String("label-names", "instance,job,region", "Comma-separated label names")
	labelCardinality := fs.String("label-cardinality", "10,5,3", "Comma-separated cardinality for each label")
	timeRangeHours := fs.Int("time-range-hours", 24, "Time range in hours")
	scrapeInterval := fs.Duration("scrape-interval", 15*time.Second, "Scrape interval")

	outputPrefix := fs.String("output-prefix", "benchmark-dataset", "Prefix for generated blocks")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")

	fs.Parse(os.Args[2:])

	logger := log.NewNopLogger()
	if *verbose {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	}

	ctx := context.Background()

	bkt, err := bucket.NewClient(ctx, storageCfg.Bucket, "parquet-dataset-builder", logger, nil)
	if err != nil {
		fmt.Printf("Failed to create bucket client: %v\n", err)
		os.Exit(1)
	}

	if _, err := bkt.Exists(ctx, "test"); err != nil {
		fmt.Printf("Failed to connect to bucket: %v\n", err)
		os.Exit(1)
	}

	config, err := parseGenerateConfig(*userID, *seriesCount, *dpm, *metricNames, *labelNames, *labelCardinality, *timeRangeHours, *scrapeInterval, *outputPrefix)
	if err != nil {
		fmt.Printf("Failed to parse configuration: %v\n", err)
		os.Exit(1)
	}

	generator := NewDatasetGenerator(bkt, logger)
	if err := generator.Generate(ctx, config); err != nil {
		fmt.Printf("Failed to generate dataset: %v\n", err)
		os.Exit(1)
	}

	// Create bucket index for the user
	if err := createBucketIndex(ctx, bkt, config.UserID, logger); err != nil {
		fmt.Printf("Failed to create bucket index for user %s: %v\n", config.UserID, err)
		os.Exit(1)
	}

	fmt.Printf("Successfully generated parquet dataset with %d series and created bucket index\n", *seriesCount)
}

func parseGenerateConfig(userID string, seriesCount, dpm int, metricNames, labelNames, labelCardinality string, timeRangeHours int, scrapeInterval time.Duration, outputPrefix string) (*DatasetConfig, error) {
	metrics := strings.Split(metricNames, ",")
	labels := strings.Split(labelNames, ",")
	cardinalityStr := strings.Split(labelCardinality, ",")

	if len(labels) != len(cardinalityStr) {
		return nil, fmt.Errorf("label-names and label-cardinality must have the same length")
	}

	cardinality := make([]int, len(cardinalityStr))
	for i, c := range cardinalityStr {
		val, err := strconv.Atoi(strings.TrimSpace(c))
		if err != nil {
			return nil, fmt.Errorf("invalid cardinality value: %s", c)
		}
		cardinality[i] = val
	}

	return &DatasetConfig{
		UserID:           userID,
		SeriesCount:      seriesCount,
		DPM:              dpm,
		MetricNames:      metrics,
		LabelNames:       labels,
		LabelCardinality: cardinality,
		TimeRangeHours:   timeRangeHours,
		ScrapeInterval:   scrapeInterval,
		OutputPrefix:     outputPrefix,
	}, nil
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

type DatasetConfig struct {
	UserID           string
	SeriesCount      int
	DPM              int
	MetricNames      []string
	LabelNames       []string
	LabelCardinality []int
	TimeRangeHours   int
	ScrapeInterval   time.Duration
	OutputPrefix     string
}
