package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"time"

	kitlog "github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

var (
	outputDir        = flag.String("output", "./benchmark-data", "Output directory for generated blocks")
	userID           = flag.String("user", "test-user", "User ID for the generated blocks")
	compression      = flag.Bool("compression", true, "Enable compression for parquet data")
	sortBy           = flag.String("sort-by", "", "Comma-separated list of fields to sort by in parquet data. If unset it still sorts by __name__.")
	storeType        = flag.String("store", "both", "Store type to generate: 'parquet', 'tsdb', or 'both'")
	metricsCount     = flag.Int("metrics", 5, "Number of different metrics to generate")
	instancesCount   = flag.Int("instances", 100, "Number of different instances to generate")
	regionsCount     = flag.Int("regions", 5, "Number of different regions to generate")
	zonesCount       = flag.Int("zones", 10, "Number of different zones to generate")
	servicesCount    = flag.Int("services", 20, "Number of different services to generate")
	environmentsCount = flag.Int("environments", 3, "Number of different environments to generate")
	sampleValue      = flag.Float64("sample-value", 0, "Fixed sample value (0 = random)")
	samplesPerSeries = flag.Int("samples", 10, "Number of samples per series")
	timeRangeHours   = flag.Int("time-range", 2, "Time range in hours for the generated block")
	verboseLogging   = flag.Bool("verbose", false, "Verbose logging")
)

func main() {
	flag.Parse()

	// Validate dimension counts
	if *metricsCount <= 0 || *instancesCount <= 0 || *regionsCount <= 0 || *zonesCount <= 0 || *servicesCount <= 0 || *environmentsCount <= 0 {
		log.Fatal("All dimension counts must be positive")
	}

	var sortByLabels []string
	if *sortBy != "" {
		fields := strings.Split(*sortBy, ",")
		for _, field := range fields {
			if trimmed := strings.TrimSpace(field); trimmed != "" {
				sortByLabels = append(sortByLabels, trimmed)
			}
		}
	}

	dimensions := dimensions{
		metrics:      *metricsCount,
		instances:    *instancesCount,
		regions:      *regionsCount,
		zones:        *zonesCount,
		services:     *servicesCount,
		environments: *environmentsCount,
	}

	if err := generateBlocks(*outputDir, *userID, dimensions, *compression, sortByLabels,
		*storeType, *sampleValue, *samplesPerSeries, *timeRangeHours, *verboseLogging); err != nil {
		log.Fatalf("Failed to generate blocks: %v", err)
	}
}

func generateBlocks(outputDir, userID string, dims dimensions, compression bool,
	sortByLabels []string, storeType string, sampleValue float64,
	samplesPerSeries, timeRangeHours int, verbose bool) error {

	ctx := context.Background()

	totalSeries := int64(dims.metrics * dims.instances * dims.regions * dims.zones * dims.services * dims.environments)
	
	if verbose {
		log.Printf("Generating %d series for user %s", totalSeries, userID)
		log.Printf("Dimensions: %d metrics × %d instances × %d regions × %d zones × %d services × %d environments",
			dims.metrics, dims.instances, dims.regions, dims.zones, dims.services, dims.environments)
		log.Printf("Output directory: %s", outputDir)
		log.Printf("Store type: %s", storeType)
		log.Printf("Samples per series: %d", samplesPerSeries)
		log.Printf("Time range: %d hours", timeRangeHours)
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate time series data
	st := teststorage.New(nil)
	defer func() { _ = st.Close() }()

	app := st.Appender(ctx)

	if err := generateSeries(app, dims, sampleValue, samplesPerSeries, timeRangeHours, verbose); err != nil {
		return fmt.Errorf("failed to generate series: %w", err)
	}

	if err := app.Commit(); err != nil {
		return fmt.Errorf("failed to commit appender: %w", err)
	}

	// Create filesystem bucket
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: outputDir})
	if err != nil {
		return fmt.Errorf("failed to create filesystem bucket: %w", err)
	}
	defer func() { _ = bkt.Close() }()

	// Create block from TSDB head
	blockDir := filepath.Join(outputDir, "temp-blocks")
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		return fmt.Errorf("failed to create block directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(blockDir) }()

	head := st.Head()
	blockID := createBlockFromHead(blockDir, head)

	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	// Inject Thanos metadata
	_, err = block.InjectThanosMeta(kitlog.NewNopLogger(), filepath.Join(blockDir, blockID.String()), block.ThanosMeta{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: block.TestSource,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to inject Thanos metadata: %w", err)
	}

	// Upload TSDB block if requested
	if storeType == "tsdb" || storeType == "both" {
		if err := block.Upload(ctx, kitlog.NewNopLogger(), userBkt, filepath.Join(blockDir, blockID.String()), nil); err != nil {
			return fmt.Errorf("failed to upload TSDB block: %w", err)
		}
		if verbose {
			log.Printf("Generated TSDB block: %s", blockID.String())
		}
	}

	// Convert to Parquet if requested
	if storeType == "parquet" || storeType == "both" {
		if err := convertToParquet(ctx, userBkt, head, blockID, compression, sortByLabels); err != nil {
			return fmt.Errorf("failed to convert to parquet: %w", err)
		}
		if verbose {
			log.Printf("Generated Parquet block: %s", blockID.String())
		}
	}

	// Create bucket index
	if err := createBucketIndex(ctx, bkt, userID); err != nil {
		return fmt.Errorf("failed to create bucket index: %w", err)
	}

	if verbose {
		log.Printf("Successfully generated blocks in: %s", outputDir)
		log.Printf("Time range: %d - %d", head.MinTime(), head.MaxTime())
	}

	return nil
}

func generateSeries(app storage.Appender, dims dimensions,
	sampleValue float64, samplesPerSeries, timeRangeHours int, verbose bool) error {

	if verbose {
		log.Printf("Generating %d samples per series over %d hours", samplesPerSeries, timeRangeHours)
	}

	// Calculate time range - use realistic timestamps
	now := time.Now()
	startTime := now.Add(-time.Duration(timeRangeHours) * time.Hour)
	endTime := now
	timeStep := time.Duration(timeRangeHours) * time.Hour / time.Duration(samplesPerSeries-1)

	if verbose {
		log.Printf("Time range: %s to %s (step: %s)", startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), timeStep)
	}

	seriesGenerated := int64(0)
	sampleCount := int64(0)

	for m := 0; m < dims.metrics; m++ {
		for i := 0; i < dims.instances; i++ {
			for r := 0; r < dims.regions; r++ {
				for z := 0; z < dims.zones; z++ {
					for s := 0; s < dims.services; s++ {
						for e := 0; e < dims.environments; e++ {
							lbls := labels.FromStrings(
								"__name__", fmt.Sprintf("test_metric_%d", m+1),
								"instance", fmt.Sprintf("instance-%d", i+1),
								"region", fmt.Sprintf("region-%d", r+1),
								"zone", fmt.Sprintf("zone-%d", z+1),
								"service", fmt.Sprintf("service-%d", s+1),
								"environment", fmt.Sprintf("environment-%d", e+1),
							)

							// Generate multiple samples for this series
							for sample := 0; sample < samplesPerSeries; sample++ {
								timestamp := startTime.Add(time.Duration(sample) * timeStep)

								value := sampleValue
								if value == 0 {
									value = rand.Float64() * 100 // Scale to 0-100 for more realistic values
								}

								_, err := app.Append(0, lbls, timestamp.UnixMilli(), value)
								if err != nil {
									return fmt.Errorf("failed to append sample for series %s at time %s: %w",
										lbls.String(), timestamp.Format(time.RFC3339), err)
								}
								sampleCount++
							}
							seriesGenerated++
						}
					}
				}
			}
		}
	}

	if verbose {
		log.Printf("Generated %d series with %d total samples", seriesGenerated, sampleCount)
	}

	return nil
}

type dimensions struct {
	metrics      int
	instances    int
	regions      int
	zones        int
	services     int
	environments int
}


func convertToParquet(ctx context.Context, userBkt objstore.InstrumentedBucket, head *tsdb.Head, blockID ulid.ULID,
	compression bool, sortByLabels []string) error {

	convertOpts := []convert.ConvertOption{
		convert.WithName(blockID.String()),
		convert.WithLabelsCompression(schema.WithCompressionEnabled(compression)),
		convert.WithChunksCompression(schema.WithCompressionEnabled(compression)),
	}

	if len(sortByLabels) > 0 {
		convertOpts = append(convertOpts, convert.WithSortBy(sortByLabels...))
	}

	_, err := convert.ConvertTSDBBlock(
		ctx,
		userBkt,
		head.MinTime(),
		head.MaxTime(),
		[]convert.Convertible{head},
		convertOpts...)

	if err != nil {
		return err
	}

	return parquetconverter.WriteConversionMark(ctx, blockID, userBkt)
}

func createBlockFromHead(dir string, head *tsdb.Head) ulid.ULID {
	opts := tsdb.LeveledCompactorOptions{
		MaxBlockChunkSegmentSize:    3 * 1024 * 1024,
		EnableOverlappingCompaction: true,
	}
	compactor, err := tsdb.NewLeveledCompactorWithOptions(context.Background(), nil, promslog.NewNopLogger(), []int64{1000000}, nil, opts)
	if err != nil {
		log.Fatalf("Failed to create compactor: %v", err)
	}

	if err := os.MkdirAll(dir, 0777); err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	ulids, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	if err != nil {
		log.Fatalf("Failed to write block: %v", err)
	}
	if len(ulids) != 1 {
		log.Fatalf("Expected 1 ULID, got %d", len(ulids))
	}
	return ulids[0]
}

func createBucketIndex(ctx context.Context, bkt objstore.Bucket, userID string) error {
	updater := bucketindex.NewUpdater(bkt, userID, nil, 16, 16, kitlog.NewNopLogger())
	idx, _, err := updater.UpdateIndex(ctx, nil)
	if err != nil {
		return err
	}
	return bucketindex.WriteIndex(ctx, bkt, userID, nil, idx)
}