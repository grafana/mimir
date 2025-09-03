package parquetbench

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func setupBenchmarkData(b *testing.B, user string, compression bool, sortByLabels []string, tsdbDir string) (bkt objstore.Bucket, mint, maxt int64) {
	ctx := context.Background()

	// If tsdbDir is provided, use existing blocks
	if tsdbDir != "" {
		return setupFromExistingBlocks(b, user, tsdbDir)
	}

	// Otherwise, generate data on the fly (existing behavior)
	st := teststorage.New(b)
	b.Cleanup(func() { _ = st.Close() })
	app := st.Appender(ctx)

	// 5 metrics × 100 instances × 5 regions × 10 zones × 20 services × 3 environments = 1,500,000 series
	metrics := 5
	instances := 100
	regions := 5
	zones := 10
	services := 20
	environments := 3

	totalSeries := metrics * instances * regions * zones * services * environments
	b.Logf("Generating %d series (%d metrics × %d instances × %d regions × %d zones × %d services × %d environments)",
		totalSeries, metrics, instances, regions, zones, services, environments)

	seriesCount := 0
	for m := range metrics {
		for i := range instances {
			for r := range regions {
				for z := range zones {
					for s := range services {
						for e := range environments {
							lbls := labels.FromStrings(
								"__name__", fmt.Sprintf("test_metric_%d", m),
								"instance", fmt.Sprintf("instance-%d", i),
								"region", fmt.Sprintf("region-%d", r),
								"zone", fmt.Sprintf("zone-%d", z),
								"service", fmt.Sprintf("service-%d", s),
								"environment", fmt.Sprintf("environment-%d", e),
							)
							_, _ = app.Append(0, lbls, 0, rand.Float64())
							seriesCount++
						}
					}
				}
			}
		}
	}
	err := app.Commit()
	require.NoError(b, err, "error committing appender")

	bkt, err = filesystem.NewBucketClient(filesystem.Config{Directory: b.TempDir()})
	require.NoError(b, err, "error creating filesystem bucket client")
	b.Cleanup(func() { _ = bkt.Close() })

	blockDir := b.TempDir()
	head := st.Head()
	blockId := createBlockFromHead(b, blockDir, head)
	userBkt := bucket.NewUserBucketClient(user, bkt, nil)

	_, err = block.InjectThanosMeta(log.NewNopLogger(), filepath.Join(blockDir, blockId.String()), block.ThanosMeta{
		Labels: labels.FromStrings("ext1", "1").Map(),
		Source: block.TestSource,
	}, nil)
	require.NoError(b, err)
	require.NoError(b, block.Upload(context.Background(), log.NewNopLogger(), userBkt, filepath.Join(blockDir, blockId.String()), nil))

	convertOpts := []convert.ConvertOption{
		convert.WithName(blockId.String()),
		convert.WithLabelsCompression(schema.WithCompressionEnabled(compression)),
		convert.WithChunksCompression(schema.WithCompressionEnabled(compression)),
	}

	if len(sortByLabels) > 0 {
		convertOpts = append(convertOpts, convert.WithSortBy(sortByLabels...))
	}

	_, err = convert.ConvertTSDBBlock(
		ctx,
		userBkt,
		head.MinTime(),
		head.MaxTime(),
		[]convert.Convertible{head},
		convertOpts...)

	require.NoError(b, err, "error converting TSDB block to Parquet")

	err = parquetconverter.WriteConversionMark(ctx, blockId, userBkt)
	require.NoError(b, err, "error writing conversion mark")

	createBucketIndex(b, bkt, user)
	return bkt, head.MinTime(), head.MaxTime()
}

// TODO: copied
func createBlockFromHead(t testing.TB, dir string, head *tsdb.Head) ulid.ULID {
	// Put a 3 MiB limit on segment files so we can test with many segment files without creating too big blocks.
	opts := tsdb.LeveledCompactorOptions{
		MaxBlockChunkSegmentSize:    3 * 1024 * 1024,
		EnableOverlappingCompaction: true,
	}
	compactor, err := tsdb.NewLeveledCompactorWithOptions(context.Background(), nil, promslog.NewNopLogger(), []int64{1000000}, nil, opts)
	assert.NoError(t, err)

	assert.NoError(t, os.MkdirAll(dir, 0777))

	// Add +1 millisecond to block maxt because block intervals are half-open: [b.MinTime, b.MaxTime).
	// Because of this block intervals are always +1 than the total samples it includes.
	ulids, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
	assert.NoError(t, err)
	assert.Len(t, ulids, 1)
	return ulids[0]
}

// TODO: copied
func createBucketIndex(t *testing.B, bkt objstore.Bucket, userID string) *bucketindex.Index {
	updater := bucketindex.NewUpdater(bkt, userID, nil, 16, 16, log.NewNopLogger())
	idx, _, err := updater.UpdateIndex(context.Background(), nil)
	require.NoError(t, err)
	require.NoError(t, bucketindex.WriteIndex(context.Background(), bkt, userID, nil, idx))

	return idx
}

// BlockMeta represents the structure of TSDB block meta.json
type BlockMeta struct {
	ULID    string `json:"ulid"`
	MinTime int64  `json:"minTime"`
	MaxTime int64  `json:"maxTime"`
}

// readBlockMeta reads the meta.json file from a block directory and extracts time range
func readBlockMeta(blockDir string) (mint, maxt int64, err error) {
	metaPath := filepath.Join(blockDir, "meta.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read meta.json from %s: %w", metaPath, err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, 0, fmt.Errorf("failed to parse meta.json: %w", err)
	}

	return meta.MinTime, meta.MaxTime, nil
}

// setupFromExistingBlocks configures the benchmark to use pre-generated TSDB blocks
func setupFromExistingBlocks(b *testing.B, user, tsdbDir string) (bkt objstore.Bucket, mint, maxt int64) {
	// Create filesystem bucket pointing to the TSDB directory
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: tsdbDir})
	require.NoError(b, err, "error creating filesystem bucket client")
	b.Cleanup(func() { _ = bkt.Close() })

	// Find the first block directory to read meta.json for time range
	userBktPath := filepath.Join(tsdbDir, user)
	entries, err := os.ReadDir(userBktPath)
	require.NoError(b, err, "error reading user bucket directory")

	var blockDir string
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 10 { // ULID-like directory names
			blockDir = filepath.Join(userBktPath, entry.Name())
			break
		}
	}
	require.NotEmpty(b, blockDir, "no block directories found in %s", userBktPath)

	// Read meta.json to get time range
	mint, maxt, err = readBlockMeta(blockDir)
	require.NoError(b, err, "error reading block metadata")

	b.Logf("Using existing blocks from %s", tsdbDir)
	b.Logf("Found block time range: %d - %d", mint, maxt)

	return bkt, mint, maxt
}
