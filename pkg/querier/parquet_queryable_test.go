package querier

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/dskit/user"

	"github.com/grafana/mimir/pkg/parquetconverter"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
)

// checkConsistency controls whether to run consistency checks in benchmarks.
// It can be set via the MIMIR_PARQUET_CHECK_CONSISTENCY environment variable.
var checkConsistency = func() bool {
	v, _ := strconv.ParseBool(os.Getenv("MIMIR_PARQUET_CHECK_CONSISTENCY"))
	return v
}()

// minimalBlocksStorageConfig returns a minimal config for mimir_tsdb.BlocksStorageConfig.
func minimalBlocksStorageConfig(storageDir string) mimir_tsdb.BlocksStorageConfig {
	return mimir_tsdb.BlocksStorageConfig{
		Bucket: bucket.Config{
			StorageBackendConfig: bucket.StorageBackendConfig{
				Backend: bucket.Filesystem,
				Filesystem: filesystem.Config{
					Directory: storageDir,
				},
			},
		},
	}
}

// QueryCreateFunc is a callback to create a Queryable from a pre-filled TestStorage. It may or may not use the provided Bucket.
type QueryCreateFunc func(tb testing.TB, st *teststorage.TestStorage) storage.Queryable

// BenchmarkCase represents a single benchmark case with its matchers and name
type BenchmarkCase struct {
	Name     string
	Matchers []*labels.Matcher
}

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
	Samples []Sample // Store all samples from all series
}

// Sample represents a single data point from a series
type Sample struct {
	Labels    labels.Labels
	Timestamp int64
	Value     float64
	Histogram *histogram.Histogram
}

// RunBenchmarks runs benchmarks for multiple cases with different matchers.
func RunBenchmarks(b *testing.B, f QueryCreateFunc, cases []BenchmarkCase, checkConsistency bool) BenchmarkResult {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "test-tenant")

	st := teststorage.New(b)
	b.Cleanup(func() { _ = st.Close() })

	q, err := f(b, st).Querier(math.MinInt64, math.MaxInt64)
	if err != nil {
		b.Fatal("error building querier: ", err)
	}
	defer q.Close()

	result := BenchmarkResult{
		Samples: make([]Sample, 0),
	}

	if checkConsistency {
		for _, bc := range cases {
			ss := q.Select(ctx, false, &storage.SelectHints{
				Start: math.MinInt64,
				End:   math.MaxInt64,
				Step:  10,
			}, bc.Matchers...)

			for ss.Next() {
				s := ss.At()
				it := s.Iterator(nil)

				for {
					v := it.Next()
					if v == chunkenc.ValNone {
						break
					}

					sample := Sample{
						Labels:    s.Labels(),
						Timestamp: it.AtT(),
					}

					switch v {
					case chunkenc.ValFloat:
						_, val := it.At()
						sample.Value = val
					case chunkenc.ValHistogram:
						_, h := it.AtHistogram(nil)
						sample.Histogram = h
					default:
						panic("unhandled default case")
					}

					result.Samples = append(result.Samples, sample)
				}
				if it.Err() != nil {
					b.Fatal(it.Err())
				}
			}
			if err := ss.Err(); err != nil {
				b.Fatal(err)
			}
		}
	}

	for _, bc := range cases {
		b.Run(bc.Name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			var series int
			for i := 0; i < b.N; i++ {
				ss := q.Select(ctx, false, &storage.SelectHints{
					Start: math.MinInt64,
					End:   math.MaxInt64,
					Step:  0,
				}, bc.Matchers...)

				for ss.Next() {
					series++
				}
				if err := ss.Err(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(series)/float64(b.N), "series/op")
		})
	}
	return result
}

func BenchmarkQueryableSelect(b *testing.B) {
	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, "test-tenant")

	bkt, dir := mimir_testutil.PrepareFilesystemBucket(b)
	b.Cleanup(func() { _ = bkt.Close() })

	numSamples := 10000
	numSeries := 10
	series := make([]labels.Labels, numSeries)
	for i := 0; i < numSeries; i++ {
		series[i] = labels.FromStrings(
			"__name__", fmt.Sprintf("test_metric_%d", i), // Unique metric name for each series
			"instance", fmt.Sprintf("%d", i), // 10000 different instances
			"region", fmt.Sprintf("region-%d", i%5), // 5 different regions
			"zone", fmt.Sprintf("zone-%d", i%10), // 10 different zones
			"service", fmt.Sprintf("service-%d", i%20), // 20 different services
			"environment", fmt.Sprintf("env-%d", i%3), // 3 different environments
			"cluster", fmt.Sprintf("cluster-%d", i%4), // 4 different clusters
			"namespace", fmt.Sprintf("ns-%d", i%8), // 8 different namespaces
			"pod", fmt.Sprintf("pod-%d", i%50), // 50 different pods
			"container", fmt.Sprintf("container-%d", i%100), // 100 different containers
		)
	}

	tenantDir := filepath.Join(dir, "test-tenant")
	require.NoError(b, os.MkdirAll(tenantDir, 0755))

	blockID, err := CreateFloatBlock(ctx, tenantDir, series, numSamples, 0, 100000, labels.EmptyLabels())
	require.NoError(b, err)

	blockDir := filepath.Join(tenantDir, blockID.String())
	uploader := &parquetconverter.InDiskUploader{Dir: tenantDir}
	convertCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	require.NoError(b, parquetconverter.TSDBBlockToParquet(convertCtx, blockID, uploader, blockDir, log.NewNopLogger()))

	chunksDir := filepath.Join(blockDir, "chunks")
	indexFile := filepath.Join(blockDir, "index")
	parquetFile := filepath.Join(blockDir, "block.parquet")

	chunksSize, err := dirSize(chunksDir)
	require.NoError(b, err)

	indexSize, err := fileSize(indexFile)
	require.NoError(b, err)

	parquetSize, err := fileSize(parquetFile)
	require.NoError(b, err)

	b.Logf("Block directory: %s", blockDir)
	b.Logf("Block sizes:\n  Chunks: %.2f MB\n  Index: %.2f MB\n  Parquet: %.2f MB",
		float64(chunksSize)/(1024*1024),
		float64(indexSize)/(1024*1024),
		float64(parquetSize)/(1024*1024))

	meta, err := block.ReadMetaFromDir(blockDir)
	require.NoError(b, err)
	pIdx := &bucketindex.ParquetIndex{
		Blocks: make(map[ulid.ULID]bucketindex.BlockWithExtension),
	}
	pIdx.Blocks[blockID] = bucketindex.BlockWithExtension{
		Block: &bucketindex.Block{
			ID:         blockID,
			MinTime:    meta.MinTime,
			MaxTime:    meta.MaxTime,
			UploadedAt: time.Now().Unix(),
		},
	}

	tenantBkt := bucket.NewPrefixedBucketClient(bkt, "test-tenant")
	require.NoError(b, bucketindex.WriteParquetIndex(ctx, tenantBkt, pIdx))

	idx := &bucketindex.Index{
		Version:   bucketindex.IndexVersion2,
		UpdatedAt: time.Now().Unix(),
		Blocks: []*bucketindex.Block{
			{
				ID:         blockID,
				MinTime:    meta.MinTime,
				MaxTime:    meta.MaxTime,
				UploadedAt: time.Now().Unix(),
			},
		},
	}
	require.NoError(b, bucketindex.WriteIndex(ctx, bkt, "test-tenant", nil, idx))

	benchmarkCases := []BenchmarkCase{
		{
			Name: "SingleMetricExact",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchEqual, "instance", "1"),
			},
		},
		{
			Name: "SingleMetricAllInstances",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
			},
		},
		{
			Name: "MultipleMetricsRange",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-5]"),
			},
		},
		{
			Name: "MultipleMetricsSparse",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_(1|5|10|15|20)"),
			},
		},
		{
			Name: "HighCardinalitySingleMetric",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "env-1"),
				labels.MustNewMatcher(labels.MatchRegexp, "container", ".*"),
			},
		},
		{
			Name: "HighCardinalityMultipleMetrics",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "env-1"),
				labels.MustNewMatcher(labels.MatchRegexp, "container", ".*"),
			},
		},
		{
			Name: "SubsetSelectorsSingleMetric",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "ns-3"),
				labels.MustNewMatcher(labels.MatchEqual, "cluster", "cluster-1"),
			},
		},
		{
			Name: "SubsetSelectorsMultipleMetrics",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-5]"),
				labels.MustNewMatcher(labels.MatchEqual, "namespace", "ns-3"),
				labels.MustNewMatcher(labels.MatchEqual, "cluster", "cluster-1"),
			},
		},
		{
			Name: "NegativeRegexSingleMetric",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "container", "(container-1.*|container-2.*)"),
			},
		},
		{
			Name: "NegativeRegexMultipleMetrics",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
				labels.MustNewMatcher(labels.MatchNotRegexp, "container", "(container-1.*|container-2.*)"),
			},
		},
		{
			Name: "NonExistentValue",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchEqual, "pod", "non-existent-pod"),
			},
		},
		{
			Name: "ExpensiveRegexSingleMetric",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchRegexp, "container", "(container-1|container-2|container-3|container-4|container-5)"),
			},
		},
		{
			Name: "ExpensiveRegexMultipleMetrics",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
				labels.MustNewMatcher(labels.MatchRegexp, "container", "(container-1|container-2|container-3|container-4|container-5)"),
			},
		},
		{
			Name: "SparseSeriesSingleMetric",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "service-1"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "env-0"),
			},
		},
		{
			Name: "SparseSeriesMultipleMetrics",
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
				labels.MustNewMatcher(labels.MatchEqual, "service", "service-1"),
				labels.MustNewMatcher(labels.MatchEqual, "environment", "env-0"),
			},
		},
		/*
			TODO investigate why this one doesnt work
			{
				Name: "LabelQueries",
				Matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric"),
					labels.MustNewMatcher(labels.MatchRegexp, "container", "container-.*"),
					labels.MustNewMatcher(labels.MatchRegexp, "pod", "pod-.*"),
				},
			},
		*/
	}

	var parquetResults, blockResults []BenchmarkResult

	b.Run("ParquetQueryable", func(bb *testing.B) {
		pq, err := func() (storage.Queryable, error) {
			limits := &blocksStoreLimitsMock{}
			logger := log.NewNopLogger()
			reg := prometheus.NewRegistry()
			cfg := Config{}
			storageCfg := minimalBlocksStorageConfig(dir)
			storageCfg.BucketStore.BucketIndex.MaxStalePeriod = 24 * time.Hour
			storageCfg.BucketStore.IndexCache.Backend = "inmemory"
			storageCfg.BucketStore.IndexCache.InMemory.MaxSizeBytes = 1 * 1024 * 1024 * 1024
			pq, err := NewParquetStoreQueryable(limits, cfg, storageCfg, logger, reg)
			if err != nil {
				return nil, err
			}

			pq.asyncRead = false

			err = pq.StartAsync(context.Background())
			if err != nil {
				return nil, err
			}
			err = pq.AwaitRunning(context.Background())
			if err != nil {
				return nil, err
			}

			bb.Cleanup(func() {
				pq.StopAsync()
				err := pq.AwaitTerminated(context.Background())
				require.NoError(bb, err)
			})

			return pq, nil
		}()
		require.NoError(bb, err)

		result := RunBenchmarks(bb, func(tb testing.TB, st *teststorage.TestStorage) storage.Queryable {
			return pq
		}, benchmarkCases, checkConsistency)
		parquetResults = append(parquetResults, result)
	})

	b.Run("BlockQueryable", func(bb *testing.B) {
		blck, err := tsdb.OpenBlock(slog.Default(), blockDir, nil, nil)
		require.NoError(bb, err)

		bb.Cleanup(func() {
			require.NoError(bb, blck.Close())
		})

		querier, err := tsdb.NewBlockQuerier(blck, math.MinInt64, math.MaxInt64)
		require.NoError(bb, err)

		q := &blockQueryable{
			querier: querier,
			cleanup: func() {
				require.NoError(bb, querier.Close())
			},
		}

		result := RunBenchmarks(bb, func(tb testing.TB, st *teststorage.TestStorage) storage.Queryable {
			return q
		}, benchmarkCases, checkConsistency)
		blockResults = append(blockResults, result)
	})

	if checkConsistency {
		require.Equal(b, len(parquetResults), len(blockResults), "Number of benchmark cases should match")
		for i := range parquetResults {
			require.Equal(b, len(parquetResults[i].Samples), len(blockResults[i].Samples),
				"Number of samples should match for case %d", i)

			for j := range parquetResults[i].Samples {
				parquetSample := parquetResults[i].Samples[j]
				blockSample := blockResults[i].Samples[j]

				require.Equal(b, parquetSample.Labels, blockSample.Labels,
					"Sample labels should match for case %d, sample %d", i, j)
				require.Equal(b, parquetSample.Timestamp, blockSample.Timestamp,
					"Sample timestamp should match for case %d, sample %d", i, j)

				if parquetSample.Histogram != nil {
					require.NotNil(b, blockSample.Histogram,
						"Block sample should have histogram for case %d, sample %d", i, j)
					require.Equal(b, parquetSample.Histogram.String(), blockSample.Histogram.String(),
						"Histogram values should match for case %d, sample %d", i, j)
				} else {
					require.Nil(b, blockSample.Histogram,
						"Block sample should not have histogram for case %d, sample %d", i, j)
					require.Equal(b, parquetSample.Value, blockSample.Value,
						"Sample value should match for case %d, sample %d", i, j)
				}
			}
		}
	}
}

// blockQueryable is a simple queryable that wraps a querier
type blockQueryable struct {
	querier storage.Querier
	cleanup func()
}

func (q *blockQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return q.querier, nil
}

func (q *blockQueryable) Close() error {
	q.cleanup()
	return nil
}

// Helper functions at the end of the file
func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func fileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// CreateFloatBlock writes a block with the given series and numSamples samples each.
// All series will contain float values.
// Samples will be in the time range [mint, maxt).
// TODO Use the block generator from tsdb when there is support for histograms
func CreateFloatBlock(
	ctx context.Context,
	dir string,
	series []labels.Labels,
	numSamples int,
	mint, maxt int64,
	extLset labels.Labels,
) (id ulid.ULID, err error) {
	headOpts := tsdb.DefaultHeadOptions()
	headOpts.ChunkDirRoot = filepath.Join(dir, "chunks")
	headOpts.ChunkRange = math.MaxInt64
	headOpts.EnableNativeHistograms.Store(false) // Disable histograms
	h, err := tsdb.NewHead(nil, nil, nil, nil, headOpts, nil)
	if err != nil {
		return id, errors.Wrap(err, "create head block")
	}
	defer func() {
		if err := h.Close(); err != nil {
			err = errors.Wrap(err, "close TSDB Head")
		}
		if e := os.RemoveAll(headOpts.ChunkDirRoot); e != nil {
			err = errors.Wrap(e, "delete chunks dir")
		}
	}()

	var g errgroup.Group
	var timeStepSize = (maxt - mint) / int64(numSamples+1)
	var batchSize = len(series) / runtime.GOMAXPROCS(0)

	for len(series) > 0 {
		l := batchSize
		if len(series) < 1000 {
			l = len(series)
		}
		batch := series[:l]
		series = series[l:]

		g.Go(func() error {
			t := mint

			for i := 0; i < numSamples; i++ {
				app := h.Appender(ctx)

				for _, lset := range batch {
					_, err := app.Append(0, lset, t, rand.Float64())
					if err != nil {
						if rerr := app.Rollback(); rerr != nil {
							err = errors.Wrapf(err, "rollback failed: %v", rerr)
						}
						return errors.Wrap(err, "add sample")
					}
				}
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}
				t += timeStepSize
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return id, err
	}
	c, err := tsdb.NewLeveledCompactor(ctx, nil, promslog.NewNopLogger(), []int64{maxt - mint}, nil, nil)
	if err != nil {
		return id, errors.Wrap(err, "create compactor")
	}

	blocks, err := c.Write(dir, h, mint, maxt, nil)
	if err != nil {
		return id, errors.Wrap(err, "write block")
	}

	if len(blocks) == 0 || (blocks[0] == ulid.ULID{}) {
		return id, errors.Errorf("nothing to write, asked for %d samples", numSamples)
	}
	if len(blocks) > 1 {
		return id, errors.Errorf("expected one block, got %d, asked for %d samples", len(blocks), numSamples)
	}

	id = blocks[0]

	blockDir := filepath.Join(dir, id.String())

	if _, err = block.InjectThanosMeta(log.NewNopLogger(), blockDir, block.ThanosMeta{
		Labels: extLset.Map(),
		Source: block.TestSource,
		Files:  []block.File{},
	}, nil); err != nil {
		return id, errors.Wrap(err, "finalize block")
	}

	if err = os.Remove(filepath.Join(dir, id.String(), "tombstones")); err != nil {
		return id, errors.Wrap(err, "remove tombstones")
	}

	return id, nil
}
