package parquetbench

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
	grpc_metadata "google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket/common"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	benchmarkStore       = flag.String("benchmark-store", "parquet", "Store type to benchmark: 'parquet' or 'tsdb'")
	benchmarkCompression = flag.Bool("benchmark-compression", true, "Enable compression for parquet data")
	benchmarkSortBy      = flag.String("benchmark-sort-by", "", "Comma-separated list of fields to sort by in parquet data")
	benchmarkTSDBDir     = flag.String("benchmark-tsdb-dir", "", "Path to directory containing pre-generated TSDB blocks (if not set, generates data on the fly)")
)

var benchmarkCases = []struct {
	name     string
	matchers []*labels.Matcher
}{
	// {
	// 	name: "SingleMetricAllSeries",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 	},
	// },
	// {
	// 	name: "SingleMetricReducedSeries",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-1"),
	// 	},
	// },
	// {
	// 	name: "SingleMetricOneSeries",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "instance", "instance-2"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "region", "region-1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "zone", "zone-3"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "service", "service-10"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-1"),
	// 	},
	// },
	// {
	// 	name: "SingleMetricSparseSeries",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "service", "service-1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "environment", "environment-1"),
	// 	},
	// },
	// TODO this one is commented because it returns no series and current implementation requires it
	// {
	// 	name: "NonExistentSeries",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "environment", "non-existent-environment"),
	// 	},
	// },

	// {
	// 	name: "MultipleMetricsRange",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-5]"),
	// 	},
	// },
	// {
	// 	name: "MultipleMetricsSparse",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_(1|5|10|15|20)"),
	// 	},
	// },

	// {
	//	name: "NegativeRegexSingleMetric",
	//	matchers: []*labels.Matcher{
	//		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	//		labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
	//	},
	// },
	// {
	// 	name: "NegativeRegexMultipleMetrics",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
	// 		labels.MustNewMatcher(labels.MatchNotRegexp, "instance", "(instance-1.*|instance-2.*)"),
	// 	},
	// },
	// {
	// 	name: "ExpensiveRegexSingleMetric",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|instance-2|container-3|instance-4|container-5)"),
	// 	},
	// },
	// {
	// 	name: "SpecificZoneWithMetricName",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "__name__", "test_metric_1"),
	// 		labels.MustNewMatcher(labels.MatchEqual, "zone", "zone-3"),
	// 	},
	// },
	// {
	// 	name: "SpecificZoneNoMetricName",
	// 	matchers: []*labels.Matcher{
	// 		labels.MustNewMatcher(labels.MatchEqual, "zone", "zone-3"),
	// 	},
	// },

	{
		name: "SpecificOpsQueryLokiSeries",
		matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "__name__", "loki_loki_pattern_ingester_aggregated_metrics_payload_bytes_bucket"),
			labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod-eu-west-0"),
			labels.MustNewMatcher(labels.MatchEqual, "pod", "pattern-ingester-0"),
			labels.MustNewMatcher(labels.MatchEqual, "service_name", "worker"),
			labels.MustNewMatcher(labels.MatchEqual, "tenant_id", "208466"),
		},
	},
	// TODO find out why it fails
	// {
	//	name: "ExpensiveRegexMultipleMetrics",
	//	matchers: []*labels.Matcher{
	//		labels.MustNewMatcher(labels.MatchRegexp, "__name__", "test_metric_[1-3]"),
	//		labels.MustNewMatcher(labels.MatchRegexp, "instance", "(container-1|container-2|container-3|container-4|container-5)"),
	//	},
	// },
}

func BenchmarkBucketStores_Series(b *testing.B) {
	flag.Parse()
	const user = "benchmark-user"
	var sortByFields []string
	if *benchmarkSortBy != "" {
		fields := strings.Split(*benchmarkSortBy, ",")
		for _, field := range fields {
			if trimmed := strings.TrimSpace(field); trimmed != "" {
				sortByFields = append(sortByFields, trimmed)
			}
		}
	}

	bkt, mint, maxt := setupBenchmarkData(b, user, *benchmarkCompression, sortByFields, *benchmarkTSDBDir)

	ctx := grpc_metadata.NewIncomingContext(b.Context(), grpc_metadata.MD{
		storegateway.GrpcContextMetadataTenantID: []string{user},
	})

	for _, tc := range benchmarkCases {
		b.Run(tc.name, func(tb *testing.B) {
			matchers, err := storepb.PromMatchersToMatchers(tc.matchers...)
			require.NoError(tb, err, "error converting matchers to Prometheus format")
			req := &storepb.SeriesRequest{
				MinTime:                  mint,
				MaxTime:                  maxt,
				Matchers:                 matchers,
				SkipChunks:               false,
				Hints:                    nil,
				StreamingChunksBatchSize: 0,
			}
			runBenchmark(tb, ctx, bkt, func(store storegatewaypb.StoreGatewayServer) {
				mockServer := newMockSeriesServer(ctx)
				err := store.Series(req, mockServer)
				require.NoError(tb, err)
				b.Logf("Received %d series and %d chunks", mockServer.seriesCount, mockServer.chunksCount)
				require.Greater(b, mockServer.seriesCount, 0, "Expected at least one series in response, tc: %s", tc.name)
				require.Greater(b, mockServer.chunksCount, 0, "Expected at least one chunk in response, tc: %s", tc.name)
			})
		})
	}
}

type benchmarkBucket struct {
	objstore.Bucket
	mtx sync.Mutex

	latency       time.Duration
	getCount      int
	getRangeCount int
}

func (b *benchmarkBucket) reset() {
	b.latency = 0
	b.getCount = 0
	b.getRangeCount = 0
}

func (b *benchmarkBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		b.mtx.Lock()
		defer b.mtx.Unlock()
		b.latency += time.Since(start)
		b.getCount++
	}()
	return b.Bucket.Get(ctx, name)
}

func (b *benchmarkBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	start := time.Now()
	defer func() {
		b.mtx.Lock()
		defer b.mtx.Unlock()
		b.latency += time.Since(start)
		b.getRangeCount++
	}()
	return b.Bucket.GetRange(ctx, name, off, length)
}

func runBenchmark(b *testing.B, ctx context.Context, bkt objstore.Bucket, operation func(storegatewaypb.StoreGatewayServer)) {
	benchBkt := &benchmarkBucket{
		Bucket: bkt,
	}

	var store storegatewaypb.StoreGatewayServer
	switch *benchmarkStore {
	case "parquet":
		pstores := createTestParquetBucketStores(b, benchBkt)
		require.NoError(b, services.StartAndAwaitRunning(ctx, pstores))
		b.Cleanup(func() {
			require.NoError(b, services.StopAndAwaitTerminated(context.Background(), pstores))
		})
		store = pstores
	case "tsdb":
		bstores := createTestBucketStores(b, benchBkt)
		require.NoError(b, services.StartAndAwaitRunning(ctx, bstores))
		b.Cleanup(func() {
			require.NoError(b, services.StopAndAwaitTerminated(context.Background(), bstores))
		})
		store = bstores
	default:
		b.Fatalf("Unknown benchmark store type: %s. Valid options are 'parquet' or 'bucket'", *benchmarkStore)
	}

	// Warm up
	operation(store)

	b.ReportAllocs()
	benchBkt.reset()
	b.ResetTimer()
	for b.Loop() {
		operation(store)
	}

	// Note: latency is accumulated over all calls, which might happen concurrently.
	// So the acum latency is not directly comparable to operation time.
	b.ReportMetric(float64(benchBkt.latency.Nanoseconds())/float64(b.N), "ns-bucket-wait/op")
	b.ReportMetric(float64(benchBkt.getCount)/float64(b.N), "bucket-get/op")
	b.ReportMetric(float64(benchBkt.getRangeCount)/float64(b.N), "bucket-get-range/op")
	b.StopTimer()
}

func createTestBucketClient(endpoint, bucketName, accessKey, secretKey string, insecure bool) (objstore.Bucket, error) {
	cfg := s3.Config{
		Endpoint:        endpoint,
		BucketName:      bucketName,
		AccessKeyID:     accessKey,
		SecretAccessKey: flagext.SecretWithValue(secretKey),
		Insecure:        insecure,
		HTTP: common.HTTPConfig{
			IdleConnTimeout:       time.Second * 90,
			ResponseHeaderTimeout: time.Second * 2,
			TLSHandshakeTimeout:   time.Second * 10,
			ExpectContinueTimeout: time.Second * 1,
		},
	}

	return s3.NewBucketClient(cfg, "benchmark", log.NewNopLogger())
}

// loadBenchmarkRequests loads and parses the requests file
func loadBenchmarkRequests(filePath string) (*BenchmarkRequests, error) {
	if filePath == "" {
		return &BenchmarkRequests{}, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read requests file: %w", err)
	}

	var requests BenchmarkRequests
	if err := json.Unmarshal(data, &requests); err != nil {
		return nil, fmt.Errorf("failed to parse requests file: %w", err)
	}

	return &requests, nil
}

func defaultLimitsConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	limits.MaxChunksPerQuery = 200_000_000
	return limits
}

func defaultLimitsOverrides() *validation.Overrides {
	return validation.NewOverrides(defaultLimitsConfig(), nil)
}

func createTestParquetBucketStores(b *testing.B, bkt objstore.Bucket) *storegateway.ParquetBucketStores {
	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = b.TempDir()
	cfg.BucketStore.IgnoreBlocksWithin = 0 // Load all blocks

	allowedTenants := util.NewAllowList(nil, nil)
	shardingStrategy := newNoShardingStrategy()

	stores, err := storegateway.NewParquetBucketStores(
		cfg,
		defaultLimitsOverrides(),
		allowedTenants,
		shardingStrategy,
		bkt,
		log.NewNopLogger(),
		prometheus.NewRegistry(),
	)
	require.NoError(b, err)
	return stores
}

func createTestBucketStores(b *testing.B, bkt objstore.Bucket) *storegateway.BucketStores {
	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = b.TempDir()
	cfg.BucketStore.IgnoreBlocksWithin = 0 // Load all blocks

	allowedTenants := util.NewAllowList(nil, nil)
	shardingStrategy := newNoShardingStrategy()

	stores, err := storegateway.NewBucketStores(
		cfg,
		shardingStrategy,
		bkt,
		allowedTenants,
		defaultLimitsOverrides(),
		log.NewNopLogger(),
		prometheus.NewRegistry(),
	)
	require.NoError(b, err)
	return stores
}

// mockSeriesServer implements storegatewaypb.StoreGateway_SeriesServer for testing
type mockSeriesServer struct {
	grpc.ServerStream
	ctx          context.Context
	seriesCount  int
	chunksCount  int
	lastResponse *storepb.SeriesResponse
}

func newMockSeriesServer(ctx context.Context) *mockSeriesServer {
	return &mockSeriesServer{
		ctx: ctx,
	}
}

func (m *mockSeriesServer) Send(resp *storepb.SeriesResponse) error {
	m.lastResponse = resp
	if resp.GetSeries() != nil {
		m.seriesCount++
		m.chunksCount += len(resp.GetSeries().Chunks)
	}
	return nil
}

func (m *mockSeriesServer) Context() context.Context {
	return m.ctx
}

func (m *mockSeriesServer) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockSeriesServer) RecvMsg(msg interface{}) error {
	return nil
}

func (m *mockSeriesServer) SetHeader(grpc_metadata.MD) error {
	return nil
}

func (m *mockSeriesServer) SendHeader(grpc_metadata.MD) error {
	return nil
}

func (m *mockSeriesServer) SetTrailer(grpc_metadata.MD) {
}

// noShardingStrategy is a no-op sharding strategy for testing
type noShardingStrategy struct{}

func newNoShardingStrategy() *noShardingStrategy {
	return &noShardingStrategy{}
}

func (s *noShardingStrategy) FilterUsers(_ context.Context, userIDs []string) ([]string, error) {
	return userIDs, nil
}

func (s *noShardingStrategy) FilterBlocks(_ context.Context, _ string, _ map[ulid.ULID]*block.Meta, _ map[ulid.ULID]struct{}, _ block.GaugeVec) error {
	return nil
}

type BenchmarkRequests struct {
	LabelValues []BenchmarkLabelValuesRequest `json:"label_values"`
	LabelNames  []BenchmarkLabelNamesRequest  `json:"label_names"`
	Series      []BenchmarkSeriesRequest      `json:"series"`
}

type BenchmarkLabelValuesRequest struct {
	Name string `json:"name"`
	storepb.LabelValuesRequest
}

type BenchmarkLabelNamesRequest struct {
	Name string `json:"name"`
	storepb.LabelNamesRequest
}

type BenchmarkSeriesRequest struct {
	Name string `json:"name"`
	storepb.SeriesRequest
}