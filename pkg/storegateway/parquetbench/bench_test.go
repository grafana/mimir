package parquetbench

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/mimir/pkg/storage/bucket/common"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
	grpc_metadata "google.golang.org/grpc/metadata"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

var (
	user            = flag.String("user", "user-1", "User ID for benchmark")
	bucketEndpoint  = flag.String("bucket.endpoint", "localhost:9000", "S3 endpoint address (e.g., minio instance)")
	bucketName      = flag.String("bucket.name", "tsdb", "S3 bucket name")
	bucketAccessKey = flag.String("bucket.access-key", "mimir", "S3 access key")
	bucketSecretKey = flag.String("bucket.secret-key", "supersecret", "S3 secret key")
	bucketInsecure  = flag.Bool("bucket.insecure", true, "Use insecure S3 connection")
	casesFile       = flag.String("cases", "example_requests.json", "Path to JSON file containing benchmark requests")
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

func BenchmarkBucketStoresComparison(b *testing.B) {
	flag.Parse()

	bkt, err := createTestBucketClient(*bucketEndpoint, *bucketName, *bucketAccessKey, *bucketSecretKey, *bucketInsecure)
	require.NoError(b, err)
	// Test bucket connection
	_, err = bkt.Exists(context.Background(), "test")
	require.NoError(b, err, "Failed to connect to bucket, aborting")

	requests, err := loadBenchmarkRequests(*casesFile)
	require.NoError(b, err)

	ctx := grpc_metadata.NewIncomingContext(b.Context(), grpc_metadata.MD{
		storegateway.GrpcContextMetadataTenantID: []string{*user},
	})

	for _, reqConfig := range requests.LabelValues {
		req := &reqConfig.LabelValuesRequest
		b.Run(fmt.Sprintf("LabelValues-%s", reqConfig.Name), func(tb *testing.B) {
			runBenchmarkComparison(tb, ctx, bkt, func(store storegatewaypb.StoreGatewayServer) error {
				_, err := store.LabelValues(ctx, req)
				return err
			})
		})
	}

	for _, reqConfig := range requests.LabelNames {
		req := &reqConfig.LabelNamesRequest
		b.Run(fmt.Sprintf("LabelNames-%s", reqConfig.Name), func(tb *testing.B) {
			runBenchmarkComparison(tb, ctx, bkt, func(store storegatewaypb.StoreGatewayServer) error {
				_, err := store.LabelNames(ctx, req)
				return err
			})
		})
	}

	for _, reqConfig := range requests.Series {
		req := &reqConfig.SeriesRequest
		b.Run(fmt.Sprintf("Series-%s", reqConfig.Name), func(tb *testing.B) {
			runBenchmarkComparison(tb, ctx, bkt, func(store storegatewaypb.StoreGatewayServer) error {
				mockServer := newMockSeriesServer(ctx)
				err := store.Series(req, mockServer)
				return err
			})
		})
	}
}

func runBenchmarkComparison(b *testing.B, ctx context.Context, bkt objstore.Bucket, operation func(storegatewaypb.StoreGatewayServer) error) {
	run := func(b *testing.B, store storegatewaypb.StoreGatewayServer) {
		fcpu, err := os.Create("profiles/" + strings.ReplaceAll(b.Name(), "/", "_") + "_cpu.prof")
		require.NoError(b, err)
		fmem, err := os.Create("profiles/" + strings.ReplaceAll(b.Name(), "/", "_") + "_mem.prof")
		require.NoError(b, err)
		runtime.GC()

		// Warm up
		require.NoError(b, operation(store))

		err = pprof.StartCPUProfile(fcpu)
		require.NoError(b, err)

		defer func() {
			pprof.StopCPUProfile()
			err = fcpu.Close()
			require.NoError(b, err)

			runtime.GC()
			err = pprof.WriteHeapProfile(fmem)
			require.NoError(b, err)
			err = fmem.Close()
			require.NoError(b, err)
		}()

		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			err := operation(store)
			require.NoError(b, err)
		}
		b.StopTimer()
	}

	b.Run("ParquetBucketStores", func(b *testing.B) {
		pstores := createTestParquetBucketStores(b, bkt)
		require.NoError(b, services.StartAndAwaitRunning(ctx, pstores))
		b.Cleanup(func() {
			require.NoError(b, services.StopAndAwaitTerminated(context.Background(), pstores))
		})

		run(b, pstores)
	})

	b.Run("BucketStores", func(b *testing.B) {
		stores := createTestBucketStores(b, bkt)
		require.NoError(b, services.StartAndAwaitRunning(ctx, stores))
		b.Cleanup(func() {
			require.NoError(b, services.StopAndAwaitTerminated(context.Background(), stores))
		})
		run(b, stores)
	})
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
	return limits
}

func defaultLimitsOverrides() *validation.Overrides {
	return validation.NewOverrides(defaultLimitsConfig(), nil)
}

func createTestParquetBucketStores(b *testing.B, bkt objstore.Bucket) *storegateway.ParquetBucketStores {
	cfg := mimir_tsdb.BlocksStorageConfig{}
	flagext.DefaultValues(&cfg)
	cfg.BucketStore.SyncDir = b.TempDir()

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
