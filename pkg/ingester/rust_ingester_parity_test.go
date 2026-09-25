// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

type parityIngester struct {
	api     client.IngesterClient
	offset  int64
	startup time.Duration
}

// Both services consume the same partition, so the comparison includes Kafka decoding and replay.
func startParityIngesters(tb testing.TB, produce func(testing.TB, ingest.KafkaConfig) int64) (parityIngester, parityIngester) {
	tb.Helper()
	buildRustIngester(tb)
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(tb)
	cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * 60 * 60 * 1000
	limits.NativeHistogramsIngestionEnabled = true
	limits.MaxGlobalExemplarsPerUser = 100
	goIngester, _, _ := createTestIngesterWithIngestStorage(tb, &cfg, validation.NewOverrides(limits, nil), nil, nil, nil)
	offset := produce(tb, cfg.IngestStorageConfig.KafkaConfig)
	require.GreaterOrEqual(tb, offset, int64(0))

	goStarted := time.Now()
	require.NoError(tb, services.StartAndAwaitRunning(ctx, goIngester))
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	require.NoError(tb, goIngester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(offset)))
	goStartup := time.Since(goStarted)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
	)
	client.RegisterIngesterServer(server, goIngester)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	go func() { _ = server.Serve(listener) }()
	goConn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	tb.Cleanup(func() {
		_ = goConn.Close()
		server.Stop()
		require.NoError(tb, services.StopAndAwaitTerminated(ctx, goIngester))
	})

	rustStarted := time.Now()
	process, rustConn := startRustIngesterAndWait(tb, cfg.IngestStorageConfig.KafkaConfig.Address[0], cfg.IngestStorageConfig.KafkaConfig.Topic, tb.TempDir(), offset)
	rustStartup := time.Since(rustStarted)
	tb.Cleanup(func() {
		_ = rustConn.Close()
		stopRustIngester(tb, process)
	})
	return parityIngester{client.NewIngesterClient(goConn), offset, goStartup}, parityIngester{client.NewIngesterClient(rustConn), offset, rustStartup}
}

func parityContext(tenant string, offset int64) context.Context {
	return metadata.AppendToOutgoingContext(context.Background(),
		"x-scope-orgid", tenant,
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", offset))
}

func paritySeries(tb testing.TB, api client.IngesterClient, ctx context.Context, request *client.QueryRequest) map[string][]string {
	tb.Helper()
	stream, err := api.QueryStream(ctx, request)
	require.NoError(tb, err)
	var series []client.QueryStreamSeries
	var chunks []client.QueryStreamSeriesChunks
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(tb, err)
		copy := proto.Clone(response).(*client.QueryStreamResponse)
		series = append(series, copy.StreamingSeries...)
		chunks = append(chunks, copy.StreamingSeriesChunks...)
	}
	actual := map[string][]string{}
	for _, s := range series {
		key := mimirpb.FromLabelAdaptersToLabels(s.Labels).String()
		_, exists := actual[key]
		require.False(tb, exists, "duplicate series %s", key)
		actual[key] = []string{}
	}
	for _, group := range chunks {
		require.Less(tb, group.SeriesIndex, uint64(len(series)))
		key := mimirpb.FromLabelAdaptersToLabels(series[group.SeriesIndex].Labels).String()
		for _, wireChunk := range group.Chunks {
			var encoding chunkenc.Encoding
			switch chunk.Encoding(wireChunk.Encoding) {
			case chunk.PrometheusXorChunk:
				encoding = chunkenc.EncXOR
			case chunk.PrometheusXor2Chunk:
				encoding = chunkenc.EncXOR2
			case chunk.PrometheusHistogramChunk:
				encoding = chunkenc.EncHistogram
			case chunk.PrometheusFloatHistogramChunk:
				encoding = chunkenc.EncFloatHistogram
			default:
				tb.Fatalf("unknown wire chunk encoding %d", wireChunk.Encoding)
			}
			decoded, err := chunkenc.FromData(encoding, wireChunk.Data)
			require.NoError(tb, err)
			it := decoded.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				var timestamp int64
				var value string
				switch typ {
				case chunkenc.ValFloat:
					var sample float64
					timestamp, sample = it.At()
					value = fmt.Sprintf("f:%016x", math.Float64bits(sample))
				case chunkenc.ValHistogram:
					var h *histogram.Histogram
					timestamp, h = it.AtHistogram(nil)
					h.CounterResetHint = histogram.UnknownCounterReset
					value = fmt.Sprintf("h:%#v", *h)
				case chunkenc.ValFloatHistogram:
					var h *histogram.FloatHistogram
					timestamp, h = it.AtFloatHistogram(nil)
					h.CounterResetHint = histogram.UnknownCounterReset
					value = fmt.Sprintf("fh:%#v", *h)
				default:
					tb.Fatalf("unexpected chunk value type %v", typ)
				}
				if timestamp >= request.StartTimestampMs && timestamp <= request.EndTimestampMs {
					actual[key] = append(actual[key], fmt.Sprintf("%d:%s", timestamp, value))
				}
			}
			require.NoError(tb, it.Err(), "series=%s encoding=%d bytes=%d", key, wireChunk.Encoding, len(wireChunk.Data))
		}
	}
	for key := range actual {
		sort.Strings(actual[key])
	}
	return actual
}

func parityRequest(start, end int64, matchers ...*client.LabelMatcher) *client.QueryRequest {
	return &client.QueryRequest{StartTimestampMs: start, EndTimestampMs: end, Matchers: matchers, StreamingChunksBatchSize: 64}
}

func parityMatcher(kind client.MatchType, name, value string) *client.LabelMatcher {
	return &client.LabelMatcher{Type: kind, Name: name, Value: value}
}

func TestGoRustIngesterParity(t *testing.T) {
	goSide, rustSide := startParityIngesters(t, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		producer, err := ingest.NewKafkaWriterClient(cfg, 20, log.NewNopLogger(), nil)
		require.NoError(tb, err)
		defer producer.Close()
		var lastOffset int64 = -1
		write := func(version int, tenant string, request *mimirpb.WriteRequest) {
			records, _, err := ingest.RecordSerializerFromVersion(version).ToRecords(cfg.Topic, 0, tenant, request, 1<<20)
			require.NoError(tb, err)
			require.NotEmpty(tb, records)
			require.NoError(tb, producer.ProduceSync(context.Background(), records...).FirstErr())
			lastOffset = records[len(records)-1].Offset
		}
		for tenantIndex, tenant := range []string{"tenant-a", "tenant-b"} {
			for version := 0; version <= 2; version++ {
				series := make([]mimirpb.PreallocTimeseries, 0, 72)
				for n := range 72 {
					samples := make([]mimirpb.Sample, 0, 9)
					for point := range 9 {
						samples = append(samples, mimirpb.Sample{TimestampMs: int64(1_000 + point*1_000), Value: float64(tenantIndex*100000+version*1000+n*10+point) / 8})
					}
					series = append(series, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
						Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "parity_metric"}, {Name: "batch", Value: fmt.Sprintf("v%d", version)}, {Name: "group", Value: fmt.Sprintf("g%d", n%7)}, {Name: "series", Value: fmt.Sprintf("s%03d", n)}},
						Samples: samples,
					}})
				}
				write(version, tenant, &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
			}
		}
		intHistogram := &histogram.Histogram{Count: 3, Sum: 5, Schema: 1, ZeroThreshold: 0.001, ZeroCount: 1, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{1, 0}}
		floatHistogram := &histogram.FloatHistogram{Count: 3.5, Sum: 6, Schema: 2, ZeroThreshold: 0.0001, ZeroCount: 0.5, PositiveSpans: []histogram.Span{{Offset: 1, Length: 2}}, PositiveBuckets: []float64{1.5, 2}}
		longFloatSamples := make([]mimirpb.Sample, 250)
		for point := range longFloatSamples {
			longFloatSamples[point] = mimirpb.Sample{TimestampMs: int64(20_000 + point*1_000), Value: float64(point*point) / 8}
		}
		write(1, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "special_metric"}, {Name: "kind", Value: "float"}},
				Samples:          []mimirpb.Sample{{TimestampMs: 1000, Value: -1.25}, {TimestampMs: 3000, Value: 0}, {TimestampMs: 5000, Value: 1.0 / 3}},
				Exemplars:        []mimirpb.Exemplar{{TimestampMs: 1000, Value: -1.25, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}}},
				CreatedTimestamp: 500,
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:     []mimirpb.LabelAdapter{{Name: "__name__", Value: "special_metric"}, {Name: "kind", Value: "int-histogram"}},
				Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(2000, intHistogram)},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:     []mimirpb.LabelAdapter{{Name: "__name__", Value: "special_metric"}, {Name: "kind", Value: "float-histogram"}},
				Histograms: []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(4000, floatHistogram)},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:  []mimirpb.LabelAdapter{{Name: "__name__", Value: "long_float_metric"}},
				Samples: longFloatSamples,
			}},
		}, Metadata: []*mimirpb.MetricMetadata{{Type: mimirpb.GAUGE, MetricFamilyName: "special_metric", Help: "parity", Unit: "items"}}})
		packedInt := make([]mimirpb.Histogram, 0, 250)
		packedFloat := make([]mimirpb.Histogram, 0, 250)
		for point := range 250 {
			integer := intHistogram.Copy()
			integer.ZeroCount += uint64(point / 3)
			integer.Count += uint64(point + point/3)
			integer.Sum += float64(point) * 1.25
			integer.PositiveBuckets[1] += int64(point)
			packedInt = append(packedInt, mimirpb.FromHistogramToHistogramProto(int64(20_000+point*1_000), integer))
			floating := floatHistogram.Copy()
			floating.Count += float64(point) * 1.5
			floating.ZeroCount += float64(point) * 0.25
			floating.Sum += float64(point) * 2.5
			floating.PositiveBuckets[0] += float64(point) * 0.5
			packedFloat = append(packedFloat, mimirpb.FromFloatHistogramToHistogramProto(int64(20_000+point*1_000), floating))
		}
		write(1, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "packed_int_metric"}}, Histograms: packedInt}},
		}, Source: mimirpb.API})
		write(1, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "packed_float_metric"}}, Histograms: packedFloat}},
		}, Source: mimirpb.API})
		edgeLabels := []mimirpb.LabelAdapter{{Name: "__name__", Value: "edge_metric"}, {Name: "long_label_name_for_parity", Value: "a value with spaces, punctuation: /._-"}}
		write(2, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels: edgeLabels,
			Samples: []mimirpb.Sample{
				{TimestampMs: 1000, Value: math.Copysign(0, -1)},
				{TimestampMs: 3000, Value: 1e-200},
				{TimestampMs: 5000, Value: 1e200},
			},
		}}}, Source: mimirpb.API})
		write(2, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels: edgeLabels, Samples: []mimirpb.Sample{{TimestampMs: 3000, Value: 1e-200}},
		}}}, Source: mimirpb.API})
		write(2, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels: edgeLabels, Samples: []mimirpb.Sample{{TimestampMs: 3000, Value: 42}, {TimestampMs: 7000, Value: 7}},
		}}}, Source: mimirpb.API})
		write(2, "tenant-a", &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "name", Value: "a"}, {Name: "name", Value: "b"}}, Samples: []mimirpb.Sample{{TimestampMs: 9000, Value: 1}}}},
			{TimeSeries: &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "after_invalid"}}, Samples: []mimirpb.Sample{{TimestampMs: 9000, Value: 1}}}},
		}, Source: mimirpb.API})
		return lastOffset
	})

	cases := []struct {
		name   string
		tenant string
		query  *client.QueryRequest
	}{
		{"all-tenant-a", "tenant-a", parityRequest(0, 10000, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))},
		{"all-tenant-b", "tenant-b", parityRequest(0, 10000, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))},
		{"version-zero", "tenant-a", parityRequest(0, 10000, parityMatcher(client.EQUAL, "batch", "v0"))},
		{"version-two", "tenant-a", parityRequest(0, 10000, parityMatcher(client.EQUAL, "batch", "v2"))},
		{"regex", "tenant-b", parityRequest(0, 10000, parityMatcher(client.REGEX_MATCH, "series", "s00[1-5]"))},
		{"negative-match", "tenant-a", parityRequest(0, 10000, parityMatcher(client.NOT_EQUAL, "group", "g1"), parityMatcher(client.EQUAL, "batch", "v1"))},
		{"narrow-time", "tenant-a", parityRequest(2500, 5500, parityMatcher(client.EQUAL, "__name__", "special_metric"))},
		{"out-of-order-and-duplicate", "tenant-a", parityRequest(0, 10_000, parityMatcher(client.EQUAL, "__name__", "edge_metric"))},
		{"missing", "tenant-b", parityRequest(0, 10000, parityMatcher(client.EQUAL, "__name__", "special_metric"))},
		{"packed-int", "tenant-a", parityRequest(20_000, 300_000, parityMatcher(client.EQUAL, "__name__", "packed_int_metric"))},
		{"packed-float", "tenant-a", parityRequest(20_000, 300_000, parityMatcher(client.EQUAL, "__name__", "packed_float_metric"))},
		{"long-float", "tenant-a", parityRequest(40_000, 160_000, parityMatcher(client.EQUAL, "__name__", "long_float_metric"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goSeries := paritySeries(t, goSide.api, parityContext(tc.tenant, goSide.offset), tc.query)
			rustSeries := paritySeries(t, rustSide.api, parityContext(tc.tenant, rustSide.offset), tc.query)
			if len(rustSeries) != len(goSeries) {
				t.Errorf("series count: Go=%d Rust=%d", len(goSeries), len(rustSeries))
			}
			for labels, goSamples := range goSeries {
				rustSamples, exists := rustSeries[labels]
				require.True(t, exists, "missing series %s", labels)
				require.Equal(t, goSamples, rustSamples, "series %s", labels)
			}
			for labels := range rustSeries {
				_, exists := goSeries[labels]
				require.True(t, exists, "Rust-only series %s", labels)
			}
		})
	}
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		t.Run("labels-"+tenant, func(t *testing.T) {
			goCtx := parityContext(tenant, goSide.offset)
			rustCtx := parityContext(tenant, rustSide.offset)
			request := &client.LabelNamesRequest{StartTimestampMs: 0, EndTimestampMs: 300_000}
			goNames, err := goSide.api.LabelNames(goCtx, request)
			require.NoError(t, err)
			rustNames, err := rustSide.api.LabelNames(rustCtx, request)
			require.NoError(t, err)
			sort.Strings(goNames.LabelNames)
			sort.Strings(rustNames.LabelNames)
			require.Equal(t, goNames.LabelNames, rustNames.LabelNames)
			for _, name := range []string{"__name__", "batch", "group", "series", "kind"} {
				request := &client.LabelValuesRequest{LabelName: name, StartTimestampMs: 0, EndTimestampMs: 300_000}
				goValues, err := goSide.api.LabelValues(goCtx, request)
				require.NoError(t, err)
				rustValues, err := rustSide.api.LabelValues(rustCtx, request)
				require.NoError(t, err)
				sort.Strings(goValues.LabelValues)
				sort.Strings(rustValues.LabelValues)
				require.Equal(t, goValues.LabelValues, rustValues.LabelValues, "label %s", name)
			}
			goStats, err := goSide.api.UserStats(goCtx, &client.UserStatsRequest{})
			require.NoError(t, err)
			rustStats, err := rustSide.api.UserStats(rustCtx, &client.UserStatsRequest{})
			require.NoError(t, err)
			require.Equal(t, goStats.NumSeries, rustStats.NumSeries)
			cardinality := func(api client.IngesterClient, ctx context.Context) map[string]map[string]uint64 {
				stream, err := api.LabelValuesCardinality(ctx, &client.LabelValuesCardinalityRequest{LabelNames: []string{"batch", "group", "kind"}})
				require.NoError(t, err)
				result := map[string]map[string]uint64{}
				for {
					response, err := stream.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					require.NoError(t, err)
					for _, item := range response.Items {
						require.NotContains(t, result, item.LabelName)
						result[item.LabelName] = item.LabelValueSeries
					}
				}
				return result
			}
			require.Equal(t, cardinality(goSide.api, goCtx), cardinality(rustSide.api, rustCtx))
		})
	}
	t.Run("exemplars-and-metadata", func(t *testing.T) {
		goCtx := parityContext("tenant-a", goSide.offset)
		rustCtx := parityContext("tenant-a", rustSide.offset)
		exemplarRequest := &client.ExemplarQueryRequest{
			StartTimestampMs: 0, EndTimestampMs: 10_000,
			Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{parityMatcher(client.EQUAL, "__name__", "special_metric")}}},
		}
		goExemplars, err := goSide.api.QueryExemplars(goCtx, exemplarRequest)
		require.NoError(t, err)
		rustExemplars, err := rustSide.api.QueryExemplars(rustCtx, exemplarRequest)
		require.NoError(t, err)
		require.Equal(t, goExemplars.Timeseries, rustExemplars.Timeseries)
		metadataRequest := &client.MetricsMetadataRequest{MetricNames: []string{"special_metric"}}
		goMetadata, err := goSide.api.MetricsMetadata(goCtx, metadataRequest)
		require.NoError(t, err)
		rustMetadata, err := rustSide.api.MetricsMetadata(rustCtx, metadataRequest)
		require.NoError(t, err)
		require.Equal(t, goMetadata.Metadata, rustMetadata.Metadata)
	})
}

func TestRustIngesterRestartsFromSavedOffset(t *testing.T) {
	buildRustIngester(t)
	const topic = "mimir"
	_, address := testkafka.CreateCluster(t, 10, topic)
	producer, err := kgo.NewClient(kgo.SeedBrokers(address), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(producer.Close)
	labels := []mimirpb.LabelAdapter{{Name: "__name__", Value: "restart_metric"}, {Name: "job", Value: "parity"}}
	produce := func(timestamp int64, value float64) int64 {
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords(topic, 0, "tenant-a", &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
				Labels: labels, Samples: []mimirpb.Sample{{TimestampMs: timestamp, Value: value}},
			}}}, Source: mimirpb.API,
		}, 1<<20)
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.NoError(t, producer.ProduceSync(t.Context(), records...).FirstErr())
		return records[0].Offset
	}
	dataDir := t.TempDir()
	firstOffset := produce(1000, 1.25)
	process, connection := startRustIngesterAndWait(t, address, topic, dataDir, firstOffset,
		"--segment-sync-interval-ms", "60000")
	t.Cleanup(func() {
		if process != nil {
			require.NoError(t, connection.Close())
			stopRustIngester(t, process)
		}
	})
	checkpointOffset := func() (int64, bool) {
		paths, err := filepath.Glob(filepath.Join(dataDir, "cluster-*", "checkpoint"))
		if err != nil || len(paths) != 1 {
			return 0, false
		}
		contents, err := os.ReadFile(paths[0])
		if err != nil || len(contents) != 24 {
			return 0, false
		}
		return int64(binary.LittleEndian.Uint64(contents[12:20])), true
	}
	require.NoError(t, connection.Close())
	require.NoError(t, process.Process.Signal(syscall.SIGTERM))
	require.NoError(t, process.Wait())
	process = nil
	connection = nil
	require.Eventually(t, func() bool {
		offset, ok := checkpointOffset()
		return ok && offset == firstOffset
	}, 10*time.Second, 10*time.Millisecond)

	secondOffset := produce(2000, 2.5)
	require.Greater(t, secondOffset, firstOffset)
	process, connection = startRustIngesterAndWait(t, address, topic, dataDir, secondOffset,
		"--segment-sync-interval-ms", "10", "--start-offset", "latest")
	require.Eventually(t, func() bool {
		offset, ok := checkpointOffset()
		return ok && offset == secondOffset
	}, 10*time.Second, 10*time.Millisecond)
	query := parityRequest(0, 3000, parityMatcher(client.EQUAL, "__name__", "restart_metric"))
	actual := paritySeries(t, client.NewIngesterClient(connection), parityContext("tenant-a", secondOffset), query)
	expected := map[string][]string{
		`{__name__="restart_metric", job="parity"}`: {
			fmt.Sprintf("1000:f:%016x", math.Float64bits(1.25)),
			fmt.Sprintf("2000:f:%016x", math.Float64bits(2.5)),
		},
	}
	require.Equal(t, expected, actual)
	require.NoError(t, connection.Close())
	stopRustIngester(t, process)
	process = nil
	connection = nil

	process, connection = startRustIngesterAndWait(t, address, topic, dataDir, secondOffset,
		"--segment-sync-interval-ms", "10", "--start-offset", "latest")
	actual = paritySeries(t, client.NewIngesterClient(connection), parityContext("tenant-a", secondOffset), query)
	require.Equal(t, expected, actual)
}

func BenchmarkGoRustIngesterSameKafka(b *testing.B) {
	const series = 5_000
	goSide, rustSide := startParityIngesters(b, func(tb testing.TB, cfg ingest.KafkaConfig) int64 {
		generator, err := ingest.NewFixtureGenerator(queryFixture(series), 1, 0)
		require.NoError(tb, err)
		records, err := generator.ProduceWriteRequests(context.Background(), flagext.StringSliceCSV(cfg.Address), cfg.Topic, 0)
		require.NoError(tb, err)
		return int64(records - 1)
	})
	request := parityRequest(math.MinInt64, math.MaxInt64, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))
	goCtx := parityContext("tenant-0", goSide.offset)
	rustCtx := parityContext("tenant-0", rustSide.offset)
	require.Equal(b, paritySeries(b, goSide.api, goCtx, request), paritySeries(b, rustSide.api, rustCtx, request))
	for _, side := range []struct {
		name   string
		api    client.IngesterClient
		replay time.Duration
	}{{"Go", goSide.api, goSide.startup}, {"Rust", rustSide.api, rustSide.startup}} {
		b.Run(side.name+"/QueryStream", func(b *testing.B) {
			benchmarkQueryStream(b, side.api, goSide.offset, series, 0)
			b.ReportMetric(float64(side.replay.Milliseconds()), "replay-ms")
		})
		b.Run(side.name+"/LabelValuesCardinality", func(b *testing.B) {
			benchmarkLabelValuesCardinality(b, side.api, goSide.offset)
			b.ReportMetric(float64(side.replay.Milliseconds()), "replay-ms")
		})
	}
}
