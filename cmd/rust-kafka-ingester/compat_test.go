// SPDX-License-Identifier: AGPL-3.0-only

package rustkafkaingester_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

var buildRustBinary sync.Once

func TestRustDecodesGoIngestStorageRecords(t *testing.T) {
	request := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}, {Name: "job", Value: "api"}},
			Samples:          []mimirpb.Sample{{TimestampMs: 1000, Value: 12.5}},
			Histograms:       []mimirpb.Histogram{{Timestamp: 1001, Count: &mimirpb.Histogram_CountInt{CountInt: 2}, Sum: 3}},
			Exemplars:        []mimirpb.Exemplar{{TimestampMs: 1000, Value: 12.5, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}}},
			CreatedTimestamp: 500,
		}}},
		Metadata: []*mimirpb.MetricMetadata{{Type: mimirpb.COUNTER, MetricFamilyName: "requests_total", Help: "requests", Unit: "requests"}},
		Source:   mimirpb.API,
	}

	for _, version := range []int{0, 1, 2} {
		t.Run(string(rune('0'+version)), func(t *testing.T) {
			serializer := ingest.RecordSerializerFromVersion(version)
			records, _, err := serializer.ToRecords("test", 0, "tenant-a", request, 1<<20)
			require.NoError(t, err)
			require.Len(t, records, 1)

			output := runRust(t, []string{"decode-record", "--version", string(rune('0' + version))}, records[0].Value)
			var decoded struct {
				Source int `json:"source"`
				Series []struct {
					Labels              [][2]string  `json:"labels"`
					Samples             [][2]float64 `json:"samples"`
					HistogramTimestamps []int64      `json:"histogram_timestamps"`
					Exemplars           []any        `json:"exemplars"`
					CreatedTimestamp    int64        `json:"created_timestamp"`
				} `json:"series"`
				Metadata []struct {
					Metric string `json:"metric"`
					Type   int    `json:"type"`
					Help   string `json:"help"`
					Unit   string `json:"unit"`
				} `json:"metadata"`
			}
			require.NoError(t, json.Unmarshal(output, &decoded), string(output))
			require.Equal(t, 0, decoded.Source)
			require.Len(t, decoded.Series, 1)
			require.Equal(t, [][2]string{{"__name__", "requests_total"}, {"job", "api"}}, decoded.Series[0].Labels)
			require.Equal(t, [][2]float64{{1000, 12.5}}, decoded.Series[0].Samples)
			require.Equal(t, int64(500), decoded.Series[0].CreatedTimestamp)
			require.Equal(t, []int64{1001}, decoded.Series[0].HistogramTimestamps)
			require.Len(t, decoded.Series[0].Samples, 1)
			require.Len(t, decoded.Series[0].Exemplars, 1)
			require.Len(t, decoded.Metadata, 1)
			require.Equal(t, "requests_total", decoded.Metadata[0].Metric)
			require.Equal(t, "requests", decoded.Metadata[0].Help)
			require.Equal(t, "requests", decoded.Metadata[0].Unit)
		})
	}
}

func TestRustXORChunkMatchesGo(t *testing.T) {
	samples := [][2]float64{{1000, 1}, {2000, 1}, {3010, 2.5}, {4000, -4}}
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)
	for _, sample := range samples {
		appender.Append(0, int64(sample[0]), sample[1])
	}

	input, err := json.Marshal(samples)
	require.NoError(t, err)
	output := runRust(t, []string{"encode-xor"}, input)
	actual, err := hex.DecodeString(string(output))
	require.NoError(t, err)
	require.Equal(t, chunk.Bytes(), actual)
}

func TestGoIngesterClientReadsRustServer(t *testing.T) {
	intHistogram := &histogram.Histogram{
		// The zero, positive (1 and 3) and negative bucket counts, as Histogram.Validate requires.
		Count: 6, Sum: 5, Schema: 1, ZeroThreshold: 0.001, ZeroCount: 1,
		PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{1, 2},
		NegativeSpans: []histogram.Span{{Offset: -1, Length: 1}}, NegativeBuckets: []int64{1},
	}
	floatHistogram := &histogram.FloatHistogram{
		Count: 3.5, Sum: 6, Schema: 2, ZeroThreshold: 0.0001, ZeroCount: 0.5,
		PositiveSpans: []histogram.Span{{Offset: 1, Length: 2}}, PositiveBuckets: []float64{1.5, 2},
		NegativeSpans: []histogram.Span{{Offset: -2, Length: 1}}, NegativeBuckets: []float64{0.5},
	}
	request := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
		Labels:           []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}, {Name: "job", Value: "api"}},
		Samples:          []mimirpb.Sample{{TimestampMs: 1000, Value: 12.5}},
		Histograms:       []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1100, intHistogram), mimirpb.FromFloatHistogramToHistogramProto(1200, floatHistogram)},
		Exemplars:        []mimirpb.Exemplar{{TimestampMs: 1000, Value: 12.5, Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}}},
		CreatedTimestamp: 500,
	}}}, Metadata: []*mimirpb.MetricMetadata{{Type: mimirpb.COUNTER, MetricFamilyName: "requests_total", Help: "requests", Unit: "requests"}}}
	serializer := ingest.RecordSerializerFromVersion(1)
	records, _, err := serializer.ToRecords("test", 0, "tenant-a", request, 1<<20)
	require.NoError(t, err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	buildBinary(t)
	dataDir := t.TempDir()
	var stderr bytes.Buffer
	cmd := exec.Command("./target/debug/mimir-rust-kafka-ingester", "serve-fixture", "--listen", address, "--version", "1", "--data-dir", dataDir, "--offset", "42", "--timestamp-ms", "1000", "--ingester.max-global-exemplars-per-user", "100")
	cmd.Dir = "."
	cmd.Stdin = bytes.NewReader(records[0].Value)
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(t.Context(), "x-scope-orgid", "tenant-a"), 10*time.Second)
	defer cancel()
	ingesterClient := client.NewIngesterClient(connection)

	var stream client.Ingester_QueryStreamClient
	for {
		stream, err = ingesterClient.QueryStream(ctx, &client.QueryRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   2000,
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "api"}},
		})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			require.NoError(t, err, stderr.String())
		case <-time.After(25 * time.Millisecond):
		}
	}

	series, err := stream.Recv()
	require.NoError(t, err, stderr.String())
	require.True(t, series.IsEndOfSeriesStream)
	require.Len(t, series.StreamingSeries, 1)
	require.Equal(t, int64(3), series.StreamingSeries[0].ChunkCount)

	chunks, err := stream.Recv()
	require.NoError(t, err, stderr.String())
	require.Len(t, chunks.StreamingSeriesChunks, 1)
	require.Len(t, chunks.StreamingSeriesChunks[0].Chunks, 3)
	chunk := chunks.StreamingSeriesChunks[0].Chunks[0]
	require.Equal(t, int32(4), chunk.Encoding)

	expected := chunkenc.NewXORChunk()
	appender, err := expected.Appender()
	require.NoError(t, err)
	appender.Append(0, 500, 0)
	appender.Append(0, 1000, 12.5)
	require.Equal(t, expected.Bytes(), []byte(chunk.Data))

	expectedInt := chunkenc.NewHistogramChunk()
	intAppender, err := expectedInt.Appender()
	require.NoError(t, err)
	_, _, _, err = intAppender.AppendHistogram(nil, 0, 1100, intHistogram, false)
	require.NoError(t, err)
	require.Equal(t, int32(5), chunks.StreamingSeriesChunks[0].Chunks[1].Encoding)
	require.Equal(t, expectedInt.Bytes(), []byte(chunks.StreamingSeriesChunks[0].Chunks[1].Data))

	expectedFloat := chunkenc.NewFloatHistogramChunk()
	floatAppender, err := expectedFloat.Appender()
	require.NoError(t, err)
	_, _, _, err = floatAppender.AppendFloatHistogram(nil, 0, 1200, floatHistogram, false)
	require.NoError(t, err)
	require.Equal(t, int32(6), chunks.StreamingSeriesChunks[0].Chunks[2].Encoding)
	require.Equal(t, expectedFloat.Bytes(), []byte(chunks.StreamingSeriesChunks[0].Chunks[2].Data))
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)

	assertReadAPIs(t, ctx, ingesterClient)
	_, err = ingesterClient.Push(ctx, &mimirpb.WriteRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))

	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	var restoredStderr bytes.Buffer
	restored := exec.Command("./target/debug/mimir-rust-kafka-ingester", "serve-fixture", "--listen", address, "--data-dir", dataDir, "--restore-only", "--ingester.max-global-exemplars-per-user", "100")
	restored.Dir = "."
	restored.Stderr = &restoredStderr
	require.NoError(t, restored.Start())
	t.Cleanup(func() {
		_ = restored.Process.Kill()
		_ = restored.Wait()
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		probe, probeErr := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if probeErr == nil {
			require.NoError(t, probe.Close())
			break
		}
		if time.Now().After(deadline) {
			require.NoError(t, probeErr, restoredStderr.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	restoredConnection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restoredConnection.Close()) })
	restoredCtx, restoredCancel := context.WithTimeout(metadata.AppendToOutgoingContext(t.Context(), "x-scope-orgid", "tenant-a"), 10*time.Second)
	defer restoredCancel()
	restoredStream, err := client.NewIngesterClient(restoredConnection).QueryStream(restoredCtx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   2000,
		Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "api"}},
	})
	require.NoError(t, err, restoredStderr.String())
	restoredSeries, err := restoredStream.Recv()
	require.NoError(t, err, restoredStderr.String())
	require.Len(t, restoredSeries.StreamingSeries, 1)
	restoredChunks, err := restoredStream.Recv()
	require.NoError(t, err, restoredStderr.String())
	require.Equal(t, chunks.StreamingSeriesChunks, restoredChunks.StreamingSeriesChunks)
}

func assertReadAPIs(t *testing.T, ctx context.Context, ingesterClient client.IngesterClient) {
	t.Helper()

	exemplars, err := ingesterClient.QueryExemplars(ctx, &client.ExemplarQueryRequest{StartTimestampMs: 0, EndTimestampMs: 2000, Matchers: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "api"}}}}})
	require.NoError(t, err)
	require.Len(t, exemplars.Timeseries, 1)
	require.Len(t, exemplars.Timeseries[0].Exemplars, 1)

	values, err := ingesterClient.LabelValues(ctx, &client.LabelValuesRequest{LabelName: "job", StartTimestampMs: 0, EndTimestampMs: 2000})
	require.NoError(t, err)
	require.Equal(t, []string{"api"}, values.LabelValues)

	names, err := ingesterClient.LabelNames(ctx, &client.LabelNamesRequest{StartTimestampMs: 0, EndTimestampMs: 2000})
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "job"}, names.LabelNames)

	stats, err := ingesterClient.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.NumSeries)
	allStats, err := ingesterClient.AllUserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	require.Len(t, allStats.Stats, 1)

	metrics, err := ingesterClient.MetricsForLabelMatchers(ctx, &client.MetricsForLabelMatchersRequest{StartTimestampMs: 0, EndTimestampMs: 2000, MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "job", Value: "api"}}}}})
	require.NoError(t, err)
	require.Len(t, metrics.Metric, 1)
	metricMetadata, err := ingesterClient.MetricsMetadata(ctx, &client.MetricsMetadataRequest{Limit: -1})
	require.NoError(t, err)
	require.Len(t, metricMetadata.Metadata, 1)

	namesAndValues, err := ingesterClient.LabelNamesAndValues(ctx, &client.LabelNamesAndValuesRequest{})
	require.NoError(t, err)
	var labelItems int
	for {
		batch, recvErr := namesAndValues.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		labelItems += len(batch.Items)
	}
	require.Equal(t, 2, labelItems)

	cardinality, err := ingesterClient.LabelValuesCardinality(ctx, &client.LabelValuesCardinalityRequest{LabelNames: []string{"job"}})
	require.NoError(t, err)
	cardinalityBatch, err := cardinality.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), cardinalityBatch.Items[0].LabelValueSeries["api"])

	active, err := ingesterClient.ActiveSeries(ctx, &client.ActiveSeriesRequest{})
	require.NoError(t, err)
	activeBatch, err := active.Recv()
	require.NoError(t, err)
	require.Len(t, activeBatch.Metric, 1)
	histogramActive, err := ingesterClient.ActiveSeries(ctx, &client.ActiveSeriesRequest{Type: client.NATIVE_HISTOGRAM_SERIES})
	require.NoError(t, err)
	histogramBatch, err := histogramActive.Recv()
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, histogramBatch.BucketCount)

	searchNames, err := ingesterClient.SearchLabelNames(ctx, &client.SearchLabelNamesRequest{StartTimestampMs: 0, EndTimestampMs: 2000, Filter: &client.SearchFilter{Terms: []string{"na"}}})
	require.NoError(t, err)
	searchNamesBatch, err := searchNames.Recv()
	require.NoError(t, err)
	require.Equal(t, "__name__", searchNamesBatch.Results[0].Value)
	searchValues, err := ingesterClient.SearchLabelValues(ctx, &client.SearchLabelValuesRequest{StartTimestampMs: 0, EndTimestampMs: 2000, Name: "job", Filter: &client.SearchFilter{Terms: []string{"ap"}}})
	require.NoError(t, err)
	searchValuesBatch, err := searchValues.Recv()
	require.NoError(t, err)
	require.Equal(t, "api", searchValuesBatch.Results[0].Value)

	shard := labels.StableHash(labels.FromStrings("__name__", "requests_total", "job", "api")) % 4
	sharded, err := ingesterClient.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   2000,
		Matchers: []*client.LabelMatcher{{
			Type: client.EQUAL, Name: "__query_shard__", Value: fmt.Sprintf("%d_of_4", shard+1),
		}},
	})
	require.NoError(t, err)
	shardedSeries, err := sharded.Recv()
	require.NoError(t, err)
	require.Len(t, shardedSeries.StreamingSeries, 1)

	narrow, err := ingesterClient.QueryStream(ctx, &client.QueryRequest{StartTimestampMs: 1000, EndTimestampMs: 1000})
	require.NoError(t, err)
	_, err = narrow.Recv()
	require.NoError(t, err)
	narrowChunks, err := narrow.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(500), narrowChunks.StreamingSeriesChunks[0].Chunks[0].StartTimestampMs)
}

func runRust(t *testing.T, args []string, input []byte) []byte {
	t.Helper()
	buildBinary(t)
	cmd := exec.Command("./target/debug/mimir-rust-kafka-ingester", args...)
	cmd.Dir = "."
	cmd.Stdin = bytes.NewReader(input)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	return output
}

func buildBinary(t *testing.T) {
	t.Helper()
	buildRustBinary.Do(func() {
		cmd := exec.Command("cargo", "build", "--offline", "--quiet")
		cmd.Dir = "."
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
	})
}
