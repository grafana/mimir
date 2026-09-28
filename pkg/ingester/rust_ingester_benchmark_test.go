// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	buildRustIngesterOnce sync.Once
	buildRustIngesterErr  error
)

func BenchmarkRustIngester_ReplayFromKafka(b *testing.B) {
	benchmarkRustIngesterStartup(b, false)
}

func BenchmarkRustIngester_RestoreFromSegments(b *testing.B) {
	benchmarkRustIngesterStartup(b, true)
}

func BenchmarkKafkaIngester_QueryStream(b *testing.B) {
	const series = 25_000
	b.Run("Go", func(b *testing.B) {
		api, targetOffset := startGoIngesterForQuery(b, series)
		benchmarkQueryStream(b, api, targetOffset, series, 0)
	})
	b.Run("Rust", func(b *testing.B) {
		api, targetOffset, processID := startRustIngesterForQuery(b, series)
		benchmarkQueryStream(b, api, targetOffset, series, processID)
	})
}

func BenchmarkKafkaIngester_LabelValuesCardinality(b *testing.B) {
	const series = 25_000
	b.Run("Go", func(b *testing.B) {
		api, targetOffset := startGoIngesterForQuery(b, series)
		benchmarkLabelValuesCardinality(b, api, targetOffset)
	})
	b.Run("Rust", func(b *testing.B) {
		api, targetOffset, _ := startRustIngesterForQuery(b, series)
		benchmarkLabelValuesCardinality(b, api, targetOffset)
	})
}

func benchmarkRustIngesterStartup(b *testing.B, warm bool) {
	const (
		partitionID = 0
		topic       = "mimir"
	)

	buildRustIngester(b)
	_, kafkaAddress := testkafka.CreateCluster(b, 10, topic)
	fixture := ingest.FixtureConfig{
		SmallTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
		MediumTenants:          ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
		LargeTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 100, TimeseriesPercent: 100, AvgTimeseriesPerReq: 350},
		AvgLabelsPerTimeseries: 19,
		AvgLabelNameLength:     9,
		AvgLabelValueLength:    17,
		AvgMetricNameLength:    40,
		NumUniqueLabelNames:    2_500,
		NumUniqueLabelValues:   1_000_000,
		TotalUniqueTimeseries:  b.N,
		TotalSamples:           b.N * 2,
	}
	generator, err := ingest.NewFixtureGenerator(fixture, 1, 0)
	require.NoError(b, err)
	records, err := generator.ProduceWriteRequests(context.Background(), flagext.StringSliceCSV{kafkaAddress}, topic, partitionID)
	require.NoError(b, err)
	targetOffset := int64(records - 1)
	dataDir := b.TempDir()

	if warm {
		process, connection := startRustIngesterAndWait(b, kafkaAddress, topic, dataDir, targetOffset)
		require.NoError(b, connection.Close())
		stopRustIngester(b, process)
	}

	b.ResetTimer()
	started := time.Now()
	stopSample := make(chan struct{})
	sampled := make(chan replayProcessUsage, 1)
	process, connection := startRustIngesterAndWaitObserved(b, kafkaAddress, topic, dataDir, targetOffset, func(pid int) {
		go func() {
			var peak replayProcessUsage
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				if usage, err := sampleReplayProcess(pid); err == nil {
					peak.cpuSeconds = usage.cpuSeconds
					peak.rssBytes = max(peak.rssBytes, usage.rssBytes)
				}
				select {
				case <-stopSample:
					sampled <- peak
					return
				case <-ticker.C:
				}
			}
		}()
	})
	elapsed := time.Since(started)
	close(stopSample)
	usage := <-sampled
	b.StopTimer()
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "series/s")
	b.ReportMetric(float64(records)/elapsed.Seconds(), "records/s")
	b.ReportMetric(float64(b.N*2)/elapsed.Seconds(), "samples/s")
	b.ReportMetric(usage.cpuSeconds, "process-CPU-s")
	b.ReportMetric(float64(usage.rssBytes), "peak-RSS-B")
	require.NoError(b, connection.Close())
	stopRustIngester(b, process)
}

func queryFixture(series int) ingest.FixtureConfig {
	return ingest.FixtureConfig{
		SmallTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
		MediumTenants:          ingest.FixtureTenantClassConfig{TenantPercent: 0, TimeseriesPercent: 0, AvgTimeseriesPerReq: 0},
		LargeTenants:           ingest.FixtureTenantClassConfig{TenantPercent: 100, TimeseriesPercent: 100, AvgTimeseriesPerReq: 350},
		AvgLabelsPerTimeseries: 19,
		AvgLabelNameLength:     9,
		AvgLabelValueLength:    17,
		AvgMetricNameLength:    40,
		NumUniqueLabelNames:    2_500,
		NumUniqueLabelValues:   1_000_000,
		TotalUniqueTimeseries:  series,
		TotalSamples:           series * 20,
	}
}

func startRustIngesterForQuery(b *testing.B, series int) (client.IngesterClient, int64, int) {
	b.Helper()
	buildRustIngester(b)
	const topic = "mimir"
	_, kafkaAddress := testkafka.CreateCluster(b, 10, topic)
	generator, err := ingest.NewFixtureGenerator(queryFixture(series), 1, 0)
	require.NoError(b, err)
	records, err := generator.ProduceWriteRequests(context.Background(), flagext.StringSliceCSV{kafkaAddress}, topic, 0)
	require.NoError(b, err)
	targetOffset := int64(records - 1)
	process, connection := startRustIngesterAndWait(b, kafkaAddress, topic, b.TempDir(), targetOffset)
	b.Cleanup(func() {
		_ = connection.Close()
		stopRustIngester(b, process)
	})
	return client.NewIngesterClient(connection), targetOffset, process.Process.Pid
}

func startGoIngesterForQuery(b *testing.B, series int) (client.IngesterClient, int64) {
	b.Helper()
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	ingester, _, _ := createTestIngesterWithIngestStorage(b, &cfg, overrides, nil, nil, nil)
	generator, err := ingest.NewFixtureGenerator(queryFixture(series), 1, 0)
	require.NoError(b, err)
	records, err := generator.ProduceWriteRequests(ctx, cfg.IngestStorageConfig.KafkaConfig.Address, cfg.IngestStorageConfig.KafkaConfig.Topic, 0)
	require.NoError(b, err)
	targetOffset := int64(records - 1)
	require.NoError(b, services.StartAndAwaitRunning(ctx, ingester))
	require.NoError(b, ingester.ingestReader.WaitReadConsistencyUntilOffsets(ctx, kmeta.NewSingleClusterPartitionOffsets(targetOffset)))

	server := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	client.RegisterIngesterServer(server, ingester)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	b.Cleanup(func() {
		_ = connection.Close()
		server.Stop()
		require.NoError(b, services.StopAndAwaitTerminated(ctx, ingester))
	})
	return client.NewIngesterClient(connection), targetOffset
}

func benchmarkQueryStream(b *testing.B, api client.IngesterClient, targetOffset int64, expectedSeries int, processID int) {
	b.Helper()
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"x-scope-orgid", "tenant-0",
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", targetOffset),
	)
	request := &client.QueryRequest{
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers: []*client.LabelMatcher{{
			Type:  client.REGEX_MATCH,
			Name:  "__name__",
			Value: ".+",
		}},
	}
	startupRSS := processRSSBytes(b, processID)
	runQuery := func() int64 {
		stream, err := api.QueryStream(ctx, request)
		require.NoError(b, err)
		var bytes int64
		seen := 0
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(b, err)
			bytes += int64(response.Size())
			seen += len(response.StreamingSeries)
		}
		require.Equal(b, expectedSeries, seen)
		return bytes
	}
	coldStarted := time.Now()
	coldBytes := runQuery()
	coldElapsed := time.Since(coldStarted)
	var responseBytes int64
	b.ResetTimer()
	for range b.N {
		responseBytes += runQuery()
	}
	b.StopTimer()
	require.Positive(b, coldBytes)
	b.ReportMetric(float64(coldElapsed.Microseconds())/1000, "cold-query-ms")
	if processID != 0 {
		b.ReportMetric(float64(startupRSS), "startup-RSS-B")
		b.ReportMetric(float64(processRSSBytes(b, processID)), "query-RSS-B")
	}
	b.ReportMetric(float64(responseBytes)/float64(b.N), "response-B/op")
}

func processRSSBytes(b *testing.B, processID int) uint64 {
	b.Helper()
	if processID == 0 {
		return 0
	}
	rss, err := readProcessRSSBytes(processID)
	require.NoError(b, err)
	return rss
}

func benchmarkLabelValuesCardinality(b *testing.B, api client.IngesterClient, targetOffset int64) {
	b.Helper()
	ctx := metadata.AppendToOutgoingContext(
		context.Background(),
		"x-scope-orgid", "tenant-0",
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", targetOffset),
	)
	request := &client.LabelValuesCardinalityRequest{
		LabelNames: []string{"label_0__"},
		Matchers: []*client.LabelMatcher{{
			Type:  client.REGEX_MATCH,
			Name:  "__name__",
			Value: ".+",
		}},
	}
	var responseBytes int64
	b.ResetTimer()
	for range b.N {
		stream, err := api.LabelValuesCardinality(ctx, request)
		require.NoError(b, err)
		for {
			response, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(b, err)
			responseBytes += int64(response.Size())
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(responseBytes)/float64(b.N), "response-B/op")
}

func buildRustIngester(tb testing.TB) {
	tb.Helper()
	buildRustIngesterOnce.Do(func() {
		command := exec.Command("cargo", "build", "--offline", "--release", "--manifest-path", "../../cmd/rust-kafka-ingester/Cargo.toml")
		command.Dir = "."
		var output bytes.Buffer
		command.Stdout = &output
		command.Stderr = &output
		if err := command.Run(); err != nil {
			buildRustIngesterErr = fmt.Errorf("build Rust ingester: %w\n%s", err, output.String())
		}
	})
	require.NoError(tb, buildRustIngesterErr)
}

func startRustIngesterAndWait(
	tb testing.TB,
	kafkaAddress string,
	topic string,
	dataDir string,
	targetOffset int64,
	extraArgs ...string,
) (*exec.Cmd, *grpc.ClientConn) {
	return startRustIngesterAndWaitObserved(tb, kafkaAddress, topic, dataDir, targetOffset, nil, extraArgs...)
}

func startRustIngesterAndWaitObserved(
	tb testing.TB,
	kafkaAddress string,
	topic string,
	dataDir string,
	targetOffset int64,
	onStarted func(int),
	extraArgs ...string,
) (*exec.Cmd, *grpc.ClientConn) {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	address := listener.Addr().String()
	require.NoError(tb, listener.Close())

	binary, err := filepath.Abs("../../cmd/rust-kafka-ingester/target/release/mimir-rust-kafka-ingester")
	require.NoError(tb, err)
	var stderr bytes.Buffer
	args := []string{
		"serve",
		"--brokers", kafkaAddress,
		"--topic", topic,
		"--partition", "0",
		"--data-dir", dataDir,
		"--listen", address,
		"--consistency-timeout-seconds", "600",
	}
	process := exec.Command(binary, append(args, extraArgs...)...)
	if listen := os.Getenv("MIMIR_RUST_INGESTER_PROFILE_LISTEN"); listen != "" {
		process = exec.Command(binary, append(append(args, "--profile-listen", listen), extraArgs...)...)
	}
	process.Stderr = &stderr
	require.NoError(tb, process.Start(), stderr.String())
	if onStarted != nil {
		onStarted(process.Process.Pid)
	}

	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(tb, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		"x-scope-orgid", "benchmark-tenant",
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", targetOffset),
	)
	_, err = client.NewIngesterClient(connection).UserStats(
		ctx,
		&client.UserStatsRequest{},
		grpc.WaitForReady(true),
	)
	// An ingester that limits reads on its utilization rejects the probe once it serves.
	if status.Code(err) == codes.Unavailable && strings.Contains(err.Error(), "too busy") {
		err = nil
	}
	if err != nil {
		_ = connection.Close()
		stopRustIngester(tb, process)
	}
	require.NoError(tb, err, stderr.String())
	return process, connection
}

type replayProcessUsage struct {
	cpuSeconds float64
	rssBytes   uint64
}

func sampleReplayProcess(pid int) (replayProcessUsage, error) {
	output, err := exec.Command("ps", "-o", "time=", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return replayProcessUsage{}, err
	}
	fields := strings.Fields(string(output))
	if len(fields) != 2 {
		return replayProcessUsage{}, fmt.Errorf("unexpected ps output: %q", output)
	}
	parts := strings.Split(fields[0], ":")
	var seconds float64
	for _, part := range parts {
		value, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return replayProcessUsage{}, err
		}
		seconds = seconds*60 + value
	}
	rssKiB, err := strconv.ParseUint(fields[1], 10, 64)
	return replayProcessUsage{cpuSeconds: seconds, rssBytes: rssKiB * 1024}, err
}

func stopRustIngester(tb testing.TB, process *exec.Cmd) {
	tb.Helper()
	if process.Process == nil {
		return
	}
	require.NoError(tb, process.Process.Kill())
	_ = process.Wait()
}
