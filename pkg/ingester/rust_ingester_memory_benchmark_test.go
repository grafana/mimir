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
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

const goIngesterMemoryHelper = "MIMIR_GO_INGESTER_MEMORY_HELPER"

func TestGoIngesterMemoryServer(t *testing.T) {
	if os.Getenv(goIngesterMemoryHelper) != "1" {
		t.Skip("started by the memory benchmark")
	}

	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.WALSegmentSizeBytes = -1
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 8
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyBatchSize = 150
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg,
		validation.NewOverrides(defaultLimitsTestConfig(), nil), nil, nil, nil,
		os.Getenv("MIMIR_MEMORY_KAFKA_ADDRESS"))
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester)) })
	targetOffset, err := strconv.ParseInt(os.Getenv("MIMIR_MEMORY_TARGET_OFFSET"), 10, 64)
	require.NoError(t, err)
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(targetOffset)))

	server := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
	)
	client.RegisterIngesterServer(server, ingester)
	listener, err := net.Listen("tcp", os.Getenv("MIMIR_MEMORY_GRPC_ADDRESS"))
	require.NoError(t, err)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	defer signal.Stop(stop)
	<-stop
}

func BenchmarkGoRustIngesterMemory(b *testing.B) {
	const series = 25_000
	const topic = "mimir"
	buildRustIngester(b)
	_, kafkaAddress := testkafka.CreateCluster(b, 10, topic)
	generator, err := ingest.NewFixtureGenerator(queryFixture(series), 1, 0)
	require.NoError(b, err)
	records, err := generator.ProduceWriteRequests(context.Background(), flagext.StringSliceCSV{kafkaAddress}, topic, 0)
	require.NoError(b, err)
	targetOffset := int64(records - 1)

	request := parityRequest(math.MinInt64, math.MaxInt64, parityMatcher(client.REGEX_MATCH, "__name__", ".+"))
	var goBytes, rustBytes int64
	var goSeries, rustSeries map[string][]string
	for _, side := range []struct {
		name string
		out  *int64
		data *map[string][]string
	}{
		{"Go", &goBytes, &goSeries},
		{"Rust", &rustBytes, &rustSeries},
	} {
		b.Run(side.name, func(b *testing.B) {
			b.StopTimer()
			var process *exec.Cmd
			var connection *grpc.ClientConn
			if side.name == "Go" {
				process, connection = startGoIngesterMemoryServer(b, kafkaAddress, targetOffset)
				defer stopGoIngesterMemoryServer(b, process)
			} else {
				process, connection = startRustIngesterAndWait(b, kafkaAddress, topic, b.TempDir(), targetOffset)
				defer stopRustIngester(b, process)
			}
			defer connection.Close()
			api := client.NewIngesterClient(connection)
			ctx := parityContext("tenant-0", targetOffset)
			rest := processRSSBytes(b, process.Process.Pid)
			b.StartTimer()
			peak := samplePeakRSS(b, process.Process.Pid, func() {
				for range b.N {
					bytes, seen := queryStreamBytes(b, api, ctx, request)
					require.Equal(b, series, seen)
					if *side.out != 0 {
						require.Equal(b, *side.out, bytes)
					}
					*side.out = bytes
				}
			})
			b.StopTimer()
			after := processRSSBytes(b, process.Process.Pid)
			peak = max(peak, rest, after)
			b.ReportMetric(float64(rest), "rest-RSS-B")
			b.ReportMetric(float64(peak), "query-peak-RSS-B")
			b.ReportMetric(float64(after), "after-query-RSS-B")
			b.ReportMetric(float64(peak-rest), "query-peak-increase-B")
			b.ReportMetric(float64(*side.out), "response-B/op")
			decoded := paritySeries(b, api, ctx, request)
			if *side.data == nil {
				*side.data = decoded
			} else {
				require.Equal(b, *side.data, decoded)
			}
		})
	}
	// Series order can change gRPC batch packing without changing the query result.
	require.Equal(b, goSeries, rustSeries)
}

func startGoIngesterMemoryServer(b *testing.B, kafkaAddress string, targetOffset int64) (*exec.Cmd, *grpc.ClientConn) {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(b, err)
	address := listener.Addr().String()
	require.NoError(b, listener.Close())

	process := exec.Command(os.Args[0], "-test.run=^TestGoIngesterMemoryServer$")
	process.Env = append(os.Environ(),
		goIngesterMemoryHelper+"=1",
		"MIMIR_MEMORY_KAFKA_ADDRESS="+kafkaAddress,
		"MIMIR_MEMORY_TARGET_OFFSET="+strconv.FormatInt(targetOffset, 10),
		"MIMIR_MEMORY_GRPC_ADDRESS="+address,
	)
	var stderr bytes.Buffer
	process.Stdout = &stderr
	process.Stderr = &stderr
	require.NoError(b, process.Start())
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(b, err)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"x-scope-orgid", "benchmark-tenant",
		"__consistency_level__", "strong",
		"__consistency_offsets__", fmt.Sprintf("v1=0:%d", targetOffset))
	_, err = client.NewIngesterClient(connection).UserStats(ctx, &client.UserStatsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		_ = connection.Close()
		stopGoIngesterMemoryServer(b, process)
	}
	require.NoError(b, err, stderr.String())
	return process, connection
}

func stopGoIngesterMemoryServer(b *testing.B, process *exec.Cmd) {
	b.Helper()
	if process.Process == nil {
		return
	}
	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		b.Errorf("stop Go ingester: %v", err)
		return
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err := <-done:
		require.NoError(b, err)
	case <-time.After(10 * time.Second):
		_ = process.Process.Kill()
		<-done
		b.Error("Go ingester did not stop within 10 seconds")
	}
}

func queryStreamBytes(b *testing.B, api client.IngesterClient, ctx context.Context, request *client.QueryRequest) (int64, int) {
	b.Helper()
	stream, err := api.QueryStream(ctx, request)
	require.NoError(b, err)
	var bytes int64
	var seen int
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return bytes, seen
		}
		require.NoError(b, err)
		bytes += int64(response.Size())
		seen += len(response.StreamingSeries)
	}
}

func samplePeakRSS(b *testing.B, pid int, run func()) uint64 {
	b.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	var peak uint64
	var sampleErr error
	go func() {
		defer close(done)
		for {
			rss, err := readProcessRSSBytes(pid)
			if err != nil {
				sampleErr = err
				return
			}
			if rss > peak {
				peak = rss
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	run()
	close(stop)
	<-done
	require.NoError(b, sampleErr)
	return peak
}

func readProcessRSSBytes(pid int) (uint64, error) {
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, err
	}
	kibibytes, err := strconv.ParseUint(strings.TrimSpace(string(output)), 10, 64)
	return kibibytes * 1024, err
}
