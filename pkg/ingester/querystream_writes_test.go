// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// countingListener counts the writes to the connections it accepts, which are the server's: each is a syscall.
type countingListener struct {
	net.Listener
	writes, bytes atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, owner: l}, nil
}

type countingConn struct {
	net.Conn
	owner *countingListener
}

func (c *countingConn) Write(b []byte) (int, error) {
	c.owner.writes.Add(1)
	c.owner.bytes.Add(int64(len(b)))
	return c.Conn.Write(b)
}

// BenchmarkIngester_QueryStreamServerWrites counts the socket writes an ingester's gRPC server makes for a QueryStream
// over TCP, by engine and by how many series the response has: a write is a syscall and a pass through the kernel's
// network stack, which a response of a few series in several writes pays several times.
func BenchmarkIngester_QueryStreamServerWrites(b *testing.B) {
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		b.Run(engine, func(b *testing.B) {
			cfg := defaultIngesterTestConfig(b)
			cfg.BlocksStorageConfig.TSDB.Engine = engine
			limitsCfg := defaultLimitsTestConfig()
			limitsCfg.MaxGlobalSeriesPerMetric = 0
			limitsCfg.MaxGlobalSeriesPerUser = 0
			ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, nil, b.TempDir(), nil)
			require.NoError(b, err)
			startAndWaitHealthy(b, ingester, r)

			ctx := user.InjectOrgID(context.Background(), userID)
			now := time.Now().UnixMilli()
			// Metrics of 1, 10 and 1000 series, each with a few samples, as most queries read.
			sizes := []int{1, 10, 1000}
			for _, size := range sizes {
				var series []mimirpb.PreallocTimeseries
				for n := range size {
					ts := &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", size)}, {Name: "pod", Value: fmt.Sprintf("pod-%05d", n)}}}
					for sample := range 8 {
						ts.Samples = append(ts.Samples, mimirpb.Sample{TimestampMs: now - int64(8-sample)*15_000, Value: float64(sample)})
					}
					series = append(series, mimirpb.PreallocTimeseries{TimeSeries: ts})
				}
				_, err = ingester.Push(ctx, &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
				require.NoError(b, err)
			}

			listener := &countingListener{}
			listener.Listener, err = net.Listen("tcp", "127.0.0.1:0")
			require.NoError(b, err)
			server := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
			client.RegisterIngesterServer(server, ingester)
			go func() { _ = server.Serve(listener) }()
			connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStreamInterceptor(middleware.StreamClientUserHeaderInterceptor))
			require.NoError(b, err)
			b.Cleanup(func() {
				_ = connection.Close()
				server.Stop()
			})
			ingesterClient := client.NewIngesterClient(connection)

			for _, size := range sizes {
				b.Run(fmt.Sprintf("series=%d", size), func(b *testing.B) {
					request := &client.QueryRequest{
						StartTimestampMs:         now - 300_000,
						EndTimestampMs:           now,
						Matchers:                 []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: fmt.Sprintf("metric_%d", size)}},
						StreamingChunksBatchSize: 256,
					}
					query := func() int {
						stream, err := ingesterClient.QueryStream(ctx, request)
						require.NoError(b, err)
						messages := 0
						for {
							_, err := stream.Recv()
							if errors.Is(err, io.EOF) {
								return messages
							}
							require.NoError(b, err)
							messages++
						}
					}
					// The connection's setup and the first query's are not the queries'.
					messages := query()
					writes, written := listener.writes.Load(), listener.bytes.Load()
					b.ResetTimer()
					for b.Loop() {
						query()
					}
					b.StopTimer()
					b.ReportMetric(float64(listener.writes.Load()-writes)/float64(b.N), "writes/op")
					b.ReportMetric(float64(listener.bytes.Load()-written)/float64(b.N), "bytes/op")
					b.ReportMetric(float64(messages), "messages/op")
				})
			}
		})
	}
}

// BenchmarkIngester_SeriesstoreShardedQueryStream is a query of a metric of many series over the head's last minutes as
// one request and as the 16 requests of its shards, through the ingester's QueryStream, response built and marshaled:
// how much more 16 shards cost the ingester than one request, and what of it a cache of the whole selection would save.
func BenchmarkIngester_SeriesstoreShardedQueryStream(b *testing.B) {
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.MaxGlobalSeriesPerMetric = 0
	limitsCfg.MaxGlobalSeriesPerUser = 0
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, nil, b.TempDir(), nil)
	require.NoError(b, err)
	startAndWaitHealthy(b, ingester, r)

	ctx := user.InjectOrgID(context.Background(), userID)
	now := time.Now().UnixMilli()
	const seriesCount = 20_000
	for first := 0; first < seriesCount; first += 1000 {
		var series []mimirpb.PreallocTimeseries
		for n := first; n < first+1000; n++ {
			ts := &mimirpb.TimeSeries{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric"}, {Name: "job", Value: fmt.Sprintf("job-%d", n%20)}, {Name: "pod", Value: fmt.Sprintf("pod-%05d", n)}}}
			for sample := range 20 {
				ts.Samples = append(ts.Samples, mimirpb.Sample{TimestampMs: now - int64(20-sample)*15_000, Value: float64(sample)})
			}
			series = append(series, mimirpb.PreallocTimeseries{TimeSeries: ts})
		}
		_, err = ingester.Push(ctx, &mimirpb.WriteRequest{Timeseries: series, Source: mimirpb.API})
		require.NoError(b, err)
	}
	stream := &marshalingStream{ctx: ctx}
	query := func(shard string) {
		matchers := []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "metric"}}
		if shard != "" {
			matchers = append(matchers, &client.LabelMatcher{Type: client.EQUAL, Name: "__query_shard__", Value: shard})
		}
		require.NoError(b, ingester.QueryStream(&client.QueryRequest{StartTimestampMs: now - 300_000, EndTimestampMs: now, Matchers: matchers, StreamingChunksBatchSize: 256}, stream))
	}
	for _, shards := range []int{1, 16} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if shards == 1 {
					query("")
					continue
				}
				for shard := range shards {
					query(fmt.Sprintf("%d_of_%d", shard+1, shards))
				}
			}
		})
	}
}
