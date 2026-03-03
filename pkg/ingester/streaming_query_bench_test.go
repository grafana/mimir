// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// noopQueryStreamServer is a minimal gRPC server stub that discards all sent messages.
type noopQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *noopQueryStreamServer) Send(*client.QueryStreamResponse) error { return nil }
func (s *noopQueryStreamServer) Context() context.Context               { return s.ctx }

// BenchmarkSendStreamingQuerySeries measures the throughput and allocation cost of
// sendStreamingQuerySeries, which is the dominant CPU consumer in the ingester
// streaming query path (f4: ~5.37% total fleet CPU).
//
// Run with:
//
//	go test -bench=BenchmarkSendStreamingQuerySeries -benchmem -count=3 ./pkg/ingester/...
func BenchmarkSendStreamingQuerySeries(b *testing.B) {
	const (
		numSeries      = 4000 // ≈p99 series/query observed in production (3,975)
		numLabels      = 8    // Typical label cardinality per series
		numHeadSamples = 10   // Lightweight — we're benchmarking the series phase, not chunk reads
	)

	cfg := defaultIngesterTestConfig(b)
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerUser = 0
	limits.MaxGlobalSeriesPerMetric = 0

	ing, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", nil)
	require.NoError(b, err)
	startAndWaitHealthy(b, ing, r)

	ctx := user.InjectOrgID(context.Background(), "bench-user")
	now := time.Now().UnixMilli()

	// Push numSeries series into the ingester head.
	samples := make([]mimirpb.Sample, numHeadSamples)
	for i := 0; i < numHeadSamples; i++ {
		samples[i] = mimirpb.Sample{TimestampMs: now - int64(numHeadSamples-i)*1000, Value: float64(i)}
	}

	for s := 0; s < numSeries; s++ {
		lbls := labels.FromStrings(
			model.MetricNameLabel, "bench_metric",
			"series", strconv.Itoa(s),
			"job", "bench_job",
			"instance", "bench_instance_"+strconv.Itoa(s%50),
			"env", "production",
			"region", "us-east-1",
			"cluster", "cluster_"+strconv.Itoa(s%10),
			"namespace", "ns_"+strconv.Itoa(s%20),
		)
		_, err := ing.Push(ctx, writeRequestSingleSeries(lbls, samples))
		require.NoError(b, err)
	}

	db := ing.getTSDB("bench-user")
	require.NotNil(b, db)

	matchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "bench_metric"),
	}
	stream := &noopQueryStreamServer{ctx: ctx}

	b.Run("head_only", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			q, err := db.ChunkQuerier(math.MinInt64, math.MaxInt64)
			require.NoError(b, err)
			node, n, err := ing.sendStreamingQuerySeries(ctx, q, math.MinInt64, math.MaxInt64, matchers, nil, stream)
			require.NoError(b, err)
			require.Equal(b, numSeries, n)
			// Drain the linked-list so pool items are returned.
			for node != nil {
				next := node.next
				putChunkSeriesNode(node)
				node = next
			}
			q.Close()
		}
	})
}
