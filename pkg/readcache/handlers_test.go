// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

// mockQueryStreamServer is a minimal client.Ingester_QueryStreamServer
// for driving Readcache.queryStream directly in unit tests. Only
// Context and Send are exercised; the embedded grpc.ServerStream
// supplies the rest of the interface and panics if an unexpected
// method is called.
type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*ingester_client.QueryStreamResponse
}

func (m *mockQueryStreamServer) Context() context.Context { return m.ctx }

func (m *mockQueryStreamServer) Send(r *ingester_client.QueryStreamResponse) error {
	m.responses = append(m.responses, r)
	return nil
}

// TestReadcache_SamplesIngestedTotal_PerPartition produces sample
// batches with distinct counts to two partitions and asserts that
// cortex_readcache_samples_ingested_total attributes them to the
// right partition label. This is the source-of-truth metric for
// "samples/sec ingested per partition" — use rate() over it in
// dashboards.
func TestReadcache_SamplesIngestedTotal_PerPartition(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 4, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0,1"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)
	reg := prometheus.NewPedanticRegistry()

	rc, err := New(cfg, limits, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	writer := makeKafkaWriter(t, kafkaCfg, reg)
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	wreqCtx := user.InjectOrgID(ctx, tenantID)
	produce := func(partition int32, series string, samples int) {
		t.Helper()
		ts := []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(
					labels.FromStrings("__name__", series, "src", "test")),
				Samples: make([]mimirpb.Sample, samples),
			}},
		}
		for i := 0; i < samples; i++ {
			ts[0].Samples[i] = mimirpb.Sample{TimestampMs: int64(i + 1), Value: float64(i)}
		}
		require.NoError(t, writer.WriteSync(wreqCtx, topic, partition, tenantID, &mimirpb.WriteRequest{Timeseries: ts}))
	}

	// Send distinct sample counts to each partition so a mis-
	// attribution would surface immediately rather than blending.
	produce(0, "p0_series", 3)
	produce(1, "p1_series", 7)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("0")) >= 3 &&
			testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("1")) >= 7
	}, 10*time.Second, 100*time.Millisecond, "samples should be counted per partition")

	assert.Equal(t, float64(3), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("0")),
		"partition 0 should be attributed exactly 3 samples")
	assert.Equal(t, float64(7), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("1")),
		"partition 1 should be attributed exactly 7 samples")
	assert.Equal(t, float64(0), testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("2")),
		"partition 2 was not owned and must have zero samples")

	// The per-user ingester-mirror counter must see all successfully
	// appended samples across partitions, keyed by tenant — the
	// readcache side of the cortex_ingester_ingested_samples_total
	// correctness comparison.
	assert.Equal(t, float64(10), testutil.ToFloat64(rc.ingestedSamples.WithLabelValues(tenantID)),
		"cortex_readcache_ingested_samples_total must count successfully-appended samples per user")
}

// TestReadcache_QueryStream_QueryLoadAttribution is the end-to-end
// proof of request 1: a QueryStream carrying QueryAttributionHint
// scopes the read to that partition's TSDB and attributes the
// scanned-samples query load to that partition's EWMA, which then
// surfaces through HashRangeStats.PartitionQueryLoads. A nil hint
// (full-fanout) bills the unnamed bucket instead. This is the signal
// the rebalancer pulls to balance query CPU load across readcache
// instances.
func TestReadcache_QueryStream_QueryLoadAttribution(t *testing.T) {
	const (
		tenantID = "user-1"
		topic    = "test-topic"
	)

	_, addr := testkafka.CreateCluster(t, 4, topic)

	cfg := newTestConfigNoKafka(t)
	cfg.KafkaTopic = topic
	cfg.OwnedPartitions = "0,1"

	var kafkaCfg ingest.KafkaConfig
	kafkaCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	kafkaCfg.Address = flagext.StringSliceCSV{addr}
	kafkaCfg.Topic = topic
	cfg.Kafka = kafkaCfg

	limits := validation.NewOverrides(validation.Limits{}, nil)
	reg := prometheus.NewPedanticRegistry()

	rc, err := New(cfg, limits, nil, log.NewNopLogger(), reg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, services.StartAndAwaitRunning(ctx, rc))
	defer func() { _ = services.StopAndAwaitTerminated(ctx, rc) }()

	writer := makeKafkaWriter(t, kafkaCfg, reg)
	defer func() { _ = services.StopAndAwaitTerminated(ctx, writer) }()

	wreqCtx := user.InjectOrgID(ctx, tenantID)
	const (
		p0Samples = 5
		p1Samples = 9
	)
	produce := func(partition int32, series string, samples int) {
		t.Helper()
		ts := []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(
					labels.FromStrings("__name__", series, "src", "test")),
				Samples: make([]mimirpb.Sample, samples),
			}},
		}
		for i := 0; i < samples; i++ {
			ts[0].Samples[i] = mimirpb.Sample{TimestampMs: int64(i + 1), Value: float64(i)}
		}
		require.NoError(t, writer.WriteSync(wreqCtx, topic, partition, tenantID, &mimirpb.WriteRequest{Timeseries: ts}))
	}
	produce(0, "p0_series", p0Samples)
	produce(1, "p1_series", p1Samples)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("0")) >= p0Samples &&
			testutil.ToFloat64(rc.samplesIngestedTotal.WithLabelValues("1")) >= p1Samples
	}, 10*time.Second, 100*time.Millisecond, "samples should be ingested before querying")

	// runQuery drives queryStream directly and returns how many
	// series were streamed back to the caller (the labels-phase
	// count), which is enough to prove the hint scoped the read to
	// the right partition TSDB.
	runQuery := func(series string, hint *ingester_client.QueryAttributionHint) int {
		t.Helper()
		req, err := ingester_client.ToQueryRequest(
			0, model.Time(time.Now().UnixMilli()),
			[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", series)},
		)
		require.NoError(t, err)
		req.QueryAttributionHint = hint

		stream := &mockQueryStreamServer{ctx: wreqCtx}
		require.NoError(t, rc.queryStream(req, stream))

		seriesCount := 0
		for _, resp := range stream.responses {
			seriesCount += len(resp.StreamingSeries)
		}
		return seriesCount
	}

	// Hint scoping: a query for p0_series hinted at partition 1
	// finds nothing (the series lives only in partition 0's TSDB),
	// while hinting partition 0 returns the series. Because no
	// samples are scanned in the wrong-partition case, nothing is
	// attributed.
	assert.Equal(t, 0, runQuery("p0_series", &ingester_client.QueryAttributionHint{PartitionId: 1}),
		"hint must scope the read to the named partition's TSDB only")

	// Named query for partition 0: scopes to partition 0's TSDB and
	// attributes p0Samples to partition 0.
	assert.Equal(t, 1, runQuery("p0_series", &ingester_client.QueryAttributionHint{PartitionId: 0}))
	// Named query for partition 1.
	assert.Equal(t, 1, runQuery("p1_series", &ingester_client.QueryAttributionHint{PartitionId: 1}))
	// Full-fanout (nil hint): scans every owned partition and bills
	// the unnamed bucket. Only partition 0 has p0_series.
	assert.Equal(t, 1, runQuery("p0_series", nil))

	// The ingester-mirror queried-samples histogram must have one
	// observation per queryStream call and a sum equal to all the
	// samples streamed back: 0 (wrong-partition hint) + p0 + p1 + p0
	// (full fanout) — the readcache side of the
	// cortex_ingester_queried_samples correctness comparison.
	var qs dto.Metric
	require.NoError(t, rc.queriedSamples.Write(&qs))
	assert.Equal(t, uint64(4), qs.Histogram.GetSampleCount(),
		"cortex_readcache_queried_samples must observe once per QueryStream")
	assert.Equal(t, float64(2*p0Samples+p1Samples), qs.Histogram.GetSampleSum(),
		"cortex_readcache_queried_samples sum must equal the total streamed samples")

	// Advance the EWMAs once (the background loop ticks every 15s,
	// far beyond this test's lifetime). With init=false the first
	// tick sets lastRate = accumulated / TickInterval.
	rc.queryLoad.Tick()

	wantP0 := float64(p0Samples) / loadstatsTickSeconds
	wantP1 := float64(p1Samples) / loadstatsTickSeconds
	wantUnnamed := float64(p0Samples) / loadstatsTickSeconds

	snap := rc.queryLoad.Snapshot()
	byPartition := map[int32]float64{}
	for _, p := range snap.PerPartition {
		byPartition[p.PartitionID] = p.SamplesEWMA
	}
	assert.InDelta(t, wantP0, byPartition[0], 1e-9, "partition 0 query-load EWMA")
	assert.InDelta(t, wantP1, byPartition[1], 1e-9, "partition 1 query-load EWMA")
	assert.InDelta(t, wantUnnamed, snap.Unnamed, 1e-9, "unnamed (full-fanout) query-load EWMA")

	// The same per-partition load must surface via HashRangeStats,
	// which is the RPC the rebalancer polls.
	resp, err := rc.hashRangeStats(ctx, &ingester_client.HashRangeStatsRequest{})
	require.NoError(t, err)
	hrByPartition := map[int32]float64{}
	for _, pl := range resp.PartitionQueryLoads {
		hrByPartition[pl.PartitionId] = pl.SamplesEwma
	}
	assert.InDelta(t, wantP0, hrByPartition[0], 1e-9, "HashRangeStats partition 0 load")
	assert.InDelta(t, wantP1, hrByPartition[1], 1e-9, "HashRangeStats partition 1 load")
	assert.InDelta(t, wantUnnamed, resp.UnnamedQuerySamplesEwma, 1e-9, "HashRangeStats unnamed load")
}

// loadstatsTickSeconds mirrors loadstats.TickInterval in seconds, used
// to compute the expected EWMA after a single tick.
const loadstatsTickSeconds = 15.0
