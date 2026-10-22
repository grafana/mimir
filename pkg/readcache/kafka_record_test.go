// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// recordSeriesLabels is a series like a Kubernetes scrape's, the series of a metric one after the other as a request
// has them: a dozen labels, most of them of few values.
func recordSeriesLabels(n, metrics, series int) []mimirpb.LabelAdapter {
	return []mimirpb.LabelAdapter{
		{Name: model.MetricNameLabel, Value: fmt.Sprintf("container_metric_%d", n*metrics/series)},
		{Name: "cluster", Value: "dev-us-central-0"},
		{Name: "container", Value: fmt.Sprintf("container-%d", n%40)},
		{Name: "endpoint", Value: "http-metrics"},
		{Name: "image", Value: fmt.Sprintf("us-docker.pkg.dev/project/image-%d:v1.2.%d", n%40, n%7)},
		{Name: "instance", Value: fmt.Sprintf("10.132.%d.%d:9100", (n/256)%256, n%256)},
		{Name: "job", Value: fmt.Sprintf("namespace-%d/job-%d", n%30, n%60)},
		{Name: "namespace", Value: fmt.Sprintf("namespace-%d", n%30)},
		{Name: "node", Value: fmt.Sprintf("gke-dev-us-central-0-pool-%d", n%80)},
		{Name: "pod", Value: fmt.Sprintf("pod-%d-%x", n%2000, n*2654435761%65536)},
		{Name: "service", Value: fmt.Sprintf("service-%d", n%60)},
	}
}

// toRecords serializes the request into version 2 records, the way distributors write them.
func toRecords(t testing.TB, tenantID string, req *mimirpb.WriteRequest) [][]byte {
	recs, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, tenantID, req, 64<<20)
	require.NoError(t, err)
	contents := make([][]byte, 0, len(recs))
	for _, rec := range recs {
		contents = append(contents, rec.Value)
	}
	return contents
}

// scrapeRecords is one sample at ts of each series, in records of batch series each.
func scrapeRecords(t testing.TB, tenantID string, series, metrics, batch int, ts int64) [][]byte {
	var records [][]byte
	for at := 0; at < series; at += batch {
		req := &mimirpb.WriteRequest{}
		for n := at; n < min(at+batch, series); n++ {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:  recordSeriesLabels(n, metrics, series),
				Samples: []mimirpb.Sample{{TimestampMs: ts, Value: float64(n)}},
			}})
		}
		records = append(records, toRecords(t, tenantID, req)...)
	}
	return records
}

// splitRanges are ranges that split each metric's hashes in two, as the rebalancer does with the metrics that are
// most of a partition's series: which range a series is in is told by its labels' hash.
func splitRanges(tenantID string, metrics, series int) []client.HashRangeEntry {
	var bounds []uint32
	for m := range metrics {
		lo, _ := mimirpb.MetricNameHashRange(tenantID, recordSeriesLabels(m*series/metrics, metrics, series)[0].Value)
		bounds = append(bounds, lo+mimirpb.LabelBitsMask/2)
	}
	slices.Sort(bounds)
	bounds = slices.Compact(bounds)
	ranges := make([]client.HashRangeEntry, 0, len(bounds)+1)
	lo := uint32(0)
	for _, bound := range bounds {
		ranges = append(ranges, client.HashRangeEntry{Lo: lo, Hi: bound - 1})
		lo = bound
	}
	return append(ranges, client.HashRangeEntry{Lo: lo, Hi: math.MaxUint32})
}

// newSeriesstorePusher is the pusher of a warm seriesstore partition with ranges, and the partition.
func newSeriesstorePusher(t testing.TB, ranges []client.HashRangeEntry) (*partitionPusher, *partitionState) {
	cfg := newTestConfig(t, true, 1)
	cfg.Engine = mimir_tsdb.EngineSeriesstore
	cfg.BlocksStorage.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	cfg.OwnedPartitions = "0"
	r, err := New(cfg, validation.NewOverrides(validation.Limits{NativeHistogramsIngestionEnabled: true}, nil), nil, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	// The series walker stays off the per-range bookkeeping, see TestPartitionPusher_WarmGatesSampleRate.
	r.seriesWalkMu.Lock()
	t.Cleanup(r.seriesWalkMu.Unlock)
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(ctx, r)) })
	r.partitionMu.RLock()
	p := r.partitions[0]
	r.partitionMu.RUnlock()
	require.NotNil(t, p)
	_, err = r.setHashRanges(ctx, &client.SetHashRangesRequest{Ranges: ranges})
	require.NoError(t, err)
	p.warm.Store(true)
	pusher := &partitionPusher{rc: r, partitionID: 0, ranges: p.ranges, warm: &p.warm, samplesIngested: prometheus.NewCounter(prometheus.CounterOpts{Name: "samples"})}
	return pusher, p
}

// pushDecoded pushes a record as the reader does for a pusher that doesn't read records: decoded into a request.
func pushDecoded(t testing.TB, ctx context.Context, pusher *partitionPusher, content []byte) error {
	req := &mimirpb.PreallocWriteRequest{}
	require.NoError(t, ingest.DeserializeRecordContent(content, req, 2))
	return pusher.PushToStorageAndReleaseRequest(ctx, &req.WriteRequest)
}

// A record pushed as it is appends what decoding it does, and counts the same samples by range: series created by
// the push, series the store has, series with histograms, and samples rejected.
func TestPartitionPusher_PushRecordAppendsWhatDecodingDoes(t *testing.T) {
	const (
		tenantID = "user-1"
		series   = 600
		metrics  = 6
	)
	ranges := splitRanges(tenantID, metrics, series)
	decoded, decodedPartition := newSeriesstorePusher(t, ranges)
	direct, directPartition := newSeriesstorePusher(t, ranges)
	require.True(t, direct.AcceptsRecords())
	ctx := user.InjectOrgID(context.Background(), tenantID)

	now := time.Now().UnixMilli()
	// The first half of the series is created by the first scrape, and the second by the second.
	records := scrapeRecords(t, tenantID, series/2, metrics, 100, now-30_000)
	records = append(records, scrapeRecords(t, tenantID, series, metrics, 100, now-15_000)...)
	mixed := &mimirpb.WriteRequest{}
	for n := range 50 {
		mixed.Timeseries = append(mixed.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels:  recordSeriesLabels(n, metrics, series),
			Samples: []mimirpb.Sample{{TimestampMs: now, Value: 1}},
		}})
	}
	// Too old for the head, which rejects it.
	mixed.Timeseries[10].Samples[0].TimestampMs = now - 24*time.Hour.Milliseconds()
	mixed.Timeseries = append(mixed.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
		Labels:     []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "histogram_metric"}},
		Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(now, tsdbutil.GenerateTestHistogram(1))},
	}})
	records = append(records, toRecords(t, tenantID, mixed)...)

	// What counting the samples by their labels' hashes gives.
	labelled := newPartitionRanges()
	var labelledRanges []assignment.HashRange
	for _, r := range ranges {
		labelledRanges = append(labelledRanges, hr(r.Lo, r.Hi))
	}
	labelled.setRanges(labelledRanges)
	rejected := 0
	for _, content := range records {
		req := &mimirpb.PreallocWriteRequest{}
		require.NoError(t, ingest.DeserializeRecordContent(content, req, 2))
		labelled.recordSampleBatch(tenantID, req.Timeseries, nil)

		want := pushDecoded(t, ctx, decoded, content)
		got := direct.PushRecord(ctx, content, 2)
		if want != nil {
			rejected++
			require.EqualError(t, got, want.Error())
		} else {
			require.NoError(t, got)
		}
	}
	require.Equal(t, 1, rejected)

	require.Equal(t, testutil.ToFloat64(decoded.samplesIngested), testutil.ToFloat64(direct.samplesIngested))
	decodedPartition.ranges.tickSampleRates()
	directPartition.ranges.tickSampleRates()
	labelled.tickSampleRates()
	wantRanges, _ := labelled.adminSnapshot()
	decodedRanges, _ := decodedPartition.ranges.adminSnapshot()
	gotRanges, _ := directPartition.ranges.adminSnapshot()
	require.Equal(t, wantRanges, decodedRanges)
	require.Equal(t, wantRanges, gotRanges)
	var total float64
	for _, r := range gotRanges {
		total += r.SampleRate
	}
	require.Positive(t, total)

	decodedDB, err := decoded.rc.getOrOpenTSDB(tenantID, 0)
	require.NoError(t, err)
	directDB, err := direct.rc.getOrOpenTSDB(tenantID, 0)
	require.NoError(t, err)
	for _, name := range []string{"container_metric_0", "container_metric_3", "histogram_metric"} {
		want := sampleTimes(t, decodedDB, name)
		require.NotEmpty(t, want, name)
		require.Equal(t, want, sampleTimes(t, directDB, name), name)
	}
}

// sampleTimes are the timestamps of the samples of the metric's series, floats and histograms, by series.
func sampleTimes(t *testing.T, db *partitionTSDB, metric string) map[string][]int64 {
	t.Helper()
	q, err := db.ChunkQuerier(0, time.Now().Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	defer func() { require.NoError(t, q.Close()) }()
	set := q.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metric))
	out := map[string][]int64{}
	for set.Next() {
		series := set.At().Labels().String()
		chunks := set.At().Iterator(nil)
		for chunks.Next() {
			samples := chunks.At().Chunk.Iterator(nil)
			for samples.Next() != chunkenc.ValNone {
				out[series] = append(out[series], samples.AtT())
			}
			require.NoError(t, samples.Err())
		}
		require.NoError(t, chunks.Err())
	}
	require.NoError(t, set.Err())
	return out
}

// BenchmarkPartitionPusher pushes records of series a seriesstore partition has, one sample each, as the partition
// reader does: decoded into a request first, or as they are.
func BenchmarkPartitionPusher(b *testing.B) {
	const (
		tenantID = "user-1"
		series   = 200_000
		metrics  = 300
		batch    = 2000
	)
	for _, path := range []string{"decoded", "record"} {
		b.Run(path, func(b *testing.B) {
			pusher, _ := newSeriesstorePusher(b, splitRanges(tenantID, metrics, series))
			ctx := user.InjectOrgID(context.Background(), tenantID)
			push := func(content []byte) {
				if path == "record" {
					require.NoError(b, pusher.PushRecord(ctx, content, 2))
				} else {
					require.NoError(b, pushDecoded(b, ctx, pusher, content))
				}
			}
			start := time.Now().Add(-time.Hour).UnixMilli()
			// The series are created first, by the appender.
			for _, content := range scrapeRecords(b, tenantID, series, metrics, batch, start) {
				push(content)
			}
			b.ReportAllocs()
			b.ResetTimer()
			step := 1
			for b.Loop() {
				b.StopTimer()
				records := scrapeRecords(b, tenantID, series, metrics, batch, start+int64(step)*15_000)
				step++
				b.StartTimer()
				for _, content := range records {
					push(content)
				}
			}
			b.ReportMetric(float64(series)*float64(b.N)/b.Elapsed().Seconds(), "series/s")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(series*b.N), "ns/series")
		})
	}
}
