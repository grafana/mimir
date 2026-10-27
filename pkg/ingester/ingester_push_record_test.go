// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// Pushing a record as it is ingests what pushing it decoded does: the same samples, histograms, exemplars and
// metadata, the same errors and the same metrics, for series the engine has and doesn't have yet.
func TestIngester_PushRecordIngestsWhatPushingTheDecodedRequestDoes(t *testing.T) {
	start := func() (*Ingester, *prometheus.Registry) {
		cfg := defaultIngesterTestConfig(t)
		cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
		limitsCfg := defaultLimitsTestConfig()
		limitsCfg.NativeHistogramsIngestionEnabled = true
		limitsCfg.MaxGlobalExemplarsPerUser = 100
		reg := prometheus.NewPedanticRegistry()
		ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limitsCfg, nil, t.TempDir(), reg)
		require.NoError(t, err)
		startAndWaitHealthy(t, ingester, r)
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
		return ingester, reg
	}
	decoded, decodedReg := start()
	direct, directReg := start()
	ctx := user.InjectOrgID(context.Background(), userID)

	now := time.Now().Truncate(time.Second).UnixMilli()
	floats := func(n int, ts ...int64) mimirpb.PreallocTimeseries {
		series := &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", fmt.Sprintf("metric_%d", n%3), "pod", fmt.Sprintf("pod-%03d", n)))}
		for _, t := range ts {
			series.Samples = append(series.Samples, mimirpb.Sample{TimestampMs: t, Value: float64(n) + float64(t%1000)})
		}
		return mimirpb.PreallocTimeseries{TimeSeries: series}
	}
	// Each step is built twice, since serializing a request takes it.
	steps := []func() *mimirpb.WriteRequest{
		// Series the engine doesn't have, a histogram, an exemplar and metadata among them.
		func() *mimirpb.WriteRequest {
			req := &mimirpb.WriteRequest{Source: mimirpb.API}
			for n := range 50 {
				req.Timeseries = append(req.Timeseries, floats(n, now-10_000))
			}
			histogram := floats(100)
			histogram.Histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(now-10_000, streamTestHistogram(1))}
			req.Timeseries = append(req.Timeseries, histogram)
			req.Timeseries[3].Exemplars = []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc"}}, Value: 1, TimestampMs: now - 10_000}}
			req.Metadata = []*mimirpb.MetricMetadata{{MetricFamilyName: "metric_1", Type: mimirpb.COUNTER, Help: "a counter"}}
			return req
		},
		// The usual scrape: floats of series the engine has, and of one it doesn't.
		func() *mimirpb.WriteRequest {
			req := &mimirpb.WriteRequest{Source: mimirpb.API}
			for n := range 51 {
				req.Timeseries = append(req.Timeseries, floats(n, now-5000))
			}
			return req
		},
		// Samples the engine rejects: out of order, a duplicate with another value, and too far in the future.
		func() *mimirpb.WriteRequest {
			req := &mimirpb.WriteRequest{Source: mimirpb.RULE}
			req.Timeseries = append(req.Timeseries, floats(1, now-8000), floats(2, now-4000, now-4000), floats(3, now+time.Hour.Milliseconds()), floats(4, now-3000))
			req.Timeseries[1].Samples[1].Value++
			return req
		},
		// What only decoding reads, in a record of the floats of series the engine has: the histograms of one, an
		// exemplar, a series it doesn't have, metadata, and a series of metadata only.
		func() *mimirpb.WriteRequest {
			req := &mimirpb.WriteRequest{Source: mimirpb.OTLP}
			for n := range 20 {
				req.Timeseries = append(req.Timeseries, floats(n, now-2500))
			}
			histogram := floats(100)
			histogram.Histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(now-2500, streamTestHistogram(2))}
			req.Timeseries = append(req.Timeseries, histogram, floats(200, now-2500), floats(201))
			req.Timeseries[5].Exemplars = []mimirpb.Exemplar{{Labels: []mimirpb.LabelAdapter{{Name: "trace_id", Value: "def"}}, Value: 2, TimestampMs: now - 2500}}
			req.Metadata = []*mimirpb.MetricMetadata{{MetricFamilyName: "metric_2", Type: mimirpb.GAUGE, Help: "a gauge", Unit: "seconds"}}
			return req
		},
		// Floats from OTLP.
		func() *mimirpb.WriteRequest {
			req := &mimirpb.WriteRequest{Source: mimirpb.OTLP}
			for n := range 10 {
				req.Timeseries = append(req.Timeseries, floats(n, now-2000, now-1000))
			}
			return req
		},
	}
	serializer := ingest.RecordSerializerFromVersion(2)
	for step, build := range steps {
		wantErr := decoded.PushToStorageAndReleaseRequest(ctx, build())
		records, _, err := serializer.ToRecords("ingest", 0, userID, build(), 64<<20)
		require.NoError(t, err)
		require.Len(t, records, 1)
		wholeBefore, subsetsBefore := recordStats.whole.Load(), recordStats.subsets.Load()
		gotErr := direct.PushRecord(ctx, records[0].Value, 2)
		if step == 3 {
			require.Equal(t, wholeBefore, recordStats.whole.Load(), "the record isn't decoded whole")
			require.Equal(t, subsetsBefore+1, recordStats.subsets.Load(), "only its series the engine doesn't append are decoded")
		}
		if step == 2 {
			require.Error(t, wantErr, "the step with rejected samples")
		}
		if wantErr == nil {
			require.NoError(t, gotErr, "step %d", step)
		} else {
			require.EqualError(t, gotErr, wantErr.Error(), "step %d", step)
		}
	}

	read := func(ingester *Ingester) []string {
		db := ingester.getTSDB(userID)
		require.NotNil(t, db)
		q, err := db.ChunkQuerier(0, now+2*time.Hour.Milliseconds())
		require.NoError(t, err)
		defer q.Close()
		var out []string
		set := q.Select(ctx, true, nil, labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+"))
		for set.Next() {
			series := set.At()
			var samples []string
			chunks := series.Iterator(nil)
			for chunks.Next() {
				it := chunks.At().Chunk.Iterator(nil)
				for kind := it.Next(); kind != chunkenc.ValNone; kind = it.Next() {
					switch kind {
					case chunkenc.ValFloat:
						ts, v := it.At()
						samples = append(samples, fmt.Sprintf("%v@%d", v, ts))
					default:
						samples = append(samples, fmt.Sprintf("%v@%d", kind, it.AtT()))
					}
				}
			}
			out = append(out, series.Labels().String()+" "+strings.Join(samples, ","))
		}
		require.NoError(t, set.Err())
		return out
	}
	require.Equal(t, read(decoded), read(direct))
	require.Len(t, read(direct), 53)
	require.Contains(t, read(direct)[0], "@")

	exemplars := func(ingester *Ingester) string {
		db := ingester.getTSDB(userID)
		querier, err := db.ExemplarQuerier(ctx)
		require.NoError(t, err)
		result, err := querier.Select(0, now+time.Hour.Milliseconds(), []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "__name__", ".+")})
		require.NoError(t, err)
		return fmt.Sprint(result)
	}
	require.Equal(t, exemplars(decoded), exemplars(direct))
	require.Contains(t, exemplars(direct), "abc")
	metadata := func(ingester *Ingester) string {
		response, err := ingester.MetricsMetadata(ctx, &client.MetricsMetadataRequest{Limit: -1, LimitPerMetric: -1})
		require.NoError(t, err)
		// In no order.
		var out []string
		for _, metadata := range response.Metadata {
			out = append(out, metadata.String())
		}
		slices.Sort(out)
		return strings.Join(out, " ")
	}
	require.Equal(t, metadata(decoded), metadata(direct))
	require.Contains(t, metadata(direct), "a counter")
	require.Contains(t, metadata(direct), "a gauge")

	for _, metric := range []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_ingested_exemplars_total",
		"cortex_discarded_samples_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_active_series",
	} {
		want, err := testutil.GatherAndCount(decodedReg, metric)
		require.NoError(t, err)
		got, err := testutil.GatherAndCount(directReg, metric)
		require.NoError(t, err)
		require.Equal(t, want, got, metric)
		require.NoError(t, testutil.GatherAndCompare(directReg, gatheredText(t, decodedReg, metric), metric), metric)
	}
}

// gatheredText is the metric in the text format, to compare another registry's with.
func gatheredText(t *testing.T, reg *prometheus.Registry, metric string) *strings.Reader {
	families, err := reg.Gather()
	require.NoError(t, err)
	var text strings.Builder
	for _, family := range families {
		if family.GetName() == metric {
			_, err := expfmt.MetricFamilyToText(&text, family)
			require.NoError(t, err)
		}
	}
	return strings.NewReader(text.String())
}

// BenchmarkIngester_PushRecord pushes records of series the ingester has, a sample each, as the ingest reader does:
// decoded into a request first, or as they are.
func BenchmarkIngester_PushRecord(b *testing.B) {
	const series, batch = 100_000, 2000
	for _, shape := range []string{"floats", "with metadata and histograms"} {
		for _, path := range []string{"decoded", "record"} {
			b.Run(shape+"/"+path, func(b *testing.B) { benchmarkPushRecord(b, series, batch, path, shape != "floats") })
		}
	}
}

func benchmarkPushRecord(b *testing.B, series, batch int, path string, extras bool) {
	withoutRefLeaksInstrumentation(b)
	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.NativeHistogramsIngestionEnabled = true
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limitsCfg, nil, b.TempDir(), nil)
	require.NoError(b, err)
	startAndWaitHealthy(b, ingester, r)
	b.Cleanup(func() { require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	ctx := user.InjectOrgID(context.Background(), userID)

	serializer := ingest.RecordSerializerFromVersion(2)
	start := time.Now().Add(-time.Hour).UnixMilli()
	records := func(step int) [][]byte {
		var out [][]byte
		for at := 0; at < series; at += batch {
			req := &mimirpb.WriteRequest{Source: mimirpb.API}
			for n := at; n < at+batch; n++ {
				ts := &mimirpb.TimeSeries{Labels: mimirpb.FromLabelsToLabelAdapters(benchRecordLabels(n))}
				if extras && n%200 == 0 {
					ts.Histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(start+int64(step)*15_000, streamTestHistogram(step))}
				} else {
					ts.Samples = []mimirpb.Sample{{TimestampMs: start + int64(step)*15_000, Value: float64(n)}}
				}
				req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
			}
			if extras {
				// As OTLP writes have, for every metric.
				for metric := range 300 {
					req.Metadata = append(req.Metadata, &mimirpb.MetricMetadata{MetricFamilyName: fmt.Sprintf("container_metric_%d", metric), Type: mimirpb.COUNTER, Help: "A counter of the containers."})
				}
			}
			recs, _, err := serializer.ToRecords("ingest", 0, userID, req, 64<<20)
			require.NoError(b, err)
			for _, rec := range recs {
				out = append(out, rec.Value)
			}
		}
		return out
	}
	push := func(content []byte) {
		if path == "record" {
			require.NoError(b, ingester.PushRecord(ctx, content, 2))
			return
		}
		req := &mimirpb.PreallocWriteRequest{}
		require.NoError(b, ingest.DeserializeRecordContent(content, req, 2))
		require.NoError(b, ingester.PushToStorageAndReleaseRequest(ctx, &req.WriteRequest))
	}
	// The series exist, and the active series tracker has them.
	for _, content := range records(0) {
		push(content)
	}
	step, pending := 1, records(1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if len(pending) == 0 {
			b.StopTimer()
			step++
			pending = records(step)
			b.StartTimer()
		}
		push(pending[0])
		pending = pending[1:]
	}
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "series/s")
}

// benchRecordLabels is a series of a Kubernetes scrape: a dozen labels, most of them of few values.
func benchRecordLabels(n int) labels.Labels {
	return labels.FromStrings(
		"__name__", fmt.Sprintf("container_metric_%d", n%300),
		"cluster", "dev-us-central-0",
		"container", fmt.Sprintf("container-%d", n%40),
		"endpoint", "http-metrics",
		"image", fmt.Sprintf("us-docker.pkg.dev/project/image-%d:v1.2.%d", n%40, n%7),
		"instance", fmt.Sprintf("10.132.%d.%d:9100", (n/256)%256, n%256),
		"job", fmt.Sprintf("namespace-%d/job-%d", n%30, n%60),
		"namespace", fmt.Sprintf("namespace-%d", n%30),
		"node", fmt.Sprintf("gke-dev-us-central-0-pool-%d", n%80),
		"pod", fmt.Sprintf("pod-%d-%x", n%2000, n*2654435761%65536),
		"service", fmt.Sprintf("service-%d", n%60),
	)
}

// withoutRefLeaksInstrumentation decodes as production does: tests otherwise copy every buffer they decode into its own
// mapping, to find references to it that outlive it.
func withoutRefLeaksInstrumentation(b *testing.B) {
	mimirpb.CustomCodecConfig{}.RegisterGlobally(nil)
	b.Cleanup(func() {
		mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig: mimirpb.InstrumentRefLeaksConfig{Percentage: 100}}.RegisterGlobally(prometheus.NewRegistry())
	})
}

// The ingest reader passes records as they are only to an ingester whose engine reads them: it decodes them for the
// Prometheus TSDB, which then takes them on as many goroutines as it has shards.
func TestIngester_AcceptsRecordsOnlyForAnEngineThatReadsThem(t *testing.T) {
	if testEngine != "" {
		t.Skip("the engine is the test's")
	}
	for _, c := range []struct {
		engine        string
		ingestStorage bool
		accepts       bool
	}{
		{mimir_tsdb.EngineSeriesstore, true, true},
		{mimir_tsdb.EngineSeriesstore, false, false},
		{mimir_tsdb.EnginePrometheus, true, false},
	} {
		cfg := defaultIngesterTestConfig(t)
		cfg.BlocksStorageConfig.TSDB.Engine = c.engine
		cfg.IngestStorageConfig.Enabled = c.ingestStorage
		require.Equal(t, c.accepts, (&Ingester{cfg: cfg}).AcceptsRecords(), "%+v", c)
	}
}

// Out-of-order histograms stay in the engine as they were appended until the out-of-order head is compacted: what they
// keep of the record they came from must be copied, since the record's buffer is another record's once released.
func TestIngester_PushRecordOutOfOrderHistogramsOutliveTheirRecords(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.Engine = mimir_tsdb.EngineSeriesstore
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.NativeHistogramsIngestionEnabled = true
	limitsCfg.OutOfOrderTimeWindow = model.Duration(time.Hour)
	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limitsCfg, nil, t.TempDir(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	ctx := user.InjectOrgID(context.Background(), userID)
	now := time.Now().Truncate(time.Second).UnixMilli()
	serializer := ingest.RecordSerializerFromVersion(2)
	push := func(ts int64, base int, metric string) {
		req := &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("__name__", metric)),
			Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(ts, streamTestHistogram(base))},
		}}}}
		records, _, err := serializer.ToRecords("ingest", 0, userID, req, 64<<20)
		require.NoError(t, err)
		require.NoError(t, ingester.PushRecord(ctx, records[0].Value, 2))
	}
	// In order, then out of order between them.
	for step := range 5 {
		push(now-60_000+int64(step)*10_000, step, "histogram_metric")
	}
	for step := range 4 {
		push(now-55_000+int64(step)*10_000, 10+step, "histogram_metric")
	}
	// Other records, which the released buffers' memory may be for.
	for step := range 50 {
		push(now-60_000+int64(step)*1000, 100+step, fmt.Sprintf("other_%d", step))
	}

	db := ingester.getTSDB(userID)
	q, err := db.ChunkQuerier(now-120_000, now)
	require.NoError(t, err)
	defer q.Close()
	set := q.(streamingChunkQuerier).ChunkQuerier.Select(ctx, true, nil, labels.MustNewMatcher(labels.MatchEqual, "__name__", "histogram_metric"))
	require.True(t, set.Next())
	got := map[int64]*histogram.Histogram{}
	chunks := set.At().Iterator(nil)
	for chunks.Next() {
		it := chunks.At().Chunk.Iterator(nil)
		for it.Next() == chunkenc.ValHistogram {
			ts, h := it.AtHistogram(nil)
			got[ts] = h
		}
	}
	require.NoError(t, set.Err())
	require.Len(t, got, 9)
	for step := range 5 {
		require.Equal(t, streamTestHistogram(step).PositiveBuckets, got[now-60_000+int64(step)*10_000].PositiveBuckets, "in order %d", step)
	}
	for step := range 4 {
		ts := now - 55_000 + int64(step)*10_000
		require.Equal(t, streamTestHistogram(10+step).PositiveBuckets, got[ts].PositiveBuckets, "out of order %d", step)
		require.Equal(t, streamTestHistogram(10+step).PositiveSpans, got[ts].PositiveSpans, "out of order %d", step)
	}
}
