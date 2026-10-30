// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

// Pushing a record as it is ingests what pushing it decoded does: the same samples, histograms, exemplars and
// metadata, the same errors and the same metrics, for series the engine has and doesn't have yet. An engine that
// doesn't read records gets them decoded.
func TestIngester_PushRecordIngestsWhatPushingTheDecodedRequestDoes(t *testing.T) {
	start := func() (*Ingester, *prometheus.Registry) {
		cfg := defaultIngesterTestConfig(t)
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
			histogram.Histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(now-10_000, util_test.GenerateTestHistogram(1))}
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
		gotErr := direct.PushRecord(ctx, records[0].Value, 2)
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
	require.Len(t, read(direct), 52)
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
		return fmt.Sprint(response.Metadata)
	}
	require.Equal(t, metadata(decoded), metadata(direct))
	require.Contains(t, metadata(direct), "a counter")

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
