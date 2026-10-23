// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// The specification is Prometheus's #19563, ported to the engine's ingest routes. What differs, on purpose:
//   - A series' versions aren't capped at five, which truncates the TSDB's: its changes are limited per window,
//     which coalesces the ones over the limit (Coalesced), and its older runs are capped far higher (Truncated).
//   - Head garbage collection drops the older runs that end before the head's min time; the TSDB keeps a series'
//     versions as long as the series.
//   - A record's order is its commit order: a tenant's records come one at a time, so the TSDB's rules for two
//     transactions open at once don't apply.

// metadataRoute appends a float of value 1 at at, with the metadata m, to each series of the engine, which exist.
type metadataRoute func(t *testing.T, e *Engine, at int64, m mimirpb.SeriesMetadata, series ...promlabels.Labels)

// The engine's ingest routes the ingester takes: a record's fast path, a record with series it leaves to the caller,
// a decoded batch, and the appender.
var metadataRoutes = map[string]metadataRoute{
	"complete record": func(t *testing.T, e *Engine, at int64, m mimirpb.SeriesMetadata, series ...promlabels.Labels) {
		leftover, _, err := e.AppendRW2(metadataRecord(t, at, m, series...), 0, 1<<62, 0, false, &rw2Sink{})
		require.NoError(t, err)
		require.Empty(t, leftover)
	},
	"incomplete record": func(t *testing.T, e *Engine, at int64, m mimirpb.SeriesMetadata, series ...promlabels.Labels) {
		// A histogram series leaves the record incomplete: the floats still take its path, and the caller appends the
		// histogram.
		req := &mimirpb.WriteRequest{}
		for _, l := range series {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: at, Value: 1}}, SeriesMetadata: m,
			}})
		}
		histogram := promlabels.FromStrings(model.MetricNameLabel, "other_histogram")
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(histogram), Histograms: []mimirpb.Histogram{histogramAt(at, 1)},
		}})
		records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
		require.NoError(t, err)
		record, err := ReadRW2(records[0].Value, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.NoError(t, err)
		defer record.Release()
		require.False(t, record.Complete() && !record.source.series[len(series)].other)
		leftover, _, err := e.AppendRW2(record, 0, 1<<62, 0, false, &rw2Sink{})
		require.NoError(t, err)
		require.Equal(t, []int{len(series)}, leftover)
		app := e.AppenderV2(context.Background())
		h := mimirpb.FromHistogramProtoToHistogram(&req.Timeseries[len(series)].Histograms[0])
		_, err = app.Append(0, histogram, 0, at, 0, h, nil, storage.AppendV2Options{})
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	},
	"batch": func(t *testing.T, e *Engine, at int64, m mimirpb.SeriesMetadata, series ...promlabels.Labels) {
		var batch []mimirpb.PreallocTimeseries
		var indices []int
		for i, l := range series {
			batch = append(batch, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: at, Value: 1}}, SeriesMetadata: m,
			}})
			indices = append(indices, i)
		}
		leftover, _, err := e.AppendFloats(batch, indices, 0, 1<<62, 0, false, &rw2Sink{})
		require.NoError(t, err)
		require.Empty(t, leftover)
	},
	"appender": func(t *testing.T, e *Engine, at int64, m mimirpb.SeriesMetadata, series ...promlabels.Labels) {
		app := e.AppenderV2(context.Background())
		for _, l := range series {
			_, err := app.Append(0, l, 0, at, 1, nil, nil, storage.AppendV2Options{Metadata: appendedMetadata(m)})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	},
}

// appendedMetadata is m as the ingester gives the appender it.
func appendedMetadata(m mimirpb.SeriesMetadata) metadata.Metadata {
	if m.IsEmpty() {
		return metadata.Metadata{}
	}
	return metadata.Metadata{Type: mimirpb.MetricMetadataMetricTypeToMetricType(m.Type), Unit: m.Unit, Help: m.Help}
}

// metadataEngine is an engine that keeps the series' own metadata and takes samples up to an hour out of order,
// with series created without metadata at created.
func metadataEngine(t *testing.T, created int64, series ...promlabels.Labels) *Engine {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeMetricMetadata(true)
	app := e.AppenderV2(context.Background())
	for _, l := range series {
		_, err := app.Append(0, l, 0, created, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return e
}

func versionsOf(t *testing.T, e *Engine, matchers ...*promlabels.Matcher) map[string][]MetricMetadataVersion {
	found, _, err := e.NativeMetricMetadata(context.Background(), [][]*promlabels.Matcher{matchers}, 0)
	require.NoError(t, err)
	out := map[string][]MetricMetadataVersion{}
	for _, s := range found {
		out[s.Labels.String()] = s.Versions
	}
	return out
}

// Every route records the series' versions as the model merges observations into them, in order and out of order.
func TestMetricMetadataVersionsOnEveryRouteLikeTheModel(t *testing.T) {
	for route, appendTo := range metadataRoutes {
		for seed := range int64(5) {
			t.Run(fmt.Sprintf("%s/%d", route, seed), func(t *testing.T) {
				r := rand.New(rand.NewSource(seed))
				var series []promlabels.Labels
				for n := range 3 {
					series = append(series, promlabels.FromStrings(model.MetricNameLabel, "requests_total", "pod", fmt.Sprint(n)))
				}
				const created = int64(10_000_000)
				e := metadataEngine(t, created, series...)
				values := []mimirpb.SeriesMetadata{
					{Type: mimirpb.COUNTER, Help: "Requests."},
					{Type: mimirpb.COUNTER, Help: "Requests served."},
					{Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"},
				}
				models := make([]metadataModel, len(series))
				used := map[int64]bool{created: true}
				now := created
				for range 40 {
					// Unique times, mostly in order, some up to a minute out of order.
					var at int64
					for at == 0 || used[at] {
						if r.Intn(4) == 0 {
							at = now - int64(r.Intn(60))*1_000 - 500
						} else {
							now += 1_000
							at = now
						}
					}
					used[at] = true
					i := r.Intn(len(series))
					v := r.Intn(len(values))
					appendTo(t, e, at, values[v], series[i])
					models[i].observe(at, uint32(v+1))
				}
				got := versionsOf(t, e, promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "requests_total"))
				for i, s := range series {
					var expected []MetricMetadataVersion
					for _, run := range models[i] {
						m := values[run.id-1]
						expected = append(expected, MetricMetadataVersion{EffectiveFrom: run.from, Metadata: metadata.Metadata{Type: model.MetricTypeCounter, Help: m.Help, Unit: m.Unit}})
					}
					require.Equal(t, expected, got[s.String()], "series %d", i)
				}
			})
		}
	}
}

// What #19563 keeps of a sample's metadata, on every route: an empty type is unknown, an empty value is none,
// staleness markers, quiet zeros and zero samples carry none, and an oversized value isn't kept, and is counted.
func TestMetricMetadataRulesOnEveryRoute(t *testing.T) {
	for route, appendTo := range metadataRoutes {
		t.Run(route, func(t *testing.T) {
			l := promlabels.FromStrings(model.MetricNameLabel, "up")
			e := metadataEngine(t, 1_000, l)
			appendTo(t, e, 2_000, mimirpb.SeriesMetadata{Help: "Up."}, l)
			appendTo(t, e, 3_000, mimirpb.SeriesMetadata{}, l)
			appendTo(t, e, 4_000, mimirpb.SeriesMetadata{Type: mimirpb.GAUGE, Help: strings.Repeat("h", maxMetricMetadataBytes)}, l)
			require.EqualValues(t, 1, e.metricMetadataStats.oversized.Load())

			// Staleness markers, quiet zeros and zero samples go through the appender, as the ingester sends them.
			app := e.AppenderV2(context.Background())
			gauge := metadata.Metadata{Type: model.MetricTypeGauge, Help: "Gauge."}
			for at, v := range map[int64]float64{5_000: math.Float64frombits(value.QuietZeroNaN), 6_000: math.Float64frombits(value.StaleNaN)} {
				_, err := app.Append(0, l, 0, at, v, nil, nil, storage.AppendV2Options{Metadata: gauge})
				require.NoError(t, err)
			}
			// A created timestamp's zero sample comes without metadata, its sample with.
			created := promlabels.FromStrings(model.MetricNameLabel, "created_total")
			_, err := app.Append(0, created, 0, 7_000, 0, nil, nil, storage.AppendV2Options{RejectOutOfOrder: true})
			require.NoError(t, err)
			_, err = app.Append(0, created, 0, 8_000, 1, nil, nil, storage.AppendV2Options{Metadata: gauge})
			require.NoError(t, err)
			// A native histogram's metadata goes with it, as a float's does.
			native := promlabels.FromStrings(model.MetricNameLabel, "latency_seconds")
			h := histogramAt(9_000, 1)
			_, err = app.Append(0, native, 0, 9_000, 0, mimirpb.FromHistogramProtoToHistogram(&h), nil, storage.AppendV2Options{Metadata: metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "seconds"}})
			require.NoError(t, err)
			require.NoError(t, app.Commit())

			got := versionsOf(t, e, promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, ".+"))
			require.Equal(t, []MetricMetadataVersion{{EffectiveFrom: 9_000, Metadata: metadata.Metadata{Type: model.MetricTypeHistogram, Unit: "seconds"}}}, got[native.String()])
			require.Equal(t, []MetricMetadataVersion{{EffectiveFrom: 2_000, Metadata: metadata.Metadata{Type: model.MetricTypeUnknown, Help: "Up."}}}, got[l.String()])
			require.Equal(t, []MetricMetadataVersion{{EffectiveFrom: 8_000, Metadata: gauge}}, got[created.String()])
		})
	}
}

// Lookups take any of the matcher sets, each a conjunction, in label order, up to the limit.
func TestNativeMetricMetadataMatchersAndLimit(t *testing.T) {
	var series []promlabels.Labels
	for n := range 6 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n%2), "pod", fmt.Sprint(n)))
	}
	e := metadataEngine(t, 1_000, series...)
	metadataRoutes["appender"](t, e, 2_000, mimirpb.SeriesMetadata{Type: mimirpb.GAUGE, Help: "Help."}, series...)
	names := func(sets [][]*promlabels.Matcher, limit int) ([]string, bool) {
		found, limited, err := e.NativeMetricMetadata(context.Background(), sets, limit)
		require.NoError(t, err)
		var out []string
		for _, s := range found {
			out = append(out, s.Labels.String())
		}
		return out, limited
	}
	eq := func(name, v string) *promlabels.Matcher {
		return promlabels.MustNewMatcher(promlabels.MatchEqual, name, v)
	}
	got, limited := names([][]*promlabels.Matcher{{eq(model.MetricNameLabel, "metric_0"), eq("pod", "2")}, {eq("pod", "1")}}, 0)
	require.Equal(t, []string{`{__name__="metric_0", pod="2"}`, `{__name__="metric_1", pod="1"}`}, got)
	require.False(t, limited)
	// The same series twice is once.
	got, _ = names([][]*promlabels.Matcher{{eq("pod", "1")}, {eq(model.MetricNameLabel, "metric_1"), eq("pod", "1")}}, 0)
	require.Len(t, got, 1)
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, ".+")}}
	got, limited = names(all, 4)
	require.Equal(t, []string{`{__name__="metric_0", pod="0"}`, `{__name__="metric_0", pod="2"}`, `{__name__="metric_0", pod="4"}`, `{__name__="metric_1", pod="1"}`}, got)
	require.True(t, limited)
	got, limited = names(all, 6)
	require.Len(t, got, 6)
	require.False(t, limited)
}

// With the switch off, a record's or a batch's metadata isn't kept, as the ingester gives the appender none then; a
// reopened engine, restored from its snapshot, starts without any.
func TestMetricMetadataDisabledAndAfterReopening(t *testing.T) {
	dir := t.TempDir()
	l := promlabels.FromStrings(model.MetricNameLabel, "up")
	up := promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "up")
	open := func() *Engine {
		e, err := OpenEngine(dir, "tenant", EngineOptions{Shards: 4, SecondaryHashFunction: secondaryHash})
		require.NoError(t, err)
		return e
	}
	e := open()
	app := e.AppenderV2(context.Background())
	_, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	gauge := mimirpb.SeriesMetadata{Type: mimirpb.GAUGE, Help: "Up."}
	metadataRoutes["complete record"](t, e, 2_000, gauge, l)
	metadataRoutes["batch"](t, e, 3_000, gauge, l)
	require.Empty(t, versionsOf(t, e, up))
	e.SetNativeMetricMetadata(true)
	metadataRoutes["complete record"](t, e, 4_000, gauge, l)
	require.Equal(t, []MetricMetadataVersion{{EffectiveFrom: 4_000, Metadata: metadata.Metadata{Type: model.MetricTypeGauge, Help: "Up."}}}, versionsOf(t, e, up)[l.String()])
	require.NoError(t, e.Close())

	e = open()
	t.Cleanup(func() { _ = e.Close() })
	require.True(t, e.Restored())
	require.Empty(t, versionsOf(t, e, up))
	require.Equal(t, int64(math.MaxInt64), e.metadataObservedFrom.Load())
}
