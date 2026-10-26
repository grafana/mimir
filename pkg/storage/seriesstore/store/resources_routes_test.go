// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/prometheus/common/model"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// The specification is Prometheus's #17740, its head's and its MemStore's tests ported to the engine's ingest routes,
// through the engine extension's lookup. The tests here hold in every build, comparing values as #17740 keeps them,
// as strings; the default build's model test is resources_test.go's, the ablation's resources_ablation_test.go's. Not
// ported: the WAL's, the Parquet sidecar's, compaction's and the web API's, which the prototype dropped, and the
// MemStore's own internals, which the engine extension doesn't show. What the default build does differently, on
// purpose:
//   - #17740's appender keeps a series' first resource of an appender; the engine keeps each sample's.
//   - An identity's description versions are the identity's, which all of its series share: a change seen through
//     one series is a version of every series with the identity. #17740's versions are each series' own.
//   - Changes over the limits coalesce (Coalesced), and the head's garbage collection drops the runs and versions
//     that end before the head's min time; #17740 has no limits and keeps a series' versions as long as the series.
//   - Values keep their types; #17740's are strings.
//   - A version is in force from its EffectiveFrom until the next one's, as #17740's VersionAt has it; an unchanged
//     resource doesn't extend #17740's MaxTime, which the lookup doesn't show.

// resourceRoute appends a float of value 1 at at, with the resource's attributes, to each series of the engine.
type resourceRoute func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels)

// resourceRequest is a request of one sample at t for each of lbls, all with the resource.
func resourceRequest(at int64, attributes []mimirpb.ResourceAttribute, lbls ...promlabels.Labels) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{ResourceTable: []mimirpb.ResourceEntry{{Attributes: attributes}}, Source: mimirpb.OTLP}
	for _, l := range lbls {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: at, Value: 1}}, ResourceRef: 1,
		}})
	}
	return req
}

func recordContent(t *testing.T, req *mimirpb.WriteRequest) []byte {
	records, _, err := ingest.RecordSerializerFromVersion(2).ToRecords("ingest", 0, "tenant", req, 64<<20)
	require.NoError(t, err)
	require.Len(t, records, 1)
	return records[0].Value
}

// The engine's ingest routes the ingester takes: a record's fast path, a record with series it leaves to the caller,
// a record the engine can't read all of, which the ingester decodes, a decoded batch, and the appender.
var resourceRoutes = map[string]resourceRoute{
	"complete record": func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) {
		leftover, _, err := e.AppendRW2(resourceRecord(t, at, attributes, series...), 0, 1<<62, 0, true, &rw2Sink{})
		require.NoError(t, err)
		require.Empty(t, leftover)
	},
	"incomplete record": func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) {
		// A histogram series leaves the record incomplete: the floats still take its path, and the caller appends the
		// histogram.
		req := resourceRequest(at, attributes, series...)
		histogram := promlabels.FromStrings(model.MetricNameLabel, "other_histogram")
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(histogram), Histograms: []mimirpb.Histogram{histogramAt(at, 1)},
		}})
		record, err := ReadRW2(recordContent(t, req), ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.NoError(t, err)
		defer record.Release()
		leftover, _, err := e.AppendRW2(record, 0, 1<<62, 0, true, &rw2Sink{})
		require.NoError(t, err)
		require.Equal(t, []int{len(series)}, leftover)
		app := e.AppenderV2(context.Background())
		h := mimirpb.FromHistogramProtoToHistogram(&req.Timeseries[len(series)].Histograms[0])
		_, err = app.Append(0, histogram, 0, at, 0, h, nil, storage.AppendV2Options{})
		require.NoError(t, err)
		require.NoError(t, app.Commit())
	},
	"partial record": func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) {
		// A top-level field the engine doesn't know makes it read the record partially, so the ingester decodes it.
		content := recordContent(t, resourceRequest(at, attributes, series...))
		content = binary.AppendUvarint(binary.AppendUvarint(content, 2000<<3), 1)
		record, err := ReadRW2(content, ingest.V2CommonSymbols.GetSlice(), ingest.V2RecordSymbolOffset)
		require.NoError(t, err)
		require.False(t, record.Complete())
		record.Release()
		var req mimirpb.PreallocWriteRequest
		require.NoError(t, ingest.DeserializeRecordContent(content, &req, 2))
		indices := make([]int, len(req.Timeseries))
		for i := range indices {
			indices[i] = i
		}
		leftover, _, err := e.AppendFloats(req.Timeseries, indices, 0, 1<<62, 0, true, &rw2Sink{})
		require.NoError(t, err)
		require.Empty(t, leftover)
	},
	"batch": func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) {
		entry := &mimirpb.ResourceEntry{Attributes: attributes}
		var batch []mimirpb.PreallocTimeseries
		var indices []int
		for i, l := range series {
			batch = append(batch, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(l), Samples: []mimirpb.Sample{{TimestampMs: at, Value: 1}}, Resource: entry,
			}})
			indices = append(indices, i)
		}
		leftover, _, err := e.AppendFloats(batch, indices, 0, 1<<62, 0, true, &rw2Sink{})
		require.NoError(t, err)
		require.Empty(t, leftover)
	},
	"appender": func(t *testing.T, e *Engine, at int64, attributes []mimirpb.ResourceAttribute, series ...promlabels.Labels) {
		app := e.AppenderV2(context.Background())
		resource := asResourceContext(attributes)
		for _, l := range series {
			_, err := app.Append(0, l, 0, at, 1, nil, nil, storage.AppendV2Options{Resource: resource})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	},
}

// resourcesEngine is an engine that keeps the series' resources and takes samples up to an hour out of order, with
// series created without resources at created.
func resourcesEngine(t *testing.T, created int64, series ...promlabels.Labels) *Engine {
	e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3_600_000, SecondaryHashFunction: secondaryHash})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	e.SetNativeResources(true)
	app := e.AppenderV2(context.Background())
	for _, l := range series {
		_, err := app.Append(0, l, 0, created, 1, nil, nil, storage.AppendV2Options{})
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	return e
}

// stringVersion is a resource version as #17740 keeps it: its attributes' values as strings, by key.
type stringVersion struct {
	From                     int64
	Identifying, Descriptive map[string]string
}

func stringAttributesOf(attributes []mimirpb.ResourceAttribute) map[string]string {
	out := map[string]string{}
	for _, a := range attributes {
		switch a.Type {
		case mimirpb.RESOURCE_ATTRIBUTE_INT:
			out[a.Key] = strconv.FormatInt(a.IntValue, 10)
		case mimirpb.RESOURCE_ATTRIBUTE_BOOL:
			out[a.Key] = strconv.FormatBool(a.BoolValue)
		default:
			out[a.Key] = a.StringValue
		}
	}
	return out
}

// stringVersionsOf returns the resource versions of the series with lbls, as #17740 keeps them.
func stringVersionsOf(t *testing.T, e *Engine, lbls promlabels.Labels) []stringVersion {
	var out []stringVersion
	for _, v := range resourceVersionsOf(t, e, lbls) {
		out = append(out, stringVersion{From: v.EffectiveFrom, Identifying: stringAttributesOf(v.Identifying), Descriptive: stringAttributesOf(v.Descriptive)})
	}
	return out
}

func testStringVersion(from int64, service, host string) stringVersion {
	return stringVersion{
		From:        from,
		Identifying: map[string]string{"service.name": service, "service.instance.id": "1"},
		Descriptive: map[string]string{"host.name": host, "host.cores": "8"},
	}
}

// What #17740 keeps of a sample's resource, on every route: the service attributes identify it and the others describe
// it; the same resource again is the same version, and another is a version from its sample on; quiet zeros, in and
// out of order, and staleness markers carry none.
func TestResourceRulesOnEveryRoute(t *testing.T) {
	for route, appendTo := range resourceRoutes {
		t.Run(route, func(t *testing.T) {
			l := promlabels.FromStrings(model.MetricNameLabel, "up")
			e := resourcesEngine(t, 1_000, l)
			appendTo(t, e, 2_000, testResourceAttributes("frontend", "node-1"), l)
			appendTo(t, e, 3_000, testResourceAttributes("frontend", "node-1"), l)
			appendTo(t, e, 4_000, testResourceAttributes("frontend", "node-2"), l)

			app := e.AppenderV2(context.Background())
			quiet := asResourceContext(testResourceAttributes("quiet", "node-3"))
			for at, v := range map[int64]float64{1_500: math.Float64frombits(value.QuietZeroNaN), 5_000: math.Float64frombits(value.QuietZeroNaN), 6_000: math.Float64frombits(value.StaleNaN)} {
				_, err := app.Append(0, l, 0, at, v, nil, nil, storage.AppendV2Options{Resource: quiet})
				require.NoError(t, err)
			}
			require.NoError(t, app.Commit())

			require.Equal(t, []stringVersion{testStringVersion(2_000, "frontend", "node-1"), testStringVersion(4_000, "frontend", "node-2")}, stringVersionsOf(t, e, l))
		})
	}
}

// A rolled back appender's resources aren't kept.
func TestResourceRollback(t *testing.T) {
	l := promlabels.FromStrings("a", "b")
	e := resourcesEngine(t, 100, l)
	app := e.AppenderV2(context.Background())
	_, err := app.Append(0, l, 0, 200, 2, nil, nil, storage.AppendV2Options{Resource: asResourceContext(testResourceAttributes("frontend", "node-1"))})
	require.NoError(t, err)
	require.NoError(t, app.Rollback())
	require.Empty(t, resourceVersionsOf(t, e, l))
	require.Zero(t, e.ResourceStats().Series)
}

// Series with the same resource share its content, as #17740's content deduplication has them.
func TestResourceContentDedup(t *testing.T) {
	for route, appendTo := range resourceRoutes {
		t.Run(route, func(t *testing.T) {
			a, b := promlabels.FromStrings(model.MetricNameLabel, "up", "pod", "a"), promlabels.FromStrings(model.MetricNameLabel, "up", "pod", "b")
			e := resourcesEngine(t, 1_000, a, b)
			appendTo(t, e, 2_000, testResourceAttributes("frontend", "node-1"), a, b)
			stats := e.ResourceStats()
			require.EqualValues(t, 2, stats.Series)
			require.EqualValues(t, 1, stats.Descriptions)
			require.Equal(t, stringVersionsOf(t, e, a), stringVersionsOf(t, e, b))
		})
	}
}

// Lookups take any of the matcher sets, each a conjunction, in label order, up to the limit, and the time range picks
// the versions in force during it.
func TestNativeResourcesMatchersRangeAndLimit(t *testing.T) {
	var series []promlabels.Labels
	for n := range 6 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n%2), "pod", fmt.Sprint(n)))
	}
	e := resourcesEngine(t, 1_000, series...)
	resourceRoutes["appender"](t, e, 2_000, testResourceAttributes("frontend", "node-1"), series...)
	resourceRoutes["appender"](t, e, 3_000, testResourceAttributes("frontend", "node-2"), series[0])
	names := func(sets [][]*promlabels.Matcher, limit int) ([]string, bool) {
		found, limited, err := e.NativeResources(context.Background(), sets, math.MinInt64, math.MaxInt64, limit)
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
	got, _ = names([][]*promlabels.Matcher{{eq("pod", "1")}, {eq(model.MetricNameLabel, "metric_1"), eq("pod", "1")}}, 0)
	require.Len(t, got, 1, "the same series twice is once")
	all := [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, ".+")}}
	got, limited = names(all, 4)
	require.Equal(t, []string{`{__name__="metric_0", pod="0"}`, `{__name__="metric_0", pod="2"}`, `{__name__="metric_0", pod="4"}`, `{__name__="metric_1", pod="1"}`}, got)
	require.True(t, limited)

	inForce := func(mint, maxt int64) []int64 {
		found, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{{eq("pod", "0")}}, mint, maxt, 0)
		require.NoError(t, err)
		var from []int64
		for _, v := range found[0].Versions {
			from = append(from, v.EffectiveFrom)
		}
		return from
	}
	require.Equal(t, []int64{2_000}, inForce(2_000, 2_999))
	require.Equal(t, []int64{2_000, 3_000}, inForce(2_500, 3_000))
	require.Equal(t, []int64{3_000}, inForce(10_000, 20_000), "the last version is in force from then on")
}

// With the switch off, nothing is kept; metric metadata and resources are switched apart; a reopened engine, restored
// from its snapshot, starts without resources, and their coverage starts again.
func TestResourcesDisabledSeparatelyAndAfterReopening(t *testing.T) {
	dir := t.TempDir()
	l := promlabels.FromStrings(model.MetricNameLabel, "up")
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
	e.SetNativeMetricMetadata(true)
	resourceRoutes["complete record"](t, e, 2_000, testResourceAttributes("frontend", "node-1"), l)
	resourceRoutes["batch"](t, e, 3_000, testResourceAttributes("frontend", "node-1"), l)
	require.Empty(t, resourceVersionsOf(t, e, l))
	e.SetNativeMetricMetadata(false)
	e.SetNativeResources(true)
	// The appender keeps what it's given, which the ingester leaves out when the switch is off.
	metadataRoutes["complete record"](t, e, 4_000, mimirpb.SeriesMetadata{Type: mimirpb.GAUGE, Help: "Up."}, l)
	resourceRoutes["complete record"](t, e, 5_000, testResourceAttributes("frontend", "node-1"), l)
	require.Equal(t, []stringVersion{testStringVersion(5_000, "frontend", "node-1")}, stringVersionsOf(t, e, l))
	require.Empty(t, versionsOf(t, e, promlabels.MustNewMatcher(promlabels.MatchEqual, model.MetricNameLabel, "up")))
	require.EqualValues(t, 5_000, e.NativeResourcesCoverageStart())
	require.NoError(t, e.Close())

	e = open()
	t.Cleanup(func() { _ = e.Close() })
	require.True(t, e.Restored())
	require.Empty(t, resourceVersionsOf(t, e, l))
	require.Equal(t, int64(math.MaxInt64), e.resourcesObservedFrom.Load())
}

// The reverse index is kept up to date from the engine's open, so that lookups made while series are appended lose
// none of them.
func TestResourceAttributeIndexUnderConcurrentAppendsAndLookups(t *testing.T) {
	e := resourcesEngine(t, 0)
	const series = 2000
	done := make(chan struct{})
	var lookups sync.WaitGroup
	lookups.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				resourceAttributeSeries(e, "service.name", "frontend")
			}
		}
	})
	for n := range series {
		resourceRoutes["appender"](t, e, 100, testResourceAttributes("frontend", "node-1"), promlabels.FromStrings("a", strconv.Itoa(n)))
	}
	close(done)
	lookups.Wait()
	require.Equal(t, series, resourceAttributeSeries(e, "service.name", "frontend"))
}

// Replaying the same records gives the same resources: of series created and changing identity and description, in
// and out of order, as an ingester replaying a partition from its start has them.
func TestReplayingResourcesGivesTheSameState(t *testing.T) {
	var series []promlabels.Labels
	for n := range 12 {
		series = append(series, promlabels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", n%3), "pod", fmt.Sprint(n)))
	}
	replay := func() ([]NativeResourcesSeries, ResourceStats) {
		e, err := OpenEngine(t.TempDir(), "tenant", EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3_600_000, SecondaryHashFunction: secondaryHash, IndexedResourceAttributes: []string{"host.name"}})
		require.NoError(t, err)
		t.Cleanup(func() { _ = e.Close() })
		e.SetNativeResources(true)
		app := e.AppenderV2(context.Background())
		for _, l := range series {
			_, err := app.Append(0, l, 0, 1_000, 1, nil, nil, storage.AppendV2Options{})
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
		for step := range 40 {
			// Each step's series get one of a few services and hosts, some steps out of order.
			at := int64(10_000 + step*1_000)
			if step%7 == 3 {
				at -= 5_500
			}
			for n, l := range series {
				attributes := testResourceAttributes(fmt.Sprintf("service-%d", (n+step/10)%3), fmt.Sprintf("node-%d", (n*step)%4))
				resourceRoutes["complete record"](t, e, at, attributes, l)
			}
		}
		found, _, err := e.NativeResources(context.Background(), [][]*promlabels.Matcher{{promlabels.MustNewMatcher(promlabels.MatchRegexp, model.MetricNameLabel, ".+")}}, math.MinInt64, math.MaxInt64, 0)
		require.NoError(t, err)
		stats := e.ResourceStats()
		stats.MemoHits, stats.MemoMisses = 0, 0
		return found, stats
	}
	first, firstStats := replay()
	second, secondStats := replay()
	require.Len(t, first, len(series))
	require.Equal(t, first, second)
	require.Equal(t, firstStats, secondStats)
}
