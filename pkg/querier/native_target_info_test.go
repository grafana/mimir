// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestSelectsMetricName(t *testing.T) {
	name := func(t labels.MatchType, v string) *labels.Matcher {
		return labels.MustNewMatcher(t, labels.MetricName, v)
	}
	job := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	for desc, tc := range map[string]struct {
		matchers         []*labels.Matcher
		wantSelects      bool
		wantOnlySelected bool
	}{
		"target_info":                          {matchers: []*labels.Matcher{name(labels.MatchEqual, "target_info"), job}, wantSelects: true, wantOnlySelected: true},
		"every info metric":                    {matchers: []*labels.Matcher{name(labels.MatchRegexp, ".+_info"), job}, wantSelects: true},
		"another info metric":                  {matchers: []*labels.Matcher{name(labels.MatchEqual, "build_info"), job}},
		"any name, which target_info is among": {matchers: []*labels.Matcher{job}, wantSelects: true},
	} {
		t.Run(desc, func(t *testing.T) {
			selects, only := selectsMetricName(tc.matchers, "target_info")
			require.Equal(t, tc.wantSelects, selects)
			require.Equal(t, tc.wantOnlySelected, only)
		})
	}
}

// A version's synthesized samples are at its start, every interval after it, and at its end, as the OTLP conversion
// writes them over a request's samples, within the select's range.
func TestAppendTargetInfoSamples(t *testing.T) {
	times := func(samples []model.SamplePair) []int64 {
		out := make([]int64, 0, len(samples))
		for _, s := range samples {
			out = append(out, int64(s.Timestamp))
		}
		return out
	}
	require.Equal(t, []int64{0, 300, 600, 900, 1000}, times(appendTargetInfoSamples(nil, 0, 1000, 0, 2000, 300)))
	// The range clips the version: no sample before its start, and none after its end.
	require.Equal(t, []int64{600, 900}, times(appendTargetInfoSamples(nil, 0, 1000, 450, 950, 300)))
	// A version that ends where it starts has the one sample.
	require.Equal(t, []int64{500}, times(appendTargetInfoSamples(nil, 500, 500, 0, 2000, 300)))
}

// fakeNativeLookups answers NativeResources with resources, or err.
type fakeNativeLookups struct {
	resources distributor.NativeResourcesResult
	err       error
	// The lookups by series' answer and error, and how many times its release was called.
	seriesResources distributor.NativeResourcesResult
	seriesErr       error
	released        int
}

func (f *fakeNativeLookups) NativeMetricMetadata(context.Context, [][]*labels.Matcher, int) (distributor.NativeMetricMetadataResult, error) {
	return distributor.NativeMetricMetadataResult{}, nil
}

func (f *fakeNativeLookups) NativeResources(context.Context, [][]*labels.Matcher, int64, int64, int, bool) (distributor.NativeResourcesResult, error) {
	return f.resources, f.err
}

func (f *fakeNativeLookups) NativeSeriesResources(context.Context, [][]*labels.Matcher, int64, int64) (distributor.NativeResourcesResult, func(), error) {
	if f.seriesErr != nil {
		return distributor.NativeResourcesResult{}, nil, f.seriesErr
	}
	return f.seriesResources, func() { f.released++ }, nil
}

// storedQueryable is storage with the series, which records the matchers of each select.
type storedQueryable struct {
	series   []storage.Series
	selected [][]*labels.Matcher
}

func (s *storedQueryable) Querier(int64, int64) (storage.Querier, error) {
	return &storedQuerier{s}, nil
}

type storedQuerier struct {
	*storedQueryable
}

func (q *storedQuerier) Select(_ context.Context, _ bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.selected = append(q.selected, matchers)
	var out []storage.Series
	for _, s := range q.series {
		if matchesAll(matchers, s.Labels()) {
			out = append(out, s)
		}
	}
	return series.NewConcreteSeriesSetFromUnsortedSeries(out)
}

func (q *storedQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *storedQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, nil
}

func (q *storedQuerier) Close() error { return nil }

func samplesOf(t *testing.T, set storage.SeriesSet) map[string][]int64 {
	out := map[string][]int64{}
	for set.Next() {
		s := set.At()
		var times []int64
		it := s.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			ts, _ := it.At()
			times = append(times, ts)
		}
		out[s.Labels().String()] = times
	}
	require.NoError(t, set.Err())
	return out
}

func TestNativeTargetInfoQuerier_Select(t *testing.T) {
	limits := validation.Limits{}
	limits.NativeResourcesEnabled = true
	overrides := validation.NewOverrides(limits, nil)
	targetInfo := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "target_info")
	job := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	nativeLabels := labels.FromStrings("__name__", "target_info", "host_name", "node-2", "instance", "a", "job", "api")
	storedLabels := labels.FromStrings("__name__", "target_info", "host_name", "node-1", "instance", "a", "job", "api")
	stored := []storage.Series{
		series.NewConcreteSeries(storedLabels, []model.SamplePair{{Timestamp: 100_000, Value: 1}, {Timestamp: 400_000, Value: 1}}, nil),
		series.NewConcreteSeries(labels.FromStrings("__name__", "build_info", "instance", "a", "job", "api", "version", "1"), []model.SamplePair{{Timestamp: 100_000, Value: 1}}, nil),
	}
	resources := distributor.NativeResourcesResult{
		// The native resources cover from 300s on: the stored target_info is in force before.
		CoverageStart: 300_000,
		Series: []ingester_client.NativeResourcesSeries{{
			Labels: []mimirpb.LabelAdapter{{Name: "instance", Value: "a"}, {Name: "job", Value: "api"}},
			Versions: []ingester_client.NativeResourceVersion{
				{MinTimeMs: 0, MaxTimeMs: 299_999, Identifying: map[string]string{"service.name": "api", "service.instance.id": "a"}, Descriptive: map[string]string{"host.name": "node-1"}},
				{MinTimeMs: 300_000, MaxTimeMs: 600_000, Identifying: map[string]string{"service.name": "api", "service.instance.id": "a"}, Descriptive: map[string]string{"host.name": "node-2"}},
			},
		}},
	}
	hints := &storage.SelectHints{Start: 0, End: 900_000}
	selectWith := func(t *testing.T, ctx context.Context, lookups *fakeNativeLookups, matchers ...*labels.Matcher) (map[string][]int64, *storedQueryable, *NativeTargetInfoMetrics) {
		next := &storedQueryable{series: stored}
		metrics := NewNativeTargetInfoMetrics(prometheus.NewRegistry())
		q, err := NewNativeTargetInfoQueryable(next, lookups, overrides, 5*time.Minute, metrics).Querier(hints.Start, hints.End)
		require.NoError(t, err)
		return samplesOf(t, q.Select(ctx, true, hints, matchers...)), next, metrics
	}
	ctx := user.InjectOrgID(context.Background(), "tenant")

	t.Run("target_info, stitched at the cutover", func(t *testing.T) {
		queryStats, ctx := stats.ContextWithEmptyStats(ctx)
		got, next, metrics := selectWith(t, ctx, &fakeNativeLookups{resources: resources}, targetInfo, job)
		// The stored series' samples before the cutover, then the native series' from it, at every 150s, its version's
		// half of the lookback delta, and at its end.
		require.Equal(t, map[string][]int64{
			storedLabels.String(): {100_000},
			nativeLabels.String(): {300_000, 450_000, 600_000},
		}, got)
		require.Len(t, next.selected, 1, "only the stored target_info before the cutover")
		require.Equal(t, 1.0, testutil.ToFloat64(metrics.lookups))
		require.Zero(t, queryStats.LoadNativeTargetInfoFallbacks())
	})

	t.Run("every info metric", func(t *testing.T) {
		got, next, _ := selectWith(t, ctx, &fakeNativeLookups{resources: resources}, labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".+_info"), job)
		require.Contains(t, got, `{__name__="build_info", instance="a", job="api", version="1"}`)
		require.Equal(t, []int64{300_000, 450_000, 600_000}, got[nativeLabels.String()])
		// The other info metrics' select leaves target_info out, which comes natively.
		require.Len(t, next.selected, 2)
		require.Contains(t, next.selected[1], labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, "target_info"))
	})

	t.Run("the join, as the request asks", func(t *testing.T) {
		got, next, metrics := selectWith(t, querierapi.ContextWithNativeTargetInfo(ctx, false), &fakeNativeLookups{resources: resources}, targetInfo, job)
		require.Equal(t, map[string][]int64{storedLabels.String(): {100_000, 400_000}}, got)
		require.Equal(t, [][]*labels.Matcher{{targetInfo, job}}, next.selected)
		require.Zero(t, testutil.ToFloat64(metrics.lookups))
	})

	// Each reason to fall back answers with the stored target_info, and is counted, in the querier's metrics and in
	// the query's stats, which tell its caller the answer is the join's.
	for reason, tc := range map[string]struct {
		lookups  *fakeNativeLookups
		matchers []*labels.Matcher
	}{
		nativeTargetInfoFallbackDisabled:  {lookups: &fakeNativeLookups{err: ingester_client.ErrNativeMetadataDisabled}, matchers: []*labels.Matcher{targetInfo, job}},
		nativeTargetInfoFallbackCap:       {lookups: &fakeNativeLookups{err: ingester_client.NativeLookupTooManySeriesError(10)}, matchers: []*labels.Matcher{targetInfo, job}},
		nativeTargetInfoFallbackLimited:   {lookups: &fakeNativeLookups{resources: distributor.NativeResourcesResult{Limited: true}}, matchers: []*labels.Matcher{targetInfo, job}},
		nativeTargetInfoFallbackUnbounded: {lookups: &fakeNativeLookups{resources: resources}, matchers: []*labels.Matcher{targetInfo, labels.MustNewMatcher(labels.MatchRegexp, "job", ".*")}},
	} {
		t.Run("falls back: "+reason, func(t *testing.T) {
			queryStats, ctx := stats.ContextWithEmptyStats(ctx)
			got, _, metrics := selectWith(t, ctx, tc.lookups, tc.matchers...)
			require.Equal(t, map[string][]int64{storedLabels.String(): {100_000, 400_000}}, got)
			require.Equal(t, 1.0, testutil.ToFloat64(metrics.fallbacks.WithLabelValues(reason)))
			require.Equal(t, uint64(1), queryStats.LoadNativeTargetInfoFallbacks())
		})
	}
}

// info()'s own mode, as the querier offers it: the decision follows Select's gate and the tenant's switch; a select in
// own mode is the stored series' only; and the lookup by series answers each series' versions as the target_info
// labels the OTLP conversion writes for them, or falls back, counted as Select's fallbacks are.
func TestNativeTargetInfoQuerier_OwnMode(t *testing.T) {
	targetInfo := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "target_info")
	job := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	storedLabels := labels.FromStrings("__name__", "target_info", "host_name", "node-1", "instance", "a", "job", "api")
	stored := []storage.Series{series.NewConcreteSeries(storedLabels, []model.SamplePair{{Timestamp: 100_000, Value: 1}, {Timestamp: 400_000, Value: 1}}, nil)}
	hints := &storage.SelectHints{Start: 0, End: 900_000}
	querierWith := func(t *testing.T, ownResources, nativeResources bool, lookups *fakeNativeLookups) (querierapi.NativeInfoResources, storage.Querier, *storedQueryable, *NativeTargetInfoMetrics) {
		limits := validation.Limits{}
		limits.NativeResourcesEnabled = nativeResources
		limits.NativeInfoOwnResourcesEnabled = ownResources
		next := &storedQueryable{series: stored}
		metrics := NewNativeTargetInfoMetrics(prometheus.NewRegistry())
		q, err := NewNativeTargetInfoQueryable(next, lookups, validation.NewOverrides(limits, nil), 5*time.Minute, metrics).Querier(hints.Start, hints.End)
		require.NoError(t, err)
		own, ok := q.(querierapi.NativeInfoResources)
		require.True(t, ok, "info() finds own mode through the querier of its info series")
		return own, q, next, metrics
	}
	ctx := user.InjectOrgID(context.Background(), "tenant")
	native, join := true, false

	t.Run("the decision", func(t *testing.T) {
		for name, tc := range map[string]struct {
			ownResources, nativeResources bool
			// The request's choice of native target_info or the join, if any.
			requested *bool
			want      bool
		}{
			"native by the tenant's native resources": {ownResources: true, nativeResources: true, want: true},
			"native by the request":                   {ownResources: true, requested: &native, want: true},
			"the join by the request":                 {ownResources: true, nativeResources: true, requested: &join},
			"without native resources":                {ownResources: true},
			"the switch off":                          {nativeResources: true},
		} {
			t.Run(name, func(t *testing.T) {
				own, _, _, _ := querierWith(t, tc.ownResources, tc.nativeResources, &fakeNativeLookups{})
				ctx := ctx
				if tc.requested != nil {
					ctx = querierapi.ContextWithNativeTargetInfo(ctx, *tc.requested)
				}
				require.Equal(t, tc.want, own.NativeInfoOwnMode(ctx))
			})
		}
	})

	t.Run("a select in own mode is the stored series'", func(t *testing.T) {
		// A lookup by target would fail the select.
		_, q, next, metrics := querierWith(t, true, true, &fakeNativeLookups{err: errors.New("no lookup by target in own mode")})
		got := samplesOf(t, q.Select(querierapi.ContextWithNativeInfoOwnMode(ctx), true, hints, targetInfo, job))
		require.Equal(t, map[string][]int64{storedLabels.String(): {100_000, 400_000}}, got)
		require.Equal(t, [][]*labels.Matcher{{targetInfo, job}}, next.selected)
		require.Zero(t, testutil.ToFloat64(metrics.lookups))
	})

	t.Run("the lookup by series", func(t *testing.T) {
		identifying := map[string]string{"service.name": "api", "service.instance.id": "a"}
		seriesLabels := []mimirpb.LabelAdapter{{Name: "__name__", Value: "up"}, {Name: "instance", Value: "a"}, {Name: "job", Value: "api"}}
		lookups := &fakeNativeLookups{seriesResources: distributor.NativeResourcesResult{
			CoverageStart: 300_000,
			Series: []ingester_client.NativeResourcesSeries{{
				Labels: seriesLabels,
				Versions: []ingester_client.NativeResourceVersion{
					{MinTimeMs: 300_000, MaxTimeMs: 599_999, Identifying: identifying, Descriptive: map[string]string{"host.name": "node-1"}},
					// Only identifying attributes: the conversion writes no target_info for it.
					{MinTimeMs: 600_000, MaxTimeMs: 900_000, Identifying: identifying},
				},
			}},
		}}
		own, _, _, metrics := querierWith(t, true, true, lookups)
		resources, ok, err := own.NativeSeriesResources(ctx, hints, job)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(300_000), resources.CoverageStart)
		require.Equal(t, []querierapi.NativeSeriesResource{{
			Labels: mimirpb.FromLabelAdaptersToLabels(seriesLabels),
			Versions: []querierapi.NativeSeriesResourceVersion{
				{MinTime: 300_000, MaxTime: 599_999, Labels: storedLabels},
				{MinTime: 600_000, MaxTime: 900_000, Labels: labels.EmptyLabels()},
			},
		}}, resources.Series)
		require.Equal(t, 1.0, testutil.ToFloat64(metrics.lookups))
		resources.Release()
		require.Equal(t, 1, lookups.released, "the release is the distributor's")
	})

	t.Run("a lookup the ingesters refuse falls back", func(t *testing.T) {
		for reason, err := range map[string]error{
			nativeTargetInfoFallbackDisabled: ingester_client.ErrNativeMetadataDisabled,
			nativeTargetInfoFallbackCap:      ingester_client.NativeLookupTooManySeriesError(100),
		} {
			t.Run(reason, func(t *testing.T) {
				queryStats, ctx := stats.ContextWithEmptyStats(ctx)
				own, _, _, metrics := querierWith(t, true, true, &fakeNativeLookups{seriesErr: err})
				_, ok, err := own.NativeSeriesResources(ctx, hints, job)
				require.NoError(t, err)
				require.False(t, ok)
				require.Equal(t, 1.0, testutil.ToFloat64(metrics.fallbacks.WithLabelValues(reason)))
				require.Equal(t, uint64(1), queryStats.LoadNativeTargetInfoFallbacks())
			})
		}
	})
}
