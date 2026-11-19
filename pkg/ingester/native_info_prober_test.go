// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nativemetadataprober"
	"github.com/grafana/mimir/pkg/querier/stats"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// harnessProberClient is the prober's client on the native info() harness: its queries run on the harness' MQE engine
// and return the query's stats that the query-frontend would, and its lookups go through the harness' distributor.
type harnessProberClient struct {
	h *nativeInfoHarness
}

func (c harnessProberClient) Query(_ context.Context, _, query string, ts time.Time, mode nativemetadataprober.Mode) (nativemetadataprober.QueryResult, error) {
	queryStats, ctx := stats.ContextWithEmptyStats(c.h.queryContext(mode == nativemetadataprober.ModeNative))
	q, err := c.h.engine.NewInstantQuery(ctx, c.h.queryable, nil, query, ts)
	if err != nil {
		return nativemetadataprober.QueryResult{}, err
	}
	defer q.Close()
	res := q.Exec(ctx)
	if res.Err != nil {
		return nativemetadataprober.QueryResult{}, res.Err
	}
	vector, err := res.Vector()
	if err != nil {
		return nativemetadataprober.QueryResult{}, err
	}
	out := nativemetadataprober.QueryResult{
		LookupResponseBytes: queryStats.LoadNativeLookupResponseBytes(),
		TargetInfoFallbacks: queryStats.LoadNativeTargetInfoFallbacks(),
	}
	for _, s := range vector {
		value := strconv.FormatFloat(s.F, 'g', -1, 64)
		if s.H != nil {
			value = s.H.String()
		}
		// Closing the query gives its labels back to the engine's pools.
		out.Samples = append(out.Samples, nativemetadataprober.Sample{Labels: s.Metric.Copy(), Value: value})
	}
	return out, nil
}

func (c harnessProberClient) Lookup(_ context.Context, _ string, lookup nativemetadataprober.Lookup, matchers []string, start, end time.Time) (nativemetadataprober.LookupResult, error) {
	ctx := c.h.queryContext(true)
	var matcherSets [][]*labels.Matcher
	for _, m := range matchers {
		set, err := promqlext.NewPromQLParser().ParseMetricSelector(m)
		if err != nil {
			return nativemetadataprober.LookupResult{}, err
		}
		matcherSets = append(matcherSets, set)
	}
	switch lookup {
	case nativemetadataprober.LookupResources:
		res, err := c.h.distributor.NativeResources(ctx, matcherSets, start.UnixMilli(), end.UnixMilli(), 0, false)
		return nativemetadataprober.LookupResult{Series: len(res.Series), Limited: res.Limited}, err
	case nativemetadataprober.LookupMetricMetadata:
		res, err := c.h.distributor.NativeMetricMetadata(ctx, matcherSets, 0)
		return nativemetadataprober.LookupResult{Series: len(res.Series), Limited: res.Limited}, err
	}
	return nativemetadataprober.LookupResult{}, fmt.Errorf("unknown lookup %q", lookup)
}

// The prober on the harness: native info() agrees with the join, a documented difference between them is a mismatch,
// and a native answer that fell back to the join is counted as not native, on the seriesstore engine.
func TestNativeInfo_Prober(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	api := otlpResource{name: "api", instance: "a", descriptive: map[string]string{"host.name": "node-1"}}

	for name, tc := range map[string]struct {
		// The minutes the target pushes at.
		pushes []int
		query  string
		// The minute the prober evaluates the query at.
		probeAt int
		// Whether the tenant's native resources are off when the prober runs: the ingesters then refuse its lookups,
		// so native info() falls back to the join.
		nativeResourcesOff bool
		// What the round counts, by metric.
		mismatches, fallbacks float64
		lookupFailures        float64
	}{
		"native equals the join": {
			pushes: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, query: `info(requests)`, probeAt: 9,
		},
		// The fixtures' documented difference of a gap inside a version: at minute 12, the join's last target_info
		// sample, at minute 4, is beyond the lookback delta, while native's version is in force through the gap.
		"a documented difference": {
			pushes: []int{0, 1, 2, 3, 4, 20, 21, 22}, query: `info(rate(requests[1h]))`, probeAt: 12,
			mismatches: 1,
		},
		"native falls back": {
			pushes: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, query: `info(requests)`, probeAt: 9,
			nativeResourcesOff: true,
			fallbacks:          1, lookupFailures: 1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.NativeResourcesEnabled = true
			h := newNativeInfoHarness(t, mimir_tsdb.EngineSeriesstore, 2, limits)
			base := time.Now().Truncate(time.Minute).Add(-30 * time.Minute)
			at := func(minute int) time.Time { return base.Add(time.Duration(minute) * time.Minute) }
			for _, m := range tc.pushes {
				h.pushOTLP(t, otlpGauge(at(m), api, 0, 1))
			}
			if tc.nativeResourcesOff {
				h.setNativeResources(false)
			}

			reg := prometheus.NewPedanticRegistry()
			cfg := nativemetadataprober.Config{
				Tenants:        []string{h.tenant},
				Queries:        []nativemetadataprober.Query{{Name: "probe", Expr: tc.query}},
				LookupMatchers: []string{`{__name__="requests"}`},
				LookupRange:    time.Hour,
			}
			prober := nativemetadataprober.NewProber(cfg, harnessProberClient{h: h}, log.NewNopLogger(), reg)
			prober.RunOnce(t.Context(), at(tc.probeAt))

			count := func(name string, labelValues ...string) float64 { return gatheredCount(t, reg, name, labelValues...) }
			require.Equal(t, 1.0, count("native_metadata_prober_query_probes_total"), "the query is compared")
			require.Zero(t, count("native_metadata_prober_query_failures_total"))
			require.Zero(t, count("native_metadata_prober_query_empty_total"), "the query fits the tenant's series")
			require.Equal(t, tc.mismatches, count("native_metadata_prober_query_mismatches_total"), "mismatches")
			require.Equal(t, tc.fallbacks, count("native_metadata_prober_query_not_native_total", "reason", "fallback"), "fallbacks")
			require.Zero(t, count("native_metadata_prober_query_not_native_total", "reason", "no_lookup"), "MQE runs info() itself")
			if tc.fallbacks == 0 {
				require.Positive(t, count("native_metadata_prober_query_lookup_response_bytes_total"), "native info() looked target_info up")
			}
			require.Equal(t, tc.lookupFailures, count("native_metadata_prober_lookup_failures_total", "lookup", "resources"), "resources lookup failures")
			if tc.lookupFailures == 0 {
				// The lookup answers per series: the target's two pods, each with the target's resource.
				require.Equal(t, 2.0, count("native_metadata_prober_lookup_series", "lookup", "resources"), "the target's series")
			}
		})
	}
}

// gatheredCount is the sum of the gathered values of the metric named name, of the series with the label values,
// given as name and value pairs.
func gatheredCount(t testing.TB, reg prometheus.Gatherer, name string, labelValues ...string) float64 {
	families, err := reg.Gather()
	require.NoError(t, err)
	want := map[string]string{}
	for i := 0; i+1 < len(labelValues); i += 2 {
		want[labelValues[i]] = labelValues[i+1]
	}
	total := 0.0
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			matched := 0
			for _, l := range m.GetLabel() {
				if v, ok := want[l.GetName()]; ok && v == l.GetValue() {
					matched++
				}
			}
			if matched == len(want) {
				total += m.GetCounter().GetValue() + m.GetGauge().GetValue()
			}
		}
	}
	return total
}
