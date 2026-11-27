// SPDX-License-Identifier: AGPL-3.0-only

package nativemetadataprober

import (
	"context"
	"errors"
	"flag"
	"io"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// fakeClient answers each mode's query, and the lookups, as a case says, and records when the queries were evaluated.
type fakeClient struct {
	answers    map[Mode]QueryResult
	errs       map[Mode]error
	lookup     LookupResult
	lookupErr  error
	queryTimes []time.Time
}

func (c *fakeClient) Query(_ context.Context, _, _ string, ts time.Time, mode Mode) (QueryResult, error) {
	c.queryTimes = append(c.queryTimes, ts)
	return c.answers[mode], c.errs[mode]
}

func (c *fakeClient) Lookup(context.Context, string, Lookup, []string, time.Time, time.Time) (LookupResult, error) {
	return c.lookup, c.lookupErr
}

func TestProber_RunOnce_Queries(t *testing.T) {
	enriched := func(host, value string) Sample {
		return Sample{Labels: labels.FromStrings("__name__", "up", "host_name", host, "instance", "a", "job", "api"), Value: value}
	}
	other := Sample{Labels: labels.FromStrings("__name__", "up", "instance", "b", "job", "api"), Value: "1"}

	for name, tc := range map[string]struct {
		native, join QueryResult
		nativeErr    error
		// What the round counts, by metric.
		probes, failures, empty, fallbacks, noLookups, mismatches, mismatchedSeries, lookupBytes float64
		// What info()'s own mode's answers count.
		ownAnswers, mixedAnswers, ownModeJoins, ownMismatches float64
	}{
		"native equals the join": {
			native: QueryResult{Samples: []Sample{enriched("node-1", "1"), other}, LookupResponseBytes: 100},
			join:   QueryResult{Samples: []Sample{other, enriched("node-1", "1")}},
			probes: 1, lookupBytes: 100,
		},
		// A series only native has, one only the join has, and one whose value differs.
		"native differs from the join": {
			native: QueryResult{Samples: []Sample{enriched("node-2", "1"), {Labels: other.Labels, Value: "2"}}, LookupResponseBytes: 100},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1"), other}},
			probes: 1, mismatches: 1, mismatchedSeries: 3, lookupBytes: 100,
		},
		// A limited lookup: made, then fallen back from, so the native answer is the join's.
		"native fell back": {
			native: QueryResult{Samples: []Sample{enriched("node-1", "1")}, LookupResponseBytes: 100, TargetInfoFallbacks: 1},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			probes: 1, fallbacks: 1, lookupBytes: 100,
		},
		// Prometheus' engine ran the query, as MQE's fallback does for a query MQE doesn't support.
		"native made no lookup": {
			native: QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			probes: 1, noLookups: 1,
		},
		// info()'s own mode enriched every inner series from its own resource.
		"own mode": {
			native: QueryResult{Samples: []Sample{enriched("node-1", "1")}, LookupResponseBytes: 100, OwnSeries: 1},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			probes: 1, lookupBytes: 100, ownAnswers: 1,
		},
		// Own mode enriched some by the join, and its answer differs from the join's: a mismatch of its own class.
		"own mode, mixed and differing": {
			native: QueryResult{Samples: []Sample{enriched("node-2", "1"), other}, LookupResponseBytes: 100, OwnSeries: 1, JoinSeries: 1},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1"), other}},
			probes: 1, lookupBytes: 100, mixedAnswers: 1, mismatches: 1, mismatchedSeries: 2, ownMismatches: 1,
		},
		// Own mode enriched every inner series by the join, with no lookup, which isn't Prometheus' engine's fallback.
		"own mode by the join": {
			native: QueryResult{Samples: []Sample{enriched("node-1", "1")}, JoinSeries: 1},
			join:   QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			probes: 1, ownModeJoins: 1,
		},
		// Nothing to enrich, which info() needn't look up for: not a native answer that wasn't native.
		"both answers empty": {
			probes: 1, empty: 1,
		},
		"a mode fails": {
			nativeErr: errors.New("query failed"),
			join:      QueryResult{Samples: []Sample{enriched("node-1", "1")}},
			failures:  1,
		},
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeClient{answers: map[Mode]QueryResult{ModeNative: tc.native, ModeJoin: tc.join}, errs: map[Mode]error{ModeNative: tc.nativeErr}}
			cfg := Config{Tenants: []string{"tenant"}, Queries: []Query{{Name: "plain", Expr: "info(up)"}}, EvaluationDelay: time.Minute}
			p := NewProber(cfg, client, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			now := time.Now()
			p.RunOnce(t.Context(), now)

			m := p.metrics
			require.Equal(t, tc.probes, testutil.ToFloat64(m.queryProbes.WithLabelValues("tenant", "plain")), "probes")
			require.Equal(t, tc.failures, testutil.ToFloat64(m.queryFailures.WithLabelValues("tenant", "plain", "native")), "failures")
			require.Equal(t, tc.empty, testutil.ToFloat64(m.queryEmpty.WithLabelValues("tenant", "plain")), "empty")
			require.Equal(t, tc.fallbacks, testutil.ToFloat64(m.queryNotNative.WithLabelValues("tenant", "plain", notNativeFallback)), "fallbacks")
			require.Equal(t, tc.noLookups, testutil.ToFloat64(m.queryNotNative.WithLabelValues("tenant", "plain", notNativeNoLookup)), "no lookups")
			require.Equal(t, tc.mismatches, testutil.ToFloat64(m.queryMismatches.WithLabelValues("tenant", "plain")), "mismatches")
			require.Equal(t, tc.mismatchedSeries, testutil.ToFloat64(m.queryMismatchedSeries.WithLabelValues("tenant", "plain")), "mismatched series")
			require.Equal(t, tc.lookupBytes, testutil.ToFloat64(m.lookupResponseBytes.WithLabelValues("tenant", "plain")), "lookup bytes")
			require.Equal(t, tc.ownAnswers, testutil.ToFloat64(m.queryOwn.WithLabelValues("tenant", "plain", ownAnswer)), "own answers")
			require.Equal(t, tc.mixedAnswers, testutil.ToFloat64(m.queryOwn.WithLabelValues("tenant", "plain", mixedAnswer)), "mixed answers")
			require.Equal(t, tc.ownModeJoins, testutil.ToFloat64(m.queryNotNative.WithLabelValues("tenant", "plain", notNativeOwnModeJoin)), "own mode's joins")
			require.Equal(t, tc.ownMismatches, testutil.ToFloat64(m.queryOwnMismatches.WithLabelValues("tenant", "plain")), "own mismatches")
			// Both modes are evaluated at the same time, before the round, so that they see the same samples.
			require.Equal(t, []time.Time{now.Add(-time.Minute), now.Add(-time.Minute)}, client.queryTimes)
		})
	}
}

func TestProber_RunOnce_Lookups(t *testing.T) {
	for name, tc := range map[string]struct {
		lookup    LookupResult
		lookupErr error
		// What each lookup counts, and the series its answer had.
		failures, limited, series float64
	}{
		"answered":          {lookup: LookupResult{Series: 7}, series: 7},
		"limited":           {lookup: LookupResult{Series: 10, Limited: true}, limited: 1, series: 10},
		"the lookup failed": {lookupErr: errors.New("disabled"), failures: 1},
	} {
		t.Run(name, func(t *testing.T) {
			client := &fakeClient{lookup: tc.lookup, lookupErr: tc.lookupErr}
			p := NewProber(Config{Tenants: []string{"tenant"}, LookupRange: time.Hour}, client, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			p.RunOnce(t.Context(), time.Now())

			for _, lookup := range []Lookup{LookupResources, LookupMetricMetadata} {
				m := p.metrics
				require.Equal(t, 1.0, testutil.ToFloat64(m.lookups.WithLabelValues("tenant", string(lookup))), lookup)
				require.Equal(t, tc.failures, testutil.ToFloat64(m.lookupFailures.WithLabelValues("tenant", string(lookup))), lookup)
				require.Equal(t, tc.limited, testutil.ToFloat64(m.lookupLimited.WithLabelValues("tenant", string(lookup))), lookup)
				require.Equal(t, tc.series, testutil.ToFloat64(m.lookupSeries.WithLabelValues("tenant", string(lookup))), lookup)
			}
		})
	}
}

func TestConfig_RegisterFlags_Queries(t *testing.T) {
	for name, tc := range map[string]struct {
		args     []string
		expected []Query
		err      string
	}{
		"without -query, the default queries": {expected: DefaultQueries},
		// A query of one's own replaces the defaults rather than adding to them.
		"repeated -query": {
			args:     []string{"-query", `plain=info(up)`, "-query", `nodes=info(node_cpu_seconds_total, {k8s_node_name=~".+"})`},
			expected: []Query{{Name: "plain", Expr: `info(up)`}, {Name: "nodes", Expr: `info(node_cpu_seconds_total, {k8s_node_name=~".+"})`}},
		},
		"a query without a name": {args: []string{"-query", `info(up)`}, err: `"info(up)" isn't name=expression`},
	} {
		t.Run(name, func(t *testing.T) {
			var cfg Config
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			cfg.RegisterFlags(fs)
			err := fs.Parse(tc.args)
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg.Queries)
		})
	}
}
