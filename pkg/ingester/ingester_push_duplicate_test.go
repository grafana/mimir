// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// writeRequestManySeries builds a single WriteRequest holding one PreallocTimeseries
// per element of samples, all sharing the same label set. This mimics an incoming
// request that carries several timeseries that only differ in their samples, e.g. what
// the distributor's merge middleware is meant to collapse before it reaches the ingester.
func writeRequestManySeries(lbls labels.Labels, samples []mimirpb.Sample) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{Source: mimirpb.API}
	for _, s := range samples {
		ts := &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
			Samples: []mimirpb.Sample{s},
		}
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
	}
	return req
}

// tsdbFloatCounter returns the value of the {type="float"} series of the named TSDB
// counter in the given registry, or 0 if that series is absent. It lets us assert on a
// single TSDB counter value without having to enumerate the sibling {type="histogram"}
// series that these vectors also emit.
func tsdbFloatCounter(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "type" && l.GetValue() == "float" {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests illustrates how the
// ingester (and, underneath, the Prometheus TSDB head appender) handles multiple
// samples that share a timestamp for the same series, across four scenarios:
//
//  1. Exact duplicate (same ts + same value) within a single WriteRequest: the extra
//     sample is silently dropped at commit time, no error, nothing discarded.
//  2. Conflicting value (same ts, different value) within a single WriteRequest: the
//     conflict is only detected at commit time, where the error is swallowed, so the
//     sample is *still* silently dropped, no error surfaced to the caller.
//  3. Exact duplicate (same ts + same value) across two separate WriteRequests: the
//     second request sees the already-committed sample at Append time, but because the
//     value matches, the TSDB tolerates it and returns no error. It is dropped just like
//     scenario 1 (no error, nothing discarded) — the across-requests counterpart of it.
//  4. Conflicting value (same ts, different value) across two separate WriteRequests:
//     the second request sees the already-committed sample at Append time and the
//     conflict is surfaced as a soft "new-value-for-timestamp" error — the
//     across-requests counterpart of scenario 2.
func TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests(t *testing.T) {
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	metricLabelSet := mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(metricLabels))
	userID := "test"

	// Metrics tracked by the ingester itself (counted at Append time, i.e. optimistically).
	sampleMetricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_discarded_samples_total",
	}

	type testCase struct {
		reqs             []*mimirpb.WriteRequest
		expectLastReqErr bool
		expectedIngested model.Matrix
		expectedMetrics  string
		// Metrics tracked by the underlying Prometheus TSDB head, counted at Commit time
		// (i.e. after in-batch duplicates/conflicts have actually been dropped). These live
		// in the per-tenant TSDB registry, which the ingester does not fully re-export.
		expectedTSDBSamplesAppended float64 // prometheus_tsdb_head_samples_appended_total{type="float"}
		expectedTSDBOutOfOrder      float64 // prometheus_tsdb_out_of_order_samples_total{type="float"}
	}

	tests := map[string]testCase{
		// Scenario 1: three timeseries with equal labels in ONE request. The first two
		// carry the exact same (ts=100, value=1) sample; the third carries (ts=101, value=2).
		// The duplicate is silently dropped as an exact duplicate at commit time.
		"exact duplicate within a single request is silently dropped": {
			reqs: []*mimirpb.WriteRequest{
				writeRequestManySeries(metricLabels, []mimirpb.Sample{
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 101, Value: 2},
				}),
			},
			expectLastReqErr: false,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 100}, {Value: 2, Timestamp: 101}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 3
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
			`,
			// The TSDB head only actually appended 2 samples: the duplicate was dropped at
			// commit time. Note the gap vs the ingester's optimistic count of 3 above. The
			// drop is not counted as out-of-order (it is an exact duplicate), so that
			// failure counter stays 0.
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Scenario 2: same as scenario 1, but the second timeseries carries a DIFFERENT
		// value (ts=100, value=2) at the same timestamp as the first (ts=100, value=1).
		// Because all three samples are appended into the same uncommitted appender, the
		// conflict is only detected at commit time, where the TSDB swallows the error.
		// The sample is still silently dropped: no error, nothing discarded.
		"conflicting value within a single request is silently dropped": {
			reqs: []*mimirpb.WriteRequest{
				writeRequestManySeries(metricLabels, []mimirpb.Sample{
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 100, Value: 2},
					{TimestampMs: 101, Value: 3},
				}),
			},
			expectLastReqErr: false,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 100}, {Value: 3, Timestamp: 101}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 3
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
			`,
			// Same as scenario 1 from the TSDB's point of view: only 2 samples were appended.
			// The conflicting (ts=100, value=2) sample produced ErrDuplicateSampleForTimestamp
			// internally at commit time, but that error is swallowed and does not map to any
			// TSDB failure counter (out-of-order stays 0), so nothing here reflects the drop
			// beyond the missing appended sample.
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Scenario 3: the exact same (ts=100, value=1) sample arrives in TWO separate
		// requests. Unlike scenario 4, the second request's Append does not error: because
		// the value matches the already-committed sample, the TSDB tolerates the exact
		// duplicate (appendable returns no error at Append time) and then drops it at commit
		// time. So this behaves like scenario 1 across requests: no error, nothing discarded,
		// only one sample actually stored.
		"exact duplicate across two requests is silently dropped": {
			reqs: []*mimirpb.WriteRequest{
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 1}}),
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 1}}),
			},
			expectLastReqErr: false,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 100}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
			`,
			// The ingester optimistically counts both requests' samples as ingested (2), but
			// the TSDB head only appended 1: the second, exact-duplicate sample was dropped at
			// commit time. Not an out-of-order failure.
			expectedTSDBSamplesAppended: 1,
			expectedTSDBOutOfOrder:      0,
		},

		// Scenario 4: the conflicting (ts=100, value=2) sample arrives in a SEPARATE
		// request, after (ts=100, value=1) has already been committed. Now the conflict
		// is detected at Append time and surfaced as a soft "new-value-for-timestamp"
		// error; the sample is discarded and reported.
		"conflicting value across two requests is surfaced as a soft error": {
			reqs: []*mimirpb.WriteRequest{
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 1}}),
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 2}}),
			},
			expectLastReqErr: true,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 100}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
			`,
			// Here the TSDB and the ingester agree: 1 sample appended. The conflict was caught
			// at Append time (against the already-committed sample) and reported, rather than
			// swallowed at commit time. It is still not an out-of-order failure.
			expectedTSDBSamplesAppended: 1,
			expectedTSDBOutOfOrder:      0,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.ReplicationFactor = 1
			limits := defaultLimitsTestConfig()

			i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
			require.NoError(t, err)
			startAndWaitHealthy(t, i, r)

			ctx := user.InjectOrgID(context.Background(), userID)

			// Push all requests. Only the last request may return an error.
			for idx, req := range testData.reqs {
				_, err := i.Push(ctx, req)
				if idx == len(testData.reqs)-1 && testData.expectLastReqErr {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}

			// Read back the samples that were actually stored.
			s := &stream{ctx: ctx}
			err = i.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*"}},
			}, s)
			require.NoError(t, err)

			res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)
			if len(res) == 0 {
				res = nil
			}
			require.Equal(t, testData.expectedIngested, res)

			// Check the samples-related metrics tracked by the ingester.
			require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), sampleMetricNames...))

			// Check what the underlying Prometheus TSDB actually appended. These metrics live
			// in the per-tenant TSDB registry, which the ingester does not fully re-export.
			tsdbReg := i.tsdbMetrics.RegistryForTenant(userID)
			require.NotNil(t, tsdbReg)
			require.Equal(t, testData.expectedTSDBSamplesAppended, tsdbFloatCounter(t, tsdbReg, "prometheus_tsdb_head_samples_appended_total"), "TSDB in-order float samples appended")
			require.Equal(t, testData.expectedTSDBOutOfOrder, tsdbFloatCounter(t, tsdbReg, "prometheus_tsdb_out_of_order_samples_total"), "TSDB out-of-order float samples rejected")
		})
	}
}
