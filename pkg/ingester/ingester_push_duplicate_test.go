// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
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

// tsdbSampleCounter returns the value of the {type=<sampleType>} series of the named
// TSDB counter in the given registry, or 0 if that series is absent. It lets us assert
// on a single TSDB counter value without enumerating the sibling type series that these
// vectors also emit.
func tsdbSampleCounter(t *testing.T, reg *prometheus.Registry, name, sampleType string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "type" && l.GetValue() == sampleType {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests illustrates how the
// ingester (and, underneath, the Prometheus TSDB head appender) handles multiple
// samples that share a timestamp for the same series, across four scenarios.
//
// PROTOTYPE of fix D: the TSDB appender now reports, via DiscardedSampleStats, the
// float samples it dropped at commit time because a sample already existed at that
// timestamp — split by whether the dropped value differed from or matched the stored
// one. The ingester reconciles its optimistic per-Append accounting with that report:
// the dropped samples are subtracted from cortex_ingester_ingested_samples_total and
// added to cortex_discarded_samples_total with reason "new-value-for-timestamp" (value
// differed) or "same-value-for-timestamp" (value matched).
//
//  1. Exact duplicate (same ts + same value) within a single WriteRequest: the extra
//     sample is dropped at commit time; now counted as discarded "same-value-for-timestamp".
//  2. Conflicting value (same ts, different value) within a single WriteRequest: the
//     conflict is detected at commit time (previously swallowed); now counted as
//     discarded "new-value-for-timestamp".
//  3. Exact duplicate (same ts + same value) across two separate WriteRequests: the
//     second request's sample is tolerated at Append time (value matches) then dropped at
//     commit; now counted as discarded "same-value-for-timestamp" — the across-requests
//     counterpart of scenario 1.
//  4. Conflicting value (same ts, different value) across two separate WriteRequests:
//     the second request's sample is rejected at *Append* time (not commit), so it takes
//     the pre-existing soft "new-value-for-timestamp" path and is unaffected by fix D —
//     the across-requests counterpart of scenario 2.
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
		// The duplicate is dropped as an exact duplicate at commit time and, with fix D,
		// counted as discarded "same-value-for-timestamp".
		"exact duplicate within a single request is discarded as same-value-for-timestamp": {
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
			// ingested is now 2, not 3: the exact duplicate is reconciled out and shows up
			// as a discarded "same-value-for-timestamp" sample instead.
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 1
			`,
			// The TSDB head appended 2 samples, matching the reconciled ingested count.
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Scenario 2: same as scenario 1, but the second timeseries carries a DIFFERENT
		// value (ts=100, value=2) at the same timestamp as the first (ts=100, value=1).
		// Because all three samples are appended into the same uncommitted appender, the
		// conflict is only detected at commit time. Previously the error was swallowed and
		// the sample silently dropped; with fix D it is counted as discarded
		// "new-value-for-timestamp".
		"conflicting value within a single request is discarded as new-value-for-timestamp": {
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
			// ingested is now 2, not 3: the conflicting sample is reconciled out and shows
			// up as a discarded "new-value-for-timestamp" sample. Note this remains a silent
			// drop from the client's perspective (no push error) — only the metric changed.
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
			`,
			// The TSDB head appended 2 samples, matching the reconciled ingested count.
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Scenario 3: the exact same (ts=100, value=1) sample arrives in TWO separate
		// requests. Unlike scenario 4, the second request's Append does not error: because
		// the value matches the already-committed sample, the TSDB tolerates the exact
		// duplicate (appendable returns no error at Append time) and then drops it at commit
		// time. With fix D this drop is reconciled: ingested drops to 1 and the sample is
		// counted as discarded "same-value-for-timestamp" — the across-requests counterpart
		// of scenario 1.
		"exact duplicate across two requests is discarded as same-value-for-timestamp": {
			reqs: []*mimirpb.WriteRequest{
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 1}}),
				writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 100, Value: 1}}),
			},
			expectLastReqErr: false,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 100}}},
			},
			// ingested is now 1, not 2: the second request's exact duplicate is reconciled
			// out at commit and counted as discarded "same-value-for-timestamp".
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 1
			`,
			// The TSDB head appended 1 sample, matching the reconciled ingested count.
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
			require.Equal(t, testData.expectedTSDBSamplesAppended, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_head_samples_appended_total", "float"), "TSDB in-order float samples appended")
			require.Equal(t, testData.expectedTSDBOutOfOrder, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_out_of_order_samples_total", "float"), "TSDB out-of-order float samples rejected")
		})
	}
}

// histogramPoint is a (timestamp, generator index) pair used to build native
// histogram samples. Two points with the same index generate equal histograms;
// different indexes generate different histograms.
type histogramPoint struct {
	ts  int64
	idx int
}

// writeRequestManyHistogramSeries builds a single WriteRequest holding one
// PreallocTimeseries per point, each carrying a single native histogram, all sharing
// the same label set — the native-histogram analog of writeRequestManySeries.
func writeRequestManyHistogramSeries(lbls labels.Labels, points []histogramPoint) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{Source: mimirpb.API}
	for _, p := range points {
		ts := &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(lbls),
			Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(p.ts, util_test.GenerateTestHistogram(p.idx))},
		}
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: ts})
	}
	return req
}

// TestIngester_Push_DuplicateTimestampHistograms is the native-histogram counterpart
// of TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests, verifying that fix D
// also reconciles histogram samples the TSDB drops at commit time because a sample
// already existed at that timestamp. It asserts the ingester and TSDB counters only
// (querying histogram values back into a model.Matrix is not needed to show the fix).
func TestIngester_Push_DuplicateTimestampHistograms(t *testing.T) {
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	userID := "test"

	sampleMetricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_discarded_samples_total",
	}

	type testCase struct {
		points                       []histogramPoint
		expectedMetrics              string
		expectedTSDBHistogramsAppend float64
	}

	tests := map[string]testCase{
		// Three histogram timeseries with equal labels in one request. The first two carry
		// the exact same histogram at ts=100; the third a different one at ts=101. The
		// duplicate is dropped at commit and counted as "same-value-for-timestamp".
		"exact duplicate histogram within a single request is discarded as same-value-for-timestamp": {
			points: []histogramPoint{{ts: 100, idx: 1}, {ts: 100, idx: 1}, {ts: 101, idx: 2}},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 1
			`,
			expectedTSDBHistogramsAppend: 2,
		},

		// Same shape, but the second histogram at ts=100 differs from the first, so it is a
		// conflict, detected at commit and counted as "new-value-for-timestamp".
		"conflicting histogram within a single request is discarded as new-value-for-timestamp": {
			points: []histogramPoint{{ts: 100, idx: 1}, {ts: 100, idx: 2}, {ts: 101, idx: 3}},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
			`,
			expectedTSDBHistogramsAppend: 2,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.ReplicationFactor = 1
			limits := defaultLimitsTestConfig()
			limits.NativeHistogramsIngestionEnabled = true

			i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
			require.NoError(t, err)
			startAndWaitHealthy(t, i, r)

			ctx := user.InjectOrgID(context.Background(), userID)

			_, err = i.Push(ctx, writeRequestManyHistogramSeries(metricLabels, testData.points))
			require.NoError(t, err)

			require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), sampleMetricNames...))

			tsdbReg := i.tsdbMetrics.RegistryForTenant(userID)
			require.NotNil(t, tsdbReg)
			require.Equal(t, testData.expectedTSDBHistogramsAppend, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_head_samples_appended_total", "histogram"), "TSDB in-order histogram samples appended")
		})
	}
}

// TestIngester_Push_DuplicateTimestampCostAttribution verifies that samples dropped at
// commit time are attributed per series to cortex_discarded_attributed_samples_total,
// with the two commit-time reasons, when cost attribution is configured for the tenant.
// It also checks these drops against cortex_distributor_received_attributed_samples_total:
// every sample in the request is counted as received (the distributor's accounting), while
// only the two dropped duplicates are counted as discarded, so received is the superset.
func TestIngester_Push_DuplicateTimestampCostAttribution(t *testing.T) {
	const userID = "test"
	// The series carries a "team" label that cost attribution is configured to track.
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test", "team", "foo")

	limits := defaultLimitsTestConfig()
	limits.CostAttributionBaseTrackers = costattributionmodel.TrackerConfigs{
		costattributionmodel.DefaultTrackerName: {Labels: costattributionmodel.Labels{{Input: "team"}}},
	}
	limits.MaxCostAttributionCardinality = 100
	overrides := validation.NewOverrides(limits, nil)

	registry := prometheus.NewRegistry()
	caRegistry := prometheus.NewRegistry()
	cam, err := costattribution.NewManager(5*time.Second, 10*time.Second, nil, overrides, registry, caRegistry)
	require.NoError(t, err)

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	i, r, err := prepareIngesterWithBlockStorageOverridesAndCostAttribution(t, cfg, overrides, nil, "", "", registry, cam)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), userID)

	// One request for a single series carrying an exact duplicate at ts=100 (same value)
	// and a conflict at ts=200 (different value). Both extra samples are dropped at commit.
	req := writeRequestManySeries(metricLabels, []mimirpb.Sample{
		{TimestampMs: 100, Value: 1},
		{TimestampMs: 100, Value: 1}, // exact duplicate -> same-value-for-timestamp
		{TimestampMs: 101, Value: 2},
		{TimestampMs: 200, Value: 5},
		{TimestampMs: 200, Value: 6}, // conflict -> new-value-for-timestamp
		{TimestampMs: 201, Value: 7},
	})

	// The received-samples accounting happens in the distributor, not the ingester, so
	// simulate it here to assert the received/discarded relationship end to end.
	cam.SampleTracker(userID).IncrementReceivedSamples(req, time.Now())

	_, err = i.Push(ctx, req)
	require.NoError(t, err)

	// All six samples are received; the two dropped duplicates are attributed to the
	// series' "team" label as discarded, split by reason.
	expected := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{team="foo",tenant="test",tracker="cost-attribution"} 6
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="new-value-for-timestamp",team="foo",tenant="test",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{reason="same-value-for-timestamp",team="foo",tenant="test",tracker="cost-attribution"} 1
	`
	require.NoError(t, testutil.GatherAndCompare(caRegistry, strings.NewReader(expected),
		"cortex_distributor_received_attributed_samples_total", "cortex_discarded_attributed_samples_total"))
}

// TestIngester_Push_DuplicateTimestampOutOfOrder verifies that duplicate out-of-order
// samples are classified by value, the same way in-order duplicates are: an OOO sample
// that conflicts (same timestamp, different value) with an already-inserted OOO sample is
// counted as new-value-for-timestamp, while an exact OOO duplicate (same value) is counted
// as same-value-for-timestamp. This exercises OOOChunk.Insert's value comparison; before
// it compared values, every OOO duplicate was reported as same-value-for-timestamp.
func TestIngester_Push_DuplicateTimestampOutOfOrder(t *testing.T) {
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	metricLabelSet := mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(metricLabels))
	const userID = "test"

	sampleMetricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_discarded_samples_total",
	}

	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), userID)

	// Advance the head's max time with an in-order sample so the later samples are OOO.
	_, err = i.Push(ctx, writeRequestManySeries(metricLabels, []mimirpb.Sample{{TimestampMs: 1000, Value: 100}}))
	require.NoError(t, err)

	// All of these are out-of-order (ts < 1000) and within the OOO window. Within this
	// request the first sample at each timestamp is inserted into the OOO head chunk; the
	// second clashes with it. ts=500 clashes with a different value (conflict), ts=600 with
	// the same value (exact duplicate).
	_, err = i.Push(ctx, writeRequestManySeries(metricLabels, []mimirpb.Sample{
		{TimestampMs: 500, Value: 1},
		{TimestampMs: 500, Value: 2}, // OOO conflict -> new-value-for-timestamp
		{TimestampMs: 600, Value: 3},
		{TimestampMs: 600, Value: 3}, // OOO exact duplicate -> same-value-for-timestamp
	}))
	require.NoError(t, err)

	// Read back stored samples: the first value at each timestamp wins.
	s := &stream{ctx: ctx}
	err = i.QueryStream(&client.QueryRequest{
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*"}},
	}, s)
	require.NoError(t, err)
	res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
	require.NoError(t, err)
	require.Equal(t, model.Matrix{
		&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{
			{Value: 1, Timestamp: 500},
			{Value: 3, Timestamp: 600},
			{Value: 100, Timestamp: 1000},
		}},
	}, res)

	// Three samples stored (one in-order + two OOO); the two duplicates are reconciled out
	// and split by value: the different-value clash as new-value-for-timestamp and the
	// same-value clash as same-value-for-timestamp.
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="test"} 3
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total{user="test"} 0
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
		cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 1
	`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), sampleMetricNames...))
}
