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

// writeRequestManySeries builds one WriteRequest with one timeseries per sample, all sharing lbls.
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

// tsdbSampleCounter returns the {type=<sampleType>} value of the named TSDB counter, or 0 if absent.
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

// TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests covers how the ingester
// accounts for multiple samples sharing a timestamp for the same series:
//
//  1. Exact duplicate (same ts + same value) within a single WriteRequest: the extra
//     sample is dropped at commit time and counted as discarded "same-value-for-timestamp".
//  2. Conflicting value (same ts, different value) within a single WriteRequest: the
//     conflict is only detectable at commit time; the extra sample is dropped and
//     counted as discarded "new-value-for-timestamp".
//  3. Exact duplicate (same ts + same value) across two separate WriteRequests: tolerated
//     at Append time (value matches the committed sample), then dropped at commit and
//     counted as discarded "same-value-for-timestamp" — the across-requests counterpart
//     of scenario 1.
//  4. Conflicting value (same ts, different value) across two separate WriteRequests:
//     rejected at Append time with the pre-existing soft "new-value-for-timestamp"
//     error — the across-requests counterpart of scenario 2.
//
// In every scenario the dropped samples are subtracted from
// cortex_ingester_ingested_samples_total, so it reflects what was actually stored.
func TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests(t *testing.T) {
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	metricLabelSet := mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(metricLabels))
	userID := "test"

	// Ingester-tracked metrics.
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
		// TSDB-head metrics, counted at commit time in the per-tenant registry.
		expectedTSDBSamplesAppended float64 // prometheus_tsdb_head_samples_appended_total{type="float"}
		expectedTSDBOutOfOrder      float64 // prometheus_tsdb_out_of_order_samples_total{type="float"}
	}

	tests := map[string]testCase{
		// Exact duplicate within one request.
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
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Conflicting value within one request: only detectable at commit time.
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
			// Still no push error for the client; only the accounting changes.
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
			expectedTSDBSamplesAppended: 2,
			expectedTSDBOutOfOrder:      0,
		},

		// Exact duplicate across two requests.
		"exact duplicate across two requests is discarded as same-value-for-timestamp": {
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
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 1
			`,
			expectedTSDBSamplesAppended: 1,
			expectedTSDBOutOfOrder:      0,
		},

		// Conflicting value across two requests: rejected at Append time with a soft error.
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

			tsdbReg := i.tsdbMetrics.RegistryForTenant(userID)
			require.NotNil(t, tsdbReg)
			require.Equal(t, testData.expectedTSDBSamplesAppended, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_head_samples_appended_total", "float"), "TSDB in-order float samples appended")
			require.Equal(t, testData.expectedTSDBOutOfOrder, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_out_of_order_samples_total", "float"), "TSDB out-of-order float samples rejected")
		})
	}
}

// histogramPoint builds native histogram samples; equal idx generates equal histograms.
type histogramPoint struct {
	ts  int64
	idx int
}

// writeRequestManyHistogramSeries is the native-histogram analog of writeRequestManySeries.
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

// TestIngester_Push_DuplicateTimestampHistograms is the native-histogram counterpart of
// TestIngester_Push_DuplicateTimestampWithinAndAcrossRequests.
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
		// Exact duplicate histogram within one request.
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

		// Conflicting histogram within one request.
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

// TestIngester_Push_DuplicateTimestampCostAttribution verifies commit-time drops are
// attributed per series in cortex_discarded_attributed_samples_total and reconciled
// against the received-samples accounting.
func TestIngester_Push_DuplicateTimestampCostAttribution(t *testing.T) {
	const userID = "test"
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

	req := writeRequestManySeries(metricLabels, []mimirpb.Sample{
		{TimestampMs: 100, Value: 1},
		{TimestampMs: 100, Value: 1}, // exact duplicate -> same-value-for-timestamp
		{TimestampMs: 101, Value: 2},
		{TimestampMs: 200, Value: 5},
		{TimestampMs: 200, Value: 6}, // conflict -> new-value-for-timestamp
		{TimestampMs: 201, Value: 7},
	})

	// Received-samples accounting lives in the distributor; simulate it here.
	cam.SampleTracker(userID).IncrementReceivedSamples(req, time.Now())

	_, err = i.Push(ctx, req)
	require.NoError(t, err)

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

// TestIngester_Push_DuplicateTimestampOutOfOrder verifies OOO duplicates are classified
// by value like in-order ones (exercises OOOChunk.Insert's value comparison).
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
