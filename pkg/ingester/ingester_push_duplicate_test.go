// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/costattribution"
	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// requestLayout is how a test request spreads the samples of one series over TimeSeries.
type requestLayout string

const (
	// multipleTimeseries puts each sample in its own TimeSeries, all with the same labels.
	multipleTimeseries requestLayout = "multiple timeseries"
	// singleTimeseries puts all samples of the series in one TimeSeries.
	singleTimeseries requestLayout = "single timeseries"
)

// writeRequestForSeries builds a WriteRequest with the samples of the series lbls, laid out as layout.
func writeRequestForSeries(layout requestLayout, lbls labels.Labels, samples []mimirpb.Sample) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{Source: mimirpb.API}
	if layout == singleTimeseries {
		req.Timeseries = []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
			Samples: slices.Clone(samples),
		}}}
		return req
	}
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
		// reqSamples are the samples of each request, all for the same series.
		reqSamples [][]mimirpb.Sample
		// layouts are the request layouts to run the case with. Defaults to all layouts;
		// a case whose requests hold one sample each only needs one.
		layouts []requestLayout
		// expectedLastReqErr is the soft error expected from the last request, nil for none.
		expectedLastReqErr error
		expectedIngested   model.Matrix
		expectedMetrics    string
		// TSDB-head metrics, counted at commit time in the per-tenant registry.
		expectedTSDBSamplesAppended float64 // prometheus_tsdb_head_samples_appended_total{type="float"}
		expectedTSDBOutOfOrder      float64 // prometheus_tsdb_out_of_order_samples_total{type="float"}
	}

	tests := map[string]testCase{
		// Exact duplicate within one request.
		"exact duplicate within a single request is discarded as same-value-for-timestamp": {
			reqSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 101, Value: 2},
				},
			},
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
			reqSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: 100, Value: 1},
					{TimestampMs: 100, Value: 2},
					{TimestampMs: 101, Value: 3},
				},
			},
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
			layouts: []requestLayout{multipleTimeseries},
			reqSamples: [][]mimirpb.Sample{
				{{TimestampMs: 100, Value: 1}},
				{{TimestampMs: 100, Value: 1}},
			},
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
			layouts: []requestLayout{multipleTimeseries},
			reqSamples: [][]mimirpb.Sample{
				{{TimestampMs: 100, Value: 1}},
				{{TimestampMs: 100, Value: 2}},
			},
			expectedLastReqErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleDuplicateTimestampError("duplicate sample for timestamp 100; overrides not allowed: existing 1, new value 2", model.Time(100), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID), codes.InvalidArgument),
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
			layouts := testData.layouts
			if layouts == nil {
				layouts = []requestLayout{multipleTimeseries, singleTimeseries}
			}
			for _, layout := range layouts {
				t.Run(string(layout), func(t *testing.T) {
					registry := prometheus.NewRegistry()

					cfg := defaultIngesterTestConfig(t)
					cfg.IngesterRing.ReplicationFactor = 1
					limits := defaultLimitsTestConfig()

					i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
					require.NoError(t, err)
					startAndWaitHealthy(t, i, r)

					ctx := user.InjectOrgID(t.Context(), userID)

					// Push all requests. Only the last request may return an error.
					for idx, samples := range testData.reqSamples {
						_, err := i.Push(ctx, writeRequestForSeries(layout, metricLabels, samples))
						if idx == len(testData.reqSamples)-1 && testData.expectedLastReqErr != nil {
							require.Error(t, err)
							errWithStatus, ok := mapPushErrorToErrorWithStatus(err).(globalerror.ErrorWithStatus)
							require.True(t, ok)
							require.Truef(t, errWithStatus.Equals(testData.expectedLastReqErr), "errors don't match \nactual:   '%v'\nexpected: '%v'", errWithStatus, testData.expectedLastReqErr)
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
		})
	}
}

// histogramPoint builds native histogram samples; equal idx generates equal histograms.
type histogramPoint struct {
	ts  int64
	idx int
}

// histogramWriteRequestForSeries is the native-histogram analog of writeRequestForSeries.
func histogramWriteRequestForSeries(layout requestLayout, lbls labels.Labels, points []histogramPoint) *mimirpb.WriteRequest {
	histograms := make([]mimirpb.Histogram, 0, len(points))
	for _, p := range points {
		histograms = append(histograms, mimirpb.FromHistogramToHistogramProto(p.ts, util_test.GenerateTestHistogram(p.idx)))
	}

	req := &mimirpb.WriteRequest{Source: mimirpb.API}
	if layout == singleTimeseries {
		req.Timeseries = []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(lbls),
			Histograms: histograms,
		}}}
		return req
	}
	for _, h := range histograms {
		ts := &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(lbls),
			Histograms: []mimirpb.Histogram{h},
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
		// reqPoints are the histograms of each request, all for the same series.
		reqPoints [][]histogramPoint
		// layouts are the request layouts to run the case with. Defaults to all layouts;
		// a case whose requests hold one histogram each only needs one.
		layouts []requestLayout
		// expectedLastReqErr is the soft error expected from the last request, nil for none.
		expectedLastReqErr           error
		expectedMetrics              string
		expectedTSDBHistogramsAppend float64
	}

	tests := map[string]testCase{
		// Exact duplicate histogram within one request.
		"exact duplicate histogram within a single request is discarded as same-value-for-timestamp": {
			reqPoints: [][]histogramPoint{{{ts: 100, idx: 1}, {ts: 100, idx: 1}, {ts: 101, idx: 2}}},
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
			reqPoints: [][]histogramPoint{{{ts: 100, idx: 1}, {ts: 100, idx: 2}, {ts: 101, idx: 3}}},
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

		// Exact duplicate histogram across two requests: accepted at Append, dropped at commit.
		"exact duplicate histogram across two requests is discarded as same-value-for-timestamp": {
			layouts:   []requestLayout{multipleTimeseries},
			reqPoints: [][]histogramPoint{{{ts: 100, idx: 1}}, {{ts: 100, idx: 1}}},
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
			expectedTSDBHistogramsAppend: 1,
		},

		// Conflicting histogram across two requests: rejected at Append time with a soft error.
		"conflicting histogram across two requests is surfaced as a soft error": {
			layouts:            []requestLayout{multipleTimeseries},
			reqPoints:          [][]histogramPoint{{{ts: 100, idx: 1}}, {{ts: 100, idx: 2}}},
			expectedLastReqErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleDuplicateTimestampError(storage.ErrDuplicateSampleForTimestamp.Error(), model.Time(100), mimirpb.FromLabelsToLabelAdapters(metricLabels)), userID), codes.InvalidArgument),
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
			expectedTSDBHistogramsAppend: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			layouts := testData.layouts
			if layouts == nil {
				layouts = []requestLayout{multipleTimeseries, singleTimeseries}
			}
			for _, layout := range layouts {
				t.Run(string(layout), func(t *testing.T) {
					registry := prometheus.NewRegistry()

					cfg := defaultIngesterTestConfig(t)
					cfg.IngesterRing.ReplicationFactor = 1
					limits := defaultLimitsTestConfig()
					limits.NativeHistogramsIngestionEnabled = true

					i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
					require.NoError(t, err)
					startAndWaitHealthy(t, i, r)

					ctx := user.InjectOrgID(t.Context(), userID)

					// Push all requests. Only the last request may return an error.
					for idx, points := range testData.reqPoints {
						_, err := i.Push(ctx, histogramWriteRequestForSeries(layout, metricLabels, points))
						if idx == len(testData.reqPoints)-1 && testData.expectedLastReqErr != nil {
							require.Error(t, err)
							errWithStatus, ok := mapPushErrorToErrorWithStatus(err).(globalerror.ErrorWithStatus)
							require.True(t, ok)
							require.Truef(t, errWithStatus.Equals(testData.expectedLastReqErr), "errors don't match \nactual:   '%v'\nexpected: '%v'", errWithStatus, testData.expectedLastReqErr)
						} else {
							require.NoError(t, err)
						}
					}

					require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), sampleMetricNames...))

					tsdbReg := i.tsdbMetrics.RegistryForTenant(userID)
					require.NotNil(t, tsdbReg)
					require.Equal(t, testData.expectedTSDBHistogramsAppend, tsdbSampleCounter(t, tsdbReg, "prometheus_tsdb_head_samples_appended_total", "histogram"), "TSDB in-order histogram samples appended")
				})
			}
		})
	}
}

// TestIngester_Push_DuplicateTimestampCostAttribution verifies commit-time drops are
// attributed to the series they were dropped from, with each series' own drop count,
// in cortex_discarded_attributed_samples_total.
func TestIngester_Push_DuplicateTimestampCostAttribution(t *testing.T) {
	const userID = "test"
	fooLabels := labels.FromStrings(model.MetricNameLabel, "test", "team", "foo")
	barLabels := labels.FromStrings(model.MetricNameLabel, "test", "team", "bar")

	for _, layout := range []requestLayout{multipleTimeseries, singleTimeseries} {
		t.Run(string(layout), func(t *testing.T) {
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

			ctx := user.InjectOrgID(t.Context(), userID)

			// The two series drop different numbers of samples for each reason.
			req := writeRequestForSeries(layout, fooLabels, []mimirpb.Sample{
				{TimestampMs: 100, Value: 1},
				{TimestampMs: 100, Value: 1}, // exact duplicate -> same-value-for-timestamp
				{TimestampMs: 100, Value: 1}, // exact duplicate -> same-value-for-timestamp
				{TimestampMs: 200, Value: 5},
				{TimestampMs: 200, Value: 6}, // conflict -> new-value-for-timestamp
			})
			req.Timeseries = append(req.Timeseries, writeRequestForSeries(layout, barLabels, []mimirpb.Sample{
				{TimestampMs: 100, Value: 1},
				{TimestampMs: 100, Value: 1}, // exact duplicate -> same-value-for-timestamp
				{TimestampMs: 200, Value: 5},
				{TimestampMs: 200, Value: 6}, // conflict -> new-value-for-timestamp
				{TimestampMs: 200, Value: 7}, // conflict -> new-value-for-timestamp
				{TimestampMs: 200, Value: 8}, // conflict -> new-value-for-timestamp
			}).Timeseries...)

			// Received-samples accounting lives in the distributor; simulate it here.
			cam.SampleTracker(userID).IncrementReceivedSamples(req, time.Now())

			_, err = i.Push(ctx, req)
			require.NoError(t, err)

			expected := `
				# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
				# TYPE cortex_distributor_received_attributed_samples_total counter
				cortex_distributor_received_attributed_samples_total{team="bar",tenant="test",tracker="cost-attribution"} 6
				cortex_distributor_received_attributed_samples_total{team="foo",tenant="test",tracker="cost-attribution"} 5
				# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
				# TYPE cortex_discarded_attributed_samples_total counter
				cortex_discarded_attributed_samples_total{reason="new-value-for-timestamp",team="bar",tenant="test",tracker="cost-attribution"} 3
				cortex_discarded_attributed_samples_total{reason="new-value-for-timestamp",team="foo",tenant="test",tracker="cost-attribution"} 1
				cortex_discarded_attributed_samples_total{reason="same-value-for-timestamp",team="bar",tenant="test",tracker="cost-attribution"} 1
				cortex_discarded_attributed_samples_total{reason="same-value-for-timestamp",team="foo",tenant="test",tracker="cost-attribution"} 2
			`
			require.NoError(t, testutil.GatherAndCompare(caRegistry, strings.NewReader(expected),
				"cortex_distributor_received_attributed_samples_total", "cortex_discarded_attributed_samples_total"))
		})
	}
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

	for _, layout := range []requestLayout{multipleTimeseries, singleTimeseries} {
		t.Run(string(layout), func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.ReplicationFactor = 1
			limits := defaultLimitsTestConfig()
			limits.OutOfOrderTimeWindow = model.Duration(time.Hour)

			i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
			require.NoError(t, err)
			startAndWaitHealthy(t, i, r)

			ctx := user.InjectOrgID(t.Context(), userID)

			// Advance the head's max time with an in-order sample so the later samples are OOO.
			_, err = i.Push(ctx, writeRequestForSeries(layout, metricLabels, []mimirpb.Sample{{TimestampMs: 1000, Value: 100}}))
			require.NoError(t, err)

			_, err = i.Push(ctx, writeRequestForSeries(layout, metricLabels, []mimirpb.Sample{
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
		})
	}
}

// TestIngester_Push_DuplicateTimestampStillCountsTowardIngestionRate verifies that samples
// dropped at commit time leave cortex_ingester_ingested_samples_total but still count toward
// the ingestion rate and user stats, because they cost the full write path.
func TestIngester_Push_DuplicateTimestampStillCountsTowardIngestionRate(t *testing.T) {
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	const (
		duplicatesUserID = "duplicates"
		controlUserID    = "control"
	)

	registry := prometheus.NewRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	// The test ticks the per-tenant ingestion rates itself.
	cfg.RateUpdatePeriod = time.Hour

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	appendedSamplesBefore := appendedSamplesStats.Total()

	// Both tenants send three samples. One of the duplicates tenant's samples is an exact duplicate.
	duplicatesCtx := user.InjectOrgID(t.Context(), duplicatesUserID)
	_, err = i.Push(duplicatesCtx, writeRequestForSeries(multipleTimeseries, metricLabels, []mimirpb.Sample{
		{TimestampMs: 100, Value: 1},
		{TimestampMs: 100, Value: 1},
		{TimestampMs: 101, Value: 2},
	}))
	require.NoError(t, err)

	controlCtx := user.InjectOrgID(t.Context(), controlUserID)
	_, err = i.Push(controlCtx, writeRequestForSeries(multipleTimeseries, metricLabels, []mimirpb.Sample{
		{TimestampMs: 100, Value: 1},
		{TimestampMs: 101, Value: 2},
		{TimestampMs: 102, Value: 3},
	}))
	require.NoError(t, err)

	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="control"} 3
		cortex_ingester_ingested_samples_total{user="duplicates"} 2
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="duplicates"} 1
	`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics),
		"cortex_ingester_ingested_samples_total", "cortex_discarded_samples_total"))

	// The usage stats count every sample accepted at append time, including the duplicate.
	require.Equal(t, int64(6), appendedSamplesStats.Total()-appendedSamplesBefore)

	for _, db := range i.tsdbs {
		db.ingestedAPISamples.Tick()
	}

	duplicatesStats, err := i.UserStats(duplicatesCtx, &client.UserStatsRequest{})
	require.NoError(t, err)
	controlStats, err := i.UserStats(controlCtx, &client.UserStatsRequest{})
	require.NoError(t, err)
	require.Positive(t, controlStats.ApiIngestionRate)
	require.Equal(t, controlStats.ApiIngestionRate, duplicatesStats.ApiIngestionRate)
}

// TestIngester_Push_DuplicateTimestampStillCountsTowardInstanceAndRuleIngestionRate verifies that
// a request made only of exact duplicates still counts toward the instance ingestion rate, which
// -ingester.instance-limits.max-ingestion-rate is checked against, and toward the tenant's rule
// ingestion rate.
func TestIngester_Push_DuplicateTimestampStillCountsTowardInstanceAndRuleIngestionRate(t *testing.T) {
	const userID = "test"
	metricLabels := labels.FromStrings(model.MetricNameLabel, "test")
	stored := []mimirpb.Sample{{TimestampMs: 100, Value: 1}}
	ctx := user.InjectOrgID(t.Context(), userID)

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	// The test ticks the per-tenant ingestion rates itself.
	cfg.RateUpdatePeriod = time.Hour
	dataDir := t.TempDir()

	// Store the sample, then restart the ingester so that it replays the sample from the WAL
	// and its instance ingestion rate starts again from zero.
	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)
	_, err = i.Push(ctx, writeRequestForSeries(singleTimeseries, metricLabels, stored))
	require.NoError(t, err)
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), i))

	registry := prometheus.NewRegistry()
	i, r, err = prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, dataDir, registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	req := writeRequestForSeries(multipleTimeseries, metricLabels, []mimirpb.Sample{stored[0], stored[0], stored[0]})
	req.Source = mimirpb.RULE
	_, err = i.Push(ctx, req)
	require.NoError(t, err)

	// Nothing new is stored: every sample is discarded as an exact duplicate.
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="test"} 0
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="same-value-for-timestamp",user="test"} 3
	`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics),
		"cortex_ingester_ingested_samples_total", "cortex_discarded_samples_total"))

	// The ingester also ticks the instance rate in the background. The duplicates are the only
	// samples pushed since the restart, so the rate is positive only if they were counted,
	// whichever tick picked them up.
	i.ingestionRate.Tick()
	require.Positive(t, i.ingestionRate.Rate())

	i.tsdbsMtx.RLock()
	for _, db := range i.tsdbs {
		db.ingestedRuleSamples.Tick()
		db.ingestedAPISamples.Tick()
	}
	i.tsdbsMtx.RUnlock()

	stats, err := i.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	require.Equal(t, 3/cfg.RateUpdatePeriod.Seconds(), stats.RuleIngestionRate)
	require.Zero(t, stats.ApiIngestionRate)
}
