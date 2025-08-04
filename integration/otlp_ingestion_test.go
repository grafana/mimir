// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestOTLPIngestion(t *testing.T) {
	t.Run("enabling OTel suffixes", func(t *testing.T) {
		testOTLPIngestion(t, true)
	})

	t.Run("disabling OTel suffixes", func(t *testing.T) {
		testOTLPIngestion(t, false)
	})
}

func testOTLPIngestion(t *testing.T, enableSuffixes bool) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName, rulesBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		RulerStorageS3Flags(),
		map[string]string{
			"-distributor.otel-metric-suffixes-enabled": strconv.FormatBool(enableSuffixes),
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	sfx := ""
	if enableSuffixes {
		sfx = "_bytes"
	}

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, expectedMatrix := generateFloatSeries("series_1", now, prompb.Label{Name: "foo", Value: "bar"})
	// Fix up the expectation wrt. suffix
	for _, s := range expectedVector {
		s.Metric[model.LabelName("__name__")] = model.LabelValue(fmt.Sprintf("series_1%s", sfx))
	}
	metadata := []mimirpb.MetricMetadata{
		{
			Help: "foo",
			Unit: "By",
		},
	}

	res, err := c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// Query the series.
	result, err := c.Query(fmt.Sprintf("series_1%s", sfx), now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	rangeResult, err := c.QueryRange(fmt.Sprintf("series_1%s", sfx), now.Add(-15*time.Minute), now, 15*time.Second)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Query the metadata
	metadataResult, err := c.GetPrometheusMetadata("")
	require.NoError(t, err)
	require.Equal(t, 200, metadataResult.StatusCode)

	metadataResponseBody, err := io.ReadAll(metadataResult.Body)
	require.NoError(t, err)

	expectedJSON := fmt.Sprintf(`
	{
	   "status":"success",
	   "data":{
		  "series_1%s":[
			 {
				"type":"gauge",
				"help":"foo",
				"unit":"By"
			 }
		  ]
	   }
	}
	`, sfx)

	require.JSONEq(t, expectedJSON, string(metadataResponseBody))

	// Push series with histograms to Mimir
	series, expectedVector, _ = generateHistogramSeries("series", now)
	res, err = c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	result, err = c.Query(fmt.Sprintf("series%s", sfx), now)
	require.NoError(t, err)

	want := expectedVector[0]
	got := result.(model.Vector)[0]
	assert.Equal(t, want.Histogram.Sum, got.Histogram.Sum)
	assert.Equal(t, want.Histogram.Count, got.Histogram.Count)
	// it is not possible to assert with assert.ElementsMatch(t, expectedVector, result.(model.Vector))
	// till https://github.com/open-telemetry/opentelemetry-proto/pull/441 is released. That is only
	// to test setup logic

	expectedJSON = fmt.Sprintf(`
		{
		   "status":"success",
		   "data":{
			  "series%s":[
				 {
					"type":"histogram",
					"help":"foo",
					"unit":"By"
				 }
			  ],
			  "series_1%s":[
				 {
					"type":"gauge",
					"help":"foo",
					"unit":"By"
				 }
			  ]
		   }
		}
	`, sfx, sfx)

	metadataResult, err = c.GetPrometheusMetadata("")
	require.NoError(t, err)
	require.Equal(t, 200, metadataResult.StatusCode)

	metadataResponseBody, err = io.ReadAll(metadataResult.Body)
	require.NoError(t, err)
	require.JSONEq(t, expectedJSON, string(metadataResponseBody))
}

func TestOTLPHistogramIngestion(t *testing.T) {
	t.Run("enabling conversion to NHCB", func(t *testing.T) {
		testOTLPHistogramIngestion(t, true)
	})

	t.Run("disabling conversion to NHCB", func(t *testing.T) {
		testOTLPHistogramIngestion(t, false)
	})
}

func testOTLPHistogramIngestion(t *testing.T, enableExplicitHistogramToNHCB bool) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName, rulesBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		RulerStorageS3Flags(),
		map[string]string{
			"-distributor.otel-convert-histograms-to-nhcb": strconv.FormatBool(enableExplicitHistogramToNHCB),
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()
	nowUnix := uint64(now.UnixNano())

	float64Ptr := func(f float64) *float64 {
		return &f
	}

	req := &metricspb.MetricsData{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "resource.attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name:        "explicit_bucket_histogram_series",
								Description: "Explicit bucket histogram series",
								Unit:        "s",
								Data: &metricspb.Metric_Histogram{
									Histogram: &metricspb.Histogram{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										DataPoints: []*metricspb.HistogramDataPoint{
											{
												Attributes: []*commonpb.KeyValue{
													{Key: "metric-attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "metric value"}}},
												},
												BucketCounts:   []uint64{0, 4, 3, 0, 3},
												ExplicitBounds: []float64{0, 5, 10, 15},
												Count:          10,
												Sum:            float64Ptr(20),
												TimeUnixNano:   nowUnix,
											},
										},
									},
								},
							},
							{
								Name:        "exponential_histogram_series",
								Description: "Exponential histogram series",
								Unit:        "s",
								Data: &metricspb.Metric_ExponentialHistogram{
									ExponentialHistogram: &metricspb.ExponentialHistogram{
										AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
										DataPoints: []*metricspb.ExponentialHistogramDataPoint{
											{
												Attributes: []*commonpb.KeyValue{
													{Key: "metric-attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "metric value"}}},
												},
												Scale:     0,
												Count:     15,
												Sum:       float64Ptr(25),
												ZeroCount: 1,
												Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
													Offset:       1,
													BucketCounts: []uint64{1, 0, 3, 2, 0, 3},
												},
												Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{
													Offset:       0,
													BucketCounts: []uint64{4, 0, 0, 1},
												},
												TimeUnixNano: nowUnix,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	protoPayload, err := proto.Marshal(req)
	require.NoError(t, err)

	res, err := c.PushOTLPPayload(protoPayload, "application/x-protobuf")
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// Query the histogram series
	if enableExplicitHistogramToNHCB {
		// Verify that when flag is disabled, OTel explicit bucket histograms are converted to native histograms with custom buckets
		result, err := c.Query("explicit_bucket_histogram_series", now)
		require.NoError(t, err)
		require.Equal(t, result.(model.Vector)[0].Histogram, &model.SampleHistogram{
			Count: model.FloatString(10),
			Sum:   model.FloatString(20),
			Buckets: model.HistogramBuckets{
				{
					Lower: model.FloatString(0),
					Upper: model.FloatString(5),
					Count: model.FloatString(4),
				},
				{
					Lower: model.FloatString(5),
					Upper: model.FloatString(10),
					Count: model.FloatString(3),
				},
				{
					Lower: model.FloatString(15),
					Upper: model.FloatString(math.Inf(1)),
					Count: model.FloatString(3),
				},
			},
		})
	} else {
		// Verify that when flag is enabled, OTel explicit bucket histograms are converted to classic histograms
		expected := []struct {
			name  string
			value model.SampleValue
		}{
			{"explicit_bucket_histogram_series_count", 10},
			{"explicit_bucket_histogram_series_sum", 20},
			{`explicit_bucket_histogram_series_bucket{le="0"}`, 0},
			{`explicit_bucket_histogram_series_bucket{le="5"}`, 4},
			{`explicit_bucket_histogram_series_bucket{le="10"}`, 7},
			{`explicit_bucket_histogram_series_bucket{le="15"}`, 7},
			{`explicit_bucket_histogram_series_bucket{le="+Inf"}`, 10},
		}
		for _, exp := range expected {
			result, err := c.Query(exp.name, now)
			require.NoError(t, err)
			require.Equal(t, exp.value, result.(model.Vector)[0].Value)
		}
	}

	// Verify that OTel exponential histograms are converted to native histograms
	result, err := c.Query("exponential_histogram_series", now)
	require.NoError(t, err)
	require.Equal(t, result.(model.Vector)[0].Histogram.Count, model.FloatString(15))
	require.Equal(t, result.(model.Vector)[0].Histogram.Sum, model.FloatString(25))
}

// TestStartTimeHandling implements E2E test for OTEL start time to
// zero sample ingestion.
func TestStartTimeHandling(t *testing.T) {
	t.Run("enableCTzero=false", func(t *testing.T) {
		testStartTimeHandling(t, false)
	})

	t.Run("enableCTzero=true", func(t *testing.T) {
		testStartTimeHandling(t, true)
	})
}

func testStartTimeHandling(t *testing.T, enableCTzero bool) {
	t.Helper()

	var zeroTs time.Time
	now := time.Now()

	testCases := map[string]struct {
		startTs      time.Time
		expectCTzero bool
	}{
		"zero start time": {
			startTs:      zeroTs,
			expectCTzero: false,
		},
		"start time too old": {
			startTs:      now.Add(-1 * time.Hour),
			expectCTzero: false,
		},
		"start time ok": {
			startTs:      now.Add(-30 * time.Second),
			expectCTzero: true,
		},
		"start time equal to sample timestamp": {
			startTs:      now,
			expectCTzero: false,
		},
		"start time in the future": {
			startTs:      now.Add(30 * time.Second),
			expectCTzero: false,
		},
	}

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName, rulesBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		RulerStorageS3Flags(),
		map[string]string{
			"-distributor.otel-created-timestamp-zero-ingestion-enabled": strconv.FormatBool(enableCTzero),
			"-distributor.otel-start-time-quiet-zero":                    strconv.FormatBool(enableCTzero),
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	count := int64(0)
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			metric := "http_requests_" + strconv.FormatInt(count, 10)
			req := &metricspb.MetricsData{
				ResourceMetrics: []*metricspb.ResourceMetrics{
					{
						Resource: &resourcepb.Resource{
							Attributes: []*commonpb.KeyValue{
								{Key: "resource.attr", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "value"}}},
							},
						},
						ScopeMetrics: []*metricspb.ScopeMetrics{
							{
								Metrics: []*metricspb.Metric{
									{
										Name:        metric,
										Description: "Number of HTTP requests",
										Unit:        "1",
										Data: &metricspb.Metric_Sum{
											Sum: &metricspb.Sum{
												AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
												IsMonotonic:            true,
												DataPoints: []*metricspb.NumberDataPoint{
													{
														Attributes: []*commonpb.KeyValue{
															{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
														},
														Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 100.0},
														StartTimeUnixNano: uint64(tc.startTs.UnixNano()),
														TimeUnixNano:      uint64(now.UnixNano()),
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			}
			protoPayload, err := proto.Marshal(req)
			require.NoError(t, err)

			res, err := c.PushOTLPPayload(protoPayload, "application/x-protobuf")
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			result, err := c.Query(metric+"[1h]", now.Add(30*time.Minute))
			require.NoError(t, err)

			m, ok := result.(model.Matrix)
			require.True(t, ok, "result is a Matrix")

			s := m[0]
			sampleIdx := 0
			if enableCTzero && tc.expectCTzero {
				sampleIdx = 1
				require.Len(t, s.Values, 2)
				require.Equal(t, tc.startTs.UnixMilli(), s.Values[0].Timestamp.Time().UnixMilli())
				require.Equal(t, 0.0, float64(s.Values[0].Value))
			} else {
				require.Len(t, s.Values, 1)
			}
			require.Equal(t, now.UnixMilli(), s.Values[sampleIdx].Timestamp.Time().UnixMilli())
			require.Equal(t, 100.0, float64(s.Values[sampleIdx].Value))
		})
		count++
	}
}
