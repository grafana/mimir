// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/grafana/regexp"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestOTLPIngestion(t *testing.T) {
	t.Run("with metric suffixes, with escaped metric and label names", func(t *testing.T) {
		testOTLPIngestion(t, testOTLPIngestionOpts{translationStrategy: otlptranslator.UnderscoreEscapingWithSuffixes})
	})
	t.Run("without metric suffixes, with escaped metric and label names", func(t *testing.T) {
		testOTLPIngestion(t, testOTLPIngestionOpts{translationStrategy: otlptranslator.UnderscoreEscapingWithoutSuffixes})
	})
	t.Run("with metric suffixes, without escaped metric and label names", func(t *testing.T) {
		testOTLPIngestion(t, testOTLPIngestionOpts{translationStrategy: otlptranslator.NoUTF8EscapingWithSuffixes})
	})
	t.Run("without metric suffixes, without escaped metric and label names", func(t *testing.T) {
		testOTLPIngestion(t, testOTLPIngestionOpts{translationStrategy: otlptranslator.NoTranslation})
	})
}

type testOTLPIngestionOpts struct {
	translationStrategy otlptranslator.TranslationStrategyOption
}

func testOTLPIngestion(t *testing.T, opts testOTLPIngestionOpts) {
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
	validationScheme := model.LegacyValidation
	if !opts.translationStrategy.ShouldEscape() {
		// Without the UTF-8 validation scheme, unescaped names will be rejected.
		validationScheme = model.UTF8Validation
	}
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		RulerStorageS3Flags(),
		map[string]string{
			"-distributor.otel-metric-suffixes-enabled": strconv.FormatBool(opts.translationStrategy.ShouldAddSuffixes()),
			"-distributor.otel-translation-strategy":    string(opts.translationStrategy),
			"-validation.name-validation-scheme":        validationScheme.String(),
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	sfx := ""
	if opts.translationStrategy.ShouldAddSuffixes() {
		sfx = "_bytes"
	}

	convertedMetricName := fmt.Sprintf("series_1%s", sfx)
	if !opts.translationStrategy.ShouldEscape() {
		convertedMetricName = fmt.Sprintf("series.1%s", sfx)
	}

	// Push some series to Mimir.
	now := time.Now()
	series, expectedVector, expectedMatrix := generateFloatSeries("series.1", now, prompb.Label{Name: "foo", Value: "bar"})
	// Fix up the expectation wrt. suffix
	for _, s := range expectedVector {
		s.Metric[model.LabelName("__name__")] = model.LabelValue(convertedMetricName)
	}
	metadata := []mimirpb.MetricMetadata{
		{
			Help: "foo",
			Unit: "bytes",
		},
	}

	res, _, err := c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// UTF-8 format names have to be in quotes.
	metricQuery := fmt.Sprintf(`{"%s"}`, convertedMetricName)

	// Query the series.
	result, err := c.Query(metricQuery, now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	assert.Equal(t, expectedVector, result.(model.Vector))

	labelValues, err := c.LabelValues("foo", v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, model.LabelValues{"bar"}, labelValues)

	labelNames, err := c.LabelNames(v1.MinTime, v1.MaxTime, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"__name__", "foo"}, labelNames)

	rangeResult, err := c.QueryRange(metricQuery, now.Add(-15*time.Minute), now, 15*time.Second)
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
		  "%s":[
			 {
				"type":"gauge",
				"help":"foo",
				"unit":"bytes"
			 }
		  ]
	   }
	}
	`, convertedMetricName)

	require.JSONEq(t, expectedJSON, string(metadataResponseBody))

	convertedHistogramName := fmt.Sprintf("series_histogram%s", sfx)
	if !opts.translationStrategy.ShouldEscape() {
		convertedHistogramName = fmt.Sprintf("series.histogram%s", sfx)
	}

	// Push series with histograms to Mimir
	series, expectedVector, _ = generateHistogramSeries("series.histogram", now)
	res, _, err = c.PushOTLP(series, metadata)
	require.NoError(t, err)
	require.Equal(t, 200, res.StatusCode)

	// Check metric to track OTLP requests
	require.NoError(t, mimir.WaitSumMetricsWithOptions(e2e.Equals(2), []string{"cortex_distributor_otlp_requests_total"}, e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// UTF-8 format names have to be in quotes.
	histogramQuery := fmt.Sprintf(`{"%s"}`, convertedHistogramName)
	result, err = c.Query(histogramQuery, now)
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
			  "%s":[
				 {
					"type":"histogram",
					"help":"foo",
					"unit":"bytes"
				 }
			  ],
			  "%s":[
				 {
					"type":"gauge",
					"help":"foo",
					"unit":"bytes"
				 }
			  ]
		   }
		}
	`, convertedHistogramName, convertedMetricName)

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

	res, _, err := c.PushOTLPPayload(protoPayload, "application/x-protobuf")
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

// TestStartTimeHandling implements E2E test for OTel start time to
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
	prevTs := now.Add(-30 * time.Second)

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
			startTs:      now.Add(-20 * time.Second),
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
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	float64Ptr := func(v float64) *float64 {
		x := v
		return &x
	}

	count := int64(0)
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			counterMetric := "http_requests_" + strconv.FormatInt(count, 10)
			histogramMetric := "http_requests_seconds" + strconv.FormatInt(count, 10)
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
										Name:        counterMetric,
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
														Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 1000.0},
														StartTimeUnixNano: uint64(prevTs.UnixNano()),
														TimeUnixNano:      uint64(prevTs.UnixNano()),
													},
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
									{
										Name:        histogramMetric,
										Description: "Latency histogram of HTTP requests",
										Unit:        "s",
										Data: &metricspb.Metric_ExponentialHistogram{
											ExponentialHistogram: &metricspb.ExponentialHistogram{
												AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
												DataPoints: []*metricspb.ExponentialHistogramDataPoint{
													{
														Attributes: []*commonpb.KeyValue{
															{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
														},
														Count: 1000,
														Sum:   float64Ptr(123114.0),
														Scale: 4, // We want this to be non-zero for regression against the injected zero sample having scale 0.
														Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
															Offset:       2,
															BucketCounts: []uint64{10, 200, 470, 300, 20},
														},
														StartTimeUnixNano: uint64(prevTs.UnixNano()),
														TimeUnixNano:      uint64(prevTs.UnixNano()),
													},
													{
														Attributes: []*commonpb.KeyValue{
															{Key: "method", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "GET"}}},
														},
														Count: 100,
														Sum:   float64Ptr(12311.4),
														Scale: 4, // We want this to be non-zero for regression against the injected zero sample having scale 0.
														Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
															Offset:       2,
															BucketCounts: []uint64{1, 20, 47, 30, 2},
														},
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

			res, _, err := c.PushOTLPPayload(protoPayload, "application/x-protobuf")
			require.NoError(t, err)
			require.Equal(t, 200, res.StatusCode)

			for _, metric := range []string{counterMetric, histogramMetric} {
				t.Run(metric, func(t *testing.T) {
					result, err := c.Query(metric+"[1h]", now.Add(30*time.Minute))
					require.NoError(t, err)

					m, ok := result.(model.Matrix)
					require.True(t, ok, "result is a Matrix")

					s := m[0]

					getSampleCount := func() int {
						if metric == counterMetric {
							return len(s.Values)
						}
						return len(s.Histograms)
					}
					getTimeStamp := func(i int) int64 {
						if metric == counterMetric {
							return s.Values[i].Timestamp.Time().UnixMilli()
						}
						return s.Histograms[i].Timestamp.Time().UnixMilli()
					}
					getValue := func(i int) float64 {
						if metric == counterMetric {
							return float64(s.Values[i].Value)
						}
						return float64(s.Histograms[i].Histogram.Count)
					}

					sampleIdx := 0

					require.Greater(t, getSampleCount(), 1)
					require.Equal(t, prevTs.UnixMilli(), getTimeStamp(sampleIdx))
					require.Equal(t, 1000.0, getValue(sampleIdx))
					sampleIdx++

					if enableCTzero && tc.expectCTzero {
						require.Equal(t, 3, getSampleCount())
						require.Equal(t, tc.startTs.UnixMilli(), getTimeStamp(sampleIdx))
						require.Equal(t, 0.0, getValue(sampleIdx))
						sampleIdx++
					} else {
						require.Equal(t, 2, getSampleCount())
					}

					require.Equal(t, now.UnixMilli(), getTimeStamp(sampleIdx))
					require.Equal(t, 100.0, getValue(sampleIdx))

					if metric != histogramMetric || !enableCTzero || !tc.expectCTzero {
						return
					}
					// Test rate doesn't loose resolution, which would happen if
					// the injected zero histogram has a scale == 0.
					// The resolution isn't returned directly, but we can check the
					// number of buckets to see if some were merged together.
					result, err = c.Query("rate("+metric+"[1m])", now.Add(1*time.Second))
					require.NoError(t, err)

					v, ok := result.(model.Vector)
					require.True(t, ok, "result is a Vector")
					require.Len(t, v, 1)
					require.Len(t, v[0].Histogram.Buckets, 5, "same number of buckets as the input")
					for _, bucket := range v[0].Histogram.Buckets {
						require.NotZero(t, bucket.Count)
					}
				})
			}
		})
		count++
	}
}

// This test validates that the responses from Mimir's OTLP ingestion endpoint adhere to spec:
// https://opentelemetry.io/docs/specs/otlp/#otlphttp-response.
func TestOTLPResponseStatusCodeSpecifications(t *testing.T) {
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
			"-ingester.max-global-exemplars-per-user": "1000",
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	now := time.Now()

	t.Run("should return 200 on partial success due to out of order exemplars", func(t *testing.T) {
		// Send a first request with an exemplar.
		req1, _, _ := generateHistogramSeries("series_with_out_of_order_exemplars", now)
		req1[0].Exemplars = []prompb.Exemplar{
			{
				Labels:    []prompb.Label{{Name: "trace_id", Value: "xxx"}},
				Value:     1,
				Timestamp: now.UnixMilli(),
			},
		}

		res, body, err := c.PushOTLP(req1, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
		require.Empty(t, body)
		require.NoError(t, mimir.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_ingested_exemplars_total"))

		// Send a second request with an out of order exemplar. The response should be 200 but the exemplar should not be ingested.
		req2, _, _ := generateHistogramSeries("series_with_out_of_order_exemplars", now.Add(time.Second))
		req2[0].Exemplars = []prompb.Exemplar{
			{
				Labels:    []prompb.Label{{Name: "trace_id", Value: "xxx"}},
				Value:     2,
				Timestamp: now.Add(-time.Second).UnixMilli(),
			},
		}

		res, body, err = c.PushOTLP(req2, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode)
		assert.Equal(t, "application/x-protobuf", res.Header.Get("Content-Type"))
		assert.Equal(t, strconv.Itoa(len(body)), res.Header.Get("Content-Length"))
		var expResp colmetricpb.ExportMetricsServiceResponse
		require.NoError(t, proto.Unmarshal(body, &expResp))
		assert.Equal(t, int64(0), expResp.PartialSuccess.RejectedDataPoints)
		assert.Regexp(t, regexp.MustCompile(`failed pushing to ingester mimir-1: user=user-1: err: out of order exemplar. timestamp=[^,]+, series=series_with_out_of_order_exemplars, exemplar=\{trace_id="xxx"\}`), expResp.PartialSuccess.ErrorMessage)
		require.NoError(t, mimir.WaitSumMetrics(e2e.Equals(1), "cortex_ingester_ingested_exemplars_total"))
	})
}
