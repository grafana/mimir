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
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	v1 "github.com/prometheus/prometheus/web/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			Unit: "By",
		},
	}

	res, err := c.PushOTLP(series, metadata)
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
				"unit":"By"
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
	res, err = c.PushOTLP(series, metadata)
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
					"unit":"By"
				 }
			  ],
			  "%s":[
				 {
					"type":"gauge",
					"help":"foo",
					"unit":"By"
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

	jsonPayload := []byte(fmt.Sprintf(`{
		"resourceMetrics": [
			{
				"resource": {
					"attributes": [
						{ "key": "resource.attr", "value": { "stringValue": "value" } }
					]
				},
				"scopeMetrics": [
					{
						"metrics": [
							{
								"name": "explicit_bucket_histogram_series",
								"description": "Explicit bucket histogram series",
								"unit": "s",
								"histogram": {
									"aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
									"dataPoints": [
										{
											"attributes": [
												{ "key": "metric-attr", "value": { "stringValue": "metric value" } }
											],
											"bucketCounts": [0, 4, 3, 0, 3],
											"explicitBounds": [0, 5, 10, 15],
											"count": 10,
											"sum": 20,
											"timeUnixNano": "%d"
										}
									]
								}
							},
							{
								"name": "exponential_histogram_series",
								"description": "Exponential histogram series",
								"unit": "s",
								"exponentialHistogram": {
									"aggregationTemporality": "AGGREGATION_TEMPORALITY_CUMULATIVE",
									"dataPoints": [
										{
											"attributes": [
												{ "key": "metric-attr", "value": { "stringValue": "metric value" } }
											],
											"scale": 0,
											"count": 15,
											"sum": 25,
											"zeroCount": 1,
											"positive": {
												"offset": 1,
												"bucketCounts": [1, 0, 3, 2, 0, 3]
											},
											"negative": {
												"offset": 0,
												"bucketCounts": [4, 0, 0, 1]
											},
											"timeUnixNano": "%d"
										}
									]
								}
							}
						]
					}
				]
			}
		]
	}`, nowUnix, nowUnix))

	res, err := c.PushOTLPPayload(jsonPayload, "application/json")
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
