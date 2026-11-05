// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const otlpDeltaCounter = "mimir_continuous_test_otlp_delta_counter_total"
const otlpDeltaHistogram = "mimir_continuous_test_otlp_delta_histogram"

type OTLPDeltaTestConfig struct {
	Enabled bool
}

func (cfg *OTLPDeltaTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.otlp-delta-test.enabled", false, "Enable the native OTLP delta write/read test, including start-timestamp and irate checks.")
}

type OTLPDeltaTest struct {
	enabled bool
	cfg     WriteReadSeriesTestConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics

	floatMetric   MetricHistory
	histMetrics   []MetricHistory
	histProfiles  []histogramProfile
	floatName     string
	floatMetadata []prompb.MetricMetadata
}

var _ Test = (*OTLPDeltaTest)(nil)

func (t *OTLPDeltaTest) Name() string {
	return "otlp-delta"
}

func NewOTLPDeltaTest(cfg OTLPDeltaTestConfig, seriesCfg WriteReadSeriesTestConfig, client *Client, logger log.Logger, reg prometheus.Registerer) *OTLPDeltaTest {
	// The dedicated writer leaves the other test instances' protocol unchanged.
	deltaClient := *client
	deltaClient.protocol = writeProtocolOtelHttp
	deltaClient.writeClient = &otlpDeltaWriter{
		writer: client.otlpClient,
	}

	test := &OTLPDeltaTest{
		enabled: cfg.Enabled,
		cfg:     seriesCfg,
		client:  &deltaClient,
		logger:  log.With(logger, "test", "otlp-delta"),
		metrics: NewTestMetrics("otlp-delta", reg),
	}
	test.floatName = otlpDeltaCounter
	test.floatMetadata = []prompb.MetricMetadata{{
		Type:             prompb.MetricMetadata_COUNTER,
		MetricFamilyName: otlpDeltaCounter,
		Help:             "A positive sine wave of delta increments.",
	}}
	test.histProfiles = []histogramProfile{deltaHistogramProfile()}
	test.histMetrics = make([]MetricHistory, len(test.histProfiles))

	return test
}

func deltaHistogramProfile() histogramProfile {
	generate := func(ts time.Time) prompb.Histogram {
		return prompb.FromIntHistogram(ts.UnixMilli(), generateIntHistogram(int64(generateSineWaveValue(ts)*100), 1, true))
	}

	return histogramProfile{
		metricName:     otlpDeltaHistogram,
		typeLabel:      "histogram_int_delta",
		otelCompatible: true,
		metadata: []prompb.MetricMetadata{{
			Type:             prompb.MetricMetadata_HISTOGRAM,
			MetricFamilyName: otlpDeltaHistogram,
			Help:             "A native histogram of delta observations.",
		}},
		generateHistogram: generate,
		generateSeries: func(name string, ts time.Time, count int, labels ...prompb.Label) []prompb.TimeSeries {
			return generateHistogramSeriesInner(name, ts, count, generate, labels...)
		},
		generateSampleHistogram: func(ts time.Time, count int) *model.SampleHistogram {
			return mimirpb.FromFloatHistogramToPromHistogram(generateIntHistogram(int64(generateSineWaveValue(ts)*100), count, true).ToFloat(nil))
		},
	}
}

type otlpDeltaWriter struct {
	writer *otlpHTTPWriter
}

func (w *otlpDeltaWriter) sendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	metadata := make([]mimirpb.MetricMetadata, 0, len(req.Metadata))
	for _, m := range req.Metadata {
		metadata = append(metadata, mimirpb.MetricMetadata{
			Type:             mimirpb.MetricMetadata_MetricType(m.Type),
			MetricFamilyName: m.MetricFamilyName,
			Help:             m.Help,
			Unit:             m.Unit,
		})
	}

	// The shared converter emits gauges and cumulative histograms; these profiles represent delta intervals.
	request := distributor.TimeseriesToOTLPRequest(req.Timeseries, metadata)
	resources := request.Metrics().ResourceMetrics()
	for i := 0; i < resources.Len(); i++ {

		scopes := resources.At(i).ScopeMetrics()
		for j := 0; j < scopes.Len(); j++ {

			metrics := scopes.At(j).Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					// Changing the metric type clears its data points.
					points := pmetric.NewNumberDataPointSlice()
					metric.Gauge().DataPoints().CopyTo(points)

					sum := metric.SetEmptySum()
					sum.SetIsMonotonic(true)
					sum.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					points.CopyTo(sum.DataPoints())

					for n := 0; n < sum.DataPoints().Len(); n++ {
						point := sum.DataPoints().At(n)
						point.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(writeInterval)))
					}
				case pmetric.MetricTypeExponentialHistogram:
					hist := metric.ExponentialHistogram()
					hist.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)

					for n := 0; n < hist.DataPoints().Len(); n++ {
						point := hist.DataPoints().At(n)
						point.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(writeInterval)))
					}
				default:
					return 0, fmt.Errorf("unsupported delta metric type %s", metric.Type())
				}
			}
		}
	}

	return w.writer.sendOTLPRequest(ctx, w.writer.writeBaseEndpoint.String()+"/otlp/v1/metrics", request)
}

type deltaQueryCheck struct {
	query     string
	value     generateValueFunc
	histogram generateSampleHistogramFunc
	history   time.Duration
}

func deltaQueryChecks(metricName string, fanOut bool, value generateValueFunc, histogram generateSampleHistogramFunc) []deltaQueryCheck {
	selector := metricName
	if fanOut {
		selector = fmt.Sprintf(`{__name__=~"%s.*"}`, metricName)
	}

	// Take the absolute error before summing so errors from different series cannot cancel.
	checks := []deltaQueryCheck{{
		query: fmt.Sprintf("sum(abs(start_timestamp(%s) - (timestamp(%s) - %d)))", selector, selector, int(writeInterval/time.Second)),
		value: func(time.Time) float64 {
			return 0
		},
	}}

	// Apply irate before aggregation so each series retains its interval boundaries.
	rate := deltaQueryCheck{
		query:   fmt.Sprintf("sum(irate(%s[%ds]))", selector, int(2*writeInterval/time.Second)),
		history: writeInterval,
	}

	if histogram == nil {
		rate.value = func(ts time.Time) float64 {
			return value(ts) / writeInterval.Seconds()
		}
	} else {
		rate.histogram = func(ts time.Time, count int) *model.SampleHistogram {
			h := histogram(ts, count)
			h.Count /= model.FloatString(writeInterval.Seconds())
			h.Sum /= model.FloatString(writeInterval.Seconds())
			for _, bucket := range h.Buckets {
				bucket.Count /= model.FloatString(writeInterval.Seconds())
			}

			return h
		}
	}

	return append(checks, rate)
}

func verifyDeltaQueryShape(matrix model.Matrix, start, end time.Time, step time.Duration, histogram bool) error {
	if len(matrix) != 1 {
		return fmt.Errorf("expected 1 series in the result but got %d", len(matrix))
	}

	series := matrix[0]
	count := int(end.Sub(start)/step) + 1
	for i := 0; i < count; i++ {
		expected := model.TimeFromUnixNano(start.Add(time.Duration(i) * step).UnixNano())
		if histogram {
			if len(series.Values) != 0 || len(series.Histograms) != count {
				return fmt.Errorf("expected %d histogram samples", count)
			}

			if series.Histograms[i].Timestamp != expected {
				return fmt.Errorf("expected histogram timestamp %d", expected)
			}
		} else {
			if len(series.Histograms) != 0 || len(series.Values) != count {
				return fmt.Errorf("expected %d float samples", count)
			}

			if series.Values[i].Timestamp != expected {
				return fmt.Errorf("expected sample timestamp %d", expected)
			}
		}
	}

	return nil
}

func (t *OTLPDeltaTest) Init(ctx context.Context, now time.Time) error {
	if !t.enabled {
		return nil
	}

	level.Info(t.logger).Log("msg", "Finding previously written samples time range to recover writes and reads from previous run")
	if t.cfg.NumSeries <= 0 {
		return errors.New("the number of series must be greater than 0")
	}

	if !t.cfg.WithFloats && !t.cfg.WithHistograms {
		return errors.New("should test either floats or histograms or both, currently testing nothing")
	}

	if t.cfg.WithFloats {
		err := t.recoverPast(ctx, now, t.floatName, querySumFloat, generateSineWaveValue, nil, &t.floatMetric)
		if err != nil {
			return err
		}

		t.metrics.InitializeCountersToZero(floatTypeLabel)
	}

	if t.cfg.WithHistograms {
		for i, histProfile := range t.histProfiles {
			err := t.recoverPast(ctx, now, histProfile.metricName, querySumHist, histProfile.generateValue, histProfile.generateSampleHistogram, &t.histMetrics[i])
			if err != nil {
				return err
			}

			t.metrics.InitializeCountersToZero(histProfile.typeLabel)
		}
	}

	return nil
}

func (t *OTLPDeltaTest) recoverPast(
	ctx context.Context,
	now time.Time,
	metricName string,
	querySum querySumFunc,
	generateValue generateValueFunc,
	generateSampleHistogram generateSampleHistogramFunc,
	records *MetricHistory,
) error {
	// Recover from raw values so corrupt start timestamps remain visible to subsequent checks.
	from, to := findPreviouslyWrittenTimeRange(ctx, now, writeInterval, t.cfg.MaxQueryAge, metricName, t.cfg.NumSeries, querySum, generateValue, generateSampleHistogram, nil, t.client, t.logger)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written samples time range found, will continue writing from the nearest interval-aligned timestamp", "metric_name", metricName)
		return nil
	}

	if to.Before(now.Add(-writeMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written samples time range found but latest written sample is too old to recover", "metric_name", metricName, "last_sample_timestamp", to)
		return nil
	}

	records.lastWrittenTimestamp = to
	records.queryMinTime = from
	records.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Successfully found previously written samples time range and recovered writes and reads from there", "metric_name", metricName, "last_written_timestamp", records.lastWrittenTimestamp, "query_min_time", records.queryMinTime, "query_max_time", records.queryMaxTime)

	return nil
}

func (t *OTLPDeltaTest) Run(ctx context.Context, now time.Time) error {
	if !t.enabled {
		return nil
	}

	// Limit catch-up writes after a restart to avoid overwhelming ingestion.
	writeLimiter := rate.NewLimiter(rate.Limit(t.cfg.NumSeries), t.cfg.NumSeries)

	var errs []error

	if t.cfg.WithFloats {
		errs = append(errs, t.runMetric(
			ctx,
			now,
			writeLimiter,
			t.floatName,
			floatTypeLabel,
			t.floatMetadata,
			querySumFloat,
			querySumFloatFanOut,
			generateSineWaveSeries,
			generateSineWaveValue,
			nil,
			&t.floatMetric,
		))
	}

	if t.cfg.WithHistograms {
		for i, histProfile := range t.histProfiles {
			errs = append(errs, t.runMetric(
				ctx,
				now,
				writeLimiter,
				histProfile.metricName,
				histProfile.typeLabel,
				histProfile.metadata,
				querySumHist,
				querySumHistFanOut,
				histProfile.generateSeries,
				histProfile.generateValue,
				histProfile.generateSampleHistogram,
				&t.histMetrics[i],
			))
		}
	}

	return errors.Join(errs...)
}

func (t *OTLPDeltaTest) runMetric(
	ctx context.Context,
	now time.Time,
	writeLimiter *rate.Limiter,
	metricName, typeLabel string,
	metricMetadata []prompb.MetricMetadata,
	querySum, querySumFanOut querySumFunc,
	generateSeries generateSeriesFunc,
	generateValue generateValueFunc,
	generateSampleHistogram generateSampleHistogramFunc,
	records *MetricHistory,
) error {
	var errs []error

	for timestamp := t.nextWriteTimestamp(now, records); !timestamp.After(now); timestamp = t.nextWriteTimestamp(now, records) {
		logger := log.With(t.logger, "timestamp", timestamp.UnixMilli(), "num_series", t.cfg.NumSeries, "metric_name", metricName)
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			errs = append(errs, err)
			return errors.Join(errs...)
		}

		series := generateSeries(metricName, timestamp, t.cfg.NumSeries, prompb.Label{Name: "protocol", Value: string(t.client.Protocol())})
		if err := writeSamples(ctx, typeLabel, timestamp, series, metricMetadata, records, t.client, t.metrics, logger); err != nil {
			errs = append(errs, err)
			break
		}
	}

	queryRanges, queryInstants, err := t.getQueryTimeRanges(now, records)
	if err != nil {
		errs = append(errs, err)
	}

	queryMetrics := []string{querySum(metricName)}
	if t.cfg.FanOutQuery {
		queryMetrics = append(queryMetrics, querySumFanOut(metricName))
	}

	for i, queryMetric := range queryMetrics {
		checks := []deltaQueryCheck{{
			query:     queryMetric,
			value:     generateValue,
			histogram: generateSampleHistogram,
		}}
		checks = append(checks, deltaQueryChecks(metricName, i > 0, generateValue, generateSampleHistogram)...)

		for _, check := range checks {
			// Rate checks need a preceding sample; raw-value checks include the first sample.
			queryHistory := *records
			queryHistory.queryMinTime = queryHistory.queryMinTime.Add(check.history)

			for _, timeRange := range queryRanges {
				for _, cached := range []bool{true, false} {
					errs = append(errs, t.runRangeQueryAndVerifyResult(ctx, timeRange[0], timeRange[1], cached, typeLabel, check.query, check.value, check.histogram, &queryHistory))
				}
			}

			for _, ts := range queryInstants {
				for _, cached := range []bool{true, false} {
					errs = append(errs, t.runInstantQueryAndVerifyResult(ctx, ts, cached, typeLabel, check.query, check.value, check.histogram, &queryHistory))
				}
			}
		}
	}

	err = t.runMetadataQueryAndVerifyResult(ctx, metricMetadata)
	errs = append(errs, err)

	return errors.Join(errs...)
}

func (t *OTLPDeltaTest) getQueryTimeRanges(now time.Time, records *MetricHistory) (ranges [][2]time.Time, instants []time.Time, err error) {
	if records.queryMinTime.IsZero() || records.queryMaxTime.IsZero() {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query")
		return nil, nil, errors.New("no valid time range to query")
	}

	adjustedQueryMinTime := maxTime(records.queryMinTime, now.Add(-t.cfg.MaxQueryAge))
	if records.queryMaxTime.Before(adjustedQueryMinTime) {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query after honoring configured max query age", "min_valid_time", records.queryMinTime, "max_valid_time", records.queryMaxTime, "max_query_age", t.cfg.MaxQueryAge)
		return nil, nil, errors.New("no valid time range to query after honoring configured max query age")
	}

	adjustedQueryMaxTime := minTime(records.queryMaxTime, now)

	if records.queryMaxTime.After(now.Add(-1 * time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-1*time.Hour)),
			adjustedQueryMaxTime,
		})
		instants = append(instants, adjustedQueryMaxTime)
	}

	if records.queryMaxTime.After(now.Add(-24*time.Hour)) && adjustedQueryMinTime.Before(now.Add(-1*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			adjustedQueryMaxTime,
		})
		instants = append(instants, maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)))
	}

	if adjustedQueryMinTime.Before(now.Add(-23*time.Hour)) && records.queryMaxTime.After(now.Add(-23*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			minTime(adjustedQueryMaxTime, now.Add(-23*time.Hour)),
		})
	}

	randMinTime := randTime(adjustedQueryMinTime, adjustedQueryMaxTime)
	ranges = append(ranges, [2]time.Time{randMinTime, randTime(randMinTime, adjustedQueryMaxTime)})
	instants = append(instants, randMinTime)

	return ranges, instants, nil
}

func (t *OTLPDeltaTest) runRangeQueryAndVerifyResult(
	ctx context.Context,
	start, end time.Time,
	resultsCacheEnabled bool,
	typeLabel, metricSumQuery string,
	generateValue generateValueFunc,
	generateSampleHistogram generateSampleHistogramFunc,
	records *MetricHistory,
) error {
	start = maxTime(records.queryMinTime, alignTimestampToInterval(start, writeInterval))
	end = minTime(records.queryMaxTime, alignTimestampToInterval(end, writeInterval))
	if end.Before(start) {
		return nil
	}

	step := getQueryStep(start, end, writeInterval)

	sp, ctx := spanlogger.New(ctx, t.logger, tracer, "OTLPDeltaTest.runRangeQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", metricSumQuery, "start", start.UnixMilli(), "end", end.UnixMilli(), "step", step, "results_cache", strconv.FormatBool(resultsCacheEnabled), "type", typeLabel, "protocol", t.client.Protocol())
	level.Debug(logger).Log("msg", "Running range query")

	t.metrics.queriesTotal.WithLabelValues(typeLabel).Inc()
	queryStart := time.Now()
	matrix, err := t.client.QueryRange(ctx, metricSumQuery, start, end, step, WithResultsCacheEnabled(resultsCacheEnabled))
	t.metrics.queriesLatency.WithLabelValues(typeLabel, strconv.FormatBool(resultsCacheEnabled)).Observe(time.Since(queryStart).Seconds())
	if err != nil {
		t.metrics.queriesFailedTotal.WithLabelValues(typeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to execute range query", "err", err)
		return fmt.Errorf("failed to execute range query: %w", err)
	}

	t.metrics.queryResultChecksTotal.WithLabelValues(typeLabel).Inc()
	err = verifyDeltaQueryShape(matrix, start, end, step, generateSampleHistogram != nil)
	if err == nil {
		_, err = verifySamplesSum(matrix, t.cfg.NumSeries, step, generateValue, generateSampleHistogram, nil)
	}

	if err != nil {
		t.metrics.queryResultChecksFailedTotal.WithLabelValues(typeLabel).Inc()
		level.Warn(logger).Log("msg", "Range query result check failed", "err", err)
		return fmt.Errorf("range query result check failed: %w", err)
	}

	level.Info(logger).Log("msg", "Range query result check succeeded")

	return nil
}

func (t *OTLPDeltaTest) runInstantQueryAndVerifyResult(
	ctx context.Context,
	ts time.Time,
	resultsCacheEnabled bool,
	typeLabel, metricSumQuery string,
	generateValue generateValueFunc,
	generateSampleHistogram generateSampleHistogramFunc,
	records *MetricHistory,
) error {
	ts = maxTime(records.queryMinTime, alignTimestampToInterval(ts, writeInterval))
	if records.queryMaxTime.Before(ts) {
		return nil
	}

	sp, ctx := spanlogger.New(ctx, t.logger, tracer, "OTLPDeltaTest.runInstantQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", metricSumQuery, "ts", ts.UnixMilli(), "results_cache", strconv.FormatBool(resultsCacheEnabled), "type", typeLabel, "protocol", t.client.Protocol())
	level.Debug(logger).Log("msg", "Running instant query")

	t.metrics.queriesTotal.WithLabelValues(typeLabel).Inc()
	queryStart := time.Now()
	vector, err := t.client.Query(ctx, metricSumQuery, ts, WithResultsCacheEnabled(resultsCacheEnabled))
	t.metrics.queriesLatency.WithLabelValues(typeLabel, strconv.FormatBool(resultsCacheEnabled)).Observe(time.Since(queryStart).Seconds())
	if err != nil {
		t.metrics.queriesFailedTotal.WithLabelValues(typeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to execute instant query", "err", err)
		return fmt.Errorf("failed to execute instant query: %w", err)
	}

	matrix := make(model.Matrix, 0, len(vector))
	for _, entry := range vector {
		ss := &model.SampleStream{
			Metric: entry.Metric,
		}

		if entry.Histogram == nil {
			ss.Values = []model.SamplePair{{
				Timestamp: entry.Timestamp,
				Value:     entry.Value,
			}}
		} else {
			ss.Histograms = []model.SampleHistogramPair{{
				Timestamp: entry.Timestamp,
				Histogram: entry.Histogram,
			}}
		}

		matrix = append(matrix, ss)
	}

	t.metrics.queryResultChecksTotal.WithLabelValues(typeLabel).Inc()
	err = verifyDeltaQueryShape(matrix, ts, ts, writeInterval, generateSampleHistogram != nil)
	if err == nil {
		_, err = verifySamplesSum(matrix, t.cfg.NumSeries, 0, generateValue, generateSampleHistogram, nil)
	}

	if err != nil {
		t.metrics.queryResultChecksFailedTotal.WithLabelValues(typeLabel).Inc()
		level.Warn(logger).Log("msg", "Instant query result check failed", "err", err)
		return fmt.Errorf("instant query result check failed: %w", err)
	}

	level.Info(logger).Log("msg", "Instant query result check succeeded")

	return nil
}

func (t *OTLPDeltaTest) runMetadataQueryAndVerifyResult(ctx context.Context, expectedMetadata []prompb.MetricMetadata) error {
	sp, ctx := spanlogger.New(ctx, t.logger, tracer, "OTLPDeltaTest.runMetadataQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "protocol", t.client.Protocol())
	level.Debug(logger).Log("msg", "Running metadata query")

	const typeLabel = "metadata"

	for _, expected := range expectedMetadata {
		t.metrics.queriesTotal.WithLabelValues(typeLabel).Inc()
		queryStart := time.Now()
		got, err := t.client.Metadata(ctx, expected.MetricFamilyName)
		t.metrics.queriesLatency.WithLabelValues(typeLabel, "false").Observe(time.Since(queryStart).Seconds())
		if err != nil {
			t.metrics.queriesFailedTotal.WithLabelValues(typeLabel).Inc()
			level.Warn(logger).Log("msg", "Failed to execute metadata query", "err", err)
			return fmt.Errorf("failed to execute metadata query: %w", err)
		}

		t.metrics.queryResultChecksTotal.WithLabelValues(typeLabel).Inc()
		var errs []error
		if expectedType := v1.MetricType(strings.ToLower(expected.Type.String())); expectedType != got.Type {
			errs = append(errs, fmt.Errorf("expected:%q got:%q", expectedType, got.Type))
		}

		if expected.Help != got.Help {
			errs = append(errs, fmt.Errorf("expected:%q got:%q", expected.Help, got.Help))
		}

		if expected.Unit != got.Unit {
			errs = append(errs, fmt.Errorf("expected:%q got:%q", expected.Unit, got.Unit))
		}

		if err := errors.Join(errs...); err != nil {
			t.metrics.queryResultChecksFailedTotal.WithLabelValues(typeLabel).Inc()
			level.Warn(logger).Log("msg", "Metadata query result check failed", "err", err)
			return fmt.Errorf("metadata query result check failed: %w", err)
		}
	}

	level.Info(logger).Log("msg", "Metadata query result check succeeded")

	return nil
}

func (t *OTLPDeltaTest) nextWriteTimestamp(now time.Time, records *MetricHistory) time.Time {
	if records.lastWrittenTimestamp.IsZero() {
		// irate needs two samples, including in smoke-test mode.
		return alignTimestampToInterval(now, writeInterval).Add(-writeInterval)
	}

	return records.lastWrittenTimestamp.Add(writeInterval)
}
