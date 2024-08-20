// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	writeInterval = 20 * time.Second
	writeMaxAge   = 50 * time.Minute
)

type WriteReadSeriesTestConfig struct {
	Name           string
	NumSeries      int
	MaxQueryAge    time.Duration
	WithFloats     bool
	WithHistograms bool
}

func (cfg *WriteReadSeriesTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Name, "tests.write-read-series-test.name", "write-read-series", "The name of the test.")
	f.IntVar(&cfg.NumSeries, "tests.write-read-series-test.num-series", 10000, "Number of series used for the test.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.write-read-series-test.max-query-age", 7*24*time.Hour, "How back in the past metrics can be queried at most.")
	f.BoolVar(&cfg.WithFloats, "tests.write-read-series-test.float-samples-enabled", true, "Set to true to use float samples")
	f.BoolVar(&cfg.WithHistograms, "tests.write-read-series-test.histogram-samples-enabled", false, "Set to true to use native histogram samples")
}

type WriteReadSeriesTestProfile struct {
	// TODO(leizor): Add comments here.
	TypeLabel        string
	QuerySum         string
	GenerateSeries   generateSeriesFunc
	VerifySamplesSum verifySamplesSumFunc
	PostRun          func(context.Context) error
	History          *MetricHistory
}

type WriteReadSeriesTest struct {
	cfg     WriteReadSeriesTestConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics

	TestProfiles []WriteReadSeriesTestProfile
}

type MetricHistory struct {
	lastWrittenTimestamp time.Time
	queryMinTime         time.Time
	queryMaxTime         time.Time
}

func NewWriteReadSeriesTest(cfg WriteReadSeriesTestConfig, client MimirClient, logger log.Logger, reg prometheus.Registerer) *WriteReadSeriesTest {
	return &WriteReadSeriesTest{
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", cfg.Name),
		metrics: NewTestMetrics(cfg.Name, reg),
	}
}

// Name implements Test.
func (t *WriteReadSeriesTest) Name() string {
	return t.cfg.Name
}

// Init implements Test.
func (t *WriteReadSeriesTest) Init(ctx context.Context, now time.Time) error {
	if !t.cfg.WithFloats && !t.cfg.WithHistograms && len(t.TestProfiles) == 0 {
		return errors.New("nothing to test")
	}

	if t.cfg.WithFloats {
		t.TestProfiles = append(t.TestProfiles, WriteReadSeriesTestProfile{
			TypeLabel: floatTypeLabel,
			QuerySum:  QuerySumFloat(floatMetricName),
			GenerateSeries: func(t time.Time, numSeries int) []prompb.TimeSeries {
				return generateSineWaveSeries(floatMetricName, t, numSeries)
			},
			VerifySamplesSum: func(matrix model.Matrix, step time.Duration) (lastMatchingIdx int, err error) {
				return verifySamplesSum(matrix, t.cfg.NumSeries, step, GenerateSineWaveValue, nil)
			},
			History: &MetricHistory{},
		})
	}
	if t.cfg.WithHistograms {
		for _, histProfile := range histogramProfiles {
			histProfile := histProfile

			t.TestProfiles = append(t.TestProfiles, WriteReadSeriesTestProfile{
				TypeLabel: histProfile.typeLabel,
				QuerySum:  querySumHist(histProfile.metricName),
				GenerateSeries: func(t time.Time, numSeries int) []prompb.TimeSeries {
					return generateHistogramSeriesInner(histProfile.metricName, t, numSeries, histProfile.generateHistogram)
				},
				VerifySamplesSum: func(matrix model.Matrix, step time.Duration) (lastMatchingIdx int, err error) {
					return verifySamplesSum(matrix, t.cfg.NumSeries, step, nil, histProfile.generateSampleHistogram)
				},
				History: &MetricHistory{},
			})
		}
	}

	level.Info(t.logger).Log("msg", "Finding previously written samples time range to recover writes and reads from previous run")

	for _, testProfile := range t.TestProfiles {
		if err := t.recoverPast(ctx, now, &testProfile); err != nil {
			return err
		}

		t.metrics.InitializeCountersToZero(testProfile.TypeLabel)
	}

	return nil
}

func (t *WriteReadSeriesTest) recoverPast(ctx context.Context, now time.Time, testProfile *WriteReadSeriesTestProfile) error {
	from, to := t.findPreviouslyWrittenTimeRange(ctx, now, testProfile)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written samples time range found, will continue writing from the nearest interval-aligned timestamp", "query", testProfile.QuerySum)
		return nil
	}
	if to.Before(now.Add(-writeMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written samples time range found but latest written sample is too old to recover", "query", testProfile.QuerySum, "last_sample_timestamp", to)
		return nil
	}

	testProfile.History.lastWrittenTimestamp = to
	testProfile.History.queryMinTime = from
	testProfile.History.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Successfully found previously written samples time range and recovered writes and reads from there", "query", testProfile.QuerySum, "last_written_timestamp", testProfile.History.lastWrittenTimestamp, "query_min_time", testProfile.History.queryMinTime, "query_max_time", testProfile.History.queryMaxTime)

	return nil
}

// Run implements Test.
func (t *WriteReadSeriesTest) Run(ctx context.Context, now time.Time) error {
	// Configure the rate limiter to send a sample for each series per second. At startup, this test may catch up
	// with previous missing writes: this rate limit reduces the chances to hit the ingestion limit on Mimir side.
	writeLimiter := rate.NewLimiter(rate.Limit(t.cfg.NumSeries), t.cfg.NumSeries)

	// Collect all errors on this test run
	errs := new(multierror.MultiError)

	for _, testProfile := range t.TestProfiles {
		t.RunInner(ctx, now, writeLimiter, errs, &testProfile)

		if testProfile.PostRun != nil {
			errs.Add(testProfile.PostRun(ctx))
		}
	}

	return errs.Err()
}

func (t *WriteReadSeriesTest) RunInner(ctx context.Context, now time.Time, writeLimiter *rate.Limiter, errs *multierror.MultiError, testProfile *WriteReadSeriesTestProfile) {
	// Write series for each expected timestamp until now.
	for timestamp := t.nextWriteTimestamp(now, testProfile.History); !timestamp.After(now); timestamp = t.nextWriteTimestamp(now, testProfile.History) {
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			errs.Add(err)
			return
		}

		series := testProfile.GenerateSeries(timestamp, t.cfg.NumSeries)
		if err := t.writeSamples(ctx, testProfile.TypeLabel, timestamp, series, testProfile.History); err != nil {
			errs.Add(err)
			break
		}
	}

	queryRanges, queryInstants, err := t.getQueryTimeRanges(now, testProfile.History)
	if err != nil {
		errs.Add(err)
	}

	for _, resultsCacheEnabled := range []bool{true, false} {
		for _, timeRange := range queryRanges {
			err := t.runRangeQueryAndVerifyResult(ctx, timeRange[0], timeRange[1], resultsCacheEnabled, testProfile)
			errs.Add(err)
		}
		for _, ts := range queryInstants {
			err := t.runInstantQueryAndVerifyResult(ctx, ts, resultsCacheEnabled, testProfile)
			errs.Add(err)
		}
	}
}

func (t *WriteReadSeriesTest) writeSamples(ctx context.Context, typeLabel string, timestamp time.Time, series []prompb.TimeSeries, records *MetricHistory) error {
	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.writeSamples")
	defer sp.Finish()
	logger := log.With(sp, "timestamp", timestamp.String(), "num_series", t.cfg.NumSeries)

	start := time.Now()
	statusCode, err := t.client.WriteSeries(ctx, series)
	t.metrics.writesLatency.WithLabelValues(typeLabel).Observe(time.Since(start).Seconds())
	t.metrics.writesTotal.WithLabelValues(typeLabel).Inc()

	if statusCode/100 != 2 {
		t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode), typeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to remote write series", "status_code", statusCode, "err", err)
	} else {
		level.Debug(logger).Log("msg", "Remote write series succeeded")
	}

	// If the write request failed because of a 4xx error, retrying the request isn't expected to succeed.
	// The series may have been not written at all or partially written (eg. we hit some limit).
	// We keep writing the next interval, but we reset the query timestamp because we can't reliably
	// assert on query results due to possible gaps.
	if statusCode/100 == 4 {
		records.lastWrittenTimestamp = timestamp
		records.queryMinTime = time.Time{}
		records.queryMaxTime = time.Time{}
		return nil
	}

	// If the write request failed because of a network or 5xx error, we'll retry to write series
	// in the next test run.
	if err != nil {
		return errors.Wrap(err, "failed to remote write series")
	}
	if statusCode/100 != 2 {
		return errors.Wrapf(err, "remote write series failed with status code %d", statusCode)
	}

	// The write request succeeded.
	records.lastWrittenTimestamp = timestamp
	records.queryMaxTime = timestamp
	if records.queryMinTime.IsZero() {
		records.queryMinTime = timestamp
	}

	return nil
}

// getQueryTimeRanges returns the start/end time ranges to use to run test range queries,
// and the timestamps to use to run test instant queries.
func (t *WriteReadSeriesTest) getQueryTimeRanges(now time.Time, records *MetricHistory) (ranges [][2]time.Time, instants []time.Time, err error) {
	// The min and max allowed query timestamps are zero if there's no successfully written data yet.
	if records.queryMinTime.IsZero() || records.queryMaxTime.IsZero() {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query")
		return nil, nil, errors.New("no valid time range to query")
	}

	// Honor the configured max age.
	adjustedQueryMinTime := maxTime(records.queryMinTime, now.Add(-t.cfg.MaxQueryAge))
	if records.queryMaxTime.Before(adjustedQueryMinTime) {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query after honoring configured max query age", "min_valid_time", records.queryMinTime, "max_valid_time", records.queryMaxTime, "max_query_age", t.cfg.MaxQueryAge)
		return nil, nil, errors.New("no valid time range to query after honoring configured max query age")
	}

	// Compute the latest queriable timestamp
	// records.queryMaxTime shouldn't be after now but we want to be sure of it
	adjustedQueryMaxTime := minTime(records.queryMaxTime, now)

	// Last 1h.
	if records.queryMaxTime.After(now.Add(-1 * time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-1*time.Hour)),
			adjustedQueryMaxTime,
		})
		instants = append(instants, adjustedQueryMaxTime)
	}

	// Last 24h (only if the actual time range is not already covered by "Last 1h").
	if records.queryMaxTime.After(now.Add(-24*time.Hour)) && adjustedQueryMinTime.Before(now.Add(-1*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			adjustedQueryMaxTime,
		})
		instants = append(instants, maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)))
	}

	// From last 23h to last 24h.
	if adjustedQueryMinTime.Before(now.Add(-23*time.Hour)) && records.queryMaxTime.After(now.Add(-23*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			minTime(adjustedQueryMaxTime, now.Add(-23*time.Hour)),
		})
	}

	// A random time range.
	randMinTime := randTime(adjustedQueryMinTime, adjustedQueryMaxTime)
	ranges = append(ranges, [2]time.Time{randMinTime, randTime(randMinTime, adjustedQueryMaxTime)})
	instants = append(instants, randMinTime)

	return ranges, instants, nil
}

func (t *WriteReadSeriesTest) runRangeQueryAndVerifyResult(ctx context.Context, start, end time.Time, resultsCacheEnabled bool, testProfile *WriteReadSeriesTestProfile) error {
	// We align start, end and step to write interval in order to avoid any false positives
	// when checking results correctness. The min/max query time is always aligned.
	start = maxTime(testProfile.History.queryMinTime, alignTimestampToInterval(start, writeInterval))
	end = minTime(testProfile.History.queryMaxTime, alignTimestampToInterval(end, writeInterval))
	if end.Before(start) {
		return nil
	}

	step := getQueryStep(start, end, writeInterval)

	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.runRangeQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", testProfile.QuerySum, "start", start.UnixMilli(), "end", end.UnixMilli(), "step", step, "results_cache", strconv.FormatBool(resultsCacheEnabled), "type", testProfile.TypeLabel)
	level.Debug(logger).Log("msg", "Running range query")

	t.metrics.queriesTotal.WithLabelValues(testProfile.TypeLabel).Inc()
	queryStart := time.Now()
	matrix, err := t.client.QueryRange(ctx, testProfile.QuerySum, start, end, step, WithResultsCacheEnabled(resultsCacheEnabled))
	t.metrics.queriesLatency.WithLabelValues(testProfile.TypeLabel, strconv.FormatBool(resultsCacheEnabled)).Observe(time.Since(queryStart).Seconds())
	if err != nil {
		t.metrics.queriesFailedTotal.WithLabelValues(testProfile.TypeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to execute range query", "err", err)
		return errors.Wrap(err, "failed to execute range query")
	}

	t.metrics.queryResultChecksTotal.WithLabelValues(testProfile.TypeLabel).Inc()
	_, err = testProfile.VerifySamplesSum(matrix, step)
	if err != nil {
		t.metrics.queryResultChecksFailedTotal.WithLabelValues(testProfile.TypeLabel).Inc()
		level.Warn(logger).Log("msg", "Range query result check failed", "err", err)
		return errors.Wrap(err, "range query result check failed")
	}

	level.Info(logger).Log("msg", "Range query result check succeeded")

	return nil
}

func (t *WriteReadSeriesTest) runInstantQueryAndVerifyResult(ctx context.Context, ts time.Time, resultsCacheEnabled bool, testProfile *WriteReadSeriesTestProfile) error {
	// We align the query timestamp to write interval in order to avoid any false positives
	// when checking results correctness. The min/max query time is always aligned.
	ts = maxTime(testProfile.History.queryMinTime, alignTimestampToInterval(ts, writeInterval))
	if testProfile.History.queryMaxTime.Before(ts) {
		return nil
	}

	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.runInstantQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", testProfile.QuerySum, "ts", ts.UnixMilli(), "results_cache", strconv.FormatBool(resultsCacheEnabled), "type", testProfile.TypeLabel)
	level.Debug(logger).Log("msg", "Running instant query")

	t.metrics.queriesTotal.WithLabelValues(testProfile.TypeLabel).Inc()
	queryStart := time.Now()
	vector, err := t.client.Query(ctx, testProfile.QuerySum, ts, WithResultsCacheEnabled(resultsCacheEnabled))
	t.metrics.queriesLatency.WithLabelValues(testProfile.TypeLabel, strconv.FormatBool(resultsCacheEnabled)).Observe(time.Since(queryStart).Seconds())
	if err != nil {
		t.metrics.queriesFailedTotal.WithLabelValues(testProfile.TypeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to execute instant query", "err", err)
		return errors.Wrap(err, "failed to execute instant query")
	}

	// Convert the vector to matrix to reuse the same results comparison utility.
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

	t.metrics.queryResultChecksTotal.WithLabelValues(testProfile.TypeLabel).Inc()
	_, err = testProfile.VerifySamplesSum(matrix, 0)
	if err != nil {
		t.metrics.queryResultChecksFailedTotal.WithLabelValues(testProfile.TypeLabel).Inc()
		level.Warn(logger).Log("msg", "Instant query result check failed", "err", err)
		return errors.Wrap(err, "instant query result check failed")
	}

	level.Info(logger).Log("msg", "Instant query result check succeeded")

	return nil
}

func (t *WriteReadSeriesTest) nextWriteTimestamp(now time.Time, records *MetricHistory) time.Time {
	if records.lastWrittenTimestamp.IsZero() {
		return alignTimestampToInterval(now, writeInterval)
	}

	return records.lastWrittenTimestamp.Add(writeInterval)
}

func (t *WriteReadSeriesTest) findPreviouslyWrittenTimeRange(ctx context.Context, now time.Time, testProfile *WriteReadSeriesTestProfile) (from, to time.Time) {
	end := alignTimestampToInterval(now, writeInterval)
	step := writeInterval

	var samples []model.SamplePair
	var histograms []model.SampleHistogramPair

	for {
		start := alignTimestampToInterval(maxTime(now.Add(-t.cfg.MaxQueryAge), end.Add(-24*time.Hour).Add(step)), writeInterval)
		if !start.Before(end) {
			// We've hit the max query age, so we'll keep the last computed valid time range (if any).
			return
		}

		logger := log.With(t.logger, "query", testProfile.QuerySum, "start", start, "end", end, "step", step)
		level.Debug(logger).Log("msg", "Executing query to find previously written samples")

		matrix, err := t.client.QueryRange(ctx, testProfile.QuerySum, start, end, step, WithResultsCacheEnabled(false))
		if err != nil {
			level.Warn(logger).Log("msg", "Failed to execute range query used to find previously written samples", "query", testProfile.QuerySum, "err", err)
			return
		}

		if len(matrix) == 0 {
			// No samples found, so we'll keep the last computed valid time range (if any).
			return
		}

		if len(matrix) != 1 {
			level.Error(logger).Log("msg", "The range query used to find previously written samples returned an unexpected number of series", "query", testProfile.QuerySum, "expected", 1, "returned", len(matrix))
			return
		}

		samples = append(matrix[0].Values, samples...)
		histograms = append(matrix[0].Histograms, histograms...)
		end = start.Add(-step)

		var fullMatrix model.Matrix
		useHistograms := false
		if len(samples) > 0 && len(histograms) == 0 {
			fullMatrix = model.Matrix{{Values: samples}}
		} else if len(histograms) > 0 && len(samples) == 0 {
			fullMatrix = model.Matrix{{Histograms: histograms}}
			useHistograms = true
		} else {
			level.Error(logger).Log("msg", "The range query used to find previously written samples returned either both floats and histograms or neither", "query", testProfile.QuerySum)
			return
		}
		lastMatchingIdx, _ := testProfile.VerifySamplesSum(fullMatrix, step)
		if lastMatchingIdx == -1 {
			return
		}

		// Update the previously written time range.
		if useHistograms {
			from = histograms[lastMatchingIdx].Timestamp.Time()
			to = histograms[len(histograms)-1].Timestamp.Time()
		} else {
			from = samples[lastMatchingIdx].Timestamp.Time()
			to = samples[len(samples)-1].Timestamp.Time()
		}

		// If the last matching sample is not the one at the beginning of the queried time range
		// then it means we've found the oldest previously written sample and we can stop searching it.
		if lastMatchingIdx != 0 || (!useHistograms && !samples[0].Timestamp.Time().Equal(start)) || (useHistograms && !histograms[0].Timestamp.Time().Equal(start)) {
			return
		}
	}
}
