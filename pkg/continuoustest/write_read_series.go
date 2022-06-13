// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"golang.org/x/time/rate"

	"github.com/grafana/dskit/multierror"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	writeInterval = 20 * time.Second
	writeMaxAge   = 50 * time.Minute
	metricName    = "mimir_continuous_test_sine_wave"
)

var (
	// We use max_over_time() with a 1s range selector in order to fetch only the samples we previously
	// wrote and ensure the PromQL lookback period doesn't influence query results. This help to avoid
	// false positives when finding the last written sample, or when restarting the testing tool with
	// a different number of configured series to write and read.
	queryMetricSum = fmt.Sprintf("sum(max_over_time(%s[1s]))", metricName)
)

type WriteReadSeriesTestConfig struct {
	NumSeries   int
	MaxQueryAge time.Duration
}

func (cfg *WriteReadSeriesTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.NumSeries, "tests.write-read-series-test.num-series", 10000, "Number of series used for the test.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.write-read-series-test.max-query-age", 7*24*time.Hour, "How back in the past metrics can be queried at most.")
}

type WriteReadSeriesTest struct {
	name    string
	cfg     WriteReadSeriesTestConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics

	lastWrittenTimestamp time.Time
	queryMinTime         time.Time
	queryMaxTime         time.Time
}

func NewWriteReadSeriesTest(cfg WriteReadSeriesTestConfig, client MimirClient, logger log.Logger, reg prometheus.Registerer) *WriteReadSeriesTest {
	const name = "write-read-series"

	return &WriteReadSeriesTest{
		name:    name,
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", name),
		metrics: NewTestMetrics(name, reg),
	}
}

// Name implements Test.
func (t *WriteReadSeriesTest) Name() string {
	return t.name
}

// Init implements Test.
func (t *WriteReadSeriesTest) Init(ctx context.Context, now time.Time) error {
	level.Info(t.logger).Log("msg", "Finding previously written samples time range to recover writes and reads from previous run")

	from, to := t.findPreviouslyWrittenTimeRange(ctx, now)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written samples time range found")
		return nil
	}
	if to.Before(now.Add(-writeMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written samples time range found but latest written sample is too old to recover", "last_sample_timestamp", to)
		return nil
	}

	t.lastWrittenTimestamp = to
	t.queryMinTime = from
	t.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Successfully found previously written samples time range and recovered writes and reads from there", "last_written_timestamp", t.lastWrittenTimestamp, "query_min_time", t.queryMinTime, "query_max_time", t.queryMaxTime)

	return nil
}

// Run implements Test.
func (t *WriteReadSeriesTest) Run(ctx context.Context, now time.Time) error {
	// Configure the rate limiter to send a sample for each series per second. At startup, this test may catch up
	// with previous missing writes: this rate limit reduces the chances to hit the ingestion limit on Mimir side.
	writeLimiter := rate.NewLimiter(rate.Limit(t.cfg.NumSeries), t.cfg.NumSeries)

	// Collect all errors on this test run
	errs := new(multierror.MultiError)

	// Write series for each expected timestamp until now.
	for timestamp := t.nextWriteTimestamp(now); !timestamp.After(now); timestamp = t.nextWriteTimestamp(now) {
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			return err
		}

		if err := t.writeSamples(ctx, timestamp); err != nil {
			errs.Add(err)
			break
		}
	}

	queryRanges, queryInstants, err := t.getQueryTimeRanges(now)
	if err != nil {
		errs.Add(err)
	}
	for _, timeRange := range queryRanges {
		err := t.runRangeQueryAndVerifyResult(ctx, timeRange[0], timeRange[1], true)
		errs.Add(err)
		err = t.runRangeQueryAndVerifyResult(ctx, timeRange[0], timeRange[1], false)
		errs.Add(err)
	}
	for _, ts := range queryInstants {
		err := t.runInstantQueryAndVerifyResult(ctx, ts, true)
		errs.Add(err)
		err = t.runInstantQueryAndVerifyResult(ctx, ts, false)
		errs.Add(err)
	}
	return errs.Err()
}

func (t *WriteReadSeriesTest) writeSamples(ctx context.Context, timestamp time.Time) error {
	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.writeSamples")
	defer sp.Finish()
	logger := log.With(sp, "timestamp", timestamp.String(), "num_series", t.cfg.NumSeries)

	statusCode, err := t.client.WriteSeries(ctx, generateSineWaveSeries(metricName, timestamp, t.cfg.NumSeries))

	t.metrics.writesTotal.Inc()
	if statusCode/100 != 2 {
		t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode)).Inc()
		level.Warn(logger).Log("msg", "Failed to remote write series", "status_code", statusCode, "err", err)
	} else {
		level.Debug(logger).Log("msg", "Remote write series succeeded")
	}

	// If the write request failed because of a 4xx error, retrying the request isn't expected to succeed.
	// The series may have been not written at all or partially written (eg. we hit some limit).
	// We keep writing the next interval, but we reset the query timestamp because we can't reliably
	// assert on query results due to possible gaps.
	if statusCode/100 == 4 {
		t.lastWrittenTimestamp = timestamp
		t.queryMinTime = time.Time{}
		t.queryMaxTime = time.Time{}
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
	t.lastWrittenTimestamp = timestamp
	t.queryMaxTime = timestamp
	if t.queryMinTime.IsZero() {
		t.queryMinTime = timestamp
	}

	return nil
}

// getQueryTimeRanges returns the start/end time ranges to use to run test range queries,
// and the timestamps to use to run test instant queries.
func (t *WriteReadSeriesTest) getQueryTimeRanges(now time.Time) (ranges [][2]time.Time, instants []time.Time, err error) {
	// The min and max allowed query timestamps are zero if there's no successfully written data yet.
	if t.queryMinTime.IsZero() || t.queryMaxTime.IsZero() {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query")
		return nil, nil, errors.New("no valid time range to query")
	}

	// Honor the configured max age.
	adjustedQueryMinTime := maxTime(t.queryMinTime, now.Add(-t.cfg.MaxQueryAge))
	if t.queryMaxTime.Before(adjustedQueryMinTime) {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query after honoring configured max query age", "min_valid_time", t.queryMinTime, "max_valid_time", t.queryMaxTime, "max_query_age", t.cfg.MaxQueryAge)
		return nil, nil, errors.New("no valid time range to query after honoring configured max query age")
	}

	// Last 1h.
	if t.queryMaxTime.After(now.Add(-1 * time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-1*time.Hour)),
			minTime(t.queryMaxTime, now),
		})
		instants = append(instants, minTime(t.queryMaxTime, now))
	}

	// Last 24h (only if the actual time range is not already covered by "Last 1h").
	if t.queryMaxTime.After(now.Add(-24*time.Hour)) && adjustedQueryMinTime.Before(now.Add(-1*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			minTime(t.queryMaxTime, now),
		})
		instants = append(instants, maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)))
	}

	// From last 23h to last 24h.
	if adjustedQueryMinTime.Before(now.Add(-23*time.Hour)) && t.queryMaxTime.After(now.Add(-23*time.Hour)) {
		ranges = append(ranges, [2]time.Time{
			maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
			minTime(t.queryMaxTime, now.Add(-23*time.Hour)),
		})
	}

	// A random time range.
	randMinTime := randTime(adjustedQueryMinTime, t.queryMaxTime)
	ranges = append(ranges, [2]time.Time{randMinTime, randTime(randMinTime, t.queryMaxTime)})
	instants = append(instants, randMinTime)

	return ranges, instants, nil
}

func (t *WriteReadSeriesTest) runRangeQueryAndVerifyResult(ctx context.Context, start, end time.Time, resultsCacheEnabled bool) error {
	// We align start, end and step to write interval in order to avoid any false positives
	// when checking results correctness. The min/max query time is always aligned.
	start = maxTime(t.queryMinTime, alignTimestampToInterval(start, writeInterval))
	end = minTime(t.queryMaxTime, alignTimestampToInterval(end, writeInterval))
	if end.Before(start) {
		return nil
	}

	step := getQueryStep(start, end, writeInterval)

	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.runRangeQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", queryMetricSum, "start", start.UnixMilli(), "end", end.UnixMilli(), "step", step, "results_cache", strconv.FormatBool(resultsCacheEnabled))
	level.Debug(logger).Log("msg", "Running range query")

	t.metrics.queriesTotal.Inc()
	matrix, err := t.client.QueryRange(ctx, queryMetricSum, start, end, step, WithResultsCacheEnabled(resultsCacheEnabled))
	if err != nil {
		t.metrics.queriesFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Failed to execute range query", "err", err)
		return errors.Wrap(err, "failed to execute range query")
	}

	t.metrics.queryResultChecksTotal.Inc()
	_, err = verifySineWaveSamplesSum(matrix, t.cfg.NumSeries, step)
	if err != nil {
		t.metrics.queryResultChecksFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Range query result check failed", "err", err)
		return errors.Wrap(err, "range query result check failed")
	}
	return nil
}

func (t *WriteReadSeriesTest) runInstantQueryAndVerifyResult(ctx context.Context, ts time.Time, resultsCacheEnabled bool) error {
	// We align the query timestamp to write interval in order to avoid any false positives
	// when checking results correctness. The min/max query time is always aligned.
	ts = maxTime(t.queryMinTime, alignTimestampToInterval(ts, writeInterval))
	if t.queryMaxTime.Before(ts) {
		return nil
	}

	sp, ctx := spanlogger.NewWithLogger(ctx, t.logger, "WriteReadSeriesTest.runInstantQueryAndVerifyResult")
	defer sp.Finish()

	logger := log.With(sp, "query", queryMetricSum, "ts", ts.UnixMilli(), "results_cache", strconv.FormatBool(resultsCacheEnabled))
	level.Debug(logger).Log("msg", "Running instant query")

	t.metrics.queriesTotal.Inc()
	vector, err := t.client.Query(ctx, queryMetricSum, ts, WithResultsCacheEnabled(resultsCacheEnabled))
	if err != nil {
		t.metrics.queriesFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Failed to execute instant query", "err", err)
		return errors.Wrap(err, "failed to execute instant query")
	}

	// Convert the vector to matrix to reuse the same results comparison utility.
	matrix := make(model.Matrix, 0, len(vector))
	for _, entry := range vector {
		matrix = append(matrix, &model.SampleStream{
			Metric: entry.Metric,
			Values: []model.SamplePair{{
				Timestamp: entry.Timestamp,
				Value:     entry.Value,
			}},
		})
	}

	t.metrics.queryResultChecksTotal.Inc()
	_, err = verifySineWaveSamplesSum(matrix, t.cfg.NumSeries, 0)
	if err != nil {
		t.metrics.queryResultChecksFailedTotal.Inc()
		level.Warn(logger).Log("msg", "Instant query result check failed", "err", err)
		return errors.Wrap(err, "instant query result check failed")
	}
	return nil
}

func (t *WriteReadSeriesTest) nextWriteTimestamp(now time.Time) time.Time {
	if t.lastWrittenTimestamp.IsZero() {
		return alignTimestampToInterval(now, writeInterval)
	}

	return t.lastWrittenTimestamp.Add(writeInterval)
}

func (t *WriteReadSeriesTest) findPreviouslyWrittenTimeRange(ctx context.Context, now time.Time) (from, to time.Time) {
	end := alignTimestampToInterval(now, writeInterval)
	step := writeInterval

	var samples []model.SamplePair

	for {
		start := alignTimestampToInterval(maxTime(now.Add(-t.cfg.MaxQueryAge), end.Add(-24*time.Hour).Add(step)), writeInterval)
		if !start.Before(end) {
			// We've hit the max query age, so we'll keep the last computed valid time range (if any).
			return
		}

		logger := log.With(t.logger, "query", queryMetricSum, "start", start, "end", end, "step", step)
		level.Debug(logger).Log("msg", "Executing query to find previously written samples")

		matrix, err := t.client.QueryRange(ctx, queryMetricSum, start, end, step, WithResultsCacheEnabled(false))
		if err != nil {
			level.Warn(logger).Log("msg", "Failed to execute range query used to find previously written samples", "err", err)
			return
		}

		if len(matrix) == 0 {
			// No samples found, so we'll keep the last computed valid time range (if any).
			return
		}

		if len(matrix) != 1 {
			level.Error(logger).Log("msg", "The range query used to find previously written samples returned an unexpected number of series", "expected", 1, "returned", len(matrix))
			return
		}

		samples = append(matrix[0].Values, samples...)
		end = start.Add(-step)

		lastMatchingIdx, _ := verifySineWaveSamplesSum(model.Matrix{{Values: samples}}, t.cfg.NumSeries, step)
		if lastMatchingIdx == -1 {
			return
		}

		// Update the previously written time range.
		from = samples[lastMatchingIdx].Timestamp.Time()
		to = samples[len(samples)-1].Timestamp.Time()

		// If the last matching sample is not the one at the beginning of the queried time range
		// then it means we've found the oldest previously written sample and we can stop searching it.
		if lastMatchingIdx != 0 || !samples[0].Timestamp.Time().Equal(start) {
			return
		}
	}
}
