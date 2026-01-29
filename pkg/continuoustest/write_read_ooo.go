package continuoustest

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/time/rate"
)

const (
	oooFloatMetricName      = "mimir_continuous_sine_wave_ooo_v2"
	inorderWriteInterval    = 1 * time.Minute
	outOfOrderWriteInterval = 20 * time.Second
)

type WriteReadOOOTestConfig struct {
	Enabled     bool
	NumSeries   int
	MaxOOOLag   time.Duration
	MaxQueryAge time.Duration
}

func (cfg *WriteReadOOOTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.write-read-ooo-test.enabled", false, "Enables a test that writes samples out-of-order and verifies query results.")
	f.IntVar(&cfg.NumSeries, "tests.write-read-ooo-test.num-series", 5000, "Number of series used for the test.")
	f.DurationVar(&cfg.MaxOOOLag, "tests.write-read-ooo-test.max-ooo-lag", 1*time.Hour, "The maximum time in which the writing of a sample might be delayed. Must be smaller than the tenant's out-of-order time window or delayed writes will be rejected.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.write-read-ooo-test.max-query-age", 7*24*time.Hour, "How back in the past metrics can be queried at most.")
}

type WriteReadOOOTest struct {
	name    string
	cfg     WriteReadOOOTestConfig
	client  MimirClient
	logger  log.Logger
	metrics *TestMetrics

	inOrderSamples    MetricHistory
	outOfOrderSamples MetricHistory
}

func NewWriteReadOOOTest(cfg WriteReadOOOTestConfig, client MimirClient, logger log.Logger, reg prometheus.Registerer) *WriteReadOOOTest {
	const name = "write-read-ooo"

	return &WriteReadOOOTest{
		name:    name,
		cfg:     cfg,
		client:  client,
		logger:  log.With(logger, "test", name),
		metrics: NewTestMetrics(name, reg),
	}
}

// Name implements Test.
func (t *WriteReadOOOTest) Name() string {
	return t.name
}

// Init implements Test.
func (t *WriteReadOOOTest) Init(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}

	err := t.recoverPast(ctx, now, oooFloatMetricName, querySumFloat, generateSineWaveValue, &t.inOrderSamples, &t.outOfOrderSamples)
	if err != nil {
		return err
	}
	t.metrics.InitializeCountersToZero(floatTypeLabel)
	return nil
}

// Run implements Test.
func (t *WriteReadOOOTest) Run(ctx context.Context, now time.Time) error {
	if !t.cfg.Enabled {
		return nil
	}

	// Configure the rate limiter to send a sample for each series per second. At startup, this test may catch up
	// with previous missing writes: this rate limit reduces the chances to hit the ingestion limit on Mimir side.
	writeLimiter := rate.NewLimiter(rate.Limit(t.cfg.NumSeries), t.cfg.NumSeries)
	// Collect all errors on this test run
	errs := new(multierror.MultiError)

	t.RunInner(ctx, now, writeLimiter, errs, oooFloatMetricName, generateSineWaveSeries)

	return errs.Err()
}

func (t *WriteReadOOOTest) RunInner(ctx context.Context, now time.Time, writeLimiter *rate.Limiter, errs *multierror.MultiError, metricName string, generateSeries generateSeriesFunc) {
	// Samples aligned with the minute are written in-order.
	// Samples at :20 and :40 past the minute are written out-of-order by approximatelyMaxOOOLag.
	// We cannot use a label to differentiate between in-order and out-of-order samples, it needs to be the same actual series.

	// First, write the in-order (minute-aligned) data.
	for timestamp := t.nextInorderWriteTimestamp(now, inorderWriteInterval, &t.inOrderSamples); !timestamp.After(now); timestamp = t.nextInorderWriteTimestamp(now, inorderWriteInterval, &t.inOrderSamples) {
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			errs.Add(err)
			return
		}

		series := generateSeries(metricName, timestamp, t.cfg.NumSeries, prompb.Label{Name: "protocol", Value: t.client.Protocol()})
		if err := t.writeSamples(ctx, floatTypeLabel, timestamp, series, metricName, floatMetricMetadata, &t.inOrderSamples); err != nil {
			errs.Add(err)
			return
		}
	}

	// Now, fill in the gaps, lagging behind by some time - writing out of order.
	stopAt := now.Add(-t.cfg.MaxOOOLag)
	for timestamp := t.nextOutOfOrderWriteTimestamp(now, outOfOrderWriteInterval, &t.outOfOrderSamples); !timestamp.After(stopAt); timestamp = t.nextOutOfOrderWriteTimestamp(now, outOfOrderWriteInterval, &t.outOfOrderSamples) {
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			errs.Add(err)
			return
		}

		series := generateSeries(metricName, timestamp, t.cfg.NumSeries, prompb.Label{Name: "protocol", Value: t.client.Protocol()})

		level.Info(t.logger).Log("msg", "Dry run OOO sample write", "timestamp", timestamp, "numSeries", len(series))
	}

}

func (t *WriteReadOOOTest) nextInorderWriteTimestamp(now time.Time, interval time.Duration, records *MetricHistory) time.Time {
	if records.lastWrittenTimestamp.IsZero() {
		return alignTimestampToInterval(now, interval)
	}

	return records.lastWrittenTimestamp.Add(interval)
}

func (t *WriteReadOOOTest) nextOutOfOrderWriteTimestamp(now time.Time, interval time.Duration, records *MetricHistory) time.Time {
	base := records.lastWrittenTimestamp.Add(interval)
	if records.lastWrittenTimestamp.IsZero() {
		base = alignTimestampToInterval(now, interval)
	}

	if t.isInOrderTimestamp(base) {
		base = base.Add(interval)
	}

	return base
}

func (t *WriteReadOOOTest) isInOrderTimestamp(ts time.Time) bool {
	return ts.Equal(alignTimestampToInterval(ts, inorderWriteInterval))
}

func (t *WriteReadOOOTest) recoverPast(ctx context.Context, now time.Time, metricName string, querySum querySumFunc, generateValue generateValueFunc, inOrderRecords, outOfOrderRecords *MetricHistory) error {
	// We have two "series" embedded into one, aligned to different steps.
	// First recover to the larger one, which represents the in-order samples.
	from, to := t.findPreviouslyWrittenTimeRange(ctx, now, inorderWriteInterval, metricName, querySum, generateValue, nil)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written samples time range found, will continue writing from the nearest interval-aligned timestamp", "metric_name", metricName)
		return nil
	}
	if to.Before(now.Add(-writeMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written samples time range found but latest written sample is too old to recover", "metric_name", metricName, "last_sample_timestamp", to)
		return nil
	}

	inOrderRecords.lastWrittenTimestamp = to
	inOrderRecords.queryMinTime = from
	inOrderRecords.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Successfully found previously written inorder samples time range and recovered writes and reads from there", "metric_name", metricName, "last_written_timestamp", inOrderRecords.lastWrittenTimestamp, "query_min_time", inOrderRecords.queryMinTime, "query_max_time", inOrderRecords.queryMaxTime)

	// Search a second time, but at a tighter step, to find how far back we've written the more dense, fully-formed series.
	// Ignore the samples from the in-order time range, only look at the gaps.
	from, to = t.findPreviouslyWrittenTimeRange(ctx, now, outOfOrderWriteInterval, metricName, querySum, generateValue, t.isInOrderTimestamp)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written OOO samples found, will continue writing from the nearest interval-aligned timestamp", "metric_name", metricName)
		return nil
	}
	if to.Before(now.Add(-writeMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written OOO samples time range found but latest written sample is too old to recover", "metric_name", metricName, "last_sample_timestamp", to)
		return nil
	}

	outOfOrderRecords.lastWrittenTimestamp = to
	outOfOrderRecords.queryMinTime = from
	outOfOrderRecords.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Found time range for densely written samples", "from", from, "to", to)

	return nil
}

func (t *WriteReadOOOTest) findPreviouslyWrittenTimeRange(ctx context.Context, now time.Time, step time.Duration, metricName string, querySum querySumFunc, generateValue generateValueFunc, skipTimestamp skipTimestampFunc) (from, to time.Time) {
	end := alignTimestampToInterval(now, step)

	var samples []model.SamplePair
	query := querySum(metricName)

	for {
		start := alignTimestampToInterval(maxTime(now.Add(-t.cfg.MaxQueryAge), end.Add(-24*time.Hour).Add(step)), step)
		if !start.Before(end) {
			// We've hit the max query age, so we'll keep the last computed valid time range (if any).
			return
		}

		logger := log.With(t.logger, "query", query, "start", start, "end", end, "step", step, "metric_name", metricName)
		level.Debug(logger).Log("msg", "Executing query to find previously written samples")

		matrix, err := t.client.QueryRange(ctx, query, start, end, step, WithResultsCacheEnabled(false))
		if err != nil {
			level.Warn(logger).Log("msg", "Failed to execute range query used to find previously written samples", "err", err)
			return
		}

		if len(matrix) == 0 {
			level.Warn(logger).Log("msg", "The range query used to find previously written samples returned no series, this should only happen if continuous-test has not ever run or has not run since the start of the query window")
			return
		}

		if len(matrix) != 1 {
			level.Error(logger).Log("msg", "The range query used to find previously written samples returned an unexpected number of series", "expected", 1, "returned", len(matrix))
			return
		}

		samples = append(matrix[0].Values, samples...)
		end = start.Add(-step)

		fullMatrix := model.Matrix{{Values: samples}}
		lastMatchingIdx, err := verifySamplesSum(fullMatrix, t.cfg.NumSeries, step, generateValue, nil, skipTimestamp)
		if lastMatchingIdx == -1 {
			level.Warn(logger).Log("msg", "The range query used to find previously written samples returned no timestamps where the returned value matched the expected value", "err", err)
			return
		}

		from = samples[lastMatchingIdx].Timestamp.Time()
		to = samples[len(samples)-1].Timestamp.Time()

		level.Info(logger).Log("msg", "Found previously written samples", "from", from, "to", to, "issue_with_earlier_data", err)
		// If the last matching sample is not the one at the beginning of the queried time range
		// then it means we've found the oldest previously written sample and we can stop searching it.
		if lastMatchingIdx != 0 || !samples[0].Timestamp.Time().Equal(start) {
			return
		}
	}
}

func (t *WriteReadOOOTest) writeSamples(ctx context.Context, typeLabel string, timestamp time.Time, series []prompb.TimeSeries, metricName string, metadata []prompb.MetricMetadata, records *MetricHistory) error {
	sp, ctx := spanlogger.New(ctx, t.logger, tracer, "WriteReadOOOTest.writeSamples")
	defer sp.Finish()
	logger := log.With(sp, "timestamp", timestamp.UnixMilli(), "num_series", t.cfg.NumSeries, "metric_name", metricName, "protocol", t.client.Protocol())

	start := time.Now()
	statusCode, err := t.client.WriteSeries(ctx, series, metadata)
	t.metrics.writesLatency.WithLabelValues(typeLabel).Observe(time.Since(start).Seconds())
	t.metrics.writesTotal.WithLabelValues(typeLabel).Inc()

	if statusCode/100 != 2 {
		t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode), typeLabel).Inc()
		level.Warn(logger).Log("msg", "Failed to remote write series", "status_code", statusCode, "err", err)
	} else {
		level.Info(logger).Log("msg", "Remote write series succeeded", "timestamp", timestamp, "numSeries", len(series))
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
		return fmt.Errorf("failed to remote write series: %w", err)
	}
	if statusCode/100 != 2 {
		return fmt.Errorf("remote write series failed with status code %d: %w", statusCode, err)
	}

	// The write request succeeded.
	records.lastWrittenTimestamp = timestamp
	records.queryMaxTime = timestamp
	if records.queryMinTime.IsZero() {
		records.queryMinTime = timestamp
	}

	return nil
}
