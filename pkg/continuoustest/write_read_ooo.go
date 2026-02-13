// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/time/rate"
)

const (
	oooFloatMetricName      = "mimir_continuous_test_sine_wave_ooo_v2"
	inorderWriteInterval    = 1 * time.Minute
	outOfOrderWriteInterval = 20 * time.Second
	oooTestWriteMaxAge      = 110 * time.Minute
)

var oooMetricMetadata = []prompb.MetricMetadata{{
	Type:             prompb.MetricMetadata_GAUGE,
	MetricFamilyName: oooFloatMetricName,
	Help:             "A neverending sine wave. Samples aligned with the minute (:00) are written in-order, in realtime. Samples at :20 and :40 past the minute are written out-of-order, lagging behind.",
	Unit:             "u",
}}

type WriteReadOOOTestConfig struct {
	Enabled     bool
	NumSeries   int
	MaxOOOLag   time.Duration
	MaxQueryAge time.Duration
}

func (cfg *WriteReadOOOTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "tests.write-read-ooo-test.enabled", false, "Enables a test that writes samples out-of-order and verifies query results.")
	f.IntVar(&cfg.NumSeries, "tests.write-read-ooo-test.num-series", 5000, "Number of series used for the test.")
	f.DurationVar(&cfg.MaxOOOLag, "tests.write-read-ooo-test.max-ooo-lag", 1*time.Hour, "The maximum time in which the writing of a sample might be delayed. Must be smaller than the tenant's out-of-order time window or delayed writes are rejected.")
	f.DurationVar(&cfg.MaxQueryAge, "tests.write-read-ooo-test.max-query-age", 7*24*time.Hour, "How back in the past metrics can be queried at most.")
}

func (cfg *WriteReadOOOTestConfig) ValidateConfig() error {
	if cfg.NumSeries <= 0 {
		return fmt.Errorf("the number of series must be greater than 0")
	}
	if cfg.MaxOOOLag >= oooTestWriteMaxAge {
		return fmt.Errorf("the max OOO lag (%s) must be less than the max write age and OOO window for the cell (%s)", cfg.MaxOOOLag, oooTestWriteMaxAge)
	}
	return nil
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
	if err := t.cfg.ValidateConfig(); err != nil {
		return err
	}
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
	// Samples aligned with the minute (:00) are always written in-order, in realtime.
	// Samples at :20 and :40 past the minute are written out-of-order, lagging behind by MaxOOOLag.
	// These must be within the exact same series, or else the writes aren't considered truly OOO. We can't differentiate them by label.
	//
	// A given series should look like the following (assuming MaxOOOLag = 1h):
	//
	//       past                                        now - 1h                                  now
	//         |                                            |                                       |
	//         v                                            v                                       v
	//         |-------- dense (every 20s) -----------------|--------- sparse (every 1m) ----------|
	//
	//         .        .        .        .        .        .       .        .        .        .  <- in-order (:00)
	//            o  o     o  o     o  o     o  o     o  o                                        <- out-of-order(:20, :40)
	//         :00:20:40:00:20:40:00:20:40:00:20:40:00:20:40:00      :00      :00      :00      :00
	//

	// First, write the in-order (minute-aligned) data.
	for timestamp := t.nextInorderWriteTimestamp(now, inorderWriteInterval, &t.inOrderSamples); !timestamp.After(now); timestamp = t.nextInorderWriteTimestamp(now, inorderWriteInterval, &t.inOrderSamples) {
		logger := log.With(t.logger, "timestamp", timestamp.UnixMilli(), "num_series", t.cfg.NumSeries, "metric_name", metricName)
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			errs.Add(err)
			return
		}

		series := generateSeries(metricName, timestamp, t.cfg.NumSeries, prompb.Label{Name: "protocol", Value: t.client.Protocol()})
		if err := writeSamples(ctx, floatTypeLabel, timestamp, series, oooMetricMetadata, &t.inOrderSamples, t.client, t.metrics, logger); err != nil {
			errs.Add(err)
			return
		}
	}

	// Now, fill in the gaps, lagging behind by some time - writing out of order.
	oooNow := now.Add(-t.cfg.MaxOOOLag)
	for timestamp := t.nextOutOfOrderWriteTimestamp(oooNow, outOfOrderWriteInterval, &t.outOfOrderSamples); !timestamp.After(oooNow); timestamp = t.nextOutOfOrderWriteTimestamp(oooNow, outOfOrderWriteInterval, &t.outOfOrderSamples) {
		logger := log.With(t.logger, "timestamp", timestamp.UnixMilli(), "num_series", t.cfg.NumSeries, "metric_name", metricName)
		if err := writeLimiter.WaitN(ctx, t.cfg.NumSeries); err != nil {
			// Context has been canceled, so we should interrupt.
			errs.Add(err)
			return
		}

		series := generateSeries(metricName, timestamp, t.cfg.NumSeries, prompb.Label{Name: "protocol", Value: t.client.Protocol()})
		if err := writeSamples(ctx, floatTypeLabel, timestamp, series, oooMetricMetadata, &t.outOfOrderSamples, t.client, t.metrics, logger); err != nil {
			errs.Add(err)
			return
		}
	}

	inorderRanges, inorderInstants, err := t.getInorderQueryTimeRanges(now)
	if err != nil {
		errs.Add(err)
	}
	for _, timeRange := range inorderRanges {
		level.Info(t.logger).Log("msg", "dry run inorder range query", "from", timeRange[0], "to", timeRange[1])
	}
	for _, ts := range inorderInstants {
		level.Info(t.logger).Log("msg", "dry run inorder instant query", "ts", ts)
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
	from, to := findPreviouslyWrittenTimeRange(ctx, now, inorderWriteInterval, t.cfg.MaxQueryAge, metricName, t.cfg.NumSeries, querySum, generateValue, nil, nil, t.client, t.logger)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written samples time range found, will continue writing from the nearest interval-aligned timestamp", "metric_name", metricName)
		return nil
	}
	if to.Before(now.Add(-oooTestWriteMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written samples time range found but latest written sample is too old to recover", "metric_name", metricName, "last_sample_timestamp", to)
		return nil
	}

	inOrderRecords.lastWrittenTimestamp = to
	inOrderRecords.queryMinTime = from
	inOrderRecords.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Successfully found previously written inorder samples time range and recovered writes and reads from there", "metric_name", metricName, "last_written_timestamp", inOrderRecords.lastWrittenTimestamp, "query_min_time", inOrderRecords.queryMinTime, "query_max_time", inOrderRecords.queryMaxTime)

	// Search a second time, but at a tighter step, to find how far back we've written the more dense, fully-formed series.
	// Ignore the samples from the in-order time range, only look at the gaps.
	from, to = findPreviouslyWrittenTimeRange(ctx, now, outOfOrderWriteInterval, t.cfg.MaxQueryAge, metricName, t.cfg.NumSeries, querySum, generateValue, nil, t.isInOrderTimestamp, t.client, t.logger)
	if from.IsZero() || to.IsZero() {
		level.Info(t.logger).Log("msg", "No valid previously written OOO samples found, will continue writing from the nearest interval-aligned timestamp", "metric_name", metricName)
		return nil
	}
	if to.Before(now.Add(-oooTestWriteMaxAge)) {
		level.Info(t.logger).Log("msg", "Previously written OOO samples time range found but latest written sample is too old to recover", "metric_name", metricName, "last_sample_timestamp", to)
		return nil
	}

	outOfOrderRecords.lastWrittenTimestamp = to
	outOfOrderRecords.queryMinTime = from
	outOfOrderRecords.queryMaxTime = to
	level.Info(t.logger).Log("msg", "Found time range for densely written samples", "from", from, "to", to)

	return nil
}

// getInorderQueryTimeRanges calculates some ranges to query to validate the in-order samples.
// We only do a spot-check, we don't deeply validate these samples.
func (t *WriteReadOOOTest) getInorderQueryTimeRanges(now time.Time) (ranges [][2]time.Time, instants []time.Time, err error) {
	records := &t.inOrderSamples

	if records.queryMinTime.IsZero() || records.queryMaxTime.IsZero() {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query")
		return nil, nil, errors.New("no valid time range to query")
	}

	adjustedQueryMinTime := maxTime(records.queryMinTime, now.Add(-t.cfg.MaxQueryAge))
	if records.queryMaxTime.Before(adjustedQueryMinTime) {
		level.Info(t.logger).Log("msg", "Skipped queries because there's no valid time range to query after honoring configured max query age", "min_valid_time", records.queryMinTime, "max_valid_time", records.queryMaxTime, "max_query_age", t.cfg.MaxQueryAge)
		return nil, nil, errors.New("no valid time range to query after honoring configured max query age")
	}

	// Compute the latest queriable timestamp
	// records.queryMaxTime shouldn't be after now but we want to be sure of it
	adjustedQueryMaxTime := minTime(records.queryMaxTime, now)

	// Last 24h range query.
	ranges = append(ranges, [2]time.Time{
		maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour)),
		adjustedQueryMaxTime,
	})

	// Instant query at the most recent point.
	instants = append(instants, adjustedQueryMaxTime)

	// Instant query at 24h ago.
	instant24hAgo := maxTime(adjustedQueryMinTime, now.Add(-24*time.Hour))
	if !instant24hAgo.Equal(adjustedQueryMaxTime) {
		instants = append(instants, instant24hAgo)
	}

	// Random instant query.
	randInstant := randTime(adjustedQueryMinTime, adjustedQueryMaxTime)
	instants = append(instants, randInstant)

	return ranges, instants, nil
}
