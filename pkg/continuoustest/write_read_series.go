// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"context"
	"flag"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	writeInterval = 20 * time.Second
	metricName    = "mimir_continuous_test_sine_wave"
)

type WriteReadSeriesTestConfig struct {
	NumSeries int
}

func (cfg *WriteReadSeriesTestConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.NumSeries, "tests.write-read-series-test.num-series", 10000, "Number of series used for the test.")
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
func (t *WriteReadSeriesTest) Init() error {
	// TODO Here we should populate lastWrittenTimestamp, queryMinTime, queryMaxTime after querying Mimir to get data previously written.
	return nil
}

// Run implements Test.
func (t *WriteReadSeriesTest) Run(ctx context.Context, now time.Time) {
	// Write series for each expected timestamp until now.
	for timestamp := t.nextWriteTimestamp(now); !timestamp.After(now); timestamp = t.nextWriteTimestamp(now) {
		statusCode, err := t.client.WriteSeries(ctx, generateSineWaveSeries(metricName, timestamp, t.cfg.NumSeries))

		t.metrics.writesTotal.Inc()
		if statusCode/100 != 2 {
			t.metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode)).Inc()
			level.Warn(t.logger).Log("msg", "Failed to remote write series", "num_series", t.cfg.NumSeries, "timestamp", timestamp.String(), "status_code", statusCode, "err", err)
		} else {
			level.Debug(t.logger).Log("msg", "Remote write series succeeded", "num_series", t.cfg.NumSeries, "timestamp", timestamp.String())
		}

		// If the write request failed because of a 4xx error, retrying the request isn't expected to succeed.
		// The series may have been not written at all or partially written (eg. we hit some limit).
		// We keep writing the next interval, but we reset the query timestamp because we can't reliably
		// assert on query results due to possible gaps.
		if statusCode/100 == 4 {
			t.lastWrittenTimestamp = timestamp

			// TODO The following reset is related to the read path (not implemented yet), but was added to ensure we don't forget about it.
			t.queryMinTime = time.Time{}
			t.queryMaxTime = time.Time{}
			continue
		}

		// If the write request failed because of a network or 5xx error, we'll retry to write series
		// in the next test run.
		if statusCode/100 != 2 || err != nil {
			break
		}

		// The write request succeeded.
		t.lastWrittenTimestamp = timestamp
	}

	// TODO Here we should query the written data and assert on correctness.
}

func (t *WriteReadSeriesTest) nextWriteTimestamp(now time.Time) time.Time {
	if t.lastWrittenTimestamp.IsZero() {
		return alignTimestampToInterval(now, writeInterval)
	}

	return t.lastWrittenTimestamp.Add(writeInterval)
}
