package continuoustest

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

// findPreviouslyWrittenTimeRange finds the most recent time range for which an expected set of continuous-test series is well-formed.
// it returns the starting and ending point of that well-formed time range.
func findPreviouslyWrittenTimeRange(ctx context.Context, now time.Time, step time.Duration, maxQueryAge time.Duration, metricName string, numSeries int, querySum querySumFunc, generateValue generateValueFunc, generateSampleHistogram generateSampleHistogramFunc, skipTimestamp skipTimestampFunc, client MimirClient, logger log.Logger) (from, to time.Time) {
	end := alignTimestampToInterval(now, step)

	var samples []model.SamplePair
	var histograms []model.SampleHistogramPair
	query := querySum(metricName)

	for {
		start := alignTimestampToInterval(maxTime(now.Add(-maxQueryAge), end.Add(-24*time.Hour).Add(step)), step)
		if !start.Before(end) {
			// We've hit the max query age, so we'll keep the last computed valid time range (if any).
			return
		}

		logger := log.With(logger, "query", query, "start", start, "end", end, "step", step, "metric_name", metricName)
		level.Debug(logger).Log("msg", "Executing query to find previously written samples")

		matrix, err := client.QueryRange(ctx, query, start, end, step, WithResultsCacheEnabled(false))
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
			level.Error(logger).Log("msg", "The range query used to find previously written samples returned either both floats and histograms or neither")
			return
		}
		lastMatchingIdx, err := verifySamplesSum(fullMatrix, numSeries, step, generateValue, generateSampleHistogram, skipTimestamp)
		if lastMatchingIdx == -1 {
			level.Warn(logger).Log("msg", "The range query used to find previously written samples returned no timestamps where the returned value matched the expected value", "err", err)
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

		level.Info(logger).Log("msg", "Found previously written samples", "from", from, "to", to, "issue_with_earlier_data", err)

		// If the last matching sample is not the one at the beginning of the queried time range
		// then it means we've found the oldest previously written sample and we can stop searching it.
		if lastMatchingIdx != 0 || (!useHistograms && !samples[0].Timestamp.Time().Equal(start)) || (useHistograms && !histograms[0].Timestamp.Time().Equal(start)) {
			return
		}
	}
}

func writeSamples(ctx context.Context, typeLabel string, timestamp time.Time, series []prompb.TimeSeries, metricName string, numSeries int, metadata []prompb.MetricMetadata, records *MetricHistory, client MimirClient, metrics *TestMetrics, logger log.Logger) error {
	sp, ctx := spanlogger.New(ctx, logger, tracer, "writeSamples")
	defer sp.Finish()
	logger = log.With(sp, "timestamp", timestamp.UnixMilli(), "num_series", numSeries, "metric_name", metricName, "protocol", client.Protocol())

	start := time.Now()
	statusCode, err := client.WriteSeries(ctx, series, metadata)
	metrics.writesLatency.WithLabelValues(typeLabel).Observe(time.Since(start).Seconds())
	metrics.writesTotal.WithLabelValues(typeLabel).Inc()

	if statusCode/100 != 2 {
		metrics.writesFailedTotal.WithLabelValues(strconv.Itoa(statusCode), typeLabel).Inc()
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
