// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/web/api/v1/api.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

// This tool expects a JSON-formatted Loki export from queries like the following:
// {namespace="<namespace>",container="query-frontend"} | logfmt | msg=`query stats` | path=`/prometheus/api/v1/query_range`
// {namespace="<namespace>",container="query-frontend"} | logfmt | msg=`query stats` | path=`/prometheus/api/v1/query`

func main() {
	instantQueryLogs := flag.String("instant-query-logs", "", "path to JSON file containing query logs for instant queries")
	rangeQueryLogs := flag.String("range-query-logs", "", "path to JSON file containing query logs for range queries")
	queryIngestersWithin := flag.Duration("query-ingesters-within", 13*time.Hour, "maximum lookback beyond which queries are not sent to ingester")
	queryStoreAfter := flag.Duration("query-store-after", 12*time.Hour, "the time after which a metric should be queried from storage and not just ingesters")
	flag.Parse()

	if err := run(*instantQueryLogs, *rangeQueryLogs, *queryIngestersWithin, *queryStoreAfter); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(instantQueryLogs string, rangeQueryLogs string, queryIngestersWithin time.Duration, queryStoreAfter time.Duration) error {
	if instantQueryLogs == "" && rangeQueryLogs == "" {
		return errors.New("must provide at least one of -instant-query-logs or -range-query-logs")
	}

	instantQueriesStats, err := loadInstantQueriesStats(instantQueryLogs, queryIngestersWithin, queryStoreAfter)
	if err != nil {
		return err
	}

	rangeQueriesStats, err := loadRangeQueriesStats(rangeQueryLogs, queryIngestersWithin, queryStoreAfter)
	if err != nil {
		return err
	}

	allStats := append(instantQueriesStats, rangeQueriesStats...)
	w := csv.NewWriter(os.Stdout)
	columnNames := []string{"Query", "Query type", "Query time range (seconds)", "Would hit ingester", "Would hit store gateway", "User agent", "Response time (ms)", "Fetched series count", "Fetched chunks size (bytes)", "Fetched chunks count"}
	if err := w.Write(columnNames); err != nil {
		return err
	}

	for _, s := range allStats {
		columns := []string{
			s.query,
			s.queryType,
			strconv.FormatFloat(s.queryRangeSeconds, 'f', 2, 64),
			strconv.FormatBool(s.wouldHitIngester),
			strconv.FormatBool(s.wouldHitStoreGateway),
			s.userAgent,
			strconv.FormatInt(s.responseTimeMilliseconds, 10),
			strconv.FormatInt(s.fetchedSeriesCount, 10),
			strconv.FormatInt(s.fetchedChunkBytes, 10),
			strconv.FormatInt(s.fetchedChunkCount, 10),
		}

		if err := w.Write(columns); err != nil {
			return err
		}
	}

	w.Flush()

	return w.Error()
}

func loadInstantQueriesStats(logsPath string, queryIngestersWithin time.Duration, queryStoreAfter time.Duration) ([]queryStats, error) {
	if logsPath == "" {
		return nil, nil
	}

	logsBytes, err := os.ReadFile(logsPath)
	if err != nil {
		return nil, fmt.Errorf("could not read file %v: %w", logsPath, err)
	}

	var logLines []instantQueryLogLine
	if err := json.Unmarshal(logsBytes, &logLines); err != nil {
		return nil, fmt.Errorf("could not unmarshal JSON from %v: %w", logsPath, err)
	}

	stats := make([]queryStats, len(logLines))

	for i, line := range logLines {
		stats[i] = queryStats{
			query:                    line.Fields.Query,
			queryType:                "instant",
			queryRangeSeconds:        0,
			wouldHitIngester:         line.Fields.QueryTime.After(line.Fields.ExecutionTimestamp.Add(-queryIngestersWithin)),
			wouldHitStoreGateway:     line.Fields.QueryTime.Before(line.Fields.ExecutionTimestamp.Add(-queryStoreAfter)),
			userAgent:                line.Fields.UserAgent,
			responseTimeMilliseconds: line.Fields.ResponseTime.Milliseconds(),
			fetchedSeriesCount:       line.Fields.FetchedSeriesCount,
			fetchedChunkBytes:        line.Fields.FetchedChunkBytes,
			fetchedChunkCount:        line.Fields.FetchedChunkCount,
		}
	}

	return stats, nil
}

func loadRangeQueriesStats(logsPath string, queryIngestersWithin time.Duration, queryStoreAfter time.Duration) ([]queryStats, error) {
	if logsPath == "" {
		return nil, nil
	}

	logsBytes, err := os.ReadFile(logsPath)
	if err != nil {
		return nil, fmt.Errorf("could not read file %v: %w", logsPath, err)
	}

	var logLines []rangeQueryLogLine
	if err := json.Unmarshal(logsBytes, &logLines); err != nil {
		return nil, fmt.Errorf("could not unmarshal JSON from %v: %w", logsPath, err)
	}

	stats := make([]queryStats, len(logLines))

	for i, line := range logLines {
		stats[i] = queryStats{
			query:                    line.Fields.Query,
			queryType:                "range",
			queryRangeSeconds:        line.Fields.QueryTimeRangeEnd.Sub(line.Fields.QueryTimeRangeStart.Time).Seconds(),
			wouldHitIngester:         line.Fields.QueryTimeRangeEnd.After(line.Fields.ExecutionTimestamp.Add(-queryIngestersWithin)),
			wouldHitStoreGateway:     line.Fields.QueryTimeRangeStart.Before(line.Fields.ExecutionTimestamp.Add(-queryStoreAfter)),
			userAgent:                line.Fields.UserAgent,
			responseTimeMilliseconds: line.Fields.ResponseTime.Milliseconds(),
			fetchedSeriesCount:       line.Fields.FetchedSeriesCount,
			fetchedChunkBytes:        line.Fields.FetchedChunkBytes,
			fetchedChunkCount:        line.Fields.FetchedChunkCount,
		}
	}

	return stats, nil
}

type instantQueryLogLine struct {
	Fields struct {
		ExecutionTimestamp time.Time      `json:"ts"`
		Query              string         `json:"param_query"`
		QueryTime          prometheusTime `json:"param_time"`
		UserAgent          string         `json:"user_agent"`
		ResponseTime       jsonDuration   `json:"response_time"`
		FetchedSeriesCount int64          `json:"fetched_series_count,string"`
		FetchedChunkBytes  int64          `json:"fetched_chunk_bytes,string"`
		FetchedChunkCount  int64          `json:"fetched_chunk_count,string"`
	} `json:"fields"`
}

type rangeQueryLogLine struct {
	Fields struct {
		ExecutionTimestamp  time.Time      `json:"ts"`
		Query               string         `json:"param_query"`
		QueryTimeRangeStart prometheusTime `json:"param_start"`
		QueryTimeRangeEnd   prometheusTime `json:"param_end"`
		UserAgent           string         `json:"user_agent"`
		ResponseTime        jsonDuration   `json:"response_time"`
		FetchedSeriesCount  int64          `json:"fetched_series_count,string"`
		FetchedChunkBytes   int64          `json:"fetched_chunk_bytes,string"`
		FetchedChunkCount   int64          `json:"fetched_chunk_count,string"`
	} `json:"fields"`
}

type queryStats struct {
	query                    string
	queryType                string  // range or instant
	queryRangeSeconds        float64 // 0 for instant queries
	wouldHitIngester         bool
	wouldHitStoreGateway     bool
	userAgent                string
	responseTimeMilliseconds int64
	fetchedSeriesCount       int64
	fetchedChunkBytes        int64
	fetchedChunkCount        int64
}

type prometheusTime struct {
	time.Time
}

func (t *prometheusTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := parsePrometheusTime(s)
	t.Time = parsed

	return err
}

type jsonDuration struct {
	time.Duration
}

func (d *jsonDuration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := time.ParseDuration(s)
	d.Duration = parsed

	return err
}

// This is taken directly from Prometheus' web/api/v1 package.
var (
	minTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	maxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = minTime.Format(time.RFC3339Nano)
	maxTimeFormatted = maxTime.Format(time.RFC3339Nano)
)

func parsePrometheusTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}
