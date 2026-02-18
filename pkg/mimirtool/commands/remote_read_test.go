// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

// parseSelectors converts string selectors to matchers for testing
func parseSelectors(t *testing.T, selectors ...string) [][]*labels.Matcher {
	var result [][]*labels.Matcher
	for _, selector := range selectors {
		matchers, err := config.CreateParser().ParseMetricSelector(selector)
		require.NoError(t, err)
		result = append(result, matchers)
	}
	return result
}

type exportTestCase struct {
	queryFrom      time.Time
	queryTo        time.Time
	series         []*prompb.TimeSeries
	expectedBlocks int
}

func TestExport(t *testing.T) {
	alignedToBlockStart := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	offsetFromBlockStart := time.Date(2025, 5, 1, 0, 5, 0, 0, time.UTC)
	metricName := "some_metric"

	testCases := map[string]exportTestCase{
		"data entirely within single block": {
			queryFrom: offsetFromBlockStart,
			queryTo:   offsetFromBlockStart.Add(5 * time.Minute),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 1.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Minute)), Value: 1.1},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(5 * time.Minute)), Value: 1.2}, // Check that we correctly return samples on the end of the query time range.
					},
				},
			},
			expectedBlocks: 1,
		},
		"data entirely within second half of single block": {
			queryFrom: offsetFromBlockStart,
			queryTo:   offsetFromBlockStart.Add(time.Hour).Add(5 * time.Minute),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour)), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour).Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour)), Value: 1.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour).Add(time.Minute)), Value: 1.1},
					},
				},
			},
			expectedBlocks: 1,
		},
		"data at extreme ends of a block, query range not including start of next block": {
			queryFrom: alignedToBlockStart,
			queryTo:   alignedToBlockStart.Add(2*time.Hour - time.Millisecond),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(2*time.Hour - time.Millisecond)), Value: 0.1},
					},
				},
			},
			expectedBlocks: 1,
		},
		"data at extreme ends of a block, query range including start of next block": {
			queryFrom: alignedToBlockStart,
			queryTo:   alignedToBlockStart.Add(2 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(2*time.Hour - time.Millisecond)), Value: 0.1},
					},
				},
			},
			expectedBlocks: 1,
		},
		"query range and data aligned to end of block": {
			queryFrom: alignedToBlockStart,
			queryTo:   alignedToBlockStart.Add(2 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(2 * time.Hour)), Value: 0.1},
					},
				},
			},
			expectedBlocks: 2,
		},
		"empty block": {
			queryFrom: alignedToBlockStart,
			queryTo:   alignedToBlockStart.Add(6 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(5 * time.Minute)), Value: 0.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(4*time.Hour + 5*time.Minute)), Value: 0.1},
					},
				},
			},
			expectedBlocks: 2,
		},
		"data over multiple blocks": {
			queryFrom: offsetFromBlockStart,
			queryTo:   offsetFromBlockStart.Add(8 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "entirely in first block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "entirely in second block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour)), Value: 1.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour).Add(time.Minute)), Value: 1.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "entirely in last block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(7 * time.Hour)), Value: 2.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(7 * time.Hour).Add(time.Minute)), Value: 2.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "across multiple blocks"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 3.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour)), Value: 3.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "across multiple blocks, with sample on block boundary"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 4.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour).Add(55 * time.Minute)), Value: 4.1},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour)), Value: 4.2},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricName},
						{Name: "case", Value: "across multiple blocks, from first to last"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 5.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(5 * time.Minute)), Value: 5.1},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(3 * time.Hour).Add(5 * time.Minute)), Value: 5.1},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(7 * time.Hour).Add(5 * time.Minute)), Value: 5.2},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(8 * time.Hour)), Value: 5.3},
					},
				},
			},
			expectedBlocks: 4,
		},
		"large amount of data in single block": {
			queryFrom:      alignedToBlockStart,
			queryTo:        alignedToBlockStart.Add(2 * time.Hour),
			series:         generateLargeDataset(alignedToBlockStart, metricName),
			expectedBlocks: 1,
		},
	}

	for name, sendChunks := range map[string]bool{"chunks": true, "samples": false} {
		t.Run(name, func(t *testing.T) {

			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						serve(t, testCase, sendChunks, r, w)
					}))

					t.Cleanup(server.Close)

					tsdbPath := t.TempDir()

					c := &RemoteReadCommand{
						address:       server.URL,
						tsdbPath:      tsdbPath,
						selectors:     parseSelectors(t, metricName),
						from:          testCase.queryFrom.Format(time.RFC3339Nano),
						to:            testCase.queryTo.Format(time.RFC3339Nano),
						readTimeout:   30 * time.Second,
						readSizeLimit: DefaultChunkedReadLimit,
						blockDuration: time.Duration(tsdb.DefaultBlockDuration) * time.Millisecond,
						logger:        log.NewNopLogger(),
					}

					require.NoError(t, c.export(nil), "expected export to complete without error")

					// Check that the data was written correctly.
					db, err := tsdb.Open(tsdbPath, nil, nil, nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close()) })

					writtenSeries, totalExpectedSamples := queryAllSamples(t, db, metricName)
					require.ElementsMatch(t, testCase.series, writtenSeries, "expected all samples to be written to TSDB")

					blocks := db.Blocks()
					require.Len(t, blocks, testCase.expectedBlocks, "expected number of blocks to be created")

					require.Equal(t, totalExpectedSamples, int(sampleCountInAllBlocks(db)), "number of samples in blocks does not match the expected number of samples from source data, were some samples written in multiple blocks?")
				})
			}
		})
	}
}

func generateLargeDataset(from time.Time, metricName string) []*prompb.TimeSeries {
	startT := from.UnixMilli()
	step := time.Millisecond.Milliseconds()

	series := make([]*prompb.TimeSeries, 0, 100)

	for seriesIdx := range cap(series) {
		samples := make([]prompb.Sample, 0, 140)

		for sampleIdx := range cap(samples) {
			samples = append(samples, prompb.Sample{
				Timestamp: startT + (step * int64(sampleIdx)),
				Value:     float64(sampleIdx),
			})
		}

		series = append(series, &prompb.TimeSeries{
			Labels: []prompb.Label{
				{Name: model.MetricNameLabel, Value: metricName},
				{Name: "idx", Value: strconv.Itoa(seriesIdx)},
			},
			Samples: samples,
		})
	}

	return series
}

func serve(t *testing.T, testCase exportTestCase, sendChunks bool, r *http.Request, w http.ResponseWriter) {
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)

	msg := &prompb.ReadRequest{}
	require.NoError(t, proto.Unmarshal(decompressed, msg))

	require.Len(t, msg.GetQueries(), 1)
	startT := msg.GetQueries()[0].GetStartTimestampMs()
	endT := msg.GetQueries()[0].GetEndTimestampMs()

	start := timestamp.Time(startT)
	end := timestamp.Time(endT)

	require.Truef(t, start.Equal(testCase.queryFrom) || start.After(testCase.queryFrom), "query request starts at %v, but query range is from %v to %v", start, testCase.queryFrom, testCase.queryTo)
	require.Truef(t, end.Equal(testCase.queryTo) || end.Before(testCase.queryTo), "query request ends at %v, but query range is from %v to %v", end, testCase.queryFrom, testCase.queryTo)

	if sendChunks {
		serveChunks(t, testCase, startT, endT, w)
	} else {
		serveSamples(t, testCase, startT, endT, w)
	}
}

func serveSamples(t *testing.T, testCase exportTestCase, startT, endT int64, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-protobuf")

	filteredSeries := []*prompb.TimeSeries{}

	for _, series := range testCase.series {
		samples := []prompb.Sample{}

		for _, sample := range series.Samples {
			if sample.Timestamp < startT || sample.Timestamp > endT {
				continue
			}

			samples = append(samples, sample)
		}

		if len(samples) == 0 {
			continue
		}

		filteredSeries = append(filteredSeries, &prompb.TimeSeries{
			Labels:  series.Labels,
			Samples: samples,
		})
	}

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: filteredSeries,
			},
		},
	}
	msg, err := proto.Marshal(resp)
	require.NoError(t, err)

	_, err = w.Write(snappy.Encode(nil, msg))
	require.NoError(t, err)
}

func serveChunks(t *testing.T, testCase exportTestCase, startT, endT int64, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	flusher, ok := w.(http.Flusher)
	require.True(t, ok)

	chunkedWriter := remote.NewChunkedWriter(w, flusher)

	for _, s := range testCase.series {
		chunk := chunkenc.NewXORChunk()
		a, err := chunk.Appender()
		require.NoError(t, err)

		sampleCount := 0
		minTime := int64(math.MaxInt64)
		maxTime := int64(math.MinInt64)

		for _, sample := range s.Samples {
			if sample.Timestamp < startT || sample.Timestamp > endT {
				continue
			}

			sampleCount++
			minTime = min(minTime, sample.Timestamp)
			maxTime = max(maxTime, sample.Timestamp)

			a.Append(0, sample.Timestamp, sample.Value)
		}

		if sampleCount == 0 {
			continue
		}

		resp := prompb.ChunkedReadResponse{
			ChunkedSeries: []*prompb.ChunkedSeries{
				{
					Labels: s.Labels,
					Chunks: []prompb.Chunk{
						{
							MinTimeMs: minTime,
							MaxTimeMs: maxTime,
							Type:      prompb.Chunk_XOR,
							Data:      chunk.Bytes(),
						},
					},
				},
			},
		}

		msg, err := proto.Marshal(&resp)
		require.NoError(t, err)
		_, err = chunkedWriter.Write(msg)
		require.NoError(t, err)
	}
}

func queryAllSamples(t *testing.T, db *tsdb.DB, metricName string) ([]*prompb.TimeSeries, int) {
	querier, err := db.Querier(0, math.MaxInt64) // Query all available data, to ensure there's no data outside the expected range.
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, querier.Close()) })

	ss := querier.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName))
	var writtenSeries []*prompb.TimeSeries
	totalSamples := 0

	for ss.Next() {
		series := ss.At()
		var samples []prompb.Sample
		it := series.Iterator(nil)

		for it.Next() != chunkenc.ValNone {
			t, v := it.At()
			samples = append(samples, prompb.Sample{
				Timestamp: t,
				Value:     v,
			})
		}

		require.NoError(t, it.Err())

		writtenSeries = append(writtenSeries, &prompb.TimeSeries{
			Labels:  prompb.FromLabels(series.Labels(), nil),
			Samples: samples,
		})

		totalSamples += len(samples)
	}

	require.NoError(t, ss.Err())

	return writtenSeries, totalSamples
}

func sampleCountInAllBlocks(db *tsdb.DB) uint64 {
	total := uint64(0)
	for _, b := range db.Blocks() {
		total += b.Meta().Stats.NumSamples
	}

	return total
}

func TestRemoteReadCommand_prepare(t *testing.T) {
	tests := []struct {
		name        string
		selectors   []string
		from        string
		to          string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "single selector",
			selectors:   []string{"up"},
			from:        "2023-01-01T00:00:00Z",
			to:          "2023-01-01T01:00:00Z",
			expectError: false,
		},
		{
			name:        "multiple selectors",
			selectors:   []string{"up", "go_memstats_alloc_bytes", "prometheus_build_info"},
			from:        "2023-01-01T00:00:00Z",
			to:          "2023-01-01T01:00:00Z",
			expectError: false,
		},
		{
			name:        "empty selectors",
			selectors:   []string{},
			from:        "2023-01-01T00:00:00Z",
			to:          "2023-01-01T01:00:00Z",
			expectError: true,
			errorMsg:    "at least one selector must be specified",
		},
		{
			name:        "invalid from time",
			selectors:   []string{"up"},
			from:        "invalid-time",
			to:          "2023-01-01T01:00:00Z",
			expectError: true,
			errorMsg:    "error parsing from",
		},
		{
			name:        "invalid to time",
			selectors:   []string{"up"},
			from:        "2023-01-01T00:00:00Z",
			to:          "invalid-time",
			expectError: true,
			errorMsg:    "error parsing to",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &RemoteReadCommand{
				address:        "invalid.com", // we only test validation
				remoteReadPath: "/api/v1/read",
				selectors:      parseSelectors(t, tt.selectors...),
				from:           tt.from,
				to:             tt.to,
				readTimeout:    30 * time.Second,
				useChunks:      true,
				logger:         log.NewNopLogger(),
			}

			_, _, _, err := cmd.parseArgsAndPrepareClient()
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSelectorFlag(t *testing.T) {
	tests := []struct {
		name           string
		selectors      []string
		expectError    bool
		expectedString string
	}{
		{
			name:           "single simple selector",
			selectors:      []string{"up"},
			expectError:    false,
			expectedString: "{__name__=\"up\"}",
		},
		{
			name:           "multiple selectors",
			selectors:      []string{"up", "go_memstats_alloc_bytes"},
			expectError:    false,
			expectedString: "{__name__=\"up\"},{__name__=\"go_memstats_alloc_bytes\"}",
		},
		{
			name:           "complex selector",
			selectors:      []string{`{__name__="http_requests_total", job="prometheus"}`},
			expectError:    false,
			expectedString: `{__name__="http_requests_total",job="prometheus"}`,
		},
		{
			name:        "invalid selector",
			selectors:   []string{"invalid{selector"},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var selectors [][]*labels.Matcher
			flag := &selectorFlag{selectors: &selectors}

			var err error
			for _, selector := range tt.selectors {
				err = flag.Set(selector)
				if tt.expectError {
					require.Error(t, err)
					assert.Contains(t, err.Error(), "error parsing selector")
					return
				}
				require.NoError(t, err)
			}

			if !tt.expectError {
				assert.Equal(t, len(tt.selectors), len(selectors))
				assert.Equal(t, tt.expectedString, flag.String())
			}
		})
	}
}

func TestParseArgsAndPrepareClient(t *testing.T) {
	alignedToBlockStart := time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC)
	metricNames := []string{"metric_one", "metric_two"}

	testCases := map[string]struct {
		selectors      []string
		expectedSeries []*prompb.TimeSeries
	}{
		"single selector": {
			selectors: []string{metricNames[0]},
			expectedSeries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricNames[0]},
						{Name: "job", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 1.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(time.Minute)), Value: 2.0},
					},
				},
			},
		},
		"multiple selectors": {
			selectors: metricNames,
			expectedSeries: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricNames[0]},
						{Name: "job", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 1.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(time.Minute)), Value: 2.0},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: model.MetricNameLabel, Value: metricNames[1]},
						{Name: "instance", Value: "localhost:8080"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(alignedToBlockStart), Value: 10.0},
						{Timestamp: timestamp.FromTime(alignedToBlockStart.Add(2 * time.Minute)), Value: 20.0},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// Set up test server that handles multiple queries
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				serveMultiQuery(t, exportTestCase{
					queryFrom: alignedToBlockStart,
					queryTo:   alignedToBlockStart.Add(time.Hour),
					series:    testCase.expectedSeries,
				}, false, r, w)
			}))
			t.Cleanup(server.Close)

			// Create RemoteReadCommand with test configuration
			c := &RemoteReadCommand{
				address:        server.URL,
				remoteReadPath: "/api/v1/read",
				selectors:      parseSelectors(t, testCase.selectors...),
				from:           alignedToBlockStart.Format(time.RFC3339Nano),
				to:             alignedToBlockStart.Add(time.Hour).Format(time.RFC3339Nano),
				readTimeout:    30 * time.Second,
				logger:         log.NewNopLogger(),
			}

			// Test parseArgsAndPrepareClient
			query, from, to, err := c.parseArgsAndPrepareClient()
			require.NoError(t, err, "parseArgsAndPrepareClient should succeed")

			// Verify returned times
			assert.Equal(t, alignedToBlockStart, from, "from time should match")
			assert.Equal(t, alignedToBlockStart.Add(time.Hour), to, "to time should match")

			// Test the returned query function
			ctx := context.Background()
			queryFrom := alignedToBlockStart
			queryTo := alignedToBlockStart.Add(time.Hour)

			seriesSet, err := query(ctx, queryFrom, queryTo)
			require.NoError(t, err, "query function should succeed")

			// Collect all series from the result
			var actualSeries []*prompb.TimeSeries
			var it chunkenc.Iterator

			for seriesSet.Next() {
				series := seriesSet.At()
				var samples []prompb.Sample

				it = series.Iterator(it)
				for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
					switch vt {
					case chunkenc.ValFloat:
						ts, v := it.At()
						samples = append(samples, prompb.Sample{
							Timestamp: ts,
							Value:     v,
						})
					}
				}
				require.NoError(t, it.Err())

				actualSeries = append(actualSeries, &prompb.TimeSeries{
					Labels:  prompb.FromLabels(series.Labels(), nil),
					Samples: samples,
				})
			}
			require.NoError(t, seriesSet.Err())

			// Verify we got the expected series back
			require.ElementsMatch(t, testCase.expectedSeries, actualSeries,
				"query function should return expected series for %d selectors", len(testCase.selectors))
		})
	}
}

func serveMultiQuery(t *testing.T, testCase exportTestCase, sendChunks bool, r *http.Request, w http.ResponseWriter) {
	body, err := io.ReadAll(r.Body)
	require.NoError(t, err)

	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)

	msg := &prompb.ReadRequest{}
	require.NoError(t, proto.Unmarshal(decompressed, msg))

	queries := msg.GetQueries()
	require.True(t, len(queries) >= 1, "expected at least one query")

	if sendChunks {
		serveMultiQueryChunks(t, testCase, queries, w)
	} else {
		serveMultiQuerySamples(t, testCase, queries, w)
	}
}

func filterSeriesForQuery(series []*prompb.TimeSeries, matchers []*prompb.LabelMatcher, startT, endT int64) []*prompb.TimeSeries {
	var filtered []*prompb.TimeSeries

	for _, s := range series {
		if seriesMatchesMatchers(s, matchers) {
			samples := []prompb.Sample{}
			for _, sample := range s.Samples {
				if sample.Timestamp >= startT && sample.Timestamp <= endT {
					samples = append(samples, sample)
				}
			}

			if len(samples) > 0 {
				filtered = append(filtered, &prompb.TimeSeries{
					Labels:  s.Labels,
					Samples: samples,
				})
			}
		}
	}
	return filtered
}

func seriesMatchesMatchers(series *prompb.TimeSeries, matchers []*prompb.LabelMatcher) bool {
	labelMap := make(map[string]string)
	for _, label := range series.Labels {
		labelMap[label.Name] = label.Value
	}

	for _, matcher := range matchers {
		labelValue, exists := labelMap[matcher.Name]
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if !exists || labelValue != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NEQ:
			if exists && labelValue == matcher.Value {
				return false
			}
		case prompb.LabelMatcher_RE:
			// For simplicity in tests, assume regex matches are exact matches
			if !exists || labelValue != matcher.Value {
				return false
			}
		case prompb.LabelMatcher_NRE:
			// For simplicity in tests, assume regex non-matches are exact non-matches
			if exists && labelValue == matcher.Value {
				return false
			}
		}
	}
	return true
}

func serveMultiQuerySamples(t *testing.T, testCase exportTestCase, queries []*prompb.Query, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-protobuf")

	var results []*prompb.QueryResult

	for _, query := range queries {
		startT := query.GetStartTimestampMs()
		endT := query.GetEndTimestampMs()
		matchers := query.GetMatchers()

		start := timestamp.Time(startT)
		end := timestamp.Time(endT)

		require.Truef(t, start.Equal(testCase.queryFrom) || start.After(testCase.queryFrom), "query request starts at %v, but query range is from %v to %v", start, testCase.queryFrom, testCase.queryTo)
		require.Truef(t, end.Equal(testCase.queryTo) || end.Before(testCase.queryTo), "query request ends at %v, but query range is from %v to %v", end, testCase.queryFrom, testCase.queryTo)

		filteredSeries := filterSeriesForQuery(testCase.series, matchers, startT, endT)
		results = append(results, &prompb.QueryResult{
			Timeseries: filteredSeries,
		})
	}

	resp := &prompb.ReadResponse{
		Results: results,
	}
	msg, err := proto.Marshal(resp)
	require.NoError(t, err)

	_, err = w.Write(snappy.Encode(nil, msg))
	require.NoError(t, err)
}

func serveMultiQueryChunks(t *testing.T, testCase exportTestCase, queries []*prompb.Query, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	flusher, ok := w.(http.Flusher)
	require.True(t, ok)

	chunkedWriter := remote.NewChunkedWriter(w, flusher)

	for _, query := range queries {
		startT := query.GetStartTimestampMs()
		endT := query.GetEndTimestampMs()
		matchers := query.GetMatchers()

		start := timestamp.Time(startT)
		end := timestamp.Time(endT)

		require.Truef(t, start.Equal(testCase.queryFrom) || start.After(testCase.queryFrom), "query request starts at %v, but query range is from %v to %v", start, testCase.queryFrom, testCase.queryTo)
		require.Truef(t, end.Equal(testCase.queryTo) || end.Before(testCase.queryTo), "query request ends at %v, but query range is from %v to %v", end, testCase.queryFrom, testCase.queryTo)

		filteredSeries := filterSeriesForQuery(testCase.series, matchers, startT, endT)

		for _, s := range filteredSeries {
			if len(s.Samples) == 0 {
				continue
			}

			chunk := chunkenc.NewXORChunk()
			a, err := chunk.Appender()
			require.NoError(t, err)

			minTime := int64(math.MaxInt64)
			maxTime := int64(math.MinInt64)

			for _, sample := range s.Samples {
				minTime = min(minTime, sample.Timestamp)
				maxTime = max(maxTime, sample.Timestamp)
				a.Append(0, sample.Timestamp, sample.Value)
			}

			resp := prompb.ChunkedReadResponse{
				ChunkedSeries: []*prompb.ChunkedSeries{
					{
						Labels: s.Labels,
						Chunks: []prompb.Chunk{
							{
								MinTimeMs: minTime,
								MaxTimeMs: maxTime,
								Type:      prompb.Chunk_XOR,
								Data:      chunk.Bytes(),
							},
						},
					},
				},
			}

			msg, err := proto.Marshal(&resp)
			require.NoError(t, err)
			_, err = chunkedWriter.Write(msg)
			require.NoError(t, err)
		}
	}
}
