// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
)

type mockSeriesSet struct {
	idx    int
	series []storage.Series

	warnings annotations.Annotations
	err      error
}

func NewMockSeriesSet(series ...storage.Series) storage.SeriesSet {
	return &mockSeriesSet{
		idx:    -1,
		series: series,
	}
}

func (m *mockSeriesSet) Next() bool {
	if m.err != nil {
		return false
	}
	m.idx++
	return m.idx < len(m.series)
}

func (m *mockSeriesSet) At() storage.Series { return m.series[m.idx] }

func (m *mockSeriesSet) Err() error { return m.err }

func (m *mockSeriesSet) Warnings() annotations.Annotations { return m.warnings }

// TestEarlyCommit writes samples of many series that don't fit into the same
// append commit. It makes sure that batching the samples into many commits
// doesn't cause the appends to advance the head block too far and make future
// appends invalid.
func TestEarlyCommit(t *testing.T) {
	maxSamplesPerBlock := 1000
	seriesCount := 100
	samplesCount := 140

	start := int64(time.Date(2023, 8, 30, 11, 42, 17, 0, time.UTC).UnixNano())
	inc := int64(time.Minute / time.Millisecond)

	samples := make([]float64, samplesCount)
	for i := 0; i < samplesCount; i++ {
		samples[i] = float64(i)
	}
	timestamps := make([]int64, samplesCount)
	for i := 0; i < samplesCount; i++ {
		timestamps[i] = start + (inc * int64(i))
	}

	series := make([]storage.Series, seriesCount)
	for i := 0; i < seriesCount; i++ {
		series[i] = storage.MockSeries(timestamps, samples, []string{"__name__", fmt.Sprintf("metric_%d", i)})
	}

	blockID, err := backfill.CreateBlock(NewMockSeriesSet(series...), maxSamplesPerBlock, t.TempDir(), time.Duration(tsdb.DefaultBlockDuration)*time.Millisecond)
	require.NoError(t, err)
	require.NotEqual(t, ulid.Zero, blockID)
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
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
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
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour)), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Hour).Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
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
						{Name: labels.MetricName, Value: metricName},
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
						{Name: labels.MetricName, Value: metricName},
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
						{Name: labels.MetricName, Value: metricName},
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
		"data over multiple blocks": {
			queryFrom: offsetFromBlockStart,
			queryTo:   offsetFromBlockStart.Add(8 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "entirely in first block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 0.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "entirely in second block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour)), Value: 1.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour).Add(time.Minute)), Value: 1.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "entirely in last block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(7 * time.Hour)), Value: 2.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(7 * time.Hour).Add(time.Minute)), Value: 2.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "across multiple blocks"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(offsetFromBlockStart), Value: 3.0},
						{Timestamp: timestamp.FromTime(offsetFromBlockStart.Add(2 * time.Hour)), Value: 3.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
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
						{Name: labels.MetricName, Value: metricName},
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
						selector:      metricName,
						from:          testCase.queryFrom.Format(time.RFC3339),
						to:            testCase.queryTo.Format(time.RFC3339),
						readTimeout:   30 * time.Second,
						readSizeLimit: DefaultChunkedReadLimit,
						blockSize:     time.Duration(tsdb.DefaultBlockDuration) * time.Millisecond,
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

			a.Append(sample.Timestamp, sample.Value)
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

	ss := querier.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName))
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
