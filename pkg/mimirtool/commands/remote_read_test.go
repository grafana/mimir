// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
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

func TestTimeSeriesIterator(t *testing.T) {

	for _, tc := range []struct {
		name               string
		seriesSet          storage.SeriesSet
		expectedLabels     []string
		expectedTimestamps []int64
		expectedValues     []float64
	}{
		{
			name:      "empty time series",
			seriesSet: storage.EmptySeriesSet(),
		},
		{
			name:      "simple",
			seriesSet: NewMockSeriesSet(storage.MockSeries([]int64{1000, 2000}, []float64{1.23, 1.24}, []string{"__name__", "up"})),
			expectedLabels: []string{
				`{__name__="up"}`,
				`{__name__="up"}`,
			},
			expectedTimestamps: []int64{
				1000,
				2000,
			},
			expectedValues: []float64{
				1.23,
				1.24,
			},
		},
		{
			name: "edge-cases",
			seriesSet: NewMockSeriesSet(
				storage.MockSeries([]int64{1000, 2000}, []float64{1.23, 1.24}, []string{}),
				storage.MockSeries([]int64{1050}, []float64{2.34}, []string{"__name__", "upper"}),
				storage.MockSeries([]int64{}, []float64{}, []string{"__name__", "uppest"}),
			),
			expectedLabels: []string{
				`{}`,
				`{}`,
				`{__name__="upper"}`,
			},
			expectedTimestamps: []int64{
				1000,
				2000,
				1050,
			},
			expectedValues: []float64{
				1.23,
				1.24,
				2.34,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {

			iter := newTimeSeriesIterator(tc.seriesSet)

			var (
				labels     []string
				timestamps []int64
				values     []float64
			)

			for {
				err := iter.Next()
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					assert.NoError(t, err, "unexpected error")
					break
				}

				labels = append(labels, iter.Labels().String())
				ts, v, _, _ := iter.Sample()
				timestamps = append(timestamps, ts)
				values = append(values, v)
			}

			assert.Equal(t, tc.expectedLabels, labels)
			assert.Equal(t, tc.expectedTimestamps, timestamps)
			assert.Equal(t, tc.expectedValues, values)
		})
	}

}

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
	end := start + (inc * int64(samplesCount))

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

	iterator := func() backfill.Iterator {
		return newTimeSeriesIterator(NewMockSeriesSet(series...))
	}
	err := backfill.CreateBlocks(iterator, start, end, maxSamplesPerBlock, t.TempDir(), true, io.Discard)
	assert.NoError(t, err)
}

type exportTestCase struct {
	queryFrom time.Time
	queryTo   time.Time
	series    []*prompb.TimeSeries
}

func TestExport(t *testing.T) {
	start := time.Date(2025, 5, 1, 0, 5, 0, 0, time.UTC)
	metricName := "some_metric"

	testCases := map[string]exportTestCase{
		"data entirely within single block": {
			queryFrom: start,
			queryTo:   start.Add(5 * time.Minute),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start), Value: 0.0},
						{Timestamp: timestamp.FromTime(start.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start), Value: 1.0},
						{Timestamp: timestamp.FromTime(start.Add(time.Minute)), Value: 1.1},
					},
				},
			},
		},
		"data entirely within second half of single block": {
			queryFrom: start,
			queryTo:   start.Add(5 * time.Minute),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "0"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start.Add(time.Hour)), Value: 0.0},
						{Timestamp: timestamp.FromTime(start.Add(time.Hour).Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "idx", Value: "1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start.Add(time.Hour)), Value: 1.0},
						{Timestamp: timestamp.FromTime(start.Add(time.Hour).Add(time.Minute)), Value: 1.1},
					},
				},
			},
		},
		"data over multiple blocks": {
			queryFrom: start,
			queryTo:   start.Add(5 * time.Hour),
			series: []*prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "entirely in first block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start), Value: 0.0},
						{Timestamp: timestamp.FromTime(start.Add(time.Minute)), Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "entirely in second block"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start.Add(2 * time.Hour)), Value: 1.0},
						{Timestamp: timestamp.FromTime(start.Add(2 * time.Hour).Add(time.Minute)), Value: 1.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "across multiple blocks"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start), Value: 2.0},
						{Timestamp: timestamp.FromTime(start.Add(2 * time.Hour)), Value: 2.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: labels.MetricName, Value: metricName},
						{Name: "case", Value: "across multiple blocks, with sample on block boundary"},
					},
					Samples: []prompb.Sample{
						{Timestamp: timestamp.FromTime(start), Value: 3.0},
						{Timestamp: timestamp.FromTime(start.Add(55 * time.Minute)), Value: 3.1},
						{Timestamp: timestamp.FromTime(start.Add(2 * time.Hour)), Value: 3.2},
					},
				},
			},
		},
	}

	for name, sendChunks := range map[string]bool{"chunks": true, "samples": false} {
		t.Run(name, func(t *testing.T) {

			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if sendChunks {
							serveChunks(t, testCase, w)
						} else {
							serveSamples(t, testCase, w)
						}
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
					}

					require.NoError(t, c.export(nil), "expected export to complete without error")

					// Check that the data was written correctly.
					db, err := tsdb.Open(tsdbPath, nil, nil, nil, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, db.Close()) })

					querier, err := db.Querier(timestamp.FromTime(testCase.queryFrom), timestamp.FromTime(testCase.queryTo))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, querier.Close()) })

					ss := querier.Select(context.Background(), true, nil, labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, metricName))
					writtenSeries := []*prompb.TimeSeries{}
					for ss.Next() {
						series := ss.At()
						samples := []prompb.Sample{}
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
					}

					require.NoError(t, ss.Err())
					require.ElementsMatch(t, testCase.series, writtenSeries, "expected all samples to be written to TSDB")
				})
			}
		})
	}
}

func serveSamples(t *testing.T, testCase exportTestCase, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-protobuf")

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: testCase.series,
			},
		},
	}
	msg, err := proto.Marshal(resp)
	require.NoError(t, err)

	_, err = w.Write(snappy.Encode(nil, msg))
	require.NoError(t, err)
}

func serveChunks(t *testing.T, testCase exportTestCase, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse")

	flusher, ok := w.(http.Flusher)
	require.True(t, ok)

	chunkedWriter := remote.NewChunkedWriter(w, flusher)

	for _, s := range testCase.series {
		chunk := chunkenc.NewXORChunk()
		a, err := chunk.Appender()
		require.NoError(t, err)

		minTime := s.Samples[0].Timestamp
		maxTime := s.Samples[0].Timestamp

		for _, sample := range s.Samples {
			a.Append(sample.Timestamp, sample.Value)
			minTime = min(minTime, sample.Timestamp)
			maxTime = max(maxTime, sample.Timestamp)
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
