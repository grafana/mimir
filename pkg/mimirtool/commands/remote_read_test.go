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
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
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
			name:        "invalid selector",
			selectors:   []string{"invalid{selector"},
			from:        "2023-01-01T00:00:00Z",
			to:          "2023-01-01T01:00:00Z",
			expectError: true,
			errorMsg:    "error parsing selector",
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
			// Create a mock server that returns success
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/x-protobuf")
				// Return empty response
				resp := &prompb.ReadResponse{}
				data, _ := proto.Marshal(resp)
				compressed := snappy.Encode(nil, data)
				w.Write(compressed)
			}))
			defer server.Close()

			cmd := &RemoteReadCommand{
				address:        server.URL,
				remoteReadPath: "/api/v1/read",
				selectors:      tt.selectors,
				from:           tt.from,
				to:             tt.to,
				readTimeout:    30 * time.Second,
				useChunks:      true,
			}

			_, _, _, err := cmd.prepare()
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoteReadCommand_executeMultipleQueries(t *testing.T) {
	tests := []struct {
		name             string
		responseType     string
		responseBody     func() []byte
		queriesCount     int
		expectedSeries   int
		expectError      bool
		errorMsg        string
	}{
		{
			name:         "sampled response single query",
			responseType: "application/x-protobuf",
			responseBody: func() []byte {
				resp := &prompb.ReadResponse{
					Results: []*prompb.QueryResult{
						{
							Timeseries: []*prompb.TimeSeries{
								{
									Labels: []prompb.Label{{Name: "__name__", Value: "up"}},
									Samples: []prompb.Sample{
										{Timestamp: 1000, Value: 1.0},
										{Timestamp: 2000, Value: 1.0},
									},
								},
							},
						},
					},
				}
				data, _ := proto.Marshal(resp)
				return snappy.Encode(nil, data)
			},
			queriesCount:   1,
			expectedSeries: 1,
			expectError:    false,
		},
		{
			name:         "sampled response multiple queries",
			responseType: "application/x-protobuf",
			responseBody: func() []byte {
				resp := &prompb.ReadResponse{
					Results: []*prompb.QueryResult{
						{
							Timeseries: []*prompb.TimeSeries{
								{
									Labels: []prompb.Label{{Name: "__name__", Value: "up"}},
									Samples: []prompb.Sample{{Timestamp: 1000, Value: 1.0}},
								},
							},
						},
						{
							Timeseries: []*prompb.TimeSeries{
								{
									Labels: []prompb.Label{{Name: "__name__", Value: "go_memstats_alloc_bytes"}},
									Samples: []prompb.Sample{{Timestamp: 1000, Value: 12345.0}},
								},
							},
						},
					},
				}
				data, _ := proto.Marshal(resp)
				return snappy.Encode(nil, data)
			},
			queriesCount:   2,
			expectedSeries: 2,
			expectError:    false,
		},
		{
			name:         "response query count mismatch",
			responseType: "application/x-protobuf",
			responseBody: func() []byte {
				resp := &prompb.ReadResponse{
					Results: []*prompb.QueryResult{
						{Timeseries: []*prompb.TimeSeries{}},
					},
				}
				data, _ := proto.Marshal(resp)
				return snappy.Encode(nil, data)
			},
			queriesCount: 2,
			expectError:  true,
			errorMsg:     "responses: want 2, got 1",
		},
		{
			name:         "http error response",
			responseType: "text/plain",
			responseBody: func() []byte { return []byte("server error") },
			queriesCount: 1,
			expectError:  true,
			errorMsg:     "remote server returned http status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.name == "http error response" {
					w.WriteHeader(http.StatusInternalServerError)
				}
				w.Header().Set("Content-Type", tt.responseType)
				w.Write(tt.responseBody())
			}))
			defer server.Close()

			cmd := &RemoteReadCommand{
				address:        server.URL,
				remoteReadPath: "/api/v1/read",
				readTimeout:    30 * time.Second,
				useChunks:      false, // Use sampled response for simplicity
			}

			// Create mock queries
			queries := make([]*prompb.Query, tt.queriesCount)
			for i := 0; i < tt.queriesCount; i++ {
				matchers, _ := parser.ParseMetricSelector("up")
				query, _ := remote.ToQuery(1000, 2000, matchers, nil)
				queries[i] = query
			}

			client, err := cmd.readClient()
			require.NoError(t, err)

			seriesSet, err := cmd.executeMultipleQueries(context.Background(), client, queries)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, seriesSet)

				// Count series
				seriesCount := 0
				for seriesSet.Next() {
					seriesCount++
				}
				assert.Equal(t, tt.expectedSeries, seriesCount)
				assert.NoError(t, seriesSet.Err())
			}
		})
	}
}

func TestCombinedSeriesSet(t *testing.T) {
	tests := []struct {
		name           string
		series         []storage.Series
		expectedCount  int
	}{
		{
			name:          "empty series",
			series:        []storage.Series{},
			expectedCount: 0,
		},
		{
			name: "single series",
			series: []storage.Series{
				storage.MockSeries([]int64{1000}, []float64{1.0}, []string{"__name__", "up"}),
			},
			expectedCount: 1,
		},
		{
			name: "multiple series",
			series: []storage.Series{
				storage.MockSeries([]int64{1000}, []float64{1.0}, []string{"__name__", "up"}),
				storage.MockSeries([]int64{1000}, []float64{12345.0}, []string{"__name__", "go_memstats_alloc_bytes"}),
				storage.MockSeries([]int64{1000}, []float64{1.0}, []string{"__name__", "prometheus_build_info"}),
			},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seriesSet := &combinedSeriesSet{
				series: tt.series,
				index:  -1,
			}

			count := 0
			for seriesSet.Next() {
				count++
				assert.NotNil(t, seriesSet.At())
			}
			assert.Equal(t, tt.expectedCount, count)
			assert.NoError(t, seriesSet.Err())
			assert.Nil(t, seriesSet.Warnings())
		})
	}
}

func TestMultiQueryChunkedIterator(t *testing.T) {
	// Test basic iterator functionality
	chunks := []prompb.Chunk{
		{
			Type: prompb.Chunk_XOR,
			Data: createMockXORChunk(t, []int64{1000, 2000}, []float64{1.0, 2.0}),
		},
	}

	iter := newMultiQueryChunkedIterator(chunks, 1000, 2000)

	// Test iteration
	vt := iter.Next()
	assert.NotEqual(t, chunkenc.ValNone, vt)

	// Test error handling
	assert.NoError(t, iter.Err())
}

// createMockXORChunk creates a simple XOR encoded chunk for testing
func createMockXORChunk(t *testing.T, timestamps []int64, values []float64) []byte {
	require.Equal(t, len(timestamps), len(values))
	
	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	require.NoError(t, err)
	
	for i := range timestamps {
		appender.Append(timestamps[i], values[i])
	}
	
	return chunk.Bytes()
}
