// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitWriteRequestByMaxMarshalSize(t *testing.T) {
	reqv1 := &WriteRequest{
		Source:              RULE,
		SkipLabelValidation: true,
		Timeseries: []PreallocTimeseries{
			{TimeSeries: &TimeSeries{
				Labels:     FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_1", "pod", "test-application-123456")),
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []Exemplar{{TimestampMs: 30}},
				Histograms: []Histogram{{Timestamp: 10}},
			}},
			{TimeSeries: &TimeSeries{
				Labels:  FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_2", "pod", "test-application-123456")),
				Samples: []Sample{{TimestampMs: 30}},
			}},
		},
		Metadata: []*MetricMetadata{
			{Type: COUNTER, MetricFamilyName: "series_1", Help: "This is the first test metric."},
			{Type: COUNTER, MetricFamilyName: "series_2", Help: "This is the second test metric."},
			{Type: COUNTER, MetricFamilyName: "series_3", Help: "This is the third test metric."},
		},
	}

	// Pre-requisite check: WriteRequest fields are set to non-zero values.
	require.NotZero(t, reqv1.Source)
	require.NotZero(t, reqv1.SkipLabelValidation)
	require.NotZero(t, reqv1.Timeseries)
	require.NotZero(t, reqv1.Metadata)

	t.Run("should return the input WriteRequest if its size is less than the size limit", func(t *testing.T) {
		partials := SplitWriteRequestByMaxMarshalSize(reqv1, reqv1.Size(), 100000)
		require.Len(t, partials, 1)
		assert.Equal(t, reqv1, partials[0])
	})

	t.Run("should return the input WriteRequest if its size is less than the size limit - RW2", func(t *testing.T) {
		reqv2 := testReqV2Static(t)
		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), 100000, 0, nil)
		require.Len(t, partials, 1)
		assert.Equal(t, reqv2, partials[0])
	})

	t.Run("should split the input WriteRequest into multiple requests, honoring the size limit", func(t *testing.T) {
		const limit = 100

		partials := SplitWriteRequestByMaxMarshalSize(reqv1, reqv1.Size(), limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{reqv1.Timeseries[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{reqv1.Timeseries[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{reqv1.Metadata[0], reqv1.Metadata[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{reqv1.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.LessOrEqual(t, partial.Size(), limit)
		}
	})

	t.Run("should split the input WriteRequest into multiple requests, honoring the size limit - RW2", func(t *testing.T) {
		// 150 is small enough to force each timeseries into its own sub-request
		const limit = 150
		reqv2 := testReqV2Static(t)

		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), limit, 0, nil)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "This is the first test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 20}},
						Exemplars:  []ExemplarRW2{{Timestamp: 30}},
						Histograms: []Histogram{{Timestamp: 10}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_2", "pod", "test-application-123456", "This is the second test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 30}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_3", "This is the third test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 3,
						},
					},
				},
			},
		}, partials)

		for _, partial := range partials {
			assert.LessOrEqual(t, partial.Size(), limit)
		}
	})

	t.Run("should split the input WriteRequest into multiple requests, honoring the size limit, and bin-packing - RW2", func(t *testing.T) {
		// 220 allows the first and second WriteRequests to fit into one request, but not the third.
		const limit = 220
		reqv2 := testReqV2Static(t)

		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), limit, 0, nil)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "This is the first test metric.", "series_2", "This is the second test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 20}},
						Exemplars:  []ExemplarRW2{{Timestamp: 30}},
						Histograms: []Histogram{{Timestamp: 10}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
					{
						LabelsRefs: []uint32{1, 6, 3, 4},
						Samples:    []Sample{{TimestampMs: 30}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 7,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_3", "This is the third test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 3,
						},
					},
				},
			},
		}, partials)

		for _, partial := range partials {
			assert.LessOrEqual(t, partial.Size(), limit)
		}
	})

	t.Run("should split the input WriteRequest into multiple requests with size bigger than limit if limit < size(symbols)", func(t *testing.T) {
		const limit = 50
		reqv2 := testReqV2Static(t)

		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), limit, 0, nil)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "This is the first test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 20}},
						Exemplars:  []ExemplarRW2{{Timestamp: 30}},
						Histograms: []Histogram{{Timestamp: 10}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_2", "pod", "test-application-123456", "This is the second test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 30}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_3", "This is the third test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 3,
						},
					},
				},
			},
		}, partials)

		for _, partial := range partials {
			assert.Greater(t, partial.Size(), limit)
		}
	})

	t.Run("should return partial WriteRequests with size bigger than limit if a single entity (Timeseries or Metadata) is larger than the limit", func(t *testing.T) {
		const limit = 10

		partials := SplitWriteRequestByMaxMarshalSize(reqv1, reqv1.Size(), limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{reqv1.Timeseries[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{reqv1.Timeseries[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{reqv1.Metadata[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{reqv1.Metadata[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{reqv1.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.Greater(t, partial.Size(), limit)
		}
	})

	t.Run("should split the input WriteRequest into multiple requests with size bigger than limit, if limit > size(symbols) but each request < limit", func(t *testing.T) {
		const limit = 70
		reqv2 := testReqV2Static(t)

		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), limit, 0, nil)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "This is the first test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 20}},
						Exemplars:  []ExemplarRW2{{Timestamp: 30}},
						Histograms: []Histogram{{Timestamp: 10}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_2", "pod", "test-application-123456", "This is the second test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2, 3, 4},
						Samples:    []Sample{{TimestampMs: 30}},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 5,
						},
					},
				},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				SymbolsRW2:          []string{"", model.MetricNameLabel, "series_3", "This is the third test metric."},
				TimeseriesRW2: []TimeSeriesRW2{
					{
						LabelsRefs: []uint32{1, 2},
						Metadata: MetadataRW2{
							Type:    METRIC_TYPE_COUNTER,
							HelpRef: 3,
						},
					},
				},
			},
		}, partials)

		for _, partial := range partials {
			assert.Greater(t, partial.Size(), limit)
		}
	})

	t.Run("v2 splitting does not mutate the input request", func(t *testing.T) {
		const limit = 220
		reqv2 := testReqV2Static(t)

		partials := SplitWriteRequestByMaxMarshalSizeRW2(reqv2, reqv2.Size(), limit, 0, nil)

		require.Len(t, partials, 2)
		require.Equal(t, testReqV2Static(t), reqv2)
	})
}

func TestSplitWriteRequestByMaxMarshalSize_Fuzzy(t *testing.T) {
	const numRuns = 1000

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

	t.Run("rw1", func(t *testing.T) {
		for r := 0; r < numRuns; r++ {
			var (
				// Gradually increasing request series in each run, up to a limit.
				numSeries           = rnd.Intn(min(r+1, 100)) + 1
				numLabelsPerSeries  = rnd.Intn(min(r+1, 100)) + 1
				numSamplesPerSeries = rnd.Intn(min(r+1, 100)) + 1
				numMetadata         = rnd.Intn(min(r+1, 100)) + 1
			)

			req := generateWriteRequest(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata)
			maxSize := req.Size() / (1 + rnd.Intn(10))
			partials := SplitWriteRequestByMaxMarshalSize(req, req.Size(), maxSize)

			// Ensure the merge of all partial requests is equal to the original one.
			merged := &WriteRequest{
				Timeseries:          []PreallocTimeseries{},
				Source:              partials[0].Source,
				Metadata:            []*MetricMetadata{},
				SkipLabelValidation: partials[0].SkipLabelValidation,
			}

			for _, partial := range partials {
				merged.Timeseries = append(merged.Timeseries, partial.Timeseries...)
				merged.Metadata = append(merged.Metadata, partial.Metadata...)
			}

			require.Equal(t, req, merged)
		}
	})

	t.Run("rw2", func(t *testing.T) {
		for r := 0; r < numRuns; r++ {
			var (
				// Gradually increasing request series in each run, up to a limit.
				numSeries           = rnd.Intn(min(r+1, 100)) + 1
				numLabelsPerSeries  = rnd.Intn(min(r+1, 100)) + 1
				numSamplesPerSeries = rnd.Intn(min(r+1, 100)) + 1
				numMetadata         = rnd.Intn(min(r+1, 100)) + 1
			)

			// Re-symbolization alters the values of the original request.
			// Remember the original request, to compare against.
			req := generateWriteRequestRW2(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata)
			reqCpy := generateWriteRequestRW2(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata)
			require.Equal(t, req, reqCpy)

			maxSize := req.Size() / (1 + rnd.Intn(10))
			partials := SplitWriteRequestByMaxMarshalSizeRW2(req, req.Size(), maxSize, 0, nil)

			// Ensure the merge of all partial requests is equal to the original one.
			merged := mergeRW2s(partials)
			require.Equal(t, reqCpy, merged)
		}
	})
}

func TestSplitWriteRequestByMaxMarshalSize_WriteRequestHasChanged(t *testing.T) {
	// Get WriteRequest field names.
	val := reflect.ValueOf(&WriteRequest{})
	typ := val.Type()

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	var fieldNames []string
	for i := 0; i < typ.NumField(); i++ {
		fieldNames = append(fieldNames, typ.Field(i).Name)
	}

	// If the fields of WriteRequest have changed, then you will probably need to modify
	// the [SplitWriteRequestByMaxMarshalSize] and [SplitWriteRequestByMaxMarshalSizeRW2] implementations accordingly!
	assert.ElementsMatch(t, []string{
		"Timeseries",
		"Source",
		"Metadata",
		"SymbolsRW2",
		"TimeseriesRW2",
		"SkipLabelValidation",
		"SkipLabelCountValidation",
		"skipUnmarshalingExemplars",
		"skipNormalizeMetadataMetricName",
		"skipDeduplicateMetadata",
		"unmarshalFromRW2",
		"rw2symbols",
		"BufferHolder",
		"sourceBufferHolders",
		"arena",
	}, fieldNames)
}

func BenchmarkSplitWriteRequestByMaxMarshalSize(b *testing.B) {
	b.Run("rw1", func(b *testing.B) {
		benchmarkSplitWriteRequestByMaxMarshalSize(b, generateWriteRequest, func(b *testing.B, req *WriteRequest, maxSize int) {
			for n := 0; n < b.N; n++ {
				SplitWriteRequestByMaxMarshalSize(req, req.Size(), maxSize)
			}
		})
	})
	b.Run("rw2", func(b *testing.B) {
		benchmarkSplitWriteRequestByMaxMarshalSize(b, generateWriteRequestRW2, func(b *testing.B, req *WriteRequest, maxSize int) {
			for n := 0; n < b.N; n++ {
				SplitWriteRequestByMaxMarshalSizeRW2(req, req.Size(), maxSize, 0, nil)
			}
		})
	})
}

func BenchmarkSplitWriteRequestByMaxMarshalSize_WithMarshalling(b *testing.B) {
	// In this benchmark we simulate a behaviour similar to distributor one, where the WriteRequest is
	// initially unmarshalled, then sharded (skipped in this test), then split by max size and finally
	// each partial request is marshalled.
	b.Run("rw1", func(b *testing.B) {
		benchmarkSplitWriteRequestByMaxMarshalSize(b, generateWriteRequest, func(b *testing.B, req *WriteRequest, maxSize int) {
			marshalledReq, err := req.Marshal()
			if err != nil {
				b.Fatal(err)
			}

			for n := 0; n < b.N; n++ {
				// Unmarshal the request.
				unmarshalledReq := &WriteRequest{}
				if err := unmarshalledReq.Unmarshal(marshalledReq); err != nil {
					b.Fatal(err)
				}

				// Split the request.
				partialReqs := SplitWriteRequestByMaxMarshalSize(unmarshalledReq, unmarshalledReq.Size(), maxSize)

				// Marshal each split request.
				for _, partialReq := range partialReqs {
					if data, err := partialReq.Marshal(); err != nil {
						b.Fatal(err)
					} else if len(data) >= maxSize {
						b.Fatalf("the marshalled partial request (%d bytes) is larger than max size (%d bytes)", len(data), maxSize)
					}
				}
			}
		})
	})

	b.Run("rw2", func(b *testing.B) {
		benchmarkSplitWriteRequestByMaxMarshalSize(b, generateWriteRequestRW2, func(b *testing.B, req *WriteRequest, maxSize int) {
			marshalledReq, err := req.Marshal()
			if err != nil {
				b.Fatal(err)
			}

			for n := 0; n < b.N; n++ {
				// Unmarshal the request.
				unmarshalledReq := &WriteRequest{
					unmarshalFromRW2: true,
				}
				if err := unmarshalledReq.Unmarshal(marshalledReq); err != nil {
					b.Fatal(err)
				}

				// Split the request.
				partialReqs := SplitWriteRequestByMaxMarshalSizeRW2(unmarshalledReq, unmarshalledReq.Size(), maxSize, 0, nil)

				// Marshal each split request.
				for _, partialReq := range partialReqs {
					if data, err := partialReq.Marshal(); err != nil {
						b.Fatal(err)
					} else if len(data) >= maxSize {
						b.Fatalf("the marshalled partial request (%d bytes) is larger than max size (%d bytes)", len(data), maxSize)
					}
				}
			}
		})
	})
}

func benchmarkSplitWriteRequestByMaxMarshalSize(b *testing.B, generator func(int, int, int, int) *WriteRequest, run func(b *testing.B, req *WriteRequest, maxSize int)) {
	tests := map[string]struct {
		numSeries           int
		numLabelsPerSeries  int
		numSamplesPerSeries int
		numMetadata         int
	}{
		"write request with few series, few labels each, and no metadata": {
			numSeries:           50,
			numLabelsPerSeries:  10,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with few series, many labels each, and no metadata": {
			numSeries:           50,
			numLabelsPerSeries:  100,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with many series, few labels each, and no metadata": {
			numSeries:           1000,
			numLabelsPerSeries:  10,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with many series, many labels each, and no metadata": {
			numSeries:           1000,
			numLabelsPerSeries:  100,
			numSamplesPerSeries: 1,
			numMetadata:         0,
		},
		"write request with few metadata, and no series": {
			numSeries:           0,
			numLabelsPerSeries:  0,
			numSamplesPerSeries: 0,
			numMetadata:         50,
		},
		"write request with many metadata, and no series": {
			numSeries:           0,
			numLabelsPerSeries:  0,
			numSamplesPerSeries: 0,
			numMetadata:         1000,
		},
		"write request with both series and metadata": {
			numSeries:           500,
			numLabelsPerSeries:  25,
			numSamplesPerSeries: 1,
			numMetadata:         500,
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			req := generator(testData.numSeries, testData.numLabelsPerSeries, testData.numSamplesPerSeries, testData.numMetadata)
			reqSize := req.Size()

			// Test with different split size.
			splitScenarios := map[string]int{
				"no splitting":           reqSize * 2,
				"split in few requests":  int(float64(reqSize) * 0.8),
				"split in many requests": int(float64(reqSize) * 0.11),
			}

			for splitName, maxSize := range splitScenarios {
				b.Run(splitName, func(b *testing.B) {
					b.ResetTimer()
					run(b, req, maxSize)
				})
			}
		})
	}
}

func generateWriteRequest(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata int) *WriteRequest {
	builder := labels.NewScratchBuilder(numLabelsPerSeries)

	// Generate timeseries.
	timeseries := make([]PreallocTimeseries, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		curr := PreallocTimeseries{TimeSeries: &TimeSeries{}}

		// Generate series labels.
		builder.Reset()
		builder.Add(model.MetricNameLabel, fmt.Sprintf("series_%d", i))
		for l := 1; l < numLabelsPerSeries; l++ {
			builder.Add(fmt.Sprintf("label_%d", l), fmt.Sprintf("this-is-the-value-of-label-%d", l))
		}
		curr.Labels = FromLabelsToLabelAdapters(builder.Labels())

		// Generate samples.
		curr.Samples = make([]Sample, 0, numSamplesPerSeries)
		for s := 0; s < numSamplesPerSeries; s++ {
			curr.Samples = append(curr.Samples, Sample{
				TimestampMs: int64(s),
				Value:       float64(s),
			})
		}

		// Add an exemplar.
		builder.Reset()
		builder.Add("trace_id", fmt.Sprintf("the-trace-id-for-%d", i))
		curr.Exemplars = []Exemplar{{
			Labels:      FromLabelsToLabelAdapters(builder.Labels()),
			TimestampMs: int64(i),
			Value:       float64(i),
		}}

		timeseries = append(timeseries, curr)
	}

	// Generate metadata.
	metadata := make([]*MetricMetadata, 0, numMetadata)
	for i := 0; i < numMetadata; i++ {
		metadata = append(metadata, &MetricMetadata{
			Type:             COUNTER,
			MetricFamilyName: fmt.Sprintf("series_%d", i),
			Help:             fmt.Sprintf("this is the help description for series %d", i),
			Unit:             "seconds",
		})
	}

	return &WriteRequest{
		Source:              RULE,
		SkipLabelValidation: true,
		Timeseries:          timeseries,
		Metadata:            metadata,
	}
}

func generateWriteRequestRW2(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata int) *WriteRequest {
	rw1 := generateWriteRequest(numSeries, numLabelsPerSeries, numSamplesPerSeries, numMetadata)
	rw2, _ := FromWriteRequestToRW2Request(rw1, nil, 0)
	return rw2
}

func testReqV2Static(t *testing.T) *WriteRequest {
	t.Helper()
	reqv2 := &WriteRequest{
		Source:              RULE,
		SkipLabelValidation: true,
		SymbolsRW2:          []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "This is the first test metric.", "series_2", "This is the second test metric.", "series_3", "This is the third test metric."},
		TimeseriesRW2: []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 5,
				},
			},
			{
				LabelsRefs: []uint32{1, 6, 3, 4},
				Samples:    []Sample{{TimestampMs: 30}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
				},
			},
			{
				LabelsRefs: []uint32{1, 8},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 9,
				},
			},
		},
	}

	require.NotZero(t, reqv2.Source)
	require.NotZero(t, reqv2.SkipLabelValidation)
	require.NotZero(t, reqv2.TimeseriesRW2)
	require.NotZero(t, reqv2.SymbolsRW2)
	return reqv2
}

func mergeRW2s(partials []*WriteRequest) *WriteRequest {
	st := NewFastSymbolsTable(0)
	timeSeries := []TimeSeriesRW2{}

	for _, partial := range partials {
		for _, ts := range partial.TimeseriesRW2 {
			newLbls := make([]uint32, len(ts.LabelsRefs))
			for i := range ts.LabelsRefs {
				strVal := partial.SymbolsRW2[ts.LabelsRefs[i]]
				newLbls[i] = st.Symbolize(strVal)
			}

			helpTxt := partial.SymbolsRW2[ts.Metadata.HelpRef]
			helpRef := st.Symbolize(helpTxt)
			unitTxt := partial.SymbolsRW2[ts.Metadata.UnitRef]
			unitRef := st.Symbolize(unitTxt)

			newExemplars := make([]ExemplarRW2, 0, len(ts.Exemplars))
			for _, ex := range ts.Exemplars {
				newLbls := make([]uint32, len(ex.LabelsRefs))
				for i := range ex.LabelsRefs {
					strVal := partial.SymbolsRW2[ex.LabelsRefs[i]]
					newLbls[i] = st.Symbolize(strVal)
				}
				newExemplars = append(newExemplars, ExemplarRW2{
					LabelsRefs: newLbls,
					Value:      ex.Value,
					Timestamp:  ex.Timestamp,
				})
			}
			if ts.Exemplars == nil {
				newExemplars = nil
			}

			newTS := TimeSeriesRW2{
				LabelsRefs:       newLbls,
				Samples:          ts.Samples,
				Exemplars:        newExemplars,
				Histograms:       ts.Histograms,
				CreatedTimestamp: ts.CreatedTimestamp,
				Metadata: MetadataRW2{
					Type:    ts.Metadata.Type,
					HelpRef: helpRef,
					UnitRef: unitRef,
				},
			}
			timeSeries = append(timeSeries, newTS)
		}
	}

	return &WriteRequest{
		TimeseriesRW2:       timeSeries,
		SymbolsRW2:          st.Symbols(),
		Source:              partials[0].Source,
		SkipLabelValidation: partials[0].SkipLabelValidation,
	}
}

func TestRW2SymbolSplitting(t *testing.T) {
	t.Run("timeseries size estimation", func(t *testing.T) {
		t.Run("symbols size matches proto size", func(t *testing.T) {
			syms := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{5, 6}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
				},
			}

			_, symbolsSize := maxRW2SeriesSizeAfterResymbolization(&ts, syms, 0)

			req := &WriteRequest{SymbolsRW2: syms}
			require.Equal(t, req.SymbolsRW2Size(), symbolsSize)
		})

		t.Run("symbols size only considers referenced symbols", func(t *testing.T) {
			syms := []string{"", model.MetricNameLabel, "series_1", "series_2", "series_3", "pod", "test-application-123456", "trace_id", "12345", "Help Text", "unrelated text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 4, 5, 6},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{7, 8}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 9,
				},
			}

			_, symbolsSize := maxRW2SeriesSizeAfterResymbolization(&ts, syms, 0)

			referencedSyms := []string{"", syms[1], syms[4], syms[5], syms[6], syms[7], syms[8], syms[9]}
			req := &WriteRequest{SymbolsRW2: referencedSyms}
			require.Equal(t, req.SymbolsRW2Size(), symbolsSize)
		})

		t.Run("upper bound grows with possible symbol magnitude", func(t *testing.T) {
			syms := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{5, 6}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
				},
			}

			req := &WriteRequest{TimeseriesRW2: []TimeSeriesRW2{ts}}
			actualSize := req.TimeseriesRW2Size()

			seriesSizeSmallRefs, _ := maxRW2SeriesSizeAfterResymbolization(&ts, syms, 0)
			require.Greater(t, seriesSizeSmallRefs, actualSize)
			// The upper bound should not be vastly larger
			require.Less(t, seriesSizeSmallRefs, 2*actualSize)

			seriesSizeBigRefs, _ := maxRW2SeriesSizeAfterResymbolization(&ts, syms, 10000)
			require.Greater(t, seriesSizeBigRefs, seriesSizeSmallRefs)
			// The upper bound should not be vastly larger
			require.Less(t, seriesSizeBigRefs, 2*actualSize)
		})

		t.Run("common symbol refs only count the ref and not the symbol", func(t *testing.T) {
			syms := []string{"", model.MetricNameLabel, "series_1", "series_2", "series_3", "pod", "test-application-123456", "trace_id", "12345", "Help Text", "unrelated text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{11, 14, 15, 16, 7, 8}, // References a couple common symbols, outside the range
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{17, 18}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 19,
				},
			}
			req := &WriteRequest{TimeseriesRW2: []TimeSeriesRW2{ts}}
			actualSize := req.TimeseriesRW2Size()

			seriesSize, symbolsSize := maxRW2SeriesSizeAfterResymbolization(&ts, syms, 10)

			referencedSyms := []string{"", syms[1], syms[4], syms[5], syms[6], syms[7], syms[8], syms[9]}
			req = &WriteRequest{SymbolsRW2: referencedSyms}
			require.Equal(t, req.SymbolsRW2Size(), symbolsSize)
			const expOverestimate = 8
			require.Equal(t, actualSize+expOverestimate, seriesSize)
		})
	})

	t.Run("resymbolizeTimeSeriesRW2", func(t *testing.T) {
		t.Run("no change required", func(t *testing.T) {
			newTable := NewFastSymbolsTable(0)
			syms := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{5, 6}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
				},
			}

			resymbolized := resymbolizeTimeSeriesRW2(&ts, syms, newTable)

			require.Equal(t, ts, resymbolized)
			require.Equal(t, ts.Size(), resymbolized.Size())
		})

		t.Run("excludes unrelated strings in original symbols", func(t *testing.T) {
			newTable := NewFastSymbolsTable(0)
			syms := []string{"", "unrelated1", "unrelated2", "unrelated3", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{4, 5, 6, 7},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{8, 9}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 10,
				},
			}

			resymbolized := resymbolizeTimeSeriesRW2(&ts, syms, newTable)

			expSymbols := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			require.Equal(t, expSymbols, newTable.Symbols())
			require.Equal(t, []uint32{1, 2, 3, 4}, resymbolized.LabelsRefs)
			require.Equal(t, []uint32{5, 6}, resymbolized.Exemplars[0].LabelsRefs)
			require.Equal(t, uint32(7), resymbolized.Metadata.HelpRef)
			require.Equal(t, ts.Size(), resymbolized.Size())
		})

		t.Run("re-uses symbols already loaded in new symbols table", func(t *testing.T) {
			newTable := NewFastSymbolsTable(0)
			newTable.Symbolize("unrelated1")
			newTable.Symbolize("unrelated2")
			newTable.Symbolize(model.MetricNameLabel)
			newTable.Symbolize("series_1")
			prevSize := newTable.SymbolsSizeProto()
			syms := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{5, 6}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
				},
			}

			resymbolized := resymbolizeTimeSeriesRW2(&ts, syms, newTable)

			expSyms := []string{"", "unrelated1", "unrelated2", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text"}
			require.Equal(t, expSyms, newTable.Symbols())
			require.Equal(t, []uint32{3, 4, 5, 6}, resymbolized.LabelsRefs)
			require.Equal(t, []uint32{7, 8}, resymbolized.Exemplars[0].LabelsRefs)
			require.Equal(t, uint32(9), resymbolized.Metadata.HelpRef)
			require.Equal(t, ts.Size(), resymbolized.Size())
			require.Greater(t, newTable.SymbolsSizeProto(), prevSize)
		})

		t.Run("resymbolize with different symbol magnitudes", func(t *testing.T) {
			newTable := NewFastSymbolsTable(0)
			// Proto encodes larger references (i.e. larger varints) with more bytes.
			// Fill the table with a bunch of junk, to make our new strings have numerically large references.
			for i := range 10000 {
				newTable.Symbolize(fmt.Sprintf("%d", i))
			}

			syms := []string{"", model.MetricNameLabel, "series_1", "pod", "test-application-123456", "trace_id", "12345", "Help Text", "unit"}
			ts := TimeSeriesRW2{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []ExemplarRW2{{Timestamp: 30, LabelsRefs: []uint32{5, 6}}},
				Histograms: []Histogram{{Timestamp: 10}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 7,
					UnitRef: 8,
				},
			}

			resymbolized := resymbolizeTimeSeriesRW2(&ts, syms, newTable)

			require.Equal(t, "", newTable.Symbols()[0])
			require.Equal(t, syms[1:], newTable.Symbols()[10001:])
			require.Equal(t, []uint32{10001, 10002, 10003, 10004}, resymbolized.LabelsRefs)
			require.Equal(t, []uint32{10005, 10006}, resymbolized.Exemplars[0].LabelsRefs)
			require.Equal(t, uint32(10007), resymbolized.Metadata.HelpRef)
			require.Equal(t, uint32(10008), resymbolized.Metadata.UnitRef)
			const expGrowth = 8
			require.Equal(t, expGrowth, resymbolized.Size()-ts.Size())
		})
	})
}
