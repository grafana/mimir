// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitWriteRequestByMaxMarshalSize(t *testing.T) {
	req := &WriteRequest{
		Source:              RULE,
		SkipLabelValidation: true,
		Timeseries: []PreallocTimeseries{
			{TimeSeries: &TimeSeries{
				Labels:     FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_1", "pod", "test-application-123456")),
				Samples:    []Sample{{TimestampMs: 20}},
				Exemplars:  []Exemplar{{TimestampMs: 30}},
				Histograms: []Histogram{{Timestamp: 10}},
			}},
			{TimeSeries: &TimeSeries{
				Labels:  FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_2", "pod", "test-application-123456")),
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
	require.NotZero(t, req.Source)
	require.NotZero(t, req.SkipLabelValidation)
	require.NotZero(t, req.Timeseries)
	require.NotZero(t, req.Metadata)

	t.Run("should return the input WriteRequest if its size is less than the size limit", func(t *testing.T) {
		partials := SplitWriteRequestByMaxMarshalSize(req, req.Size(), 100000)
		require.Len(t, partials, 1)
		assert.Equal(t, req, partials[0])
	})

	t.Run("should split the input WriteRequest into multiple requests, honoring the size limit", func(t *testing.T) {
		const limit = 100

		partials := SplitWriteRequestByMaxMarshalSize(req, req.Size(), limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{req.Metadata[0], req.Metadata[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{req.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.Less(t, partial.Size(), limit)
		}
	})

	t.Run("should return partial WriteRequests with size bigger than limit if a single entity (Timeseries or Metadata) is larger than the limit", func(t *testing.T) {
		const limit = 10

		partials := SplitWriteRequestByMaxMarshalSize(req, req.Size(), limit)
		assert.Equal(t, []*WriteRequest{
			{
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{req.Timeseries[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Timeseries:          []PreallocTimeseries{req.Timeseries[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{req.Metadata[0]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{req.Metadata[1]},
			}, {
				Source:              RULE,
				SkipLabelValidation: true,
				Metadata:            []*MetricMetadata{req.Metadata[2]},
			},
		}, partials)

		for _, partial := range partials {
			assert.Greater(t, partial.Size(), limit)
		}
	})
}

func TestSplitWriteRequestByMaxMarshalSize_Fuzzy(t *testing.T) {
	const numRuns = 1000

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Log("random generator seed:", seed)

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

		assert.Equal(t, req, merged)
	}
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
	// the SplitWriteRequestByMaxMarshalSize() implementation accordingly!
	assert.ElementsMatch(t, []string{"Timeseries", "Source", "Metadata", "SkipLabelValidation", "skipUnmarshalingExemplars"}, fieldNames)
}

func BenchmarkSplitWriteRequestByMaxMarshalSize(b *testing.B) {
	benchmarkSplitWriteRequestByMaxMarshalSize(b, func(b *testing.B, req *WriteRequest, maxSize int) {
		for n := 0; n < b.N; n++ {
			SplitWriteRequestByMaxMarshalSize(req, req.Size(), maxSize)
		}
	})
}

func BenchmarkSplitWriteRequestByMaxMarshalSize_WithMarshalling(b *testing.B) {
	// In this benchmark we simulate a behaviour similar to distributor one, where the WriteRequest is
	// initially unmarshalled, then sharded (skipped in this test), then split by max size and finally
	// each partial request is marshalled.
	benchmarkSplitWriteRequestByMaxMarshalSize(b, func(b *testing.B, req *WriteRequest, maxSize int) {
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
			partialReqs := SplitWriteRequestByMaxMarshalSize(req, req.Size(), maxSize)

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
}

func benchmarkSplitWriteRequestByMaxMarshalSize(b *testing.B, run func(b *testing.B, req *WriteRequest, maxSize int)) {
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
			req := generateWriteRequest(testData.numSeries, testData.numLabelsPerSeries, testData.numSamplesPerSeries, testData.numMetadata)
			reqSize := req.Size()

			// Test with different split size.
			splitScenarios := map[string]int{
				"no splitting":           reqSize * 2,
				"split in few requests":  int(float64(reqSize) * 0.8),
				"split in many requests": int(float64(reqSize) * 0.11),
			}

			for splitName, maxSize := range splitScenarios {
				b.Run(splitName, func(b *testing.B) {
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
		builder.Add(labels.MetricName, fmt.Sprintf("series_%d", i))
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
