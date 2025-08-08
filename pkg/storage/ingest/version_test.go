// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestV2SymbolsCompat(t *testing.T) {
	t.Run("v2 symbols cannot be larger than v2 offset", func(t *testing.T) {
		require.LessOrEqual(t, len(V2CommonSymbols.GetSlice()), V2RecordSymbolOffset)
		require.LessOrEqual(t, len(V2CommonSymbols.GetMap()), V2RecordSymbolOffset)
	})

	t.Run("the zeroth symbol in v2 symbols must be empty string", func(t *testing.T) {
		require.Empty(t, V2CommonSymbols.GetSlice()[0])
		require.Equal(t, uint32(0), V2CommonSymbols.GetMap()[""])
	})
}

func TestRecordVersionHeader(t *testing.T) {
	t.Run("no version header is assumed to be v0", func(t *testing.T) {
		rec := &kgo.Record{
			Headers: []kgo.RecordHeader{},
		}
		parsedVersion := ParseRecordVersion(rec)
		require.Equal(t, 0, parsedVersion)
	})

	tests := []struct {
		version int
	}{
		{
			version: 0,
		},
		{
			version: 1,
		},
		{
			version: 255,
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("version %d", tt.version), func(t *testing.T) {
			rec := &kgo.Record{
				Headers: []kgo.RecordHeader{RecordVersionHeader(tt.version)},
			}

			parsed := ParseRecordVersion(rec)
			require.Equal(t, tt.version, parsed)
		})
	}
}

func TestDeserializeRecordContent(t *testing.T) {
	t.Run("invalid version", func(t *testing.T) {
		content := make([]byte, 0)

		wr := mimirpb.PreallocWriteRequest{}
		err := DeserializeRecordContent(content, &wr, 255)

		require.ErrorContains(t, err, "unsupported version")
	})

	t.Run("v0", func(t *testing.T) {
		reqv0 := &mimirpb.PreallocWriteRequest{
			WriteRequest: mimirpb.WriteRequest{
				Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithAll("series_0"),
					mockPreallocTimeseriesWithAll("series_1"),
					mockPreallocTimeseriesWithAll("series_2"),
				},
				Source: mimirpb.API,
				Metadata: []*mimirpb.MetricMetadata{
					mockMetricMetadata("series_0"),
					mockMetricMetadata("series_1"),
					mockMetricMetadata("series_2"),
				},
			},
		}
		v0bytes, err := reqv0.Marshal()
		require.NoError(t, err)

		wr := mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(v0bytes, &wr, 0)

		require.NoError(t, err)
		wr.ClearTimeseriesUnmarshalData()
		require.Equal(t, reqv0, &wr)
	})

	t.Run("v1", func(t *testing.T) {
		reqv1 := &mimirpb.PreallocWriteRequest{
			WriteRequest: mimirpb.WriteRequest{
				Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithAll("series_0"),
					mockPreallocTimeseriesWithAll("series_1"),
					mockPreallocTimeseriesWithAll("series_2"),
				},
				Source: mimirpb.API,
				Metadata: []*mimirpb.MetricMetadata{
					mockMetricMetadata("series_0"),
					mockMetricMetadata("series_1"),
					mockMetricMetadata("series_2"),
				},
			},
		}
		v1bytes, err := reqv1.Marshal()
		require.NoError(t, err)

		wr := mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(v1bytes, &wr, 1)

		require.NoError(t, err)
		wr.ClearTimeseriesUnmarshalData()
		require.Equal(t, reqv1, &wr)
	})

	t.Run("v2", func(t *testing.T) {
		syms := test.NewSymbolTableBuilderWithCommon(nil, V2RecordSymbolOffset, V2CommonSymbols.GetSlice())
		reqv2 := &mimirpb.WriteRequestRW2{
			Timeseries: []mimirpb.TimeSeriesRW2{{
				LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("job"), syms.GetSymbol("test_job")},
				Samples: []mimirpb.Sample{
					{
						Value:       123.456,
						TimestampMs: 1234567890,
					},
				},
				Exemplars: []mimirpb.ExemplarRW2{
					{
						Value:      123.456,
						Timestamp:  1234567890,
						LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("traceID"), syms.GetSymbol("1234567890abcdef")},
					},
				},
				Histograms: []mimirpb.Histogram{
					{
						Timestamp:      1234567890,
						Count:          &mimirpb.Histogram_CountInt{CountInt: 10},
						Sum:            100,
						Schema:         3,
						ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
						PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
						PositiveDeltas: []int64{1},
						NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
						NegativeDeltas: []int64{1},
					},
				},
				Metadata: mimirpb.MetadataRW2{
					Type:    mimirpb.METRIC_TYPE_COUNTER,
					HelpRef: syms.GetSymbol("Help for test_metric_total"),
					UnitRef: syms.GetSymbol("seconds"),
				},
			}},
		}
		reqv2.Symbols = syms.GetSymbols()
		// Symbols should not contain common labels "__name__" and "job"
		require.Equal(t, []string{"", "test_metric_total", "test_job", "traceID", "1234567890abcdef", "Help for test_metric_total", "seconds"}, reqv2.Symbols)
		v2bytes, err := reqv2.Marshal()
		require.NoError(t, err)

		wr := mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(v2bytes, &wr, 2)
		require.NoError(t, err)
		require.Len(t, wr.Timeseries, 1)
		expLabels := []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric_total"}, {Name: "job", Value: "test_job"}}
		require.Equal(t, expLabels, wr.Timeseries[0].Labels)
		expSamples := []mimirpb.Sample{{Value: 123.456, TimestampMs: 1234567890}}
		require.Equal(t, expSamples, wr.Timeseries[0].Samples)
		expExemplars := []mimirpb.Exemplar{
			{
				Value:       123.456,
				TimestampMs: 1234567890,
				Labels:      []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_metric_total"}, {Name: "traceID", Value: "1234567890abcdef"}}},
		}
		require.Equal(t, expExemplars, wr.Timeseries[0].Exemplars)
		expHistograms := []mimirpb.Histogram{
			{
				Timestamp:      1234567890,
				Count:          &mimirpb.Histogram_CountInt{CountInt: 10},
				Sum:            100,
				Schema:         3,
				ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
				PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
				PositiveDeltas: []int64{1},
				NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
				NegativeDeltas: []int64{1},
			},
		}
		require.Equal(t, expHistograms, wr.Timeseries[0].Histograms)
		expMetadata := []*mimirpb.MetricMetadata{{
			Type:             mimirpb.COUNTER,
			MetricFamilyName: "test_metric_total",
			Help:             "Help for test_metric_total",
			Unit:             "seconds",
		}}
		require.Equal(t, expMetadata, wr.Metadata)
	})
}

func TestRecordSerializer(t *testing.T) {
	t.Run("v2", func(t *testing.T) {
		req := &mimirpb.WriteRequest{
			Source:              mimirpb.RULE,
			SkipLabelValidation: true,
			Timeseries: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_1", "pod", "test-application-123456")),
					Samples:    []mimirpb.Sample{{TimestampMs: 20}},
					Exemplars:  []mimirpb.Exemplar{{TimestampMs: 30}},
					Histograms: []mimirpb.Histogram{{Timestamp: 10}},
				}},
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_2", "pod", "test-application-123456")),
					Samples:   []mimirpb.Sample{{TimestampMs: 30}},
					Exemplars: []mimirpb.Exemplar{},
				}},
			},
			Metadata: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_1", Help: "This is the first test metric."},
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_2", Help: "This is the second test metric."},
				{Type: mimirpb.COUNTER, MetricFamilyName: "series_3", Help: "This is the third test metric."},
			},
		}

		serializer := versionTwoRecordSerializer{}
		records, err := serializer.ToRecords(1234, "user-1", req, 100000)
		require.NoError(t, err)
		require.Len(t, records, 1)
		record := records[0]

		require.Equal(t, 2, ParseRecordVersion(record))

		resultReq := &mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(record.Value, resultReq, 2)
		require.NoError(t, err)

		require.Len(t, resultReq.Timeseries, 5)
		require.Equal(t, req.Timeseries, resultReq.Timeseries[0:2])

		// The only way to carry a metadata in RW2.0 is attached to a timeseries.
		// Metadata not attached to any series in the request must fabricate extra timeseries to house it.
		// In format V2 we also avoid the metadata-to-series matching operation to avoid ambiguity in the event multiple series share a family name.
		expMetadataSeries := []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_1")),
				Samples:   []mimirpb.Sample{},
				Exemplars: []mimirpb.Exemplar{},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_2")),
				Samples:   []mimirpb.Sample{},
				Exemplars: []mimirpb.Exemplar{},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "series_3")),
				Samples:   []mimirpb.Sample{},
				Exemplars: []mimirpb.Exemplar{},
			}},
		}
		require.Equal(t, expMetadataSeries, resultReq.Timeseries[2:5])

		require.Nil(t, resultReq.SymbolsRW2)
		require.Nil(t, resultReq.TimeseriesRW2)
		// Metadata order is currently not preserved by the RW2 deser layer.
		require.ElementsMatch(t, req.Metadata, resultReq.Metadata)
	})
}

func BenchmarkDeserializeRecordContent(b *testing.B) {
	// Generate a write request in each version
	reqv1 := &mimirpb.PreallocWriteRequest{
		WriteRequest: mimirpb.WriteRequest{
			Timeseries: make([]mimirpb.PreallocTimeseries, 10000),
		},
	}
	for i := range reqv1.Timeseries {
		reqv1.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
	}
	v1bytes, err := reqv1.Marshal()
	require.NoError(b, err)

	reqv2 := &mimirpb.PreallocWriteRequest{
		WriteRequest: mimirpb.WriteRequest{
			TimeseriesRW2: make([]mimirpb.TimeSeriesRW2, 10000),
			SymbolsRW2:    make([]string, 0, 1+1+10000),
		},
	}
	reqv2.SymbolsRW2 = append(reqv2.SymbolsRW2, "")
	reqv2.SymbolsRW2 = append(reqv2.SymbolsRW2, "__name__")
	for i := range reqv2.TimeseriesRW2 {
		reqv2.TimeseriesRW2[i] = mimirpb.TimeSeriesRW2{
			LabelsRefs: []uint32{V2RecordSymbolOffset + 1, V2RecordSymbolOffset + uint32(i) + 2},
			Samples:    []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
			Exemplars:  []mimirpb.ExemplarRW2{},
		}
		reqv2.SymbolsRW2 = append(reqv2.SymbolsRW2, fmt.Sprintf("series_%d", i))
	}
	v2bytes, err := reqv2.Marshal()
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("deserialize v1", func(b *testing.B) {
		for range b.N {
			wr := &mimirpb.PreallocWriteRequest{}
			err := DeserializeRecordContent(v1bytes, wr, 1)
			if err != nil {
				b.Fatal(err)
			}
			mimirpb.ReuseSlice(wr.Timeseries)
		}
	})

	b.Run("deserialize v2", func(b *testing.B) {
		for range b.N {
			wr := &mimirpb.PreallocWriteRequest{}
			err := DeserializeRecordContent(v2bytes, wr, 2)
			if err != nil {
				b.Fatal(err)
			}
			mimirpb.ReuseSlice(wr.Timeseries)
		}
	})
}

func BenchmarkRecordSerializer(b *testing.B) {
	numSeries := 2000
	numLabels := 30
	gen := rand.New(rand.NewSource(789456123))
	// Generate a WriteRequest.
	req := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, numSeries)}
	for i := 0; i < len(req.Timeseries); i++ {
		req.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
		for j := 0; j < numLabels; j++ {
			mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(fmt.Sprintf("label_%d", j), fmt.Sprintf("%d", gen.Uint64())))
		}
	}

	//v2s := versionTwoRecordSerializer{}
	//v1s := versionOneRecordSerializer{}

	/*b.Run("v2 serialize (full flow)", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := v2s.ToRecords(123, "user-1", req, 16000000)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("v1 serialize (full flow)", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, err := v1s.ToRecords(123, "user-1", req, 16000000)
			if err != nil {
				b.Fatal(err)
			}
		}
	})*/

	b.Run("v1 -> v2 convert", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			rwv2, err := mimirpb.FromWriteRequestToRW2Request(req, V2CommonSymbols, V2RecordSymbolOffset)
			if err != nil {
				b.Fatal("error %w", err)
			}
			if len(rwv2.TimeseriesRW2) == 0 {
				b.Fatal("unexpectedly empty")
			}
			mimirpb.ReuseRW2(rwv2)
		}
	})
}
