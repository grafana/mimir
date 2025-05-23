// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/test"
)

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
		syms := test.NewSymbolTableBuilderWithOffset(nil, V2RecordSymbolOffset)
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
