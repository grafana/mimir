// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/rw2util"
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
		bufh := wr.BufferHolder
		defer bufh.FreeBuffer()
		wr.BufferHolder = mimirpb.BufferHolder{} // We don't want to compare this.
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
		bufh := wr.BufferHolder
		defer bufh.FreeBuffer()
		wr.BufferHolder = mimirpb.BufferHolder{} // We don't want to compare this.
		require.Equal(t, reqv1, &wr)
	})

	t.Run("v2", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, V2RecordSymbolOffset, V2CommonSymbols.GetSlice())
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
		// A series with data keeps its metadata as its own: records carry metric families' in series without data.
		require.Equal(t, mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Help for test_metric_total", Unit: "seconds"}, wr.Timeseries[0].SeriesMetadata)
		require.Empty(t, wr.Metadata)
	})

	t.Run("v2 skips metadata normalization", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, V2RecordSymbolOffset, V2CommonSymbols.GetSlice())
		reqv2 := &mimirpb.WriteRequestRW2{
			Timeseries: []mimirpb.TimeSeriesRW2{{
				LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_histogram_seconds_bucket")},
				Metadata: mimirpb.MetadataRW2{
					Type:    mimirpb.METRIC_TYPE_HISTOGRAM,
					HelpRef: syms.GetSymbol("Help for test_histogram_seconds_bucket"),
					UnitRef: syms.GetSymbol("seconds"),
				},
			}},
		}
		reqv2.Symbols = syms.GetSymbols()
		// Symbols should not contain common labels "__name__" and "job"
		require.Equal(t, []string{"", "test_histogram_seconds_bucket", "Help for test_histogram_seconds_bucket", "seconds"}, reqv2.Symbols)
		v2bytes, err := reqv2.Marshal()
		require.NoError(t, err)

		wr := mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(v2bytes, &wr, 2)
		require.NoError(t, err)
		require.Len(t, wr.Timeseries, 1)
		expLabels := []mimirpb.LabelAdapter{{Name: "__name__", Value: "test_histogram_seconds_bucket"}}
		require.Equal(t, expLabels, wr.Timeseries[0].Labels)
		require.Empty(t, wr.Timeseries[0].Samples)
		require.Empty(t, wr.Timeseries[0].Exemplars)
		require.Empty(t, wr.Timeseries[0].Histograms)
		expMetadata := []*mimirpb.MetricMetadata{{
			Type:             mimirpb.HISTOGRAM,
			MetricFamilyName: "test_histogram_seconds_bucket",
			Help:             "Help for test_histogram_seconds_bucket",
			Unit:             "seconds",
		}}
		require.Equal(t, expMetadata, wr.Metadata)
	})

	t.Run("v2 preserves conflicting metadata", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, V2RecordSymbolOffset, V2CommonSymbols.GetSlice())
		reqv2 := &mimirpb.WriteRequestRW2{
			Timeseries: []mimirpb.TimeSeriesRW2{
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_histogram_seconds_bucket")},
					Metadata: mimirpb.MetadataRW2{
						Type:    mimirpb.METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("Help for test_histogram_seconds_bucket"),
						UnitRef: syms.GetSymbol("seconds"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_histogram_seconds_bucket")},
					Metadata: mimirpb.MetadataRW2{
						Type:    mimirpb.METRIC_TYPE_GAUGE,
						HelpRef: syms.GetSymbol("Help for test_histogram_seconds_bucket, but different"),
						UnitRef: syms.GetSymbol("seconds"),
					},
				},
			},
		}
		reqv2.Symbols = syms.GetSymbols()
		// Symbols should not contain common labels "__name__" and "job"
		require.Equal(t, []string{"", "test_histogram_seconds_bucket", "Help for test_histogram_seconds_bucket", "seconds", "Help for test_histogram_seconds_bucket, but different"}, reqv2.Symbols)
		v2bytes, err := reqv2.Marshal()
		require.NoError(t, err)

		wr := mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(v2bytes, &wr, 2)
		require.NoError(t, err)
		require.Len(t, wr.Timeseries, 2)
		expMetadata := []*mimirpb.MetricMetadata{
			{
				Type:             mimirpb.HISTOGRAM,
				MetricFamilyName: "test_histogram_seconds_bucket",
				Help:             "Help for test_histogram_seconds_bucket",
				Unit:             "seconds",
			},
			{
				Type:             mimirpb.GAUGE,
				MetricFamilyName: "test_histogram_seconds_bucket",
				Help:             "Help for test_histogram_seconds_bucket, but different",
				Unit:             "seconds",
			},
		}
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
					Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_1", "pod", "test-application-123456")),
					Samples:    []mimirpb.Sample{{TimestampMs: 20}},
					Exemplars:  []mimirpb.Exemplar{{TimestampMs: 30}},
					Histograms: []mimirpb.Histogram{{Timestamp: 10}},
				}},
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_2", "pod", "test-application-123456")),
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
		inputSize := req.Size() // Capture input RW1 size before serialization
		records, returnedSize, err := serializer.ToRecords("test", 1234, "user-1", req, 100000)
		require.NoError(t, err)
		require.Len(t, records, 1)
		record := records[0]

		// Verify that ToRecords returns the input size (RW1), not the output size (RW2).
		require.Equal(t, inputSize, returnedSize, "ToRecords should return input RW1 size, not output RW2 size")

		require.Equal(t, "test", record.Topic)
		require.Equal(t, int32(1234), record.Partition)
		require.Equal(t, 2, ParseRecordVersion(record))

		resultReq := &mimirpb.PreallocWriteRequest{}
		err = DeserializeRecordContent(record.Value, resultReq, 2)
		require.NoError(t, err)

		require.Len(t, resultReq.Timeseries, 5)
		require.Equal(t, req.Timeseries, resultReq.Timeseries[0:2])

		// The only way to carry a metadata in RW2.0 is attached to a timeseries.
		// Each metric family's metadata goes in an extra timeseries without data, which stands in for the family.
		var expMetadataSeries []mimirpb.PreallocTimeseries
		for _, name := range []string{"series_1", "series_2", "series_3"} {
			expMetadataSeries = append(expMetadataSeries, mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
				Labels:    mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, name)),
				Samples:   []mimirpb.Sample{},
				Exemplars: []mimirpb.Exemplar{},
			}})
		}
		require.Equal(t, expMetadataSeries, resultReq.Timeseries[2:])

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

	v2s := versionTwoRecordSerializer{}
	v1s := versionOneRecordSerializer{}

	b.Run("v2 serialize (full flow)", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _, err := v2s.ToRecords("test", 123, "user-1", req, 16000000)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("v1 serialize (full flow)", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _, err := v1s.ToRecords("test", 123, "user-1", req, 16000000)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

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

// Only version 2 records carry resources: the others don't carry the series' refs, so they leave out the table.
func TestRecordSerializerResources(t *testing.T) {
	newRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
				Labels:      mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "series_1")),
				Samples:     []mimirpb.Sample{{TimestampMs: 20}},
				ResourceRef: 1,
			}}},
			ResourceTable: []mimirpb.ResourceEntry{{Attributes: []mimirpb.ResourceAttribute{{Key: "service.name", StringValue: "frontend"}}}},
		}
	}
	for version, serializer := range map[int]recordSerializer{0: versionZeroRecordSerializer{}, 1: versionOneRecordSerializer{}, 2: versionTwoRecordSerializer{}} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			req := newRequest()
			records, _, err := serializer.ToRecords("test", 1, "user-1", req, 100000)
			require.NoError(t, err)
			require.Len(t, records, 1)
			require.Equal(t, newRequest(), req, "the request isn't changed")

			decoded := &mimirpb.PreallocWriteRequest{}
			require.NoError(t, DeserializeRecordContent(records[0].Value, decoded, version))
			if version < 2 {
				require.Empty(t, decoded.ResourceTable)
				return
			}
			require.Equal(t, req.ResourceTable, decoded.ResourceTable)
			require.Equal(t, uint32(1), decoded.Timeseries[0].ResourceRef)
		})
	}
}

// The v2 records' CPU and bytes per sample, with resources and without, for 100 series per resource.
func BenchmarkRecordSerializerResources(b *testing.B) {
	const (
		numSeries         = 2000
		seriesPerResource = 100
	)
	newRequest := func(resources bool) *mimirpb.WriteRequest {
		req := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, numSeries)}
		for i := range req.Timeseries {
			req.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
		}
		if !resources {
			return req
		}
		for r := range numSeries / seriesPerResource {
			entry := mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
				{Key: "service.name", StringValue: fmt.Sprintf("service-%d", r)},
				{Key: "service.instance.id", StringValue: fmt.Sprintf("instance-%d", r)},
			}}
			for i := range 18 {
				entry.Attributes = append(entry.Attributes, mimirpb.ResourceAttribute{Key: fmt.Sprintf("k8s.label.%d", i), StringValue: fmt.Sprintf("value-%d-%d", r, i)})
			}
			req.ResourceTable = append(req.ResourceTable, entry)
		}
		for i := range req.Timeseries {
			req.Timeseries[i].ResourceRef = uint32(i/seriesPerResource) + 1
		}
		return req
	}

	for _, resources := range []bool{false, true} {
		b.Run(fmt.Sprintf("resources=%t", resources), func(b *testing.B) {
			req := newRequest(resources)
			var bytes int
			for b.Loop() {
				records, _, err := versionTwoRecordSerializer{}.ToRecords("test", 123, "user-1", req, 16000000)
				if err != nil {
					b.Fatal(err)
				}
				bytes = len(records[0].Value)
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*numSeries), "ns/sample")
			b.ReportMetric(float64(bytes)/numSeries, "bytes/sample")
		})
	}
}
