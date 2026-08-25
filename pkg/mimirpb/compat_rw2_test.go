// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"reflect"
	"strings"
	"testing"

	rw2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"github.com/xlab/treeprint"

	"github.com/grafana/mimir/pkg/util/rw2util"
	"github.com/grafana/mimir/pkg/util/test"
)

// Tests related to Prometheus Remote Write v2 (RW2) compatibility.

func TestRW2TypesCompatible(t *testing.T) {
	expectedType := reflect.TypeOf(rw2.Request{})
	actualType := reflect.TypeOf(WriteRequestRW2{})

	expectedTree := treeprint.NewWithRoot("<root>")
	// We ignore the XXX_ fields because RW2 in Prometheus has them,
	// but we don't. Which also means that the offsets would be different.
	// But we are not going to cast between the two types, so offsets
	// don't matter.
	test.AddTypeToTree(expectedType, expectedTree, false, true, true, false)

	actualTree := treeprint.NewWithRoot("<root>")
	test.AddTypeToTree(actualType, actualTree, false, true, true, false)

	// mimirpb.Sample fields order MUST match promql.FPoint so that we can
	// cast types between them. However this makes test.RequireSameShape
	// fail because the order is different.
	// So we need to reverse the order of the fields in the tree.
	// Also the name of the Timestamp field is slightly different in the
	// two types.
	var firstValue, secondValue string
	rootNode, _ := actualTree.(*treeprint.Node)
	firstValue, _ = rootNode.Nodes[1].Nodes[1].Nodes[0].Value.(string)
	secondValue, _ = rootNode.Nodes[1].Nodes[1].Nodes[1].Value.(string)
	rootNode.Nodes[1].Nodes[1].Nodes[0].Value = secondValue
	rootNode.Nodes[1].Nodes[1].Nodes[1].Value = strings.ReplaceAll(firstValue, "TimestampMs", "Timestamp")

	// Our Sample and Histogram messages now carry StartTimestamp (matching upstream's RW2 spec), and
	// TimeSeries/TimeSeriesRW2 no longer carry CreatedTimestamp (reserved), so the shapes should match directly.
	require.Equal(t, expectedTree.String(), actualTree.String(), "Proto types are not compatible")
}

func TestRW2Unmarshal(t *testing.T) {
	t.Run("rw2 compatible produces expected WriteRequest", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		expected := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
				Timeseries: []PreallocTimeseries{
					{
						TimeSeries: &TimeSeries{
							Labels: []LabelAdapter{
								{
									Name:  "__name__",
									Value: "test_metric_total",
								},
								{
									Name:  "job",
									Value: "test_job",
								},
							},
							Samples: []Sample{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
								},
							},
							Exemplars: []Exemplar{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
									Labels: []LabelAdapter{
										{
											Name:  "__name__",
											Value: "test_metric_total",
										},
										{
											Name:  "traceID",
											Value: "1234567890abcdef",
										},
									},
								},
							},
						},
					},
				},
				Metadata: []*MetricMetadata{
					{
						MetricFamilyName: "test_metric_total",
						Type:             COUNTER,
						Help:             "test_metric_help",
						Unit:             "test_metric_unit",
					},
				},
				unmarshalFromRW2: true,
			},
			UnmarshalFromRW2: true,
		}

		// Check that the unmarshalled data matches the original data.
		require.Equal(t, expected, &received)
	})

	t.Run("Sample and Histogram StartTimestamp round-trip via RW2", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		writeRequest := &WriteRequest{
			TimeseriesRW2: []TimeSeriesRW2{
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
					Samples: []Sample{
						{Value: 123.456, TimestampMs: 1000, StartTimestamp: 500},
					},
					Histograms: []Histogram{
						FromHistogramToHistogramProto(2000, test.GenerateTestHistogram(1)),
					},
				},
			},
		}
		writeRequest.TimeseriesRW2[0].Histograms[0].StartTimestamp = 1500
		writeRequest.SymbolsRW2 = syms.GetSymbols()
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Timeseries, 1)
		require.Len(t, received.Timeseries[0].Samples, 1)
		require.Equal(t, int64(500), received.Timeseries[0].Samples[0].StartTimestamp)
		require.Len(t, received.Timeseries[0].Histograms, 1)
		require.Equal(t, int64(1500), received.Timeseries[0].Histograms[0].StartTimestamp)
	})

	t.Run("Sample and Histogram StartTimestamp round-trip via RW1", func(t *testing.T) {
		writeRequest := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
						Samples: []Sample{
							{Value: 123.456, TimestampMs: 1000, StartTimestamp: 500},
						},
						Histograms: []Histogram{
							FromHistogramToHistogramProto(2000, test.GenerateTestHistogram(1)),
						},
					},
				},
			},
		}
		writeRequest.Timeseries[0].Histograms[0].StartTimestamp = 1500
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		received := PreallocWriteRequest{}
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Timeseries, 1)
		require.Len(t, received.Timeseries[0].Samples, 1)
		require.Equal(t, int64(500), received.Timeseries[0].Samples[0].StartTimestamp)
		require.Len(t, received.Timeseries[0].Histograms, 1)
		require.Equal(t, int64(1500), received.Timeseries[0].Histograms[0].StartTimestamp)
	})

	t.Run("legacy per-series created_timestamp (pre-final RW2 wire shape) fans out to Samples and Histograms", func(t *testing.T) {
		// Simulate a legacy sender that still sets the reserved TimeSeriesRW2 field 6 (formerly
		// created_timestamp) instead of a per-sample/per-histogram StartTimestamp. The current
		// TimeSeriesRW2 Go type can no longer express that field (it's reserved), so the wire bytes
		// are built by hand.
		syms := rw2util.NewSymbolTableBuilder(nil)
		ts := TimeSeriesRW2{
			LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000},
				{Value: 2, TimestampMs: 2000},
			},
			Histograms: []Histogram{
				FromHistogramToHistogramProto(3000, test.GenerateTestHistogram(1)),
			},
		}
		tsData, err := ts.Marshal()
		require.NoError(t, err)
		tsData = appendVarintField(tsData, 6, 500)

		var data []byte
		for _, s := range syms.GetSymbols() {
			data = appendBytesField(data, 4, []byte(s))
		}
		data = appendBytesField(data, 5, tsData)

		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Timeseries, 1)
		require.Len(t, received.Timeseries[0].Samples, 2)
		require.Equal(t, int64(500), received.Timeseries[0].Samples[0].StartTimestamp)
		require.Equal(t, int64(500), received.Timeseries[0].Samples[1].StartTimestamp)
		require.Len(t, received.Timeseries[0].Histograms, 1)
		require.Equal(t, int64(500), received.Timeseries[0].Histograms[0].StartTimestamp)
	})

	t.Run("legacy per-series created_timestamp does not override an explicit per-sample StartTimestamp", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		ts := TimeSeriesRW2{
			LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000, StartTimestamp: 900},
			},
		}
		tsData, err := ts.Marshal()
		require.NoError(t, err)
		tsData = appendVarintField(tsData, 6, 500)

		var data []byte
		for _, s := range syms.GetSymbols() {
			data = appendBytesField(data, 4, []byte(s))
		}
		data = appendBytesField(data, 5, tsData)

		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Timeseries, 1)
		require.Len(t, received.Timeseries[0].Samples, 1)
		require.Equal(t, int64(900), received.Timeseries[0].Samples[0].StartTimestamp)
	})

	t.Run("legacy per-series created_timestamp fans out via the plain TimeSeries.Unmarshal too", func(t *testing.T) {
		// The plain (non-RW2) TimeSeries.Unmarshal is used not just for Remote Write 1.0, but also
		// for internal distributor->ingester gRPC and ingest-storage Kafka records. A not-yet-
		// upgraded Mimir component in those internal paths would still encode the reserved field 6
		// (formerly created_timestamp) instead of a per-sample/per-histogram StartTimestamp, so
		// this must fan out here too, not just in UnmarshalRW2.
		ts := TimeSeries{
			Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000},
				{Value: 2, TimestampMs: 2000},
			},
			Histograms: []Histogram{
				FromHistogramToHistogramProto(3000, test.GenerateTestHistogram(1)),
			},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)
		data = appendVarintField(data, 6, 500)

		received := TimeSeries{}
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Samples, 2)
		require.Equal(t, int64(500), received.Samples[0].StartTimestamp)
		require.Equal(t, int64(500), received.Samples[1].StartTimestamp)
		require.Len(t, received.Histograms, 1)
		require.Equal(t, int64(500), received.Histograms[0].StartTimestamp)
	})

	t.Run("legacy per-series created_timestamp via the plain TimeSeries.Unmarshal does not override an explicit per-sample StartTimestamp", func(t *testing.T) {
		ts := TimeSeries{
			Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000, StartTimestamp: 900},
			},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)
		data = appendVarintField(data, 6, 500)

		received := TimeSeries{}
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Samples, 1)
		require.Equal(t, int64(900), received.Samples[0].StartTimestamp)
	})

	t.Run("zero timeseries does not panic", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		syms.GetSymbol("unused_symbol")
		req := &WriteRequest{
			SymbolsRW2: syms.GetSymbols(),
		}

		data, err := req.Marshal()
		require.NoError(t, err)

		received := PreallocWriteRequest{
			UnmarshalFromRW2: true,
		}
		require.NoError(t, received.Unmarshal(data))
		require.Empty(t, received.Timeseries)
		require.Empty(t, received.Metadata)
	})

	t.Run("metadata for all metric types map to expected values", func(t *testing.T) {
		tc := []struct {
			name    string
			rw2Type MetadataRW2_MetricType
			rw1Type MetricMetadata_MetricType
		}{
			{"UNKNOWN", METRIC_TYPE_UNSPECIFIED, UNKNOWN},
			{"COUNTER", METRIC_TYPE_COUNTER, COUNTER},
			{"GAUGE", METRIC_TYPE_GAUGE, GAUGE},
			{"HISTOGRAM", METRIC_TYPE_HISTOGRAM, HISTOGRAM},
			{"GAUGEHISTOGRAM", METRIC_TYPE_GAUGEHISTOGRAM, GAUGEHISTOGRAM},
			{"SUMMARY", METRIC_TYPE_SUMMARY, SUMMARY},
			{"INFO", METRIC_TYPE_INFO, INFO},
			{"STATESET", METRIC_TYPE_STATESET, STATESET},
		}

		for _, tt := range tc {
			t.Run(tt.name, func(t *testing.T) {
				syms := rw2util.NewSymbolTableBuilder(nil)
				writeRequest := &WriteRequest{
					TimeseriesRW2: []TimeSeriesRW2{
						{
							LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
							Metadata: MetadataRW2{
								Type:    tt.rw2Type,
								HelpRef: syms.GetSymbol("test_metric_help"),
								UnitRef: syms.GetSymbol("test_metric_unit"),
							},
						},
					},
				}
				writeRequest.SymbolsRW2 = syms.GetSymbols()
				data, err := writeRequest.Marshal()
				require.NoError(t, err)

				// Unmarshal the data back into Mimir's WriteRequest.
				received := PreallocWriteRequest{}
				received.UnmarshalFromRW2 = true
				err = received.Unmarshal(data)
				require.NoError(t, err)

				expected := &PreallocWriteRequest{
					WriteRequest: WriteRequest{
						Timeseries: []PreallocTimeseries{
							{
								TimeSeries: &TimeSeries{
									Labels: []LabelAdapter{
										{
											Name:  "__name__",
											Value: "test_metric_total",
										},
									},
									Samples:   []Sample{},
									Exemplars: []Exemplar{},
								},
							},
						},
						Metadata: []*MetricMetadata{
							{
								MetricFamilyName: "test_metric_total",
								Type:             tt.rw1Type,
								Help:             "test_metric_help",
								Unit:             "test_metric_unit",
							},
						},
						unmarshalFromRW2: true,
					},
					UnmarshalFromRW2: true,
				}
				// Check that the unmarshalled data matches the original data.
				require.Equal(t, expected, &received)
			})
		}
	})

	t.Run("metadata metric family name is normalized based on type", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		writeRequest := &WriteRequest{
			TimeseriesRW2: []TimeSeriesRW2{
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_summary_count")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_SUMMARY,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_summary_sum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_SUMMARY,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_bucket")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_count")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_sum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_bucket")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_gcount")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_gsum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
			},
		}
		writeRequest.SymbolsRW2 = syms.GetSymbols()
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		// 8 metadata carrier series
		require.Len(t, received.Timeseries, 8)
		expMetadata := []*MetricMetadata{
			{
				Type:             SUMMARY,
				MetricFamilyName: "test_metric_summary",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             HISTOGRAM,
				MetricFamilyName: "test_metric_histogram",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             GAUGEHISTOGRAM,
				MetricFamilyName: "test_metric_gaugehistogram",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
		}
		require.Equal(t, expMetadata, received.Metadata)
	})

	t.Run("metadata metric family name is not normalized if SkipNormalizeMetricName is set", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		writeRequest := &WriteRequest{
			TimeseriesRW2: []TimeSeriesRW2{
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_summary_count")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_SUMMARY,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_summary_sum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_SUMMARY,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_bucket")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_count")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_histogram_sum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_HISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_bucket")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_gcount")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_gaugehistogram_gsum")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_GAUGEHISTOGRAM,
						HelpRef: syms.GetSymbol("test_metric_help"),
						UnitRef: syms.GetSymbol("test_metric_unit"),
					},
				},
			},
		}
		writeRequest.SymbolsRW2 = syms.GetSymbols()
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{
			SkipNormalizeMetadataMetricName: true,
		}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		// 8 metadata carrier series
		require.Len(t, received.Timeseries, 8)
		expMetadata := []*MetricMetadata{
			{
				Type:             SUMMARY,
				MetricFamilyName: "test_metric_summary_count",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             SUMMARY,
				MetricFamilyName: "test_metric_summary_sum",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             HISTOGRAM,
				MetricFamilyName: "test_metric_histogram_bucket",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             HISTOGRAM,
				MetricFamilyName: "test_metric_histogram_count",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             HISTOGRAM,
				MetricFamilyName: "test_metric_histogram_sum",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             GAUGEHISTOGRAM,
				MetricFamilyName: "test_metric_gaugehistogram_bucket",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             GAUGEHISTOGRAM,
				MetricFamilyName: "test_metric_gaugehistogram_gcount",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
			{
				Type:             GAUGEHISTOGRAM,
				MetricFamilyName: "test_metric_gaugehistogram_gsum",
				Help:             "test_metric_help",
				Unit:             "test_metric_unit",
			},
		}
		require.Equal(t, expMetadata, received.Metadata)
	})

	t.Run("rw2 with offset produces expected WriteRequest", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, 256, nil)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		received.RW2SymbolOffset = 256
		err = received.Unmarshal(data)
		require.NoError(t, err)

		expected := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
				Timeseries: []PreallocTimeseries{
					{
						TimeSeries: &TimeSeries{
							Labels: []LabelAdapter{
								{
									Name:  "__name__",
									Value: "test_metric_total",
								},
								{
									Name:  "job",
									Value: "test_job",
								},
							},
							Samples: []Sample{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
								},
							},
							Exemplars: []Exemplar{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
									Labels: []LabelAdapter{
										{
											Name:  "__name__",
											Value: "test_metric_total",
										},
										{
											Name:  "traceID",
											Value: "1234567890abcdef",
										},
									},
								},
							},
						},
					},
				},
				Metadata: []*MetricMetadata{
					{
						MetricFamilyName: "test_metric_total",
						Type:             COUNTER,
						Help:             "test_metric_help",
						Unit:             "test_metric_unit",
					},
				},
				unmarshalFromRW2: true,
				rw2symbols:       rw2PagedSymbols{offset: 256},
			},
			UnmarshalFromRW2: true,
			RW2SymbolOffset:  256,
		}

		// Check that the unmarshalled data matches the original data.
		require.Equal(t, expected, &received)
	})

	t.Run("wrong offset fails to unmarshal", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, 256, nil)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		// If the offset is so high that references become invalid, reject the request.
		received.RW2SymbolOffset = 258
		err = received.Unmarshal(data)
		require.ErrorContains(t, err, "invalid")

		// Unmarshal the data back into Mimir's WriteRequest.
		received = PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		// If the offset is so low that references point to the common symbols range, with no common symbols defined,
		// fail the request.
		received.RW2SymbolOffset = 255
		err = received.Unmarshal(data)

		require.ErrorContains(t, err, "invalid")
	})

	t.Run("offset and shared symbols produces expected write request", func(t *testing.T) {
		commonSymbols := []string{"", "__name__", "job"}
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, uint32(len(commonSymbols)), commonSymbols)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		received.RW2SymbolOffset = uint32(len(commonSymbols))
		received.RW2CommonSymbols = commonSymbols
		err = received.Unmarshal(data)
		require.NoError(t, err)

		expected := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
				Timeseries: []PreallocTimeseries{
					{
						TimeSeries: &TimeSeries{
							Labels: []LabelAdapter{
								{
									Name:  "__name__",
									Value: "test_metric_total",
								},
								{
									Name:  "job",
									Value: "test_job",
								},
							},
							Samples: []Sample{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
								},
							},
							Exemplars: []Exemplar{
								{
									Value:       123.456,
									TimestampMs: 1234567890,
									Labels: []LabelAdapter{
										{
											Name:  "__name__",
											Value: "test_metric_total",
										},
										{
											Name:  "traceID",
											Value: "1234567890abcdef",
										},
									},
								},
							},
						},
					},
				},
				Metadata: []*MetricMetadata{
					{
						MetricFamilyName: "test_metric_total",
						Type:             COUNTER,
						Help:             "test_metric_help",
						Unit:             "test_metric_unit",
					},
				},
				unmarshalFromRW2: true,
				rw2symbols:       rw2PagedSymbols{offset: 3, commonSymbols: commonSymbols},
			},
			UnmarshalFromRW2: true,
			RW2SymbolOffset:  3,
			RW2CommonSymbols: commonSymbols,
		}

		// Check that the unmarshalled data matches the original data.
		require.Equal(t, expected, &received)
	})

	t.Run("common symbol received but none defined", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, 256, nil)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		writeRequest.TimeseriesRW2[0].LabelsRefs[0] = 128 // In the reserved space
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		received.RW2SymbolOffset = 256
		received.RW2CommonSymbols = nil
		err = received.Unmarshal(data)
		require.ErrorContains(t, err, "invalid")
	})

	t.Run("zero refs translate to empty string despite offset", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, 256, nil)
		writeRequest := &rw2.Request{
			Timeseries: []rw2.TimeSeries{
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("job"), syms.GetSymbol("test_job")},
					Samples: []rw2.Sample{
						{
							Value:     123.456,
							Timestamp: 1234567890,
						},
					},
					Exemplars: []rw2.Exemplar{
						{
							Value:      123.456,
							Timestamp:  1234567890,
							LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("traceID"), syms.GetSymbol("1234567890abcdef")},
						},
					},
					Metadata: rw2.Metadata{
						Type:    rw2.Metadata_METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("test_metric_help"),
						// UnitRef: left default!
					},
				},
			},
		}
		writeRequest.Symbols = syms.GetSymbols()
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		received.RW2SymbolOffset = 256
		err = received.Unmarshal(data)
		require.NoError(t, err)
		require.Equal(t, "", received.Metadata[0].Unit)
	})

	t.Run("common symbol out of bounds", func(t *testing.T) {
		commonSyms := []string{"__name__"}
		syms := rw2util.NewSymbolTableBuilderWithCommon(nil, 256, commonSyms)
		// Create a new WriteRequest with some sample data.
		writeRequest := makeTestRW2WriteRequest(syms)
		writeRequest.TimeseriesRW2[0].LabelsRefs[0] = 1 // Out of bounds common symbol.
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		received.RW2SymbolOffset = 256
		received.RW2CommonSymbols = commonSyms
		err = received.Unmarshal(data)
		require.ErrorContains(t, err, "invalid")
	})

	t.Run("messages where the first symbol is not empty string are rejected", func(t *testing.T) {
		writeRequest := &rw2.Request{
			Symbols: []string{"__name__", "test_metric_total", "job", "my_job"},
			Timeseries: []rw2.TimeSeries{
				{
					LabelsRefs: []uint32{0, 1, 2, 3},
					Samples: []rw2.Sample{
						{
							Value:     123.456,
							Timestamp: 1234567890,
						},
					},
				},
			},
		}
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.ErrorContains(t, err, "symbols must start with empty string")
	})

	t.Run("metadata order is deterministic", func(t *testing.T) {
		const numRuns = 1000

		for range numRuns {
			syms := rw2util.NewSymbolTableBuilder(nil)
			// Create a new WriteRequest with some sample data.
			writeRequest := makeTestRW2WriteRequest(syms)
			writeRequest.TimeseriesRW2 = []TimeSeriesRW2{
				// Keep the one we already built
				writeRequest.TimeseriesRW2[0],
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_2")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("metric_2 help text."),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_3")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("metric_3 help text."),
					},
				},
				// Duplicate, should be filtered out.
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_2")},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("duplicated metric_2 help text, but different."),
					},
				},
			}
			writeRequest.SymbolsRW2 = syms.GetSymbols()
			data, err := writeRequest.Marshal()
			require.NoError(t, err)

			// Unmarshal the data back into Mimir's WriteRequest.
			received := PreallocWriteRequest{}
			received.UnmarshalFromRW2 = true
			err = received.Unmarshal(data)
			require.NoError(t, err)

			require.Len(t, received.Metadata, 3)
			require.Equal(t, "test_metric_total", received.Metadata[0].MetricFamilyName)
			require.Equal(t, "metric_2", received.Metadata[1].MetricFamilyName)
			require.Equal(t, "metric_3", received.Metadata[2].MetricFamilyName)

			require.Equal(t, "metric_2 help text.", received.Metadata[1].Help)
		}
	})

	t.Run("conflicting metadata, first metadata wins by default", func(t *testing.T) {
		writeRequest := &WriteRequest{
			SymbolsRW2: []string{"", "__name__", "my_cool_series", "It's a cool series, but old description.", "It's a cool series, but new description.", "megawatts"},
			TimeseriesRW2: []TimeSeriesRW2{
				{
					LabelsRefs: []uint32{1, 2},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: 3,
						UnitRef: 5,
					},
				},
				{
					LabelsRefs: []uint32{1, 2},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: 4,
						UnitRef: 5,
					},
				},
			},
		}
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Metadata, 1)
		require.Equal(t, received.Metadata[0].MetricFamilyName, "my_cool_series")
		require.Equal(t, received.Metadata[0].Type, COUNTER)
		require.Equal(t, received.Metadata[0].Help, "It's a cool series, but old description.")
		require.Equal(t, received.Metadata[0].Unit, "megawatts")
	})

	t.Run("conflicting metadata, skipDeduplicateMetadata is true, both metadata and their order is preserved", func(t *testing.T) {
		writeRequest := &WriteRequest{
			SymbolsRW2: []string{"", "__name__", "my_cool_series", "It's a cool series, but old description.", "It's a cool series, but new description.", "megawatts"},
			TimeseriesRW2: []TimeSeriesRW2{
				{
					LabelsRefs: []uint32{1, 2},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: 3,
						UnitRef: 5,
					},
				},
				{
					LabelsRefs: []uint32{1, 2},
					Metadata: MetadataRW2{
						Type:    METRIC_TYPE_COUNTER,
						HelpRef: 4,
						UnitRef: 5,
					},
				},
			},
		}
		data, err := writeRequest.Marshal()
		require.NoError(t, err)

		// Unmarshal the data back into Mimir's WriteRequest.
		received := PreallocWriteRequest{
			SkipDeduplicateMetadata: true,
		}
		received.UnmarshalFromRW2 = true
		err = received.Unmarshal(data)
		require.NoError(t, err)

		require.Len(t, received.Metadata, 2)
		require.Equal(t, received.Metadata[0].MetricFamilyName, "my_cool_series")
		require.Equal(t, received.Metadata[0].Type, COUNTER)
		require.Equal(t, received.Metadata[0].Help, "It's a cool series, but old description.")
		require.Equal(t, received.Metadata[0].Unit, "megawatts")
		require.Equal(t, received.Metadata[1].MetricFamilyName, "my_cool_series")
		require.Equal(t, received.Metadata[1].Type, COUNTER)
		require.Equal(t, received.Metadata[1].Help, "It's a cool series, but new description.")
		require.Equal(t, received.Metadata[1].Unit, "megawatts")
	})
}

// TestMarshalLegacyCreatedTimestamp verifies that TimeSeries/TimeSeriesRW2 marshalling still
// writes the reserved per-series created_timestamp field (6), for the benefit of not-yet-upgraded
// readers (e.g. during a rolling upgrade) that only understand that field, not the newer
// per-sample/per-histogram StartTimestamp fields.
func TestMarshalLegacyCreatedTimestamp(t *testing.T) {
	t.Run("TimeSeries.Marshal writes the first sample's StartTimestamp into the legacy field", func(t *testing.T) {
		ts := TimeSeries{
			Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000, StartTimestamp: 500},
				{Value: 2, TimestampMs: 2000, StartTimestamp: 500},
			},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)

		got, found := readVarintField(data, 6)
		require.True(t, found)
		require.Equal(t, int64(500), got)
	})

	t.Run("TimeSeries.Marshal falls back to the first histogram's StartTimestamp when there are no samples", func(t *testing.T) {
		ts := TimeSeries{
			Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Histograms: []Histogram{
				FromHistogramToHistogramProto(3000, test.GenerateTestHistogram(1)),
			},
		}
		ts.Histograms[0].StartTimestamp = 700
		data, err := ts.Marshal()
		require.NoError(t, err)

		got, found := readVarintField(data, 6)
		require.True(t, found)
		require.Equal(t, int64(700), got)
	})

	t.Run("TimeSeries.Marshal omits the legacy field when there is no start timestamp to report", func(t *testing.T) {
		ts := TimeSeries{
			Labels:  []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Samples: []Sample{{Value: 1, TimestampMs: 1000}},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)

		_, found := readVarintField(data, 6)
		require.False(t, found)
	})

	t.Run("TimeSeriesRW2.Marshal writes the first sample's StartTimestamp into the legacy field", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		ts := TimeSeriesRW2{
			LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000, StartTimestamp: 500},
			},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)

		got, found := readVarintField(data, 6)
		require.True(t, found)
		require.Equal(t, int64(500), got)
	})

	t.Run("TimeSeriesRW2.Marshal falls back to the first histogram's StartTimestamp when there are no samples", func(t *testing.T) {
		syms := rw2util.NewSymbolTableBuilder(nil)
		ts := TimeSeriesRW2{
			LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total")},
			Histograms: []Histogram{
				FromHistogramToHistogramProto(3000, test.GenerateTestHistogram(1)),
			},
		}
		ts.Histograms[0].StartTimestamp = 700
		data, err := ts.Marshal()
		require.NoError(t, err)

		got, found := readVarintField(data, 6)
		require.True(t, found)
		require.Equal(t, int64(700), got)
	})

	t.Run("a round trip through Marshal and Unmarshal by an upgraded reader is unaffected", func(t *testing.T) {
		// The legacy field is redundant once decoded by an upgraded reader: every sample/histogram
		// already carries its own StartTimestamp from the modern fields, so the fan-out in
		// Unmarshal (which only fills in a zero StartTimestamp) is a no-op here.
		ts := TimeSeries{
			Labels: []LabelAdapter{{Name: "__name__", Value: "test_metric_total"}},
			Samples: []Sample{
				{Value: 1, TimestampMs: 1000, StartTimestamp: 500},
				{Value: 2, TimestampMs: 2000, StartTimestamp: 900},
			},
		}
		data, err := ts.Marshal()
		require.NoError(t, err)

		received := TimeSeries{}
		require.NoError(t, received.Unmarshal(data))
		require.Equal(t, ts, received)
	})
}

func makeTestRW2WriteRequest(syms *rw2util.SymbolTableBuilder) *WriteRequest {
	req := &WriteRequest{
		TimeseriesRW2: []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("job"), syms.GetSymbol("test_job")},
				Samples: []Sample{
					{
						Value:       123.456,
						TimestampMs: 1234567890,
					},
				},
				Exemplars: []ExemplarRW2{
					{
						Value:      123.456,
						Timestamp:  1234567890,
						LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("test_metric_total"), syms.GetSymbol("traceID"), syms.GetSymbol("1234567890abcdef")},
					},
				},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: syms.GetSymbol("test_metric_help"),
					UnitRef: syms.GetSymbol("test_metric_unit"),
				},
			},
		},
	}
	req.SymbolsRW2 = syms.GetSymbols()

	return req
}

// appendVarint appends v to buf using standard protobuf varint encoding.
func appendVarint(buf []byte, v uint64) []byte {
	for v >= 0x80 {
		buf = append(buf, byte(v)|0x80)
		v >>= 7
	}
	return append(buf, byte(v))
}

// appendVarintField appends a varint-wiretype field (tag + value) to buf, to hand-craft wire
// messages containing fields that are no longer expressible through the generated Go types
// (e.g. reserved fields).
func appendVarintField(buf []byte, fieldNum int, value int64) []byte {
	buf = appendVarint(buf, uint64(fieldNum)<<3) // wire type 0: varint
	return appendVarint(buf, uint64(value))
}

// appendBytesField appends a length-delimited-wiretype field (tag + length + data) to buf.
func appendBytesField(buf []byte, fieldNum int, data []byte) []byte {
	buf = appendVarint(buf, uint64(fieldNum)<<3|2)
	buf = appendVarint(buf, uint64(len(data)))
	return append(buf, data...)
}

// readVarint reads a standard protobuf varint from the start of buf, returning the value and the
// number of bytes consumed (0 if buf doesn't start with a valid varint).
func readVarint(buf []byte) (uint64, int) {
	var v uint64
	for i, b := range buf {
		v |= uint64(b&0x7F) << (7 * i)
		if b < 0x80 {
			return v, i + 1
		}
	}
	return 0, 0
}

// readVarintField scans data for the first occurrence of fieldNum with a varint wire type and
// returns its value, without going through the (symbol-aware) generated Unmarshal path. Used to
// verify legacy-compat fields are actually present on the wire, independent of whether the
// current Go types can still decode them into a named field.
func readVarintField(data []byte, fieldNum int) (int64, bool) {
	i := 0
	for i < len(data) {
		tag, n := readVarint(data[i:])
		if n == 0 {
			return 0, false
		}
		i += n
		fn := int(tag >> 3)
		wt := int(tag & 0x7)
		if fn == fieldNum && wt == 0 {
			v, n := readVarint(data[i:])
			if n == 0 {
				return 0, false
			}
			return int64(v), true
		}
		switch wt {
		case 0: // varint
			_, n := readVarint(data[i:])
			if n == 0 {
				return 0, false
			}
			i += n
		case 1: // fixed64
			i += 8
		case 2: // length-delimited
			l, n := readVarint(data[i:])
			if n == 0 {
				return 0, false
			}
			i += n + int(l)
		case 5: // fixed32
			i += 4
		default:
			return 0, false
		}
	}
	return 0, false
}
