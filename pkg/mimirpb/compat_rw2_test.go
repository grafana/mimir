// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"reflect"
	"strings"
	"testing"

	rw2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
	"github.com/xlab/treeprint"

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

	require.Equal(t, expectedTree.String(), actualTree.String(), "Proto types are not compatible")
}

func TestRW2Unmarshal(t *testing.T) {
	t.Run("rw2 compatible produces expected WriteRequest", func(t *testing.T) {
		syms := test.NewSymbolTableBuilder(nil)
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
				syms := test.NewSymbolTableBuilder(nil)
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
		syms := test.NewSymbolTableBuilder(nil)
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
		syms := test.NewSymbolTableBuilder(nil)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, 256, nil)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, 256, nil)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, uint32(len(commonSymbols)), commonSymbols)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, 256, nil)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, 256, nil)
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
		syms := test.NewSymbolTableBuilderWithCommon(nil, 256, commonSyms)
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
			syms := test.NewSymbolTableBuilder(nil)
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

func makeTestRW2WriteRequest(syms *test.SymbolTableBuilder) *WriteRequest {
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
