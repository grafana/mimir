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
		writeRequest.Timeseries[0].LabelsRefs[0] = 128 // In the reserved space
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
		writeRequest.Timeseries[0].LabelsRefs[0] = 1 // Out of bounds common symbol.
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
			writeRequest.Timeseries = []rw2.TimeSeries{
				// Keep the one we already built
				writeRequest.Timeseries[0],
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_2")},
					Metadata: rw2.Metadata{
						Type:    rw2.Metadata_METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("metric_2 help text."),
					},
				},
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_3")},
					Metadata: rw2.Metadata{
						Type:    rw2.Metadata_METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("metric_3 help text."),
					},
				},
				// Duplicate, should be filtered out.
				{
					LabelsRefs: []uint32{syms.GetSymbol("__name__"), syms.GetSymbol("metric_2")},
					Metadata: rw2.Metadata{
						Type:    rw2.Metadata_METRIC_TYPE_COUNTER,
						HelpRef: syms.GetSymbol("duplicated metric_2 help text, but different."),
					},
				},
			}
			writeRequest.Symbols = syms.GetSymbols()
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
}

func makeTestRW2WriteRequest(syms *test.SymbolTableBuilder) *rw2.Request {
	req := &rw2.Request{
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
					UnitRef: syms.GetSymbol("test_metric_unit"),
				},
			},
		},
	}
	req.Symbols = syms.GetSymbols()

	return req
}
