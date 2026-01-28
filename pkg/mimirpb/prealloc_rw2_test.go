// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"reflect"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteRequestRW2Conversion(t *testing.T) {
	t.Run("WriteRequest preserves Mimir meta options", func(t *testing.T) {
		req := &WriteRequest{
			SkipLabelValidation:             true,
			SkipLabelCountValidation:        true,
			skipUnmarshalingExemplars:       true,
			skipNormalizeMetadataMetricName: true,
			skipDeduplicateMetadata:         true,
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		require.NoError(t, err)
		require.True(t, rw2.SkipLabelValidation)
		require.True(t, rw2.SkipLabelCountValidation)
		require.True(t, rw2.skipUnmarshalingExemplars)
		require.True(t, rw2.skipNormalizeMetadataMetricName)
		require.True(t, rw2.skipDeduplicateMetadata)
	})

	t.Run("nil request turns into nil request", func(t *testing.T) {
		var req *WriteRequest

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		require.NoError(t, err)
		require.Nil(t, rw2)
	})

	t.Run("WriteRequest Source", func(t *testing.T) {
		tc := []struct {
			name string
			in   WriteRequest_SourceEnum
			out  WriteRequest_SourceEnum
		}{
			{
				name: "api",
				in:   API,
				out:  API,
			},
			{
				name: "rule",
				in:   RULE,
				out:  RULE,
			},
			{
				name: "default",
				in:   0,
				out:  API,
			},
		}

		for _, tt := range tc {
			req := &WriteRequest{
				Source: tt.in,
			}

			rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

			require.NoError(t, err)
			require.Equal(t, tt.out, rw2.Source)
		}
	})

	t.Run("TimeSeries CreatedTimestamp", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						CreatedTimestamp: 1234567890,
					},
				},
				{
					TimeSeries: &TimeSeries{
						CreatedTimestamp: 1234567900,
					},
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		require.NoError(t, err)
		require.Len(t, rw2.TimeseriesRW2, 2)
		require.Equal(t, int64(1234567890), rw2.TimeseriesRW2[0].CreatedTimestamp)
		require.Equal(t, int64(1234567900), rw2.TimeseriesRW2[1].CreatedTimestamp)
	})

	t.Run("samples", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
					},
				},
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "asdf/jkl"})),
						Samples: []Sample{{Value: 135, TimestampMs: 1234567890}, {Value: 246, TimestampMs: 1234567900}},
					},
				},
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_other_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 112, TimestampMs: 1234567890}, {Value: 358, TimestampMs: 1234567900}},
					},
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "job", "foo/bar", "asdf/jkl", "my_other_cool_series"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
			},
			{
				LabelsRefs: []uint32{1, 2, 3, 5},
				Samples:    []Sample{{Value: 135, TimestampMs: 1234567890}, {Value: 246, TimestampMs: 1234567900}},
			},
			{
				LabelsRefs: []uint32{1, 6, 3, 4},
				Samples:    []Sample{{Value: 112, TimestampMs: 1234567890}, {Value: 358, TimestampMs: 1234567900}},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("histograms", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels: FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_histogram", "job": "foo/bar"})),
						Histograms: []Histogram{
							{
								Count:  &Histogram_CountInt{CountInt: 200},
								Sum:    1000,
								Schema: -1,
								PositiveSpans: []BucketSpan{
									{
										Offset: 0,
										Length: 2,
									},
								},
								PositiveDeltas: []int64{150, -100},
								Timestamp:      1234567890,
							},
						},
					},
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_histogram", "job", "foo/bar"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Histograms: []Histogram{
					{
						Count:  &Histogram_CountInt{CountInt: 200},
						Sum:    1000,
						Schema: -1,
						PositiveSpans: []BucketSpan{
							{
								Offset: 0,
								Length: 2,
							},
						},
						PositiveDeltas: []int64{150, -100},
						Timestamp:      1234567890,
					},
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("exemplars", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels: FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_histogram", "job": "foo/bar"})),
						Exemplars: []Exemplar{
							{
								Labels:      FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"trace_id": "abc123", "span_id": "def456"})),
								Value:       42.5,
								TimestampMs: 1234567890,
							},
						},
					},
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_histogram", "job", "foo/bar", "span_id", "def456", "trace_id", "abc123"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Exemplars: []ExemplarRW2{
					{
						LabelsRefs: []uint32{5, 6, 7, 8},
						Value:      42.5,
						Timestamp:  1234567890,
					},
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("metadata matching existing series", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
					},
				},
			},
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "job", "foo/bar", "It's a cool series.", "megawatts"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 5,
					UnitRef: 6,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("metadata without corresponding series", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
					},
				},
			},
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series_2",
					Help:             "It's a second cool series.",
					Unit:             "gigawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "job", "foo/bar", "It's a cool series.", "megawatts", "my_cool_series_2", "It's a second cool series.", "gigawatts"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 5,
					UnitRef: 6,
				},
			},
			{
				LabelsRefs: []uint32{1, 7},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 8,
					UnitRef: 9,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("metadata only", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: nil,
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "It's a cool series.", "megawatts"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 3,
					UnitRef: 4,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("metadata with UNKNOWN type", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: nil,
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
				{
					Type:             UNKNOWN,
					MetricFamilyName: "my_cool_series_2",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "It's a cool series.", "megawatts", "my_cool_series_2"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 3,
					UnitRef: 4,
				},
			},
			{
				LabelsRefs: []uint32{1, 5},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_UNSPECIFIED,
					HelpRef: 3,
					UnitRef: 4,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("when matching to timeseries, repeated metadata are deduplicated", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
					},
				},
			},
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series.",
					Unit:             "megawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "job", "foo/bar", "It's a cool series.", "megawatts"}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 5,
					UnitRef: 6,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})

	t.Run("when matching to timeseries, and metadata conflicts, the first one is matched and the rest are preserved", func(t *testing.T) {
		req := &WriteRequest{
			Timeseries: []PreallocTimeseries{
				{
					TimeSeries: &TimeSeries{
						Labels:  FromLabelsToLabelAdapters(labels.FromMap(map[string]string{"__name__": "my_cool_series", "job": "foo/bar"})),
						Samples: []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
					},
				},
			},
			Metadata: []*MetricMetadata{
				{
					Type:             COUNTER,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series, but old description.",
					Unit:             "megawatts",
				},
				{
					Type:             GAUGE,
					MetricFamilyName: "my_cool_series",
					Help:             "It's a cool series, but new description.",
					Unit:             "megawatts",
				},
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req, nil, 0)

		expSymbols := []string{"", "__name__", "my_cool_series", "job", "foo/bar", "It's a cool series, but old description.", "megawatts", "It's a cool series, but new description."}
		expTimeseries := []TimeSeriesRW2{
			{
				LabelsRefs: []uint32{1, 2, 3, 4},
				Samples:    []Sample{{Value: 123, TimestampMs: 1234567890}, {Value: 456, TimestampMs: 1234567900}},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_COUNTER,
					HelpRef: 5,
					UnitRef: 6,
				},
			},
			{
				LabelsRefs: []uint32{1, 2},
				Metadata: MetadataRW2{
					Type:    METRIC_TYPE_GAUGE,
					HelpRef: 7,
					UnitRef: 6,
				},
			},
		}
		require.NoError(t, err)
		require.Nil(t, rw2.Timeseries)
		require.Nil(t, rw2.Metadata)
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})
}

func TestExemplarConversion(t *testing.T) {
	symbols := writev2.NewSymbolTable()
	// Pre-populate symbols table with some common values
	symbols.Symbolize("trace_id")
	symbols.Symbolize("abc123")
	symbols.Symbolize("span_id")
	symbols.Symbolize("def456")

	tests := []struct {
		name string
		v1   []Exemplar
		v2   []ExemplarRW2
	}{
		{
			name: "empty exemplars",
			v1:   []Exemplar{},
			v2:   []ExemplarRW2{},
		},
		{
			name: "single exemplar with trace and span",
			v1: []Exemplar{
				{
					Labels: []LabelAdapter{
						{Name: "trace_id", Value: "abc123"},
						{Name: "span_id", Value: "def456"},
					},
					Value:       42.5,
					TimestampMs: 1234567890,
				},
			},
			v2: []ExemplarRW2{
				{
					LabelsRefs: []uint32{1, 2, 3, 4},
					Value:      42.5,
					Timestamp:  1234567890,
				},
			},
		},
		{
			name: "multiple exemplars",
			v1: []Exemplar{
				{
					Labels: []LabelAdapter{
						{Name: "trace_id", Value: "abc123"},
					},
					Value:       42.5,
					TimestampMs: 1234567890,
				},
				{
					Labels: []LabelAdapter{
						{Name: "span_id", Value: "def456"},
					},
					Value:       43.5,
					TimestampMs: 1234567891,
				},
			},
			v2: []ExemplarRW2{
				{
					LabelsRefs: []uint32{1, 2},
					Value:      42.5,
					Timestamp:  1234567890,
				},
				{
					LabelsRefs: []uint32{3, 4},
					Value:      43.5,
					Timestamp:  1234567891,
				},
			},
		},
		{
			name: "exemplar with zero value and timestamp",
			v1: []Exemplar{
				{
					Labels: []LabelAdapter{
						{Name: "trace_id", Value: "abc123"},
					},
					Value:       0,
					TimestampMs: 0,
				},
			},
			v2: []ExemplarRW2{
				{
					LabelsRefs: []uint32{1, 2},
					Value:      0,
					Timestamp:  0,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotV2 := FromExemplarsToExemplarsRW2(tt.v1, &symbols)
			require.Equal(t, tt.v2, gotV2)
		})
	}
}

func TestMetricMetadataConversion(t *testing.T) {
	symbols := writev2.NewSymbolTable()
	symbols.Symbolize("help text")
	symbols.Symbolize("unit text")

	tests := []struct {
		name    string
		v1      *MetricMetadata
		v2      MetadataRW2
		symbols []string
	}{
		{
			name: "complete metadata",
			v1: &MetricMetadata{
				Type: COUNTER,
				Help: "help text",
				Unit: "unit text",
			},
			v2: MetadataRW2{
				Type:    MetadataRW2_MetricType(COUNTER),
				HelpRef: 1,
				UnitRef: 2,
			},
			symbols: symbols.Symbols(),
		},
		{
			name: "metadata without help and unit",
			v1: &MetricMetadata{
				Type: GAUGE,
			},
			v2: MetadataRW2{
				Type: MetadataRW2_MetricType(GAUGE),
			},
			symbols: symbols.Symbols(),
		},
		{
			name:    "nil metadata",
			v1:      nil,
			v2:      MetadataRW2{},
			symbols: symbols.Symbols(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotV2 := FromMetricMetadataToMetadataRW2(tt.v1, &symbols)
			require.Equal(t, tt.v2, gotV2)
		})
	}
}

func TestWriteRequestRW2Conversion_WriteRequestHasChanged(t *testing.T) {
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
	// the [FromWriteRequestToRW2Request] implementation accordingly!
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
