package mimirpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestWriteRequestRW2Conversion(t *testing.T) {
	t.Run("PreallocWriteRequest preserves Mimir meta options", func(t *testing.T) {
		req := &PreallocWriteRequest{
			SkipUnmarshalingExemplars: true,
		}

		rw2, err := FromWriteRequestToRW2Request(req)

		require.NoError(t, err)
		require.True(t, rw2.SkipUnmarshalingExemplars)
	})

	t.Run("Sets RW2-specific meta options", func(t *testing.T) {
		req := &PreallocWriteRequest{
			UnmarshalFromRW2: false,
			RW2SymbolOffset:  0,
		}

		rw2, err := FromWriteRequestToRW2Request(req)

		require.NoError(t, err)
		require.True(t, rw2.UnmarshalFromRW2)
		require.Equal(t, uint32(0), rw2.RW2SymbolOffset)
	})

	t.Run("WriteRequest preserves Mimir meta options", func(t *testing.T) {
		req := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
				SkipLabelValidation:      true,
				SkipLabelCountValidation: true,
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req)

		require.NoError(t, err)
		require.True(t, rw2.SkipLabelValidation)
		require.True(t, rw2.SkipLabelCountValidation)
	})

	t.Run("nil request turns into nil request", func(t *testing.T) {
		var req *PreallocWriteRequest

		rw2, err := FromWriteRequestToRW2Request(req)

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
			req := &PreallocWriteRequest{
				WriteRequest: WriteRequest{
					Source: tt.in,
				},
			}

			rw2, err := FromWriteRequestToRW2Request(req)

			require.NoError(t, err)
			require.Equal(t, tt.out, rw2.Source)
		}
	})

	t.Run("TimeSeries CreatedTimestamp", func(t *testing.T) {
		req := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
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
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req)

		require.NoError(t, err)
		require.Len(t, rw2.TimeseriesRW2, 2)
		require.Equal(t, int64(1234567890), rw2.TimeseriesRW2[0].CreatedTimestamp)
		require.Equal(t, int64(1234567900), rw2.TimeseriesRW2[1].CreatedTimestamp)
	})

	t.Run("samples", func(t *testing.T) {
		req := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
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
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req)

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
		req := &PreallocWriteRequest{
			WriteRequest: WriteRequest{
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
			},
		}

		rw2, err := FromWriteRequestToRW2Request(req)

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
		require.Equal(t, expSymbols, rw2.SymbolsRW2)
		require.Equal(t, expTimeseries, rw2.TimeseriesRW2)
	})
}
