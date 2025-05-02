// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
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
			LabelsRefs: []uint32{1, uint32(i) + 2},
			Samples:    []mimirpb.Sample{{TimestampMs: 1, Value: 2}},
			Exemplars:  []mimirpb.ExemplarRW2{},
		}
		reqv2.SymbolsRW2 = append(reqv2.SymbolsRW2, fmt.Sprintf("series_%d", i))
	}
	v2bytes, err := reqv2.Marshal()
	require.NoError(b, err)

	b.Run("deserialize v1", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			wr := &mimirpb.PreallocWriteRequest{}
			err := DeserializeRecordContent(v1bytes, wr, 1)
			if err != nil {
				b.Fatal(err)
			}
			defer mimirpb.ReuseSlice(wr.Timeseries)
		}
	})

	b.Run("deserialize v2", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			wr := &mimirpb.PreallocWriteRequest{}
			err := DeserializeRecordContent(v2bytes, wr, 2)
			if err != nil {
				b.Fatal(err)
			}
			defer mimirpb.ReuseSlice(wr.Timeseries)
		}
	})
}
