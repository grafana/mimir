// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"testing"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func BenchmarkMarshalWriteRequestToRecord(b *testing.B) {
	for _, numSeries := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("series=%d", numSeries), func(b *testing.B) {
			req := &mimirpb.WriteRequest{
				Timeseries: make([]mimirpb.PreallocTimeseries, numSeries),
				Source:     mimirpb.API,
			}
			for i := 0; i < numSeries; i++ {
				req.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
			}
			reqSize := req.Size()

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				rec, err := marshalWriteRequestToRecord(0, "user-1", req, reqSize)
				if err != nil {
					b.Fatal(err)
				}
				if rec == nil {
					b.Fatal("unexpected nil record")
				}
			}
		})
	}
}

func BenchmarkMarshalWriteRequestsToRecords(b *testing.B) {
	for _, numReqs := range []int{2, 5, 10} {
		b.Run(fmt.Sprintf("reqs=%d", numReqs), func(b *testing.B) {
			reqs := make([]*mimirpb.WriteRequest, numReqs)
			for i := 0; i < numReqs; i++ {
				req := &mimirpb.WriteRequest{
					Timeseries: make([]mimirpb.PreallocTimeseries, 100),
					Source:     mimirpb.API,
				}
				for j := 0; j < 100; j++ {
					req.Timeseries[j] = mockPreallocTimeseries(fmt.Sprintf("series_%d_%d", i, j))
				}
				reqs[i] = req
			}

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				records, err := marshalWriteRequestsToRecords(0, "user-1", reqs)
				if err != nil {
					b.Fatal(err)
				}
				if len(records) != numReqs {
					b.Fatalf("expected %d records but got %d", numReqs, len(records))
				}
			}
		})
	}
}

func BenchmarkMarshalWriteRequestToRecordsEndToEnd(b *testing.B) {
	for _, numSeries := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("nosplit/series=%d", numSeries), func(b *testing.B) {
			req := &mimirpb.WriteRequest{
				Timeseries: make([]mimirpb.PreallocTimeseries, numSeries),
				Source:     mimirpb.API,
			}
			for i := 0; i < numSeries; i++ {
				req.Timeseries[i] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
			}
			reqSize := req.Size()

			b.ResetTimer()
			b.ReportAllocs()
			for n := 0; n < b.N; n++ {
				records, err := marshalWriteRequestToRecords(0, "user-1", req, reqSize, reqSize*2, mimirpb.SplitWriteRequestByMaxMarshalSize)
				if err != nil {
					b.Fatal(err)
				}
				if len(records) != 1 {
					b.Fatalf("expected 1 record but got %d", len(records))
				}
			}
		})
	}
}
