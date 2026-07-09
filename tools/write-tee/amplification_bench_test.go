// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// BenchmarkAmplifyRequestBody benchmarks the amplification of RW1 and RW2 requests
// with various series counts and amplification factors.
func BenchmarkAmplifyRequestBody(b *testing.B) {
	seriesCounts := []int{1000}
	ampFactors := []int{2, 5}

	for _, numSeries := range seriesCounts {
		// Build RW1 request with simple label construction
		var timeseries []mimirpb.PreallocTimeseries
		for i := 0; i < numSeries; i++ {
			timeseries = append(timeseries, mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)},
						{Name: "id", Value: fmt.Sprintf("id_%d", i)},
						{Name: "instance", Value: "localhost:9090"},
						{Name: "job", Value: "prometheus"},
						{Name: "namespace", Value: "default"},
						{Name: "pod", Value: fmt.Sprintf("pod_%d", i)},
					},
					Samples: []mimirpb.Sample{{Value: float64(i), TimestampMs: 1000}},
				},
			})
		}
		rw1 := mimirpb.WriteRequest{Timeseries: timeseries}

		// Compress RW1 request
		rw1Marshaled, err := proto.Marshal(&rw1)
		if err != nil {
			b.Fatal(err)
		}
		rw1Compressed := snappy.Encode(nil, rw1Marshaled)

		// Convert to RW2 and compress
		rw2, _ := mimirpb.FromWriteRequestToRW2Request(&rw1, nil, 0)
		rw2Marshaled, err := proto.Marshal(rw2)
		if err != nil {
			b.Fatal(err)
		}
		rw2Compressed := snappy.Encode(nil, rw2Marshaled)

		for _, factor := range ampFactors {
			b.Run(fmt.Sprintf("RW1/series=%d/factor=%dx", numSeries, factor), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := AmplifyRequestBody(rw1Compressed, 1, factor)
					if err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run(fmt.Sprintf("RW2/series=%d/factor=%dx", numSeries, factor), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_, err := AmplifyRequestBody(rw2Compressed, 1, factor)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
