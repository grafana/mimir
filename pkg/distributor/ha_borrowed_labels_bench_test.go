// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"fmt"
	"testing"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func BenchmarkHAReplicaGroupingOwnership(b *testing.B) {
	for _, pairs := range []int{1, 100} {
		b.Run(fmt.Sprintf("pairs=%d", pairs), func(b *testing.B) {
			req := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, 1000)}
			for i := range req.Timeseries {
				req.Timeseries[i].TimeSeries = &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: mimirpb.BorrowString("cluster"), Value: mimirpb.BorrowString(fmt.Sprintf("cluster-%d", i%pairs))},
						{Name: mimirpb.BorrowString("replica"), Value: mimirpb.BorrowString("replica-a")},
					},
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				replicas, infos := getReplicasAndInfos(req, "replica", "cluster", 0, false)
				if len(infos) != pairs {
					b.Fatal("incorrect replica grouping")
				}
				clear(*replicas)
				haReplicaSlicePool.Put(replicas)
			}
		})
	}
}
