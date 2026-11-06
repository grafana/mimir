// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestHAReplicaInfoOwnsLabelStorage(t *testing.T) {
	clusterBytes := []byte("cluster-a")
	replicaBytes := []byte("replica-a")
	cluster := mimirpb.BorrowString(unsafe.String(unsafe.SliceData(clusterBytes), len(clusterBytes)))
	replica := mimirpb.BorrowString(unsafe.String(unsafe.SliceData(replicaBytes), len(replicaBytes)))
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{{TimeSeries: &mimirpb.TimeSeries{
		Labels: []mimirpb.LabelAdapter{
			{Name: mimirpb.BorrowString("cluster"), Value: cluster},
			{Name: mimirpb.BorrowString("replica"), Value: replica},
		},
		Samples: []mimirpb.Sample{{TimestampMs: 10}},
	}}}}

	replicas, infos := getReplicasAndInfos(req, "replica", "cluster", 20, true)
	t.Cleanup(func() {
		clear(*replicas)
		haReplicaSlicePool.Put(replicas)
	})
	require.Len(t, infos, 1)
	key := haReplica{cluster: mimirpb.BorrowString("cluster-a"), replica: mimirpb.BorrowString("replica-a")}
	info := infos[key]
	require.NotNil(t, info)
	require.Equal(t, int64(10), info.earliestSampleTimestamp)

	clear(clusterBytes)
	clear(replicaBytes)
	require.Same(t, info, infos[key], "canonical map key must not alias the request buffer")
	require.Equal(t, "cluster-a", info.cluster)
	require.Equal(t, "replica-a", info.replica)
	require.False(t, (*replicas)[0].cluster.EqualString(info.cluster), "per-series lookup values remain borrowed")
}
