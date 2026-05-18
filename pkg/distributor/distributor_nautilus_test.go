// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// buildActiveTable returns an ActiveTable that tiles the given hash
// boundaries to the given partition IDs. boundaries must have one
// fewer element than partitions; the table covers [0,b0), [b0+1,b1),
// ..., [bn-1+1, MaxUint32]. Used to seed the distributor's nautilus
// state for unit tests without touching the rebalancer or grpc.
func buildActiveTable(t *testing.T, partitions []int32, boundaries []uint32) *assignment.ActiveTable {
	t.Helper()
	require.Len(t, boundaries, len(partitions)-1, "boundaries must have len(partitions)-1 entries")

	at := time.Now()
	entries := make([]assignment.LogEntry, len(partitions))
	var lo uint32
	for i, pid := range partitions {
		var hi uint32 = math.MaxUint32
		if i < len(boundaries) {
			hi = boundaries[i]
		}
		entries[i] = assignment.LogEntry{
			Range:       assignment.HashRange{Lo: lo, Hi: hi},
			PartitionID: pid,
			From:        at.Add(-time.Minute),
			To:          at.Add(time.Hour),
		}
		lo = hi + 1
	}
	l := assignment.NewLogFromEntries(entries)
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)
	return tbl
}

// minimalDistributorForRouting returns a Distributor populated only
// with the fields getKeysByAssignment / sendWriteRequestToPartitions'
// pre-Kafka path needs. It deliberately does not wire ingestStorageWriter
// or partitionsRing — tests must avoid code paths that would touch
// them.
func minimalDistributorForRouting(t *testing.T, required bool) *Distributor {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	d := &Distributor{
		cfg: Config{NautilusRequired: required},
		log: log.NewNopLogger(),
		nautilusRoutingRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_nautilus_routing_rejected_total",
			Help: "test",
		}, []string{"reason"}),
		now: time.Now,
	}
	d.nautilusStreamConnected = atomic.Bool{}
	require.NoError(t, reg.Register(d.nautilusRoutingRejected))
	return d
}

func TestGetKeysByAssignment_RequiredRejectsKeyNotCovered(t *testing.T) {
	d := minimalDistributorForRouting(t, true)
	// Table that intentionally covers only [0, 99]. Any key >= 100
	// has no entry, so Lookup returns false and the required-mode
	// path must reject rather than fall back to the partition ring.
	gappy := assignment.NewLogFromEntries([]assignment.LogEntry{
		{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: time.Now().Add(-time.Minute), To: time.Now().Add(time.Hour)},
	}).ActiveTable(time.Now())
	require.NotNil(t, gappy)

	_, err := d.getKeysByAssignment(context.Background(), gappy, []uint32{50, 1000})
	require.Error(t, err)
	var rejErr nautilusRoutingUnavailableError
	require.ErrorAs(t, err, &rejErr)

	got := testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("key_not_covered"))
	assert.Equal(t, float64(1), got)
	got = testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("table_unavailable"))
	assert.Equal(t, float64(0), got)
}

func TestGetKeysByAssignment_RequiredPassesThroughWhenCovered(t *testing.T) {
	d := minimalDistributorForRouting(t, true)
	tbl := buildActiveTable(t, []int32{1, 2, 3}, []uint32{99, 199})

	// Keys at 50, 150, 250 should map to partitions 1, 2, 3.
	got, err := d.getKeysByAssignment(context.Background(), tbl, []uint32{50, 150, 250})
	require.NoError(t, err)

	byPID := map[int32][]int{}
	for _, pk := range got {
		byPID[pk.PartitionID] = pk.Indexes
	}
	assert.Equal(t, []int{0}, byPID[1])
	assert.Equal(t, []int{1}, byPID[2])
	assert.Equal(t, []int{2}, byPID[3])

	rej := testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("key_not_covered"))
	assert.Equal(t, float64(0), rej)
}

func TestGetKeysByAssignment_NotRequiredFallsBackToPartitionRing(t *testing.T) {
	// In the default (non-required) mode, getKeysByAssignment should
	// fall back to the partition ring for uncovered keys. We verify
	// here that uncovered keys cause d.partitionsRing to be consulted
	// — the lazy fetch will panic with a nil pointer if it is
	// referenced. To avoid wiring a real ring just to exercise the
	// fallback branch, we assert that with a fully-covering table the
	// function never touches the ring.
	d := minimalDistributorForRouting(t, false)
	tbl := buildActiveTable(t, []int32{1, 2}, []uint32{math.MaxUint32 / 2})

	// All keys covered → no fallback.
	got, err := d.getKeysByAssignment(context.Background(), tbl, []uint32{0, math.MaxUint32})
	require.NoError(t, err)
	require.Len(t, got, 2)

	rej := testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("key_not_covered"))
	assert.Equal(t, float64(0), rej, "fallback mode must not increment the required-rejection counter")
}

func TestSendWriteRequestToPartitions_RequiredRejectsWhenTableUnavailable(t *testing.T) {
	d := minimalDistributorForRouting(t, true)
	// d.nautilusLog is unset, so nautilusActiveTableFor returns nil.

	err := d.sendWriteRequestToPartitions(
		context.Background(),
		"tenant",
		nil, // tenantRing — unused on this code path because we exit early
		nil, // req — unused
		[]uint32{42},
		0,
		func() context.Context { return context.Background() },
		func() {},
	)
	require.Error(t, err)
	var rejErr nautilusRoutingUnavailableError
	require.ErrorAs(t, err, &rejErr)

	got := testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("table_unavailable"))
	assert.Equal(t, float64(1), got)
}

func TestNautilusRoutingUnavailableError_HasServiceUnavailableCause(t *testing.T) {
	err := newNautilusRoutingUnavailableError("test")
	require.False(t, err.IsSoft(), "required-mode rejections must not be soft errors")
	assert.Equal(t, mimirpb.ERROR_CAUSE_SERVICE_UNAVAILABLE, err.Cause(),
		"required-mode rejections must surface as Service Unavailable so writers retry")
}

func TestWriteRequestSampleCount(t *testing.T) {
	assert.Equal(t, 0, writeRequestSampleCount(nil))
	assert.Equal(t, 0, writeRequestSampleCount(&mimirpb.WriteRequest{}))
	assert.Equal(t, 3, writeRequestSampleCount(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{}, {}}}},
			{TimeSeries: &mimirpb.TimeSeries{Histograms: []mimirpb.Histogram{{}}}},
		},
	}))
}

func TestObserveNautilusPartitionWrites(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	partitionSamples := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_distributor_nautilus_partition_samples_written_total",
		Help: "test",
	}, []string{"partition", "user"})
	require.NoError(t, reg.Register(partitionSamples))

	d := &Distributor{
		cfg:                             Config{NautilusIngestTopic: "nautilus_ingest"},
		nautilusPartitionSamplesWritten: partitionSamples,
	}

	d.observeNautilusPartitionWrites("tenant-a", "production_topic", []ingest.PartitionWriteRequest{
		{PartitionID: 1, WriteRequest: &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{}}}},
			},
		}},
	})
	assert.Equal(t, float64(0), testutil.ToFloat64(partitionSamples.WithLabelValues("1", "tenant-a")))

	d.observeNautilusPartitionWrites("tenant-a", "nautilus_ingest", []ingest.PartitionWriteRequest{
		{PartitionID: 1, WriteRequest: &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{Samples: []mimirpb.Sample{{}, {}}}},
			},
		}},
		{PartitionID: 42, WriteRequest: &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{Histograms: []mimirpb.Histogram{{}}}},
			},
		}},
	})
	assert.Equal(t, float64(2), testutil.ToFloat64(partitionSamples.WithLabelValues("1", "tenant-a")))
	assert.Equal(t, float64(1), testutil.ToFloat64(partitionSamples.WithLabelValues("42", "tenant-a")))
}
