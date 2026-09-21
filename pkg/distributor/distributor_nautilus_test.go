// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/validation"
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
		nautilusRoutingRejected: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_distributor_nautilus_routing_rejected_total",
			Help: "test",
		}, []string{"reason"}),
		now: time.Now,
	}
	d.nautilusStreamConnected = atomic.Bool{}
	return d
}

func installTenantSnapshot(d *Distributor, now, validUntil time.Time, entries []assignment.LogEntry) {
	assignmentLog := assignment.NewLogFromEntries(entries)
	table := assignmentLog.ActiveTable(now)
	d.nautilusLog.Store(assignmentLog)
	d.nautilusActiveTable.Store(table)
	d.nautilusAssignmentSnapshot.Store(&nautilusAssignmentSnapshot{
		log:          assignmentLog,
		table:        table,
		generation:   1,
		validUntil:   validUntil,
		tenantScoped: true,
	})
}

func TestWritePartitionTopicsConcurrently(t *testing.T) {
	t.Run("single topic", func(t *testing.T) {
		writes := []partitionTopicWrite{{topic: "ingest"}}
		var called []string

		err := writePartitionTopicsConcurrently(context.Background(), writes, func(_ context.Context, topicWrite partitionTopicWrite) error {
			called = append(called, topicWrite.topic)
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, []string{"ingest"}, called)
	})

	t.Run("tee topics overlap", func(t *testing.T) {
		writes := []partitionTopicWrite{{topic: "ingest"}, {topic: "ingest_nautilus"}}
		started := make(chan string, len(writes))
		release := make(chan struct{})
		done := make(chan error, 1)

		go func() {
			done <- writePartitionTopicsConcurrently(context.Background(), writes, func(_ context.Context, topicWrite partitionTopicWrite) error {
				started <- topicWrite.topic
				<-release
				return nil
			})
		}()

		got := map[string]bool{}
		for range writes {
			select {
			case topic := <-started:
				got[topic] = true
			case <-time.After(time.Second):
				t.Fatal("topic writes did not overlap")
			}
		}
		assert.Equal(t, map[string]bool{"ingest": true, "ingest_nautilus": true}, got)

		close(release)
		require.NoError(t, <-done)
	})
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

	_, err := d.getKeysByAssignment(context.Background(), "test", "", gappy, []uint32{50, 1000})
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
	got, err := d.getKeysByAssignment(context.Background(), "test", "", tbl, []uint32{50, 150, 250})
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

func TestGetKeysByAssignment_NotRequiredRoutesCompleteTiling(t *testing.T) {
	// Non-required mode may use the ring only when the entire live stream is
	// unavailable. Once a tenant exists, its assignment must be complete.
	d := minimalDistributorForRouting(t, false)
	tbl := buildActiveTable(t, []int32{1, 2}, []uint32{math.MaxUint32 / 2})

	got, err := d.getKeysByAssignment(context.Background(), "test", "", tbl, []uint32{0, math.MaxUint32})
	require.NoError(t, err)
	require.Len(t, got, 2)

	rej := testutil.ToFloat64(d.nautilusRoutingRejected.WithLabelValues("key_not_covered"))
	assert.Equal(t, float64(0), rej, "fallback mode must not increment the required-rejection counter")
}

func TestTenantScopedWriteRouting(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	half := uint32(math.MaxUint32 / 2)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, now.Add(time.Minute), []assignment.LogEntry{
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: half}, PartitionID: 1, From: now},
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: half + 1, Hi: math.MaxUint32}, PartitionID: 2, From: now},
		{TenantID: "tenant-b", Range: assignment.HashRange{Lo: 0, Hi: half}, PartitionID: 3, From: now},
		{TenantID: "tenant-b", Range: assignment.HashRange{Lo: half + 1, Hi: math.MaxUint32}, PartitionID: 4, From: now},
	})

	for tenantID, expected := range map[string][]int32{
		"tenant-a": {1, 2},
		"tenant-b": {3, 4},
	} {
		routing := d.nautilusRoutingForTenant(tenantID, now)
		got, err := d.getKeysByTenantAssignment(t.Context(), tenantID, routing, []uint32{1, math.MaxUint32})
		require.NoError(t, err)
		var partitions []int32
		for _, keys := range got {
			partitions = append(partitions, keys.PartitionID)
		}
		assert.ElementsMatch(t, expected, partitions)
	}
}

func TestTenantScopedUnknownWriteBootstrapsOnPartitionZero(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, now.Add(time.Minute), []assignment.LogEntry{{
		TenantID: "known", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 7, From: now,
	}})

	routing := d.nautilusRoutingForTenant("new-tenant", now)
	require.True(t, routing.snapshotAvailable)
	require.False(t, routing.tenantKnown)
	got, err := d.getKeysByTenantAssignment(t.Context(), "new-tenant", routing, []uint32{10, 20, 30})
	require.NoError(t, err)
	require.Equal(t, []ring.PartitionKeys{{PartitionID: 0, Indexes: []int{0, 1, 2}}}, got)
}

func TestTenantScopedHandoffWriteAndBootstrapQuery(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	handoff := now.Add(-time.Hour)
	secondHandoff := handoff.Add(30 * time.Minute)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, now.Add(time.Minute), []assignment.LogEntry{
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 9, From: handoff, To: secondHandoff},
		{TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 10, From: secondHandoff},
	})

	routing := d.nautilusRoutingForTenant("tenant-a", now)
	got, err := d.getKeysByTenantAssignment(t.Context(), "tenant-a", routing, []uint32{42})
	require.NoError(t, err)
	require.Equal(t, []ring.PartitionKeys{{PartitionID: 10, Indexes: []int{0}}}, got)

	snapshot := d.nautilusSnapshotAt(now)
	assert.Equal(t, []int32{0, 9, 10}, partitionsForNautilusQuery(snapshot, "tenant-a", handoff.Add(-time.Hour), now.Add(time.Minute), nil, false))
	assert.Equal(t, []int32{9, 10}, partitionsForNautilusQuery(snapshot, "tenant-a", handoff, now.Add(time.Minute), nil, false))
	assert.Equal(t, []int32{0}, partitionsForNautilusQuery(snapshot, "tenant-b", handoff.Add(-time.Hour), now.Add(time.Minute), nil, false))
}

func TestTenantScopedExpiredGenerationIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, now, []assignment.LogEntry{{
		TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1, From: now.Add(-time.Hour),
	}})

	assert.False(t, d.nautilusRoutingForTenant("tenant-a", now).snapshotAvailable)
	assert.Nil(t, d.nautilusSnapshotAt(now))
}

func TestTenantScopedMissingGenerationValidityIsRejected(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, time.Time{}, []assignment.LogEntry{{
		TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1, From: now.Add(-time.Hour),
	}})

	assert.Nil(t, d.nautilusSnapshotAt(now))
}

func TestLegacyAssignmentStillUsesPerRangeLeaseFreshness(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }
	assignmentLog := assignment.NewLogFromEntries([]assignment.LogEntry{{
		Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1, From: now.Add(-time.Minute), To: now.Add(time.Minute),
	}})
	d.nautilusLog.Store(assignmentLog)

	assert.True(t, d.nautilusRoutingForTenant("any-tenant", now).snapshotAvailable)
	assert.False(t, d.nautilusRoutingForTenant("any-tenant", now.Add(time.Minute)).snapshotAvailable)
}

func TestExistingTenantPartialTilingNeverBootstraps(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, false)
	d.now = func() time.Time { return now }
	installTenantSnapshot(d, now, now.Add(time.Minute), []assignment.LogEntry{{
		TenantID: "tenant-a", Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: now,
	}})

	routing := d.nautilusRoutingForTenant("tenant-a", now)
	require.True(t, routing.tenantKnown)
	_, err := d.getKeysByTenantAssignment(t.Context(), "tenant-a", routing, []uint32{100})
	require.Error(t, err)
}

func TestApplyNautilusAssignmentResponse_DeltaResetReplayKeepsGeneration(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	full := assignment.HashRange{Lo: 0, Hi: math.MaxUint32}
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }

	entry := func(tenantID string, partitionID int32) rebalancer.LogEntry {
		return rebalancer.EntriesToProto([]assignment.LogEntry{{
			TenantID: tenantID, Range: full, PartitionID: partitionID, From: now,
		}})[0]
	}
	d.applyNautilusAssignmentResponse(&rebalancer.WatchAssignmentsResponse{
		Reset_:                     true,
		Entries:                    []rebalancer.LogEntry{entry("tenant-a", 1)},
		AssignmentGeneration:       7,
		AssignmentValidUntilUnixMs: now.Add(time.Minute).UnixMilli(),
	}, true)
	d.applyNautilusAssignmentResponse(&rebalancer.WatchAssignmentsResponse{
		Entries:                    []rebalancer.LogEntry{entry("tenant-b", 2)},
		AssignmentGeneration:       8,
		AssignmentValidUntilUnixMs: now.Add(2 * time.Minute).UnixMilli(),
	}, false)
	d.applyNautilusAssignmentResponse(&rebalancer.WatchAssignmentsResponse{
		AssignmentGeneration:       8,
		AssignmentValidUntilUnixMs: now.Add(3 * time.Minute).UnixMilli(),
	}, false)

	snapshot := d.nautilusAssignmentSnapshot.Load()
	require.NotNil(t, snapshot)
	assert.Equal(t, uint64(8), snapshot.generation)
	assert.True(t, now.Add(3*time.Minute).Equal(snapshot.validUntil))
	assert.True(t, snapshot.log.HasTenantHistory("tenant-a"))
	assert.True(t, snapshot.log.HasTenantHistory("tenant-b"))

	d.applyNautilusAssignmentResponse(&rebalancer.WatchAssignmentsResponse{
		Reset_: true,
		Entries: []rebalancer.LogEntry{
			entry("tenant-a", 1),
			entry("tenant-b", 2),
		},
		AssignmentGeneration:       8,
		AssignmentValidUntilUnixMs: now.Add(3 * time.Minute).UnixMilli(),
	}, false)
	snapshot = d.nautilusAssignmentSnapshot.Load()
	assert.Equal(t, uint64(8), snapshot.generation)
	assert.True(t, now.Add(3*time.Minute).Equal(snapshot.validUntil))
	assert.Equal(t, 2, snapshot.log.Len())
}

func TestApplyNautilusAssignmentResponse_EmptySnapshotIsTenantScopedBootstrap(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	d := minimalDistributorForRouting(t, true)
	d.now = func() time.Time { return now }

	d.applyNautilusAssignmentResponse(&rebalancer.WatchAssignmentsResponse{
		Reset_:                     true,
		AssignmentGeneration:       1,
		AssignmentValidUntilUnixMs: now.Add(time.Minute).UnixMilli(),
	}, true)

	snapshot := d.nautilusAssignmentSnapshot.Load()
	require.NotNil(t, snapshot)
	assert.True(t, snapshot.tenantScoped)
	assert.Equal(t, uint64(1), snapshot.generation)
	assert.True(t, now.Add(time.Minute).Equal(snapshot.validUntil))

	routing := d.nautilusRoutingForTenant("new-tenant", now)
	require.True(t, routing.snapshotAvailable)
	require.False(t, routing.tenantKnown)
	got, err := d.getKeysByTenantAssignment(t.Context(), "new-tenant", routing, []uint32{10, 20, 30, 40})
	require.NoError(t, err)
	assert.Equal(t, []ring.PartitionKeys{{PartitionID: 0, Indexes: []int{0, 1, 2, 3}}}, got,
		"every write item, including metadata indexes, must stay together on partition 0")
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
	partitionSamples := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_distributor_nautilus_partition_samples_written_total",
		Help: "test",
	}, []string{"partition", "user"})
	partitionsPerRequest := promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_distributor_nautilus_partitions_written_per_request",
		Help:    "Number of distinct partitions successfully written to the nautilus ingest Kafka topic by a push request.",
		Buckets: []float64{1, 2, 3},
	})

	d := &Distributor{
		cfg:                                 Config{NautilusIngestTopic: "nautilus_ingest"},
		nautilusPartitionSamplesWritten:     partitionSamples,
		nautilusPartitionsWrittenPerRequest: partitionsPerRequest,
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
		{PartitionID: 42},
	})
	assert.Equal(t, float64(2), testutil.ToFloat64(partitionSamples.WithLabelValues("1", "tenant-a")))
	assert.Equal(t, float64(1), testutil.ToFloat64(partitionSamples.WithLabelValues("42", "tenant-a")))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_distributor_nautilus_partitions_written_per_request Number of distinct partitions successfully written to the nautilus ingest Kafka topic by a push request.
# TYPE cortex_distributor_nautilus_partitions_written_per_request histogram
cortex_distributor_nautilus_partitions_written_per_request_bucket{le="1"} 0
cortex_distributor_nautilus_partitions_written_per_request_bucket{le="2"} 1
cortex_distributor_nautilus_partitions_written_per_request_bucket{le="3"} 1
cortex_distributor_nautilus_partitions_written_per_request_bucket{le="+Inf"} 1
cortex_distributor_nautilus_partitions_written_per_request_sum 2
cortex_distributor_nautilus_partitions_written_per_request_count 1
`), "cortex_distributor_nautilus_partitions_written_per_request"))
}

func TestIngestStorageTopicsForTenant(t *testing.T) {
	const tenantID = "tenant-a"

	nautilusLimits := validation.NewOverrides(validation.Limits{
		NautilusIngestRouting: validation.NautilusIngestRoutingNautilus,
		ReadcacheReadRouting:  validation.ReadcacheReadRoutingNautilus,
	}, nil)
	disabledLimits := validation.NewOverrides(validation.Limits{}, nil)

	tests := map[string]struct {
		limits          *validation.Overrides
		writeIngest     bool
		writeNautilus   bool
		nautilusTopic   string
		expectedTopics  []string
		expectedComment string
	}{
		"non-enrolled tenant writes production only": {
			limits:         disabledLimits,
			writeNautilus:  true,
			nautilusTopic:  "nautilus",
			expectedTopics: []string{"ingest"},
		},
		"enrolled tenant defaults to nautilus only": {
			limits:         nautilusLimits,
			writeNautilus:  true,
			nautilusTopic:  "nautilus",
			expectedTopics: []string{"nautilus"},
		},
		"enrolled tenant can tee to production and nautilus": {
			limits:         nautilusLimits,
			writeIngest:    true,
			writeNautilus:  true,
			nautilusTopic:  "nautilus",
			expectedTopics: []string{"ingest", "nautilus"},
		},
		"enrolled tenant can write production only by startup config": {
			limits:         nautilusLimits,
			writeIngest:    true,
			nautilusTopic:  "nautilus",
			expectedTopics: []string{"ingest"},
		},
		"empty nautilus topic preserves production fallback": {
			limits:         nautilusLimits,
			writeNautilus:  true,
			expectedTopics: []string{"ingest"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			d := &Distributor{
				cfg: Config{
					IngestStorageConfig: ingest.Config{
						KafkaConfig: ingest.KafkaConfig{Topic: "ingest"},
					},
					NautilusIngestTopic:                tc.nautilusTopic,
					NautilusIngestWriteToIngestTopic:   tc.writeIngest,
					NautilusIngestWriteToNautilusTopic: tc.writeNautilus,
				},
				limits: tc.limits,
			}
			assert.Equal(t, tc.expectedTopics, d.ingestStorageTopicsForTenant(tenantID), tc.expectedComment)
		})
	}
}
