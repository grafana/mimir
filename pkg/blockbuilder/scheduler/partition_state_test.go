// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
	"github.com/grafana/mimir/pkg/util/test"
)

func newTestPartitionState(t *testing.T, topic string, partition int32) *partitionState {
	return newTestPartitionStateWithClusters(t, topic, partition, 1, false)
}

func newTestPartitionStateWithClusters(t *testing.T, topic string, partition int32, numClusters int, compartmentsEnabled bool) *partitionState {
	m := newSchedulerMetrics(prometheus.NewPedanticRegistry(), compartmentsEnabled, numClusters)
	return newPartitionState(topic, partition, numClusters, compartmentsEnabled, &m, test.NewTestingLogger(t))
}

func TestPartitionState(t *testing.T) {
	pt := newTestPartitionState(t, "topic", 0)
	sz := 1 * time.Hour

	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	var job *schedulerpb.JobSpec
	var err error

	pt.updateEndOffset(0, 100)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)
	pt.updateEndOffset(0, 200)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 100,
		EndOffset:   200,
	}, job)
	require.Nil(t, err)

	pt.updateEndOffset(0, 201)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.NoError(t, err)
	pt.updateEndOffset(0, 202)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 2, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	pt.updateEndOffset(0, 203)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 3, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)

	pt.updateEndOffset(0, 300)
	job, err = pt.updateTime(z.Add(2*time.Hour), sz)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 200,
		EndOffset:   300,
	}, job)
	require.NoError(t, err)

	// And, if the time goes backwards, we return an error.
	pt.updateEndOffset(0, 300)
	job, err = pt.updateTime(z.Add(-2*time.Hour), sz)
	require.Nil(t, job)
	require.ErrorContains(t, err, "time went backwards")
	require.ErrorContains(t, err, "[300,300)", "error should include the offsets for diagnostics")
}

func TestPartitionState_TerminallyDormantPartition(t *testing.T) {
	pt := newTestPartitionState(t, "topic", 0)
	sz := 1 * time.Hour
	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		z = z.Add(7 * time.Minute)
		pt.updateEndOffset(0, 0)
		j, err := pt.updateTime(z, sz)
		assert.Nil(t, j)
		assert.NoError(t, err)
	}
}

func TestPartitionState_PartitionBecomesInactive(t *testing.T) {
	pt := newTestPartitionState(t, "topic", 0)
	sz := 1 * time.Hour

	// A bunch of data observed:
	var j *schedulerpb.JobSpec
	var err error
	pt.updateEndOffset(0, 10)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	pt.updateEndOffset(0, 11)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 11, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 12, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	// data ceases. continue to get observations in the same bucket.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 13, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// as we cross into the next bucket, there's still no new data.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 0, 0, time.UTC), sz)
	assert.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 10,
		EndOffset:   12,
	}, j)
	assert.NoError(t, err)
	// and we keep getting the same offset.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 2, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 3, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// And in the next job bucket, still no new data.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 12, 1, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
}

func TestPartitionState_MultipleOffsetsBeforeTimeUpdate(t *testing.T) {
	ps := newTestPartitionState(t, "topic", 0)
	jobSize := time.Hour
	ts := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)

	// Seed the start offset and the current bucket.
	ps.updateEndOffset(0, 100)
	job, err := ps.updateTime(ts, jobSize)
	require.NoError(t, err)
	require.Nil(t, job)

	// Several observations within the same bucket, with no time update between them.
	ps.updateEndOffset(0, 150)
	ps.updateEndOffset(0, 175)
	ps.updateEndOffset(0, 200)

	// Crossing into the next bucket emits a single job spanning the seeded start
	// offset to the latest observed offset.
	job, err = ps.updateTime(ts.Add(jobSize), jobSize)
	require.NoError(t, err)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 100,
		EndOffset:   200,
	}, job)
}

func TestPartitionState_TimeAdvancesBeforeAnyOffset(t *testing.T) {
	pt := newTestPartitionState(t, "topic", 0)
	sz := time.Hour
	z := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)

	// Advance time across several buckets without ever observing an end offset.
	// No jobs should be emitted.
	for i := 0; i < 5; i++ {
		job, err := pt.updateTime(z.Add(time.Duration(i)*sz), sz)
		require.NoError(t, err)
		require.Nil(t, job)
	}
}

func TestPartitionState_CompartmentsEmitOffsetRanges(t *testing.T) {
	pt := newTestPartitionStateWithClusters(t, "topic", 0, 3, true)
	sz := time.Hour
	base := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)

	// Seed the first bucket. Clusters 0 and 1 observe data; cluster 2 is never sampled.
	pt.updateEndOffset(0, 100)
	pt.updateEndOffset(1, 500)
	job, err := pt.updateTime(base, sz)
	require.NoError(t, err)
	require.Nil(t, job)

	// More data in the same bucket for both sampled clusters.
	pt.updateEndOffset(0, 150)
	pt.updateEndOffset(1, 600)

	// Crossing into the next bucket emits a single compartment-encoded job. It carries
	// one range per cluster with new data and no top-level offsets; the unsampled
	// cluster 2 is omitted.
	job, err = pt.updateTime(base.Add(sz), sz)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, "topic", job.Topic)
	require.Equal(t, int32(0), job.Partition)
	require.Zero(t, job.StartOffset)
	require.Zero(t, job.EndOffset)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 100, EndOffset: 150},
		1: {StartOffset: 500, EndOffset: 600},
	}, job.OffsetRanges)

	// Only cluster 0 advances in the next bucket. The emitted job continues from each
	// cluster's previous end offset and drops the cluster that saw no new data.
	pt.updateEndOffset(0, 175)
	job, err = pt.updateTime(base.Add(2*sz), sz)
	require.NoError(t, err)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 150, EndOffset: 175},
	}, job.OffsetRanges)

	// The previously unsampled cluster 2 starts receiving data. Its first range begins at
	// the seeded start offset (not zero), cluster 0 keeps advancing, and cluster 1 is
	// dropped as it saw no new data.
	pt.updateEndOffset(2, 900)
	pt.updateEndOffset(2, 950)
	pt.updateEndOffset(0, 200)
	job, err = pt.updateTime(base.Add(3*sz), sz)
	require.NoError(t, err)
	require.Equal(t, map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 175, EndOffset: 200},
		2: {StartOffset: 900, EndOffset: 950},
	}, job.OffsetRanges)

	// In compartment mode, diagnostics prefix each range with its cluster ID and list
	// every cluster.
	_, err = pt.updateTime(base.Add(-sz), sz)
	require.EqualError(t, err, "time went backwards: 2025-03-01 09:00:00 +0000 UTC < 2025-03-01 13:00:00 +0000 UTC (offsets: 0:[200,200) 1:[600,600) 2:[950,950))")
}

func TestPartitionState_CompartmentsTrackOffsetsPerCluster(t *testing.T) {
	pt := newTestPartitionStateWithClusters(t, "topic", 1, 3, true)

	pt.initCommit(0, 100)
	pt.initCommit(1, 500)
	pt.initCommit(2, 700)
	require.Equal(t, int64(100), pt.committedOffset(0))
	require.Equal(t, int64(500), pt.committedOffset(1))
	require.Equal(t, int64(700), pt.committedOffset(2))
	require.Equal(t, int64(100), pt.plannedOffset(0))
	require.Equal(t, int64(500), pt.plannedOffset(1))
	require.Equal(t, int64(700), pt.plannedOffset(2))

	// A job that spans clusters 0 and 2 only, skipping cluster 1.
	spec := schedulerpb.JobSpec{
		Topic:     "topic",
		Partition: 1,
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			2: {StartOffset: 700, EndOffset: 900},
		},
	}

	pt.addPlannedJob("job1", spec)
	// Planning advances the planned offset of the job's clusters, leaves committed untouched,
	// and does not affect cluster 1, which the job omits.
	require.Equal(t, int64(200), pt.plannedOffset(0))
	require.Equal(t, int64(500), pt.plannedOffset(1))
	require.Equal(t, int64(900), pt.plannedOffset(2))
	require.Equal(t, int64(100), pt.committedOffset(0))
	require.Equal(t, int64(500), pt.committedOffset(1))
	require.Equal(t, int64(700), pt.committedOffset(2))

	require.NoError(t, pt.completeJob("job1"))
	// Completion advances committed for the job's clusters only; cluster 1 stays put.
	require.Equal(t, int64(200), pt.committedOffset(0))
	require.Equal(t, int64(500), pt.committedOffset(1))
	require.Equal(t, int64(900), pt.committedOffset(2))
}

func TestPartitionState_CompartmentsBeyondSpecRequiresAllClusters(t *testing.T) {
	pt := newTestPartitionStateWithClusters(t, "topic", 1, 3, true)
	pt.initCommit(0, 200)
	pt.initCommit(1, 900)
	// Cluster 2 has a committed offset but never appears in the specs below. committedBeyondSpec
	// only considers the clusters the job covers, so cluster 2 must not affect the result.
	pt.initCommit(2, 500)

	// A job wholly at or under the committed offsets of the clusters it covers is beyond the commit.
	require.True(t, pt.committedBeyondSpec(schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 800, EndOffset: 900},
		},
	}))

	// Cluster 1 ends past its committed offset (1000 > 900), so it is not yet consumed and the
	// whole job is not beyond, even though cluster 0 is.
	require.False(t, pt.committedBeyondSpec(schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 900, EndOffset: 1000},
		},
	}))
}

func TestPartitionState_CompartmentsPlannedBeyondSpec(t *testing.T) {
	pt := newTestPartitionStateWithClusters(t, "topic", 1, 3, true)
	pt.initCommit(0, 100)
	pt.initCommit(1, 500)
	pt.initCommit(2, 700)

	// Plan a job to advance the planned offsets of clusters 0 and 1 past their committed
	// offsets. Cluster 2 is left untouched.
	pt.addPlannedJob("seed", schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 500, EndOffset: 900},
		},
	})

	// A job wholly at or under both clusters' planned offsets is beyond the planned offset,
	// even though it is not beyond the (lower) committed offset.
	spec := schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 800, EndOffset: 900},
		},
	}
	require.True(t, pt.plannedBeyondSpec(spec))
	require.False(t, pt.committedBeyondSpec(spec))

	// Cluster 1 ends past its planned offset (1000 > 900), so it is not yet planned and the
	// whole job is not beyond, even though cluster 0 is.
	require.False(t, pt.plannedBeyondSpec(schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 900, EndOffset: 1000},
		},
	}))
}

func TestPartitionState_CompartmentsAddPlannedJobPanicsWhenAnyClusterBehind(t *testing.T) {
	pt := newTestPartitionStateWithClusters(t, "topic", 1, 2, true)
	pt.initCommit(0, 100)
	pt.initCommit(1, 500)

	pt.addPlannedJob("job1", schedulerpb.JobSpec{
		OffsetRanges: map[int32]schedulerpb.OffsetRange{
			0: {StartOffset: 100, EndOffset: 200},
			1: {StartOffset: 500, EndOffset: 600},
		},
	})

	// A job that is a valid next step for cluster 0 but whose cluster 1 range is already
	// planned must panic: the guard rejects a job behind the planned offset in any single
	// cluster, even when the others would advance cleanly.
	require.Panics(t, func() {
		pt.addPlannedJob("job2", schedulerpb.JobSpec{
			OffsetRanges: map[int32]schedulerpb.OffsetRange{
				0: {StartOffset: 200, EndOffset: 300},
				1: {StartOffset: 500, EndOffset: 600},
			},
		})
	})
}

func TestPartitionState_CompartmentsNoInducedGaps(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newSchedulerMetrics(reg, true, 2)
	pt := newPartitionState("topic", 0, 2, true, &m, test.NewTestingLogger(t))
	sz := time.Hour
	base := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)

	jobNum := 0
	// crossBucket advances into the given bucket and plans+completes any emitted job,
	// mirroring the scheduler's normal flow.
	crossBucket := func(bucket time.Time) {
		job, err := pt.updateTime(bucket, sz)
		require.NoError(t, err)
		if job == nil {
			return
		}
		jobNum++
		id := fmt.Sprintf("job-%d", jobNum)
		pt.addPlannedJob(id, *job)
		require.NoError(t, pt.completeJob(id))
	}

	// Seed the first bucket for both clusters.
	pt.updateEndOffset(0, 100)
	pt.updateEndOffset(1, 500)
	crossBucket(base)

	// Both clusters advance.
	pt.updateEndOffset(0, 150)
	pt.updateEndOffset(1, 600)
	crossBucket(base.Add(sz))

	// Only cluster 0 advances; cluster 1 drops out of the job.
	pt.updateEndOffset(0, 175)
	crossBucket(base.Add(2 * sz))

	// Only cluster 1 advances, reappearing after being absent.
	pt.updateEndOffset(1, 700)
	crossBucket(base.Add(3 * sz))

	// Both advance again.
	pt.updateEndOffset(0, 200)
	pt.updateEndOffset(1, 800)
	crossBucket(base.Add(4 * sz))

	// Clusters advanced at different rates and dropped out of some jobs entirely, but an absent
	// cluster's offset doesn't move, so each cluster's next range always resumes exactly where
	// its previous one ended. No cluster ever sees a gap.
	requireGaps(t, reg, 0, 0)
	require.Equal(t, int64(200), pt.committedOffset(0))
	require.Equal(t, int64(800), pt.committedOffset(1))
}

func TestPartitionState_CompartmentsDetectPerClusterGap(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	m := newSchedulerMetrics(reg, true, 3)
	pt := newPartitionState("topic", 0, 3, true, &m, test.NewTestingLogger(t))
	pt.initCommit(0, 100)
	pt.initCommit(1, 500)
	pt.initCommit(2, 900)

	// Contiguous next job for clusters 0 and 1, omitting cluster 2: valid, no gap.
	contiguous := schedulerpb.JobSpec{OffsetRanges: map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 100, EndOffset: 150},
		1: {StartOffset: 500, EndOffset: 550},
	}}
	require.True(t, pt.plannedValidNextSpec(contiguous))
	pt.addPlannedJob("job1", contiguous)
	requireGaps(t, reg, 0, 0, "a contiguous multi-cluster job should not register a gap")

	// Cluster 1's range jumps ahead of its planned offset (550 -> 600): a gap in that
	// cluster only. Cluster 0 stays contiguous.
	gapped := schedulerpb.JobSpec{OffsetRanges: map[int32]schedulerpb.OffsetRange{
		0: {StartOffset: 150, EndOffset: 200},
		1: {StartOffset: 600, EndOffset: 650},
	}}
	require.False(t, pt.plannedValidNextSpec(gapped))
	pt.addPlannedJob("job2", gapped)
	requireGaps(t, reg, 1, 0, "a gap in a single cluster's range should register one planned gap")

	// A single job can gap in more than one cluster at once: cluster 1 jumps again
	// (650 -> 700) and cluster 2 appears for the first time above its planned offset
	// (900 -> 1000). Each gapped range is counted, so the planned gap total rises by two.
	multiGap := schedulerpb.JobSpec{OffsetRanges: map[int32]schedulerpb.OffsetRange{
		1: {StartOffset: 700, EndOffset: 750},
		2: {StartOffset: 1000, EndOffset: 1050},
	}}
	require.False(t, pt.plannedValidNextSpec(multiGap))
	pt.addPlannedJob("job3", multiGap)
	requireGaps(t, reg, 3, 0, "gaps in two clusters within one job should register two planned gaps")
}

func TestPartitionState_ParallelJobs(t *testing.T) {
	t.Run("planned job order required", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// It is a logical error to add a job to the planned list out of order. The safe completion logic requires it.

		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})
		require.Panics(t, func() {
			ps.addPlannedJob("job2", schedulerpb.JobSpec{
				Topic:       "ingest",
				Partition:   1,
				StartOffset: 200,
				EndOffset:   300,
			})
		})
	})

	// Test 1: Complete a single job at the front of the queue
	t.Run("complete_single_job_at_front", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		jobSpec := schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		}
		ps.addPlannedJob("job1", jobSpec)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(100), ps.offsets[0].committed.offset())

		err := ps.completeJob("job1")
		require.NoError(t, err)

		// Verify job was completed and garbage collected
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(200), ps.offsets[0].committed.offset())
	})

	t.Run("complete_nonexistent_job", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)
		// Try to complete a job that doesn't exist
		err := ps.completeJob("nonexistent_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)
	})

	// Test 3: Complete multiple jobs in order (garbage collection)
	t.Run("complete_multiple_jobs_in_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// Add multiple planned jobs
		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job2", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 200,
			EndOffset:   300,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})

		// Verify initial state
		require.Equal(t, 3, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 3)
		require.Equal(t, int64(100), ps.offsets[0].committed.offset())

		// Complete first job - should be garbage collected
		err := ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 2, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 2)
		require.Equal(t, int64(200), ps.offsets[0].committed.offset())

		// Complete second job - should be garbage collected
		err = ps.completeJob("job2")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(300), ps.offsets[0].committed.offset())

		// Complete third job - should be garbage collected
		err = ps.completeJob("job3")
		require.NoError(t, err)
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(400), ps.offsets[0].committed.offset())
	})

	// Test 4: Complete jobs out of order (only front jobs get garbage collected)
	t.Run("complete_jobs_out_of_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// Add multiple planned jobs
		ps.addPlannedJob("job1", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		})
		ps.addPlannedJob("job2", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 200,
			EndOffset:   300,
		})
		ps.addPlannedJob("job3", schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 300,
			EndOffset:   400,
		})

		// Complete job2 first (out of order)
		err := ps.completeJob("job2")
		require.NoError(t, err)

		// Job2 should be marked complete but not garbage collected yet
		require.Equal(t, 3, ps.plannedJobs.Len(), "expecting no garbage collection yet")
		require.Len(t, ps.plannedJobsMap, 3, "expecting no garbage collection yet")
		require.Equal(t, int64(100), ps.offsets[0].committed.offset(), "should be no advancement yet")

		// Complete job1 - should garbage collect both job1 and job2
		err = ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len(), "expecting garbage collection of job1 and job2")
		require.Len(t, ps.plannedJobsMap, 1, "expecting garbage collection of job1 and job2")
		require.Equal(t, int64(300), ps.offsets[0].committed.offset(), "should be advanced commit to the end of job3")
		require.Equal(t, "job3", ps.plannedJobs.Front().Value.(*jobState).jobID, "expecting job3 to be the remaining planned job")
	})

	// Test 5: Complete job with empty partition state
	t.Run("complete_job_empty_partition", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// Try to complete a job when no jobs exist
		err := ps.completeJob("any_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)

		// Verify state remains unchanged
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(100), ps.offsets[0].committed.offset())
	})
}

func TestNewSingleClusterOffsets(t *testing.T) {
	m := newSchedulerMetrics(prometheus.NewPedanticRegistry(), false, 1)
	o := newSingleClusterOffsets(&m, test.NewTestingLogger(t))

	// Before observing any end offset, both offsets are empty.
	require.Equal(t, offsetEmpty, o.startOffset)
	require.Equal(t, offsetEmpty, o.endOffset)

	// The first observed end offset seeds the start offset, so they're equal.
	o.updateEndOffset(100)
	require.Equal(t, int64(100), o.startOffset)
	require.Equal(t, int64(100), o.endOffset)

	// Subsequent observations only advance the latest offset, so they diverge.
	o.updateEndOffset(200)
	require.Equal(t, int64(100), o.startOffset)
	require.Equal(t, int64(200), o.endOffset)

	// Observing the same offset again is allowed and changes nothing.
	o.updateEndOffset(200)
	require.Equal(t, int64(100), o.startOffset)
	require.Equal(t, int64(200), o.endOffset)

	// An end offset that moves backwards is a logical error and panics.
	require.Panics(t, func() { o.updateEndOffset(150) })
}
