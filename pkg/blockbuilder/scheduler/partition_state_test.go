// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

func TestPartitionState(t *testing.T) {
	pt := newPartitionState("topic", 0, 1, false, testMetrics(t), log.NewNopLogger())
	sz := 1 * time.Hour

	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	var job *schedulerpb.JobSpec
	var err error

	pt.updateEndOffset(0, 100)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	pt.updateEndOffset(0, 200)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Equal(t, ptrSpec("topic", 0, 100, 200), job)

	pt.updateEndOffset(0, 201)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	pt.updateEndOffset(0, 202)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 2, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	pt.updateEndOffset(0, 203)
	job, err = pt.updateTime(time.Date(2025, 3, 1, 11, 3, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)

	pt.updateEndOffset(0, 300)
	job, err = pt.updateTime(z.Add(2*time.Hour), sz)
	require.NoError(t, err)
	require.Equal(t, ptrSpec("topic", 0, 200, 300), job)

	// If the time goes backwards, we return an error.
	pt.updateEndOffset(0, 300)
	job, err = pt.updateTime(z.Add(-2*time.Hour), sz)
	require.Nil(t, job)
	require.ErrorContains(t, err, "time went backwards")
}

func TestPartitionState_TerminallyDormantPartition(t *testing.T) {
	pt := newPartitionState("topic", 0, 1, false, testMetrics(t), log.NewNopLogger())
	sz := 1 * time.Hour
	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		z = z.Add(7 * time.Minute)
		pt.updateEndOffset(0, 0)
		j, err := pt.updateTime(z, sz)
		assert.NoError(t, err)
		assert.Nil(t, j)
	}
}

func TestPartitionState_PartitionBecomesInactive(t *testing.T) {
	pt := newPartitionState("topic", 0, 1, false, testMetrics(t), log.NewNopLogger())
	sz := 1 * time.Hour

	// A bunch of data observed:
	var j *schedulerpb.JobSpec
	var err error
	pt.updateEndOffset(0, 10)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)
	pt.updateEndOffset(0, 11)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 11, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 12, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)
	// data ceases. continue to get observations in the same bucket.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 10, 1, 13, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)

	// as we cross into the next bucket, there's still no new data.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 1, 0, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Equal(t, ptrSpec("topic", 0, 10, 12), j)
	// and we keep getting the same offset.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 2, 0, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 11, 3, 0, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)

	// And in the next job bucket, still no new data.
	pt.updateEndOffset(0, 12)
	j, err = pt.updateTime(time.Date(2025, 3, 1, 12, 1, 0, 0, time.UTC), sz)
	assert.NoError(t, err)
	assert.Nil(t, j)
}

func TestPartitionState_ParallelJobs(t *testing.T) {

	t.Run("planned job order required", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// It is a logical error to add a job to the planned list out of order. The safe completion logic requires it.

		ps.addPlannedJob("job1", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 100, 200))
		ps.addPlannedJob("job3", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 300, 400))
		require.Panics(t, func() {
			ps.addPlannedJob("job2", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 200, 300))
		})
	})

	// Test 1: Complete a single job at the front of the queue
	t.Run("complete_single_job_at_front", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		jobSpec := schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 100, 200)
		ps.addPlannedJob("job1", jobSpec)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(100), ps.committedOffset(0))

		err := ps.completeJob("job1")
		require.NoError(t, err)

		// Verify job was completed and garbage collected
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(200), ps.committedOffset(0))
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
		ps.addPlannedJob("job1", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 100, 200))
		ps.addPlannedJob("job2", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 200, 300))
		ps.addPlannedJob("job3", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 300, 400))

		// Verify initial state
		require.Equal(t, 3, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 3)
		require.Equal(t, int64(100), ps.committedOffset(0))

		// Complete first job - should be garbage collected
		err := ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 2, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 2)
		require.Equal(t, int64(200), ps.committedOffset(0))

		// Complete second job - should be garbage collected
		err = ps.completeJob("job2")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(300), ps.committedOffset(0))

		// Complete third job - should be garbage collected
		err = ps.completeJob("job3")
		require.NoError(t, err)
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(400), ps.committedOffset(0))
	})

	// Test 4: Complete jobs out of order (only front jobs get garbage collected)
	t.Run("complete_jobs_out_of_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(0, 100)

		// Add multiple planned jobs
		ps.addPlannedJob("job1", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 100, 200))
		ps.addPlannedJob("job2", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 200, 300))
		ps.addPlannedJob("job3", schedulerpb.NewNonCompartmentJobSpec("ingest", 1, 300, 400))

		// Complete job2 first (out of order)
		err := ps.completeJob("job2")
		require.NoError(t, err)

		// Job2 should be marked complete but not garbage collected yet
		require.Equal(t, 3, ps.plannedJobs.Len(), "expecting no garbage collection yet")
		require.Len(t, ps.plannedJobsMap, 3, "expecting no garbage collection yet")
		require.Equal(t, int64(100), ps.committedOffset(0), "should be no advancement yet")

		// Complete job1 - should garbage collect both job1 and job2
		err = ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len(), "expecting garbage collection of job1 and job2")
		require.Len(t, ps.plannedJobsMap, 1, "expecting garbage collection of job1 and job2")
		require.Equal(t, int64(300), ps.committedOffset(0), "should be advanced commit to the end of job3")
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
		require.Equal(t, int64(100), ps.committedOffset(0))
	})
}
