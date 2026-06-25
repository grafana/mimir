// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

func TestPartitionState(t *testing.T) {
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
	sz := 1 * time.Hour

	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	var job *schedulerpb.JobSpec
	var err error

	job, err = pt.updateEndOffset(100, time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)
	job, err = pt.updateEndOffset(200, time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 100,
		EndOffset:   200,
	}, job)
	require.Nil(t, err)

	job, err = pt.updateEndOffset(201, time.Date(2025, 3, 1, 11, 1, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.NoError(t, err)
	job, err = pt.updateEndOffset(202, time.Date(2025, 3, 1, 11, 2, 10, 0, time.UTC), sz)
	require.NoError(t, err)
	require.Nil(t, job)
	job, err = pt.updateEndOffset(203, time.Date(2025, 3, 1, 11, 3, 10, 0, time.UTC), sz)
	require.Nil(t, job)
	require.Nil(t, err)

	job, err = pt.updateEndOffset(300, z.Add(2*time.Hour), sz)
	require.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 200,
		EndOffset:   300,
	}, job)
	require.NoError(t, err)

	// And, if the time goes backwards, we return an error.
	job, err = pt.updateEndOffset(300, z.Add(-2*time.Hour), sz)
	require.Nil(t, job)
	require.ErrorContains(t, err, "time went backwards")
}

func TestPartitionState_TerminallyDormantPartition(t *testing.T) {
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
	sz := 1 * time.Hour
	z := time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC)

	for i := 0; i < 1000; i++ {
		z = z.Add(7 * time.Minute)
		j, err := pt.updateEndOffset(0, z, sz)
		assert.Nil(t, j)
		assert.NoError(t, err)
	}
}

func TestPartitionState_PartitionBecomesInactive(t *testing.T) {
	pt := &partitionState{
		topic:     "topic",
		partition: 0,
	}
	sz := 1 * time.Hour

	// A bunch of data observed:
	var j *schedulerpb.JobSpec
	var err error
	j, err = pt.updateEndOffset(10, time.Date(2025, 3, 1, 10, 1, 10, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(11, time.Date(2025, 3, 1, 10, 1, 11, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 10, 1, 12, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	// data ceases. continue to get observations in the same bucket.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 10, 1, 13, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// as we cross into the next bucket, there's still no new data.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 1, 0, 0, time.UTC), sz)
	assert.Equal(t, &schedulerpb.JobSpec{
		Topic:       "topic",
		Partition:   0,
		StartOffset: 10,
		EndOffset:   12,
	}, j)
	assert.NoError(t, err)
	// and we keep getting the same offset.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 2, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 11, 3, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)

	// And in the next job bucket, still no new data.
	j, err = pt.updateEndOffset(12, time.Date(2025, 3, 1, 12, 1, 0, 0, time.UTC), sz)
	assert.Nil(t, j)
	assert.NoError(t, err)
}

func TestPartitionState_ParallelJobs(t *testing.T) {
	t.Run("planned job order required", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

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
		ps.initCommit(100)

		jobSpec := schedulerpb.JobSpec{
			Topic:       "ingest",
			Partition:   1,
			StartOffset: 100,
			EndOffset:   200,
		}
		ps.addPlannedJob("job1", jobSpec)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(100), ps.committed.offset())

		err := ps.completeJob("job1")
		require.NoError(t, err)

		// Verify job was completed and garbage collected
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(200), ps.committed.offset())
	})

	t.Run("complete_nonexistent_job", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)
		// Try to complete a job that doesn't exist
		err := ps.completeJob("nonexistent_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)
	})

	// Test 3: Complete multiple jobs in order (garbage collection)
	t.Run("complete_multiple_jobs_in_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

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
		require.Equal(t, int64(100), ps.committed.offset())

		// Complete first job - should be garbage collected
		err := ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 2, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 2)
		require.Equal(t, int64(200), ps.committed.offset())

		// Complete second job - should be garbage collected
		err = ps.completeJob("job2")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 1)
		require.Equal(t, int64(300), ps.committed.offset())

		// Complete third job - should be garbage collected
		err = ps.completeJob("job3")
		require.NoError(t, err)
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(400), ps.committed.offset())
	})

	// Test 4: Complete jobs out of order (only front jobs get garbage collected)
	t.Run("complete_jobs_out_of_order", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

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
		require.Equal(t, int64(100), ps.committed.offset(), "should be no advancement yet")

		// Complete job1 - should garbage collect both job1 and job2
		err = ps.completeJob("job1")
		require.NoError(t, err)
		require.Equal(t, 1, ps.plannedJobs.Len(), "expecting garbage collection of job1 and job2")
		require.Len(t, ps.plannedJobsMap, 1, "expecting garbage collection of job1 and job2")
		require.Equal(t, int64(300), ps.committed.offset(), "should be advanced commit to the end of job3")
		require.Equal(t, "job3", ps.plannedJobs.Front().Value.(*jobState).jobID, "expecting job3 to be the remaining planned job")
	})

	// Test 5: Complete job with empty partition state
	t.Run("complete_job_empty_partition", func(t *testing.T) {
		sched, _ := mustScheduler(t, 4)
		ps := sched.getPartitionState("ingest", 1)
		ps.initCommit(100)

		// Try to complete a job when no jobs exist
		err := ps.completeJob("any_job")
		require.Error(t, err)
		require.ErrorIs(t, err, errJobNotFound)

		// Verify state remains unchanged
		require.Equal(t, 0, ps.plannedJobs.Len())
		require.Len(t, ps.plannedJobsMap, 0)
		require.Equal(t, int64(100), ps.committed.offset())
	})
}
