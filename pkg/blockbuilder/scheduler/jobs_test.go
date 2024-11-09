// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/heap"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestAssign(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))

	j0, j0spec, err := s.assign("w0")
	require.Empty(t, j0.id)
	require.Zero(t, j0spec)
	require.ErrorIs(t, err, errNoJobAvailable)

	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	j1, j1spec, err := s.assign("w0")
	require.NotEmpty(t, j1.id)
	require.NotZero(t, j1spec)
	require.NoError(t, err)
	require.Equal(t, "w0", s.jobs[j1.id].assignee)

	j2, j2spec, err := s.assign("w0")
	require.Zero(t, j2.id)
	require.Zero(t, j2spec)
	require.ErrorIs(t, err, errNoJobAvailable)

	s.addOrUpdate("job2", jobSpec{topic: "hello2", commitRecTs: time.Now()})
	j3, j3spec, err := s.assign("w0")
	require.NotZero(t, j3.id)
	require.NotZero(t, j3spec)
	require.NoError(t, err)
	require.Equal(t, "w0", s.jobs[j3.id].assignee)
}

func TestAssignComplete(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))

	{
		err := s.completeJob(jobKey{"rando job", 965}, "w0")
		require.ErrorIs(t, err, errJobNotFound)
	}

	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	jk, jspec, err := s.assign("w0")
	require.NotZero(t, jk)
	require.NotZero(t, jspec)
	require.NoError(t, err)
	j, ok := s.jobs[jk.id]
	require.True(t, ok)
	require.Equal(t, "w0", j.assignee)

	{
		err := s.completeJob(jobKey{"rando job", 64}, "w0")
		require.ErrorIs(t, err, errJobNotFound)
	}
	{
		err := s.completeJob(jk, "rando worker")
		require.ErrorIs(t, err, errJobNotAssigned)
	}
	{
		err := s.completeJob(jobKey{jk.id, 9999}, "w0")
		require.ErrorIs(t, err, errBadEpoch)
	}

	{
		err := s.completeJob(jk, "w0")
		require.NoError(t, err)

		err2 := s.completeJob(jk, "w0")
		require.ErrorIs(t, err2, errJobNotFound)
	}

	j2k, j2spec, err := s.assign("w0")
	require.Zero(t, j2k.id, "should be no job available")
	require.Zero(t, j2spec, "should be no job available")
	require.ErrorIs(t, err, errNoJobAvailable)
}

func TestLease(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))
	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	jk, jspec, err := s.assign("w0")
	require.NotZero(t, jk.id)
	require.NotZero(t, jspec)
	require.NoError(t, err)

	j, ok := s.jobs[jk.id]
	require.True(t, ok)
	require.Equal(t, "w0", j.assignee)

	// Expire the lease.
	j.leaseExpiry = time.Now().Add(-1 * time.Minute)
	s.clearExpiredLeases()

	j2k, j2spec, err := s.assign("w1")
	require.NotZero(t, j2k.id, "should be able to assign a job whose lease was invalidated")
	require.NotZero(t, j2spec, "should be able to assign a job whose lease was invalidated")
	require.Equal(t, j.spec, j2spec)
	require.NoError(t, err)
	j2, ok := s.jobs[j2k.id]
	require.True(t, ok)
	require.Equal(t, "w1", j2.assignee)

	t.Run("renewals", func(t *testing.T) {
		prevExpiry := j2.leaseExpiry
		e1 := s.renewLease(j2k, "w1")
		require.NoError(t, e1)
		require.True(t, j2.leaseExpiry.After(prevExpiry))

		e2 := s.renewLease(j2k, "w0")
		require.ErrorIs(t, e2, errJobNotAssigned)

		e3 := s.renewLease(jobKey{"job_404", 1}, "w0")
		require.ErrorIs(t, e3, errJobNotFound)
	})
}

// TestImportJob tests the importJob method - the method that is called to learn
// about jobs in-flight from a previous scheduler instance.
func TestImportJob(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))
	spec := jobSpec{commitRecTs: time.Now().Add(-1 * time.Hour)}
	require.NoError(t, s.importJob(jobKey{"job1", 122}, "w0", spec))
	require.NoError(t, s.importJob(jobKey{"job1", 123}, "w2", spec))
	require.ErrorIs(t, errBadEpoch, s.importJob(jobKey{"job1", 122}, "w0", spec))
	require.ErrorIs(t, errBadEpoch, s.importJob(jobKey{"job1", 60}, "w98", spec))
	require.ErrorIs(t, errJobNotAssigned, s.importJob(jobKey{"job1", 123}, "w512", spec))
	require.NoError(t, s.importJob(jobKey{"job1", 123}, "w2", spec))

	j, ok := s.jobs["job1"]
	require.True(t, ok)
	require.Equal(t, jobKey{"job1", 123}, j.key)
	require.Equal(t, spec, j.spec)
	require.Equal(t, "w2", j.assignee)
}

func TestMinHeap(t *testing.T) {
	n := 517
	jobs := make([]*job, n)
	order := make([]int, n)
	for i := 0; i < n; i++ {
		jobs[i] = &job{
			key: jobKey{
				id:    fmt.Sprintf("job%d", i),
				epoch: 0,
			},
			spec: jobSpec{topic: "hello", commitRecTs: time.Unix(int64(i), 0)},
		}
		order[i] = i
	}

	// Push them in random order.
	r := rand.New(rand.NewSource(9900))
	r.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })

	h := jobHeap{}
	for _, j := range order {
		heap.Push(&h, jobs[j])
	}

	require.Len(t, h, n)

	for i := 0; i < len(jobs); i++ {
		p := heap.Pop(&h).(*job)
		require.Equal(t, jobs[i], p, "pop order should be in increasing commitRecTs")
	}

	require.Empty(t, h)
}
