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

	j0, err := s.assign("w0")
	require.Nil(t, j0)
	require.ErrorIs(t, err, errNoJobAvailable)

	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	j, err := s.assign("w0")
	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)

	j2, err := s.assign("w0")
	require.Nil(t, j2)
	require.ErrorIs(t, err, errNoJobAvailable)

	s.addOrUpdate("job2", jobSpec{topic: "hello2", commitRecTs: time.Now()})
	j3, err := s.assign("w0")
	require.NotNil(t, j3)
	require.NoError(t, err)
	require.Equal(t, "w0", j3.assignee)
}

func TestAssignComplete(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))

	{
		err := s.completeJob("rando job", "w0")
		require.ErrorIs(t, err, errJobNotFound)
	}

	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	j, err := s.assign("w0")
	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)

	{
		err := s.completeJob("rando job", "w0")
		require.ErrorIs(t, err, errJobNotFound)
	}
	{
		err := s.completeJob(j.id, "rando worker")
		require.ErrorIs(t, err, errJobNotAssigned)
	}

	{
		err := s.completeJob(j.id, "w0")
		require.NoError(t, err)

		err2 := s.completeJob(j.id, "w0")
		require.ErrorIs(t, err2, errJobNotFound)
	}

	j2, err := s.assign("w0")
	require.Nil(t, j2, "should be no job available")
	require.ErrorIs(t, err, errNoJobAvailable)
}

func TestLease(t *testing.T) {
	s := newJobQueue(988*time.Hour, test.NewTestingLogger(t))
	s.addOrUpdate("job1", jobSpec{topic: "hello", commitRecTs: time.Now()})
	j, err := s.assign("w0")
	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)

	// Expire the lease.
	j.leaseExpiry = time.Now().Add(-1 * time.Minute)
	s.clearExpiredLeases()

	j2, err := s.assign("w1")
	require.NotNil(t, j2, "should be able to assign a job whose lease was invalidated")
	require.NoError(t, err)
	require.Equal(t, "w1", j2.assignee)

	t.Run("renewals", func(t *testing.T) {
		prevExpiry := j2.leaseExpiry
		e1 := s.renewLease(j2.id, "w1")
		require.NoError(t, e1)
		require.True(t, j2.leaseExpiry.After(prevExpiry))

		e2 := s.renewLease(j2.id, "w0")
		require.ErrorIs(t, e2, errJobNotAssigned)

		e3 := s.renewLease("job_404", "w0")
		require.ErrorIs(t, e3, errJobNotFound)
	})
}

func TestMinHeap(t *testing.T) {
	n := 140
	jobs := make([]*job, n)
	order := make([]int, n)
	for i := 0; i < n; i++ {
		jobs[i] = &job{
			id:   fmt.Sprintf("job%d", i),
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
