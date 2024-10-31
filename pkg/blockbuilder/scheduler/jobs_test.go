package scheduler

import (
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

	s.addOrUpdate("job1", time.Now(), jobSpec{topic: "hello"})
	j, err := s.assign("w0")
	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)

	j2, err := s.assign("w0")
	require.Nil(t, j2)
	require.ErrorIs(t, err, errNoJobAvailable)

	s.addOrUpdate("job2", time.Now(), jobSpec{topic: "hello2"})
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

	s.addOrUpdate("job1", time.Now(), jobSpec{topic: "hello"})
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
	s.addOrUpdate("job1", time.Now(), jobSpec{topic: "hello"})
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
