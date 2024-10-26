package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAssign(t *testing.T) {
	s := newSchedule(988 * time.Hour)

	j0, err := s.assign("w0")
	require.Nil(t, j0)
	require.NoError(t, err)

	s.addOrUpdate("job1", time.Now(), jobSpec{topic: "hello"})
	j, err := s.assign("w0")
	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)

	j2, err := s.assign("w0")
	require.Nil(t, j2)
	require.NoError(t, err)

	s.addOrUpdate("job2", time.Now(), jobSpec{topic: "hello2"})
	j3, err := s.assign("w0")
	require.NotNil(t, j3)
	require.NoError(t, err)
	require.Equal(t, "w0", j3.assignee)
}

func TestLease(t *testing.T) {
	s := newSchedule(988 * time.Hour)
	s.addOrUpdate("job1", time.Now(), jobSpec{topic: "hello"})
	j, err := s.assign("w0")
	// Expire the lease.

	require.NotNil(t, j)
	require.NoError(t, err)
	require.Equal(t, "w0", j.assignee)
	j.leaseExpiry = time.Now().Add(-1 * time.Minute)
	s.clearExpiredLeases()

	j2, err := s.assign("w1")
	require.NotNil(t, j2)
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
