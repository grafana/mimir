// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/test"
)

type testSpec struct {
	topic       string
	commitRecTs time.Time
}

func TestAssign(t *testing.T) {
	s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
	require.NotNil(t, s)

	j0, j0spec, err := s.assign("w0")
	require.Empty(t, j0.id)
	require.Zero(t, j0spec)
	require.ErrorIs(t, err, errNoJobAvailable)

	e := s.add("job1", testSpec{topic: "hello", commitRecTs: time.Now()})
	require.NoError(t, e)
	j1, j1spec, err := s.assign("w0")
	require.NotEmpty(t, j1.id)
	require.NotZero(t, j1spec)
	require.NoError(t, err)
	require.Equal(t, "w0", s.jobs[j1.id].assignee)

	j2, j2spec, err := s.assign("w0")
	require.Zero(t, j2.id)
	require.Zero(t, j2spec)
	require.ErrorIs(t, err, errNoJobAvailable)

	e = s.add("job2", testSpec{topic: "hello2", commitRecTs: time.Now()})
	require.NoError(t, e)
	j3, j3spec, err := s.assign("w0")
	require.NotZero(t, j3.id)
	require.NotZero(t, j3spec)
	require.NoError(t, err)
	require.Equal(t, "w0", s.jobs[j3.id].assignee)
}

func TestAssignComplete(t *testing.T) {
	s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
	require.NotNil(t, s)

	{
		err := s.completeJob(jobKey{"rando job", 965}, "w0")
		require.ErrorIs(t, err, errJobNotFound)
	}

	e := s.add("job1", testSpec{topic: "hello", commitRecTs: time.Now()})
	require.NoError(t, e)
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
	s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
	require.NotNil(t, s)

	e := s.add("job1", testSpec{topic: "hello", commitRecTs: time.Now()})
	require.NoError(t, e)
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

func TestPersistentFailure(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(reg), test.NewTestingLogger(t))
	require.NotNil(t, s)

	require.NoError(t, s.add("job1", testSpec{topic: "hello", commitRecTs: time.Now()}))

	jk, jspec, err := s.assign("w0")
	require.NotZero(t, jk.id)
	require.NotZero(t, jspec)
	require.NoError(t, err)

	j, ok := s.jobs[jk.id]
	require.True(t, ok)
	require.Equal(t, "w0", j.assignee)

	for range s.jobFailuresAllowed + 3 {
		// Expire the lease.
		j.leaseExpiry = time.Now().Add(-1 * time.Minute)
		s.clearExpiredLeases()

		a2k, a2spec, err := s.assign("w0")
		require.Equal(t, jk.id, a2k.id)
		require.Equal(t, jspec, a2spec)
		require.NoError(t, err)
	}

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(
		`# HELP cortex_blockbuilder_scheduler_persistent_job_failures_total The number of times a job failed persistently beyond the allowed max fail count.
		# TYPE cortex_blockbuilder_scheduler_persistent_job_failures_total counter
		cortex_blockbuilder_scheduler_persistent_job_failures_total{} 3
	`), "cortex_blockbuilder_scheduler_persistent_job_failures_total"), "expect 3 failures in excess of the max fail count")
}

// TestImportJob tests the importJob method - the method that is called to learn
// about jobs in-flight from a previous scheduler instance.
func TestImportJob(t *testing.T) {
	s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
	require.NotNil(t, s)

	spec := testSpec{commitRecTs: time.Now().Add(-1 * time.Hour)}
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

func TestJobCreationPolicies(t *testing.T) {
	t.Run("noOpJobCreationPolicy", func(t *testing.T) {
		s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
		require.NotNil(t, s)

		// noOp policy should always allow job creation
		require.NoError(t, s.add("job1", testSpec{topic: "topic1"}))
		require.NoError(t, s.add("job2", testSpec{topic: "topic2"}))

		_, ok1 := s.jobs["job1"]
		require.True(t, ok1, "job1 should be created")
		_, ok2 := s.jobs["job2"]
		require.True(t, ok2, "job2 should be created")
	})

	t.Run("allowNone", func(t *testing.T) {
		s := newJobQueue(988*time.Hour, noOpJobCreationPolicy[testSpec]{}, 2, newSchedulerMetrics(prometheus.NewPedanticRegistry()), test.NewTestingLogger(t))
		require.NotNil(t, s)
		s.creationPolicy = allowNoneJobCreationPolicy[testSpec]{}

		// allowNone policy should never allow job creation
		require.ErrorIs(t, s.add("job1", testSpec{topic: "topic1"}), errJobCreationDisallowed)
		require.ErrorIs(t, s.add("job2", testSpec{topic: "topic2"}), errJobCreationDisallowed)

		_, ok1 := s.jobs["job1"]
		require.False(t, ok1, "job1 should not be created")
		_, ok2 := s.jobs["job2"]
		require.False(t, ok2, "job2 should not be created")
	})
}

// allowNoneJobCreationPolicy is a job creation policy that never allows job creation.
type allowNoneJobCreationPolicy[T any] struct{}

func (p allowNoneJobCreationPolicy[T]) canCreateJob(_ jobKey, _ *T, _ []*T) bool { // nolint:unused
	return false
}

var _ jobCreationPolicy[any] = (*allowNoneJobCreationPolicy[any])(nil)
