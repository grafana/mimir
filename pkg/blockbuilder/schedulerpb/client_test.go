// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/util/test"
)

func TestGetJob(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.jobs = []mockJob{
		{key: JobKey{Id: "foo/983/585", Epoch: 4523}, spec: JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}},
	}

	cli, cliErr := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 5*time.Minute, 20*time.Minute)
	require.NoError(t, cliErr)
	ctx := context.Background()
	key, spec, err := cli.GetJob(ctx)

	require.NoError(t, err)
	require.Equal(t, sched.jobs[0].key, key)
	require.Equal(t, sched.jobs[0].spec, spec)
	require.Equal(t, 1, sched.assignCalls)
}

func TestCompleteJob(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.jobs = []mockJob{
		{key: JobKey{Id: "foo/983/585", Epoch: 4523}, spec: JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}},
	}

	clii, cliErr := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 5*time.Minute, 64*time.Hour)
	require.NoError(t, cliErr)
	cli := clii.(*schedulerClient)
	ctx := context.Background()
	key, _, err := cli.GetJob(ctx)
	require.NoError(t, err)

	require.NoError(t, cli.CompleteJob(key))
	require.True(t, cli.jobs[key].complete)
	require.True(t, cli.jobs[key].forgetTime.After(time.Now()))

	require.ErrorContains(t, cli.CompleteJob(JobKey{Id: "erroneous id"}), "not found")
}

func TestSendUpdates(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.jobs = []mockJob{
		{key: JobKey{Id: "foo/983/585", Epoch: 4523}, spec: JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}},
		{key: JobKey{Id: "foo/4/92842", Epoch: 4524}, spec: JobSpec{Topic: "foo", Partition: 4, StartOffset: 92842}},
	}

	clii, cliErr := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 5*time.Minute, 100*time.Hour)
	require.NoError(t, cliErr)
	cli := clii.(*schedulerClient)
	ctx := context.Background()
	k, _, err := cli.GetJob(ctx)
	require.NoError(t, err)

	require.Zero(t, sched.updateCalls)

	cli.sendUpdates(ctx)
	require.Equal(t, 1, sched.updateCalls)
	cli.sendUpdates(ctx)
	require.Equal(t, 2, sched.updateCalls)

	// Introduce another job.
	require.NoError(t, cli.CompleteJob(k))
	_, _, err2 := cli.GetJob(ctx)
	require.NoError(t, err2)

	// Now we should be sending updates for two jobs.
	cli.sendUpdates(ctx)
	require.Equal(t, 4, sched.updateCalls)
	cli.sendUpdates(ctx)
	require.Equal(t, 6, sched.updateCalls)
}

// a mutator for tests.
func (c *schedulerClient) completeJobForTest(k JobKey, forgetTime time.Time) {
	c.jobs[k].complete = true
	c.jobs[k].forgetTime = forgetTime
}

func TestForget(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.jobs = []mockJob{
		{key: JobKey{Id: "foo/983/585", Epoch: 4523}, spec: JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}},
		{key: JobKey{Id: "foo/4/92842", Epoch: 4524}, spec: JobSpec{Topic: "foo", Partition: 4, StartOffset: 92842}},
	}

	clii, cliErr := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 5*time.Minute, 20*time.Minute)
	require.NoError(t, cliErr)
	ctx := context.Background()
	cli := clii.(*schedulerClient)
	k, _, err := cli.GetJob(ctx)
	require.NoError(t, err)
	require.Len(t, cli.jobs, 1)

	k2, _, err2 := cli.GetJob(ctx)
	require.NoError(t, err2)
	require.Len(t, cli.jobs, 2)

	// Forgetting with no eligible jobs should do nothing.
	cli.completeJobForTest(k, time.Now().Add(1_000_000*time.Hour))
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 2, "job count should be unchanged with no eligible jobs for purging")

	// Forgetting with some eligible jobs.
	cli.completeJobForTest(k, time.Now().Add(-1*time.Minute))
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 1, "job should have been purged")

	// Forget the other...
	cli.completeJobForTest(k2, time.Now().Add(-1*time.Minute))
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 0, "job should have been purged")

	// And make sure we can handle an empty map.
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 0, "forgetting an empty map should do nothing")
}

type mockSchedulerClient struct {
	mu          sync.Mutex
	jobs        []mockJob
	currentJob  int
	assignCalls int
	updateCalls int
}

type mockJob struct {
	key  JobKey
	spec JobSpec
}

func (m *mockSchedulerClient) AssignJob(_ context.Context, _ *AssignJobRequest, _ ...grpc.CallOption) (*AssignJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignCalls++

	if m.currentJob >= len(m.jobs) {
		return nil, errors.New("no jobs available")
	}

	j := m.jobs[m.currentJob]
	m.currentJob++

	return &AssignJobResponse{
		Key:  &j.key,
		Spec: &j.spec,
	}, nil
}

func (m *mockSchedulerClient) UpdateJob(_ context.Context, _ *UpdateJobRequest, _ ...grpc.CallOption) (*UpdateJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateCalls++

	return &UpdateJobResponse{}, nil
}

var _ BlockBuilderSchedulerClient = (*mockSchedulerClient)(nil)
