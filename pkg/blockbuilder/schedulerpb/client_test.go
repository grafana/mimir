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
	require.Empty(t, sched.updates)

	cli.sendUpdates(ctx)
	require.ElementsMatch(t, []*UpdateJobRequest{
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
	}, sched.updates)

	cli.sendUpdates(ctx)
	require.ElementsMatch(t, []*UpdateJobRequest{
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
	}, sched.updates)

	require.NoError(t, cli.CompleteJob(k))
	_, _, err2 := cli.GetJob(ctx)
	require.NoError(t, err2)

	// Now we have one complete job and one incomplete. We should be sending updates for both.
	cli.sendUpdates(ctx)
	require.ElementsMatch(t, []*UpdateJobRequest{
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: true},
		{Key: &sched.jobs[1].key, WorkerId: "worker1", Spec: &sched.jobs[1].spec, Complete: false},
	}, sched.updates)

	cli.sendUpdates(ctx)
	require.ElementsMatch(t, []*UpdateJobRequest{
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: true},
		{Key: &sched.jobs[1].key, WorkerId: "worker1", Spec: &sched.jobs[1].spec, Complete: false},
		{Key: &sched.jobs[0].key, WorkerId: "worker1", Spec: &sched.jobs[0].spec, Complete: true},
		{Key: &sched.jobs[1].key, WorkerId: "worker1", Spec: &sched.jobs[1].spec, Complete: false},
	}, sched.updates)
}

// a mutator for tests.
func (c *schedulerClient) completeJobWithForgetTime(k JobKey, forgetTime time.Time) error {
	if err := c.CompleteJob(k); err != nil {
		return err
	}
	c.jobs[k].forgetTime = forgetTime
	return nil
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
	require.NoError(t, cli.completeJobWithForgetTime(k, time.Now().Add(1_000_000*time.Hour)))
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 2, "job count should be unchanged with no eligible jobs for purging")

	// Forgetting with some eligible jobs.
	require.NoError(t, cli.completeJobWithForgetTime(k, time.Now().Add(-1*time.Minute)))
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 1, "job should have been purged")

	// Forget the other...
	require.NoError(t, cli.completeJobWithForgetTime(k2, time.Now().Add(-1*time.Minute)))
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
	updates     []*UpdateJobRequest
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

func (m *mockSchedulerClient) UpdateJob(_ context.Context, r *UpdateJobRequest, _ ...grpc.CallOption) (*UpdateJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, r)

	return &UpdateJobResponse{}, nil
}

var _ BlockBuilderSchedulerClient = (*mockSchedulerClient)(nil)
