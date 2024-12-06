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
	sched.key = JobKey{Id: "foo/983/585", Epoch: 4523}
	sched.spec = JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}

	cli := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 0, 0)
	ctx := context.Background()
	key, spec, err := cli.GetJob(ctx)

	require.NoError(t, err)
	require.Equal(t, sched.key, key)
	require.Equal(t, sched.spec, spec)
	require.Equal(t, 1, sched.assignCalls)
}

func TestCompleteJob(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.key = JobKey{Id: "foo/983/585", Epoch: 4523}
	sched.spec = JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}

	cli := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 0, 64*time.Hour).(*schedulerClient)
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
	sched.key = JobKey{Id: "foo/983/585", Epoch: 4523}
	sched.spec = JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}

	cli := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 0, 0).(*schedulerClient)
	ctx := context.Background()
	_, _, err := cli.GetJob(ctx)
	require.NoError(t, err)

	require.Zero(t, sched.updateCalls)

	cli.sendUpdates(ctx)
	require.Equal(t, 1, sched.updateCalls)
	cli.sendUpdates(ctx)
	require.Equal(t, 2, sched.updateCalls)
}

func TestForget(t *testing.T) {
	sched := &mockSchedulerClient{}
	sched.key = JobKey{Id: "foo/983/585", Epoch: 4523}
	sched.spec = JobSpec{Topic: "foo", Partition: 983, StartOffset: 585}

	cli := NewSchedulerClient("worker1", sched, test.NewTestingLogger(t), 0, 0).(*schedulerClient)
	ctx := context.Background()
	_, _, err := cli.GetJob(ctx)
	require.NoError(t, err)
	require.Len(t, cli.jobs, 1)

	// Forgetting with no eligible jobs should do nothing.
	cli.jobs[sched.key] = &job{spec: sched.spec, complete: true, forgetTime: time.Now().Add(1_000_000 * time.Hour)}
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 1, "job count should be unchanged with no eligible jobs for purging")

	// Forgetting with some eligible jobs.
	cli.jobs[sched.key] = &job{spec: sched.spec, complete: true, forgetTime: time.Now().Add(-1 * time.Minute)}
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 0, "job should have been purged")

	// And make sure we can handle an empty map.
	cli.forgetOldJobs()
	require.Len(t, cli.jobs, 0, "forgetting an empty map should do nothing")
}

type mockSchedulerClient struct {
	mu          sync.Mutex
	key         JobKey
	spec        JobSpec
	assignCalls int
	updateCalls int
}

func (m *mockSchedulerClient) AssignJob(_ context.Context, _ *AssignJobRequest, _ ...grpc.CallOption) (*AssignJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignCalls++

	if m.key.Id == "" {
		return nil, errors.New("no jobs available")
	}

	return &AssignJobResponse{
		Key:  &m.key,
		Spec: &m.spec,
	}, nil
}

func (m *mockSchedulerClient) UpdateJob(_ context.Context, _ *UpdateJobRequest, _ ...grpc.CallOption) (*UpdateJobResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateCalls++

	return &UpdateJobResponse{}, nil
}

var _ BlockBuilderSchedulerClient = (*mockSchedulerClient)(nil)
