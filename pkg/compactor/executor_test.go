// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/grafana/mimir/pkg/compactor/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// mockCompactorSchedulerClient implements CompactorSchedulerClient
type mockCompactorSchedulerClient struct {
	mu                 sync.Mutex
	leaseJobCallCount  int
	updateJobCallCount int
	LeaseJobFunc       func(ctx context.Context, in *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error)
	UpdateJobFunc      func(ctx context.Context, in *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error)
	PlannedJobsFunc    func(ctx context.Context, in *schedulerpb.PlannedJobsRequest) (*schedulerpb.PlannedJobsResponse, error)
	UpdatePlanJobFunc  func(ctx context.Context, in *schedulerpb.UpdatePlanJobRequest) (*schedulerpb.UpdateJobResponse, error)
}

func (m *mockCompactorSchedulerClient) LeaseJob(ctx context.Context, in *schedulerpb.LeaseJobRequest, opts ...grpc.CallOption) (*schedulerpb.LeaseJobResponse, error) {
	m.mu.Lock()
	m.leaseJobCallCount++
	m.mu.Unlock()
	return m.LeaseJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) UpdateCompactionJob(ctx context.Context, in *schedulerpb.UpdateCompactionJobRequest, opts ...grpc.CallOption) (*schedulerpb.UpdateJobResponse, error) {
	m.mu.Lock()
	m.updateJobCallCount++
	m.mu.Unlock()
	return m.UpdateJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) PlannedJobs(ctx context.Context, in *schedulerpb.PlannedJobsRequest, opts ...grpc.CallOption) (*schedulerpb.PlannedJobsResponse, error) {
	return m.PlannedJobsFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) UpdatePlanJob(ctx context.Context, in *schedulerpb.UpdatePlanJobRequest, opts ...grpc.CallOption) (*schedulerpb.UpdateJobResponse, error) {
	return m.UpdatePlanJobFunc(ctx, in)
}

func TestSchedulerExecutor_JobStatusUpdates(t *testing.T) {

	testCases := map[string]struct {
		setupMock           func(*mockCompactorSchedulerClient)
		expectedFinalStatus schedulerpb.UpdateType
	}{
		"successful_job_sends_complete": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					return &schedulerpb.LeaseJobResponse{
						Key: &schedulerpb.JobKey{Id: "success-job"},
						Spec: &schedulerpb.JobSpec{
							Tenant: "test-tenant",
							Job:    &schedulerpb.CompactionJob{Split: false},
						},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, in *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedFinalStatus: schedulerpb.COMPLETE,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			tc.setupMock(mockSchedulerClient)

			// Track first and last status updates. We wrap the test's UpdateJobFunc w. a new UpdateJobFunc
			// that stores the first update message sent
			var firstUpdate, lastUpdate schedulerpb.UpdateType
			mockSchedulerClient.UpdateJobFunc = func(ctx context.Context, in *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error) {
				if mockSchedulerClient.updateJobCallCount == 0 {
					firstUpdate = in.Update
				}
				lastUpdate = in.Update
				return &schedulerpb.UpdateJobResponse{}, nil
			}

			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
			cfg.SchedulerUpdateInterval = 1 * time.Hour
			cfg.CompactionConcurrency = 1

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("test-tenant/", []string{}, nil)
			bucketClient.MockIter("test-tenant/markers/", []string{}, nil)

			schedulerExec := &SchedulerExecutor{
				cfg:             cfg,
				logger:          log.NewNopLogger(),
				schedulerClient: mockSchedulerClient,
			}

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient
			c.shardingStrategy = schedulerExec.CreateShardingStrategy(
				c.compactorCfg.EnabledTenants,
				c.compactorCfg.DisabledTenants,
				c.ring,
				c.ringLifecycler,
				c.cfgProvider,
			)

			gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, "compactor-1")
			require.NoError(t, err)
			require.True(t, gotWork, "should return true to indicate a job was acquired from the scheduler")

			// Wait for job status goroutine to send the final status
			time.Sleep(100 * time.Millisecond)

			require.GreaterOrEqual(t, mockSchedulerClient.updateJobCallCount, 2, "should have at least two status updates")
			assert.Equal(t, schedulerpb.IN_PROGRESS.String(), firstUpdate.String(), "first job status should be IN_PROGRESS")
			assert.Equal(t, tc.expectedFinalStatus.String(), lastUpdate.String(), "final job status should match expected")
		})
	}
}

func TestSchedulerExecutor_BackoffBehavior(t *testing.T) {
	var IDs = [][]byte{[]byte("block-1"), []byte("block-2")}

	tests := map[string]struct {
		setupMock           func(*mockCompactorSchedulerClient)
		expectedLeaseCalls  int
		expectedUpdateCalls int
	}{
		"scheduler_errors_should_trigger_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					return nil, errors.New("error")
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 0,
		},
		"successful_execution_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					resp := &schedulerpb.LeaseJobResponse{
						Key: &schedulerpb.JobKey{Id: "test-job"},
						Spec: &schedulerpb.JobSpec{
							Tenant: "user-1",
							Job:    &schedulerpb.CompactionJob{Split: true, BlockIds: IDs},
						},
					}
					return resp, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, _ *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 6,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			tc.setupMock(mockSchedulerClient)

			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			cfg := prepareConfig(t)
			cfg.ShardingRing.Common.InstanceID = "compactor-1"
			cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
			cfg.ShardingRing.Common.KVStore.Mock = ringStore
			cfg.SchedulerAddress = "localhost:9095"
			cfg.PlanningMode = planningModeScheduler
			// set cfg.SchedulerUpdateInterval long, only testing initial progress update
			cfg.SchedulerUpdateInterval = 1 * time.Hour

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("user-1/", []string{}, nil)
			bucketClient.MockIter("user-1/markers/", []string{}, nil)

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient

			reg := prometheus.NewPedanticRegistry()
			c.ring, c.ringLifecycler, _ = newRingAndLifecycler(cfg.ShardingRing, log.NewNopLogger(), reg)

			// Create a scheduler executor with the mock client
			schedulerExec := &SchedulerExecutor{
				cfg:             cfg,
				logger:          c.logger,
				schedulerClient: mockSchedulerClient,
			}
			c.shardingStrategy = schedulerExec.CreateShardingStrategy(
				c.compactorCfg.EnabledTenants,
				c.compactorCfg.DisabledTenants,
				c.ring,
				c.ringLifecycler,
				c.cfgProvider,
			)

			errCh := make(chan error, 1)
			go func() {
				errCh <- schedulerExec.Run(context.Background(), c)
			}()

			require.Eventually(t, func() bool {
				mockSchedulerClient.mu.Lock()
				defer mockSchedulerClient.mu.Unlock()
				return (mockSchedulerClient.leaseJobCallCount == tc.expectedLeaseCalls) &&
					(mockSchedulerClient.updateJobCallCount == tc.expectedUpdateCalls)
			}, 6*time.Second, 100*time.Millisecond)

			assert.Empty(t, errCh, "error channel should be empty")
		})
	}
}

func TestSchedulerExecutor_ServicesLifecycle(t *testing.T) {
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	inmem := objstore.NewInMemBucket()

	cfg := prepareConfig(t)
	cfg.ShardingRing.Common.InstanceID = "compactor-1"
	cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.Common.KVStore.Mock = ringStore
	cfg.SchedulerAddress = "localhost:9095"
	cfg.PlanningMode = planningModeScheduler
	c, _, _, _, _ := prepare(t, cfg, inmem)

	assert.Nil(t, c.executor, "executor should be nil before service start")
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	require.NotNil(t, c.executor, "executor should be initialized after starting")

	schedulerExec, ok := c.executor.(*SchedulerExecutor)

	require.True(t, ok, "executor should be a SchedulerExecutor in scheduler mode")
	assert.NotNil(t, schedulerExec.schedulerClient, "scheduler client should be initialized within executor")

	assert.True(t, c.ringSubservices.IsHealthy())
	require.NoError(t, c.blocksCleaner.AwaitRunning(context.Background()))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
	assert.Equal(t, schedulerExec.schedulerConn.GetState(), connectivity.Shutdown)
}

func TestSchedulerExecutor_UnreachableScheduler(t *testing.T) {

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	inmem := objstore.NewInMemBucket()

	cfg := prepareConfig(t)
	cfg.ShardingRing.Common.InstanceID = "compactor-1"
	cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.Common.KVStore.Mock = ringStore
	cfg.SchedulerAddress = "unreachable-scheduler:9095"
	cfg.PlanningMode = planningModeScheduler

	c, _, _, _, _ := prepare(t, cfg, inmem)

	// Starting should succeed if a valid cfg.SchedulerAddress string is passed, do not Dial w. block
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c), "compactor should start even when scheduler is unreachable")
	assert.Equal(t, services.Running, c.State())

	require.NotNil(t, c.executor, "executor should be initialized")
	schedulerExec, ok := c.executor.(*SchedulerExecutor)
	require.True(t, ok, "executor should be a SchedulerExecutor")
	require.NotNil(t, schedulerExec.schedulerClient, "scheduler client should be created")
	require.NotNil(t, schedulerExec.schedulerConn, "scheduler connection should be created")

	// Check LeaseJob call fails due to unreachable scheduler
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := schedulerExec.schedulerClient.LeaseJob(ctx, &schedulerpb.LeaseJobRequest{WorkerId: "test"})
	assert.Error(t, err, "LeaseJob should fail when scheduler is unreachable")

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}
