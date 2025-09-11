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
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func makeTestCompactorConfig(PlanningMode, schedulerAddress string) Config {
	return Config{
		PlanningMode:                        PlanningMode,
		SchedulerAddress:                    schedulerAddress,
		SchedulerUpdateInterval:             20 * time.Second,
		CompactionJobsOrder:                 CompactionOrderOldestFirst,
		SchedulerMinLeasingBackoff:          100 * time.Millisecond,
		SchedulerMaxLeasingBackoff:          1 * time.Second,
		MaxOpeningBlocksConcurrency:         1,
		MaxClosingBlocksConcurrency:         1,
		SymbolsFlushersConcurrency:          1,
		MaxBlockUploadValidationConcurrency: 1,
		BlockRanges:                         mimir_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour},
	}
}

// mockCompactorSchedulerClient implements CompactorSchedulerClient
type mockCompactorSchedulerClient struct {
	mu                 sync.Mutex
	leaseJobCallCount  int
	updateJobCallCount int
	firstUpdate        schedulerpb.UpdateType
	lastUpdate         schedulerpb.UpdateType
	recvPlannedReq     bool
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
	if m.updateJobCallCount == 0 {
		m.firstUpdate = in.Update
	}
	m.lastUpdate = in.Update
	m.updateJobCallCount++
	m.mu.Unlock()
	return m.UpdateJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) PlannedJobs(ctx context.Context, in *schedulerpb.PlannedJobsRequest, opts ...grpc.CallOption) (*schedulerpb.PlannedJobsResponse, error) {
	m.mu.Lock()
	m.recvPlannedReq = true
	m.mu.Unlock()
	return m.PlannedJobsFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) UpdatePlanJob(ctx context.Context, in *schedulerpb.UpdatePlanJobRequest, opts ...grpc.CallOption) (*schedulerpb.UpdateJobResponse, error) {
	m.mu.Lock()
	if m.updateJobCallCount == 0 {
		m.firstUpdate = in.Update
	}
	m.lastUpdate = in.Update
	m.updateJobCallCount++
	m.mu.Unlock()
	return m.UpdatePlanJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) GetFirstUpdate() schedulerpb.UpdateType {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.firstUpdate
}

func (m *mockCompactorSchedulerClient) GetLastUpdate() schedulerpb.UpdateType {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastUpdate
}

func (m *mockCompactorSchedulerClient) ReceivedPlannedRequest() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recvPlannedReq
}

func (m *mockCompactorSchedulerClient) GetUpdateJobCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateJobCallCount
}

func TestSchedulerExecutor_JobStatusUpdates(t *testing.T) {

	testCases := map[string]struct {
		setupMock           func(*mockCompactorSchedulerClient)
		expectedFinalStatus schedulerpb.UpdateType
	}{
		"successful_compaction_job_sends_complete": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					return &schedulerpb.LeaseJobResponse{
						Key:  &schedulerpb.JobKey{Id: "compaction-job"},
						Spec: &schedulerpb.JobSpec{Tenant: "test-tenant", Job: &schedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-1")}}, JobType: schedulerpb.COMPACTION},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, in *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedFinalStatus: schedulerpb.COMPLETE,
		},
		"successful_planning_job_no_status_update": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					return &schedulerpb.LeaseJobResponse{
						Key:  &schedulerpb.JobKey{Id: "planning-job"},
						Spec: &schedulerpb.JobSpec{Tenant: "test-tenant", Job: &schedulerpb.CompactionJob{}, JobType: schedulerpb.PLANNING},
					}, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, in *schedulerpb.UpdatePlanJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, in *schedulerpb.PlannedJobsRequest) (*schedulerpb.PlannedJobsResponse, error) {
					return &schedulerpb.PlannedJobsResponse{}, nil
				}
			},
			expectedFinalStatus: schedulerpb.IN_PROGRESS,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			tc.setupMock(mockSchedulerClient)

			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
			cfg.SchedulerUpdateInterval = 1 * time.Hour
			cfg.CompactionConcurrency = 1

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("test-tenant/", []string{}, nil)
			bucketClient.MockIter("test-tenant/markers/", []string{}, nil)

			schedulerExec := &schedulerExecutor{
				cfg:             cfg,
				logger:          log.NewNopLogger(),
				schedulerClient: mockSchedulerClient,
			}

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient
			c.shardingStrategy = schedulerExec.createShardingStrategy(
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

			// Check behavior based on test case
			if tc.expectedFinalStatus == schedulerpb.IN_PROGRESS {
				// Planning job: should only have initial IN_PROGRESS update, no final status
				require.True(t, mockSchedulerClient.ReceivedPlannedRequest(), "planning jobs should send PlannedJobs message")
				assert.Equal(t, schedulerpb.IN_PROGRESS.String(), mockSchedulerClient.GetFirstUpdate().String(), "planning jobs should only send IN_PROGRESS status")
				assert.Equal(t, schedulerpb.IN_PROGRESS.String(), mockSchedulerClient.GetLastUpdate().String(), "planning jobs should not send final status update")
			} else {
				// Compaction job: should have at least one status update
				require.GreaterOrEqual(t, mockSchedulerClient.GetUpdateJobCallCount(), 1, "compaction jobs should have at least one status update")
				assert.Equal(t, tc.expectedFinalStatus.String(), mockSchedulerClient.GetLastUpdate().String(), "final job status should match expected")
				assert.False(t, mockSchedulerClient.ReceivedPlannedRequest(), "compaction jobs should not send PlannedJobs message")
			}
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
		"successful_compaction_execution_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					resp := &schedulerpb.LeaseJobResponse{
						Key: &schedulerpb.JobKey{Id: "compaction-job"},
						Spec: &schedulerpb.JobSpec{
							Tenant:  "user-1",
							Job:     &schedulerpb.CompactionJob{Split: true, BlockIds: IDs},
							JobType: schedulerpb.COMPACTION,
						},
					}
					return resp, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, _ *schedulerpb.UpdateCompactionJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 3, // Only final COMPLETE status updates, no immediate IN_PROGRESS
		},
		"successful_planning_execution_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *schedulerpb.LeaseJobRequest) (*schedulerpb.LeaseJobResponse, error) {
					resp := &schedulerpb.LeaseJobResponse{
						Key: &schedulerpb.JobKey{Id: "planning-job"},
						Spec: &schedulerpb.JobSpec{
							Tenant:  "user-1",
							Job:     &schedulerpb.CompactionJob{Split: false, BlockIds: [][]byte{}}, // Empty BlockIds = planning
							JobType: schedulerpb.PLANNING,
						},
					}
					return resp, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, _ *schedulerpb.UpdatePlanJobRequest) (*schedulerpb.UpdateJobResponse, error) {
					return &schedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, _ *schedulerpb.PlannedJobsRequest) (*schedulerpb.PlannedJobsResponse, error) {
					return &schedulerpb.PlannedJobsResponse{}, nil
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 0, // Planning jobs no longer send initial IN_PROGRESS updates, only periodic ones from ticker
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
			schedulerExec := &schedulerExecutor{
				cfg:             cfg,
				logger:          c.logger,
				schedulerClient: mockSchedulerClient,
			}
			c.shardingStrategy = schedulerExec.createShardingStrategy(
				c.compactorCfg.EnabledTenants,
				c.compactorCfg.DisabledTenants,
				c.ring,
				c.ringLifecycler,
				c.cfgProvider,
			)

			errCh := make(chan error, 1)
			go func() {
				errCh <- schedulerExec.run(context.Background(), c)
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

	schedulerExec, ok := c.executor.(*schedulerExecutor)

	require.True(t, ok, "executor should be a schedulerExecutor in scheduler mode")
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
	schedulerExec, ok := c.executor.(*schedulerExecutor)
	require.True(t, ok, "executor should be a schedulerExecutor")
	require.NotNil(t, schedulerExec.schedulerClient, "scheduler client should be created")
	require.NotNil(t, schedulerExec.schedulerConn, "scheduler connection should be created")

	// Check LeaseJob call fails due to unreachable scheduler
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err := schedulerExec.schedulerClient.LeaseJob(ctx, &schedulerpb.LeaseJobRequest{WorkerId: "test"})
	assert.Error(t, err, "LeaseJob should fail when scheduler is unreachable")

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}
