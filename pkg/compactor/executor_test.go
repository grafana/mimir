// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
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
		MetaSyncConcurrency:                 1, // Must be > 0 to avoid blocking in MetaFetcher
		CompactionConcurrency:               1, // Must be > 0 for BucketCompactor
		BlockSyncConcurrency:                1, // Must be > 0 for block syncing
	}
}

// mockCompactorSchedulerClient implements CompactorSchedulerClient
type mockCompactorSchedulerClient struct {
	mu                 sync.Mutex
	leaseJobCallCount  int
	updateJobCallCount int
	firstUpdate        compactorschedulerpb.UpdateType
	lastUpdate         compactorschedulerpb.UpdateType
	recvPlannedReq     bool
	LeaseJobFunc       func(ctx context.Context, in *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error)
	UpdateJobFunc      func(ctx context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error)
	PlannedJobsFunc    func(ctx context.Context, in *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error)
	UpdatePlanJobFunc  func(ctx context.Context, in *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error)
}

func (m *mockCompactorSchedulerClient) LeaseJob(ctx context.Context, in *compactorschedulerpb.LeaseJobRequest, opts ...grpc.CallOption) (*compactorschedulerpb.LeaseJobResponse, error) {
	m.mu.Lock()
	m.leaseJobCallCount++
	m.mu.Unlock()
	return m.LeaseJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) UpdateCompactionJob(ctx context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest, opts ...grpc.CallOption) (*compactorschedulerpb.UpdateJobResponse, error) {
	m.mu.Lock()
	if m.updateJobCallCount == 0 {
		m.firstUpdate = in.Update
	}
	m.lastUpdate = in.Update
	m.updateJobCallCount++
	m.mu.Unlock()
	return m.UpdateJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) PlannedJobs(ctx context.Context, in *compactorschedulerpb.PlannedJobsRequest, opts ...grpc.CallOption) (*compactorschedulerpb.PlannedJobsResponse, error) {
	m.mu.Lock()
	m.recvPlannedReq = true
	m.mu.Unlock()
	return m.PlannedJobsFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) UpdatePlanJob(ctx context.Context, in *compactorschedulerpb.UpdatePlanJobRequest, opts ...grpc.CallOption) (*compactorschedulerpb.UpdateJobResponse, error) {
	m.mu.Lock()
	if m.updateJobCallCount == 0 {
		m.firstUpdate = in.Update
	}
	m.lastUpdate = in.Update
	m.updateJobCallCount++
	m.mu.Unlock()
	return m.UpdatePlanJobFunc(ctx, in)
}

func (m *mockCompactorSchedulerClient) GetFirstUpdate() compactorschedulerpb.UpdateType {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.firstUpdate
}

func (m *mockCompactorSchedulerClient) GetLastUpdate() compactorschedulerpb.UpdateType {
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
		expectedFinalStatus compactorschedulerpb.UpdateType
	}{
		"successful_compaction_job_sends_complete": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{[]byte("block-1")}}, JobType: compactorschedulerpb.COMPACTION},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedFinalStatus: compactorschedulerpb.COMPLETE,
		},
		"successful_planning_job_no_status_update": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "planning-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.PLANNING},
					}, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, in *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, in *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
					return &compactorschedulerpb.PlannedJobsResponse{}, nil
				}
			},
			expectedFinalStatus: compactorschedulerpb.IN_PROGRESS,
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

			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)
			schedulerExec.schedulerClient = mockSchedulerClient

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient

			gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, "compactor-1")
			require.NoError(t, err)
			require.True(t, gotWork, "should return true to indicate a job was acquired from the scheduler")

			// Wait for job status goroutine to send the final status
			time.Sleep(100 * time.Millisecond)

			if tc.expectedFinalStatus == compactorschedulerpb.IN_PROGRESS {
				// Planning job: should only have initial IN_PROGRESS update, no final status
				require.True(t, mockSchedulerClient.ReceivedPlannedRequest(), "planning jobs should send PlannedJobs message")
				assert.Equal(t, compactorschedulerpb.IN_PROGRESS.String(), mockSchedulerClient.GetFirstUpdate().String(), "planning jobs should only send IN_PROGRESS status")
				assert.Equal(t, compactorschedulerpb.IN_PROGRESS.String(), mockSchedulerClient.GetLastUpdate().String(), "planning jobs should not send final status update")
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
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return nil, errors.New("error")
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 0,
		},
		"successful_compaction_execution_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					resp := &compactorschedulerpb.LeaseJobResponse{
						Key: &compactorschedulerpb.JobKey{Id: "compaction-job"},
						Spec: &compactorschedulerpb.JobSpec{
							Tenant:  "user-1",
							Job:     &compactorschedulerpb.CompactionJob{Split: true, BlockIds: IDs},
							JobType: compactorschedulerpb.COMPACTION,
						},
					}
					return resp, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectedLeaseCalls:  3,
			expectedUpdateCalls: 3, // Only final COMPLETE status updates, no immediate IN_PROGRESS
		},
		"successful_planning_execution_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					resp := &compactorschedulerpb.LeaseJobResponse{
						Key: &compactorschedulerpb.JobKey{Id: "planning-job"},
						Spec: &compactorschedulerpb.JobSpec{
							Tenant:  "user-1",
							Job:     &compactorschedulerpb.CompactionJob{Split: false, BlockIds: [][]byte{}}, // Empty BlockIds = planning
							JobType: compactorschedulerpb.PLANNING,
						},
					}
					return resp, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, _ *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
					return &compactorschedulerpb.PlannedJobsResponse{}, nil
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
			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)
			schedulerExec.schedulerClient = mockSchedulerClient

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
	_, err := schedulerExec.schedulerClient.LeaseJob(ctx, &compactorschedulerpb.LeaseJobRequest{WorkerId: "test"})
	assert.Error(t, err, "LeaseJob should fail when scheduler is unreachable")

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
}

func TestSchedulerExecutor_PlannedJobsRetryBehavior(t *testing.T) {
	callCount := 0
	failuresBeforeSuccess := 3

	mockSchedulerClient := &mockCompactorSchedulerClient{
		LeaseJobFunc: func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
			return &compactorschedulerpb.LeaseJobResponse{
				Key:  &compactorschedulerpb.JobKey{Id: "planning-job"},
				Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.PLANNING},
			}, nil
		},
		UpdatePlanJobFunc: func(_ context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
		PlannedJobsFunc: func(_ context.Context, _ *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
			callCount++
			if callCount <= failuresBeforeSuccess {
				return nil, status.Error(codes.Unavailable, "scheduler unavailable")
			}
			return &compactorschedulerpb.PlannedJobsResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")

	schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	schedulerExec.schedulerClient = mockSchedulerClient

	c, _, _, _, _ := prepareWithConfigProvider(t, cfg, &bucket.ClientMock{}, newMockConfigProvider())

	gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, "compactor-1")
	require.NoError(t, err, "should eventually succeed with plannedJobs retry policy")
	require.True(t, gotWork)
	require.Equal(t, failuresBeforeSuccess+1, callCount)
}

func TestSchedulerExecutor_NoGoRoutineLeak(t *testing.T) {
	defer testutil.VerifyNoLeak(t, goleak.IgnoreCurrent())

	mockSchedulerClient := &mockCompactorSchedulerClient{
		LeaseJobFunc: func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
			return &compactorschedulerpb.LeaseJobResponse{
				Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
				Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.COMPACTION},
			}, nil
		},
		UpdateJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
	cfg.SchedulerUpdateInterval = 10 * time.Millisecond // Short interval to trigger the updater quickly

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("test-tenant/", []string{}, nil)
	bucketClient.MockIter("test-tenant/markers/", []string{}, nil)

	schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	schedulerExec.schedulerClient = mockSchedulerClient

	defer func(t *testing.T) {
		if schedulerExec.schedulerConn != nil {
			schedulerExec.schedulerConn.Close()
		}
	}(t)

	c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
	c.bucketClient = bucketClient

	ctx, cancel := context.WithCancel(context.Background())
	gotWork, err := schedulerExec.leaseAndExecuteJob(ctx, c, "compactor-1")
	require.NoError(t, err)
	require.True(t, gotWork)

	// wait for leaseAndExecuteJob internal updater to start, cancel the context, and then wait
	// to verify no goroutine leak, goleak.VerifyNone will fail if there are any leaked goroutines
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestSchedulerExecutor_JobCancellationOn_NotFoundResponse(t *testing.T) {

	updateCallCount := 0
	jobCanceled := make(chan struct{})

	mockSchedulerClient := &mockCompactorSchedulerClient{
		UpdateJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			updateCallCount++
			// Return NOT_FOUND after first heartbeat to simulate job cancellation by scheduler
			if updateCallCount > 1 {
				return nil, status.Error(codes.NotFound, "job not found")
			}
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
	cfg.SchedulerUpdateInterval = 10 * time.Millisecond // Short interval for fast test

	schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	defer schedulerExec.schedulerConn.Close()
	schedulerExec.schedulerClient = mockSchedulerClient

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	jobCancelFunc := func(cause error) {
		cancel(cause)
		close(jobCanceled)
	}

	// Test the startJobStatusUpdater directly
	jobKey := &compactorschedulerpb.JobKey{Id: "test-job"}
	jobSpec := &compactorschedulerpb.JobSpec{Tenant: "test-tenant", JobType: compactorschedulerpb.COMPACTION}

	go schedulerExec.startJobStatusUpdater(ctx, jobKey, jobSpec, jobCancelFunc)

	// Wait for the job to cancel after NOT_FOUND
	select {
	case <-jobCanceled:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("job was not canceled")
	}

	// check that exactly 2 updates were sent. First recv OK, next recv NOT_FOUND to trigge cancel
	require.Equal(t, 2, updateCallCount, "should have sent exactly 2 updates: one successful, then one NOT_FOUND")
}

// TestSchedulerExecutor_ExecuteCompactionJob_InvalidInput tests that executeCompactionJob
// properly validates input and returns appropriate status codes for invalid job specs.
// ABANDON status indicates unrecoverable errors (likely scheduler bugs) that should not be retried.
func TestSchedulerExecutor_ExecuteCompactionJob_InvalidInput(t *testing.T) {
	tests := map[string]struct {
		spec           *compactorschedulerpb.JobSpec
		expectedStatus compactorschedulerpb.UpdateType
	}{
		"nil_job_spec_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant:  "test-tenant",
				Job:     nil,
				JobType: compactorschedulerpb.COMPACTION,
			},
			expectedStatus: compactorschedulerpb.ABANDON,
		},
		"empty_block_ids_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{},
					Split:    false,
				},
				JobType: compactorschedulerpb.COMPACTION,
			},
			expectedStatus: compactorschedulerpb.ABANDON,
		},
		"invalid_block_id_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{[]byte("not-a-valid-ulid")},
					Split:    false,
				},
				JobType: compactorschedulerpb.COMPACTION,
			},
			expectedStatus: compactorschedulerpb.ABANDON,
		},
		"multiple_blocks_with_one_invalid_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{
						[]byte("01HZBE7WEN8ZGXWXRGEKWZXP1N"), // Valid
						[]byte("invalid"),                    // Invalid
					},
					Split: false,
				},
				JobType: compactorschedulerpb.COMPACTION,
			},
			expectedStatus: compactorschedulerpb.ABANDON,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, &bucket.ClientMock{}, newMockConfigProvider())

			status, err := schedulerExec.executeCompactionJob(context.Background(), c, tc.spec)

			require.Error(t, err)
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}

// TestSchedulerExecutor_ExecuteCompactionJob_BlockMetadataSync tests that executeCompactionJob
// properly syncs block metadata from object storage and validates that all specified blocks exist.
// This is critical because the scheduler only sends block IDs, not full metadata.
func TestSchedulerExecutor_ExecuteCompactionJob_BlockMetadataSync(t *testing.T) {
	tests := map[string]struct {
		setupBucket         func(*testing.T, objstore.Bucket) []string
		expectedStatus      compactorschedulerpb.UpdateType
		expectedErrorSubstr string
	}{
		// "block_not_found_in_bucket": {
		// 	setupBucket: func(t *testing.T, bkt objstore.Bucket) []string {
		// 		return []string{"01HZBE7WEN8ZGXWXRGEKWZXP1N"}
		// 	},
		// 	expectedStatus:      compactorschedulerpb.REASSIGN,
		// 	expectedErrorSubstr: "not found in synced metadata",
		// },
		"successful_sync_with_single_block": {
			// Test that metadata syncing works correctly when the block exists
			setupBucket: func(t *testing.T, bkt objstore.Bucket) []string {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 11, 2, nil)
				return []string{block1.String()}
			},
			expectedStatus:      compactorschedulerpb.COMPLETE,
			expectedErrorSubstr: "",
		},
		// "successful_sync_with_multiple_blocks": {
		// 	// Test that metadata syncing works correctly with multiple blocks
		// 	setupBucket: func(t *testing.T, bkt objstore.Bucket) []string {
		// 		block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
		// 		block2 := createTSDBBlock(t, bkt, "test-tenant", 20, 30, 2, nil)
		// 		return []string{block1.String(), block2.String()}
		// 	},
		// 	expectedStatus:      compactorschedulerpb.COMPLETE,
		// 	expectedErrorSubstr: "",
		// },
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
			bkt := objstore.NewInMemBucket()

			blockIDs := tc.setupBucket(t, bkt)
			logger := util_log.MakeLeveledLogger(os.Stdout, "info")
			schedulerExec, err := newSchedulerExecutor(cfg, logger, nil)
			require.NoError(t, err)

			c, tsdbCompactor, tsdbPlanner, _, _ := prepareWithConfigProvider(t, cfg, bkt, newMockConfigProvider())
			c.bucketClient = bkt

			// The planner and compactor are normally set during service startup (in the starting() method),
			// but since we're testing the executor directly without starting the full service,
			// we need to set them manually.
			c.blocksPlanner = tsdbPlanner
			c.blocksCompactor = tsdbCompactor

			// Configure mocks: planner returns blocks as-is (like SplitAndMergePlanner)
			tsdbPlanner.On("Plan", mock.Anything, mock.Anything).Return(
				mock.AnythingOfType("[]*block.Meta"),
				nil,
			).Maybe()

			// Build job spec from block IDs
			blockIDBytes := make([][]byte, len(blockIDs))
			for i, id := range blockIDs {
				blockIDBytes[i] = []byte(id)
			}

			spec := &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: blockIDBytes,
					Split:    false,
				},
				JobType: compactorschedulerpb.COMPACTION,
			}

			status, err := schedulerExec.executeCompactionJob(context.Background(), c, spec)

			if tc.expectedErrorSubstr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErrorSubstr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}

// TestSchedulerExecutor_ExecuteCompactionJob_SplitCompaction tests that the split flag
// from the job spec is properly respected when creating and executing compaction jobs.
// Split compaction divides blocks into multiple shards for better parallelization.
func TestSchedulerExecutor_ExecuteCompactionJob_SplitCompaction(t *testing.T) {
	tests := map[string]struct {
		splitEnabled bool
		// TODO: Add assertions about split behavior once we have a full working test
	}{
		"split_disabled": {
			splitEnabled: false,
		},
		"split_enabled": {
			splitEnabled: true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Skip("TODO: Implement full compaction test with mock blocks")
			_ = tc // TODO: use tc when implementing test

			// This test would:
			// 1. Create mock blocks in the bucket
			// 2. Call executeCompactionJob with split enabled/disabled
			// 3. Verify that BucketCompactor.runCompactionJob receives a Job with correct split settings
			// 4. Verify the number of output blocks matches expectations (1 for merge, N for split)
		})
	}
}

// TestSchedulerExecutor_ExecuteCompactionJob_ContextCancellation tests that executeCompactionJob
// properly respects context cancellation. This is important because the scheduler can cancel jobs
// via the heartbeat mechanism (returning NOT_FOUND), and we need to stop work immediately.
func TestSchedulerExecutor_ExecuteCompactionJob_ContextCancellation(t *testing.T) {
	tests := map[string]struct {
		cancelBeforeSync bool
	}{
		"cancel_before_metadata_sync": {
			// Test that canceling context before metadata sync is detected early
			cancelBeforeSync: true,
		},
		// TODO: Add test for cancellation during compaction execution
		// This would require more complex setup with actual block data
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("test-tenant/", []string{}, nil)
			bucketClient.MockIter("test-tenant/markers/", []string{}, nil)

			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient

			spec := &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{[]byte("01HZBE7WEN8ZGXWXRGEKWZXP1N")},
					Split:    false,
				},
				JobType: compactorschedulerpb.COMPACTION,
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancelBeforeSync {
				cancel()
			}

			status, err := schedulerExec.executeCompactionJob(ctx, c, spec)

			// Context cancellation should result in an error
			if tc.cancelBeforeSync {
				require.Error(t, err)
				assert.Equal(t, compactorschedulerpb.REASSIGN, status)
			}
		})
	}
}

// TestSchedulerExecutor_ExecuteCompactionJob_TenantIsolation tests that executeCompactionJob
// properly isolates tenant data by using tenant-specific bucket clients and loggers.
// This is critical for multi-tenancy to prevent data leakage between tenants.
func TestSchedulerExecutor_ExecuteCompactionJob_TenantIsolation(t *testing.T) {
	tests := map[string]struct {
		tenantID string
	}{
		"tenant_a": {
			tenantID: "tenant-a",
		},
		"tenant_b": {
			tenantID: "tenant-b",
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Skip("TODO: Implement test that verifies tenant-specific bucket client is used")
			_ = tc // TODO: use tc when implementing test

			// This test would:
			// 1. Create a mock bucket that tracks which tenant paths are accessed
			// 2. Call executeCompactionJob for a specific tenant
			// 3. Verify that only that tenant's blocks are accessed
			// 4. Verify tenant ID appears in log messages
		})
	}
}

// TestSchedulerExecutor_ExecuteCompactionJob_MetricsReporting tests that executeCompactionJob
// properly reports metrics for compaction jobs. This includes:
// - Number of blocks compacted
// - Compaction duration
// - Success/failure counters
func TestSchedulerExecutor_ExecuteCompactionJob_MetricsReporting(t *testing.T) {
	tests := map[string]struct {
		// TODO: Add test cases for different metric scenarios
	}{
		"successful_compaction_reports_metrics": {
			// Verify that successful compaction increments appropriate counters
		},
	}

	for testName := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Skip("TODO: Implement metrics verification test")

			// This test would:
			// 1. Create a compactor with a test registry
			// 2. Execute a compaction job
			// 3. Verify expected metrics are updated (compaction duration, block counts, etc.)
		})
	}
}
