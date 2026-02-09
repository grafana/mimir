// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

var (
	testBlockID1 = ulid.MustNew(ulid.Timestamp(time.Unix(1600000000, 0)), rand.New(rand.NewSource(1)))
	testBlockID2 = ulid.MustNew(ulid.Timestamp(time.Unix(1600000001, 0)), rand.New(rand.NewSource(2)))
)

func makeTestCompactorConfig(PlanningMode, schedulerAddress string) Config {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.PlanningMode = PlanningMode
	cfg.SchedulerEndpoint = schedulerAddress
	cfg.DataDir = "/tmp/compactor-test"
	cfg.SparseIndexHeadersSamplingRate = 32
	return cfg
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
		setupBucket         func(objstore.Bucket)
		expectedFinalStatus compactorschedulerpb.UpdateType
	}{
		"compaction_job_reassigns_when_block_not_found": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testBlockID1.Bytes()}}, JobType: compactorschedulerpb.COMPACTION},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			setupBucket:         func(bkt objstore.Bucket) {},
			expectedFinalStatus: compactorschedulerpb.REASSIGN,
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
			setupBucket:         func(bkt objstore.Bucket) {},
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

			bucketClient := objstore.NewInMemBucket()
			tc.setupBucket(bucketClient)

			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)
			schedulerExec.schedulerClient = mockSchedulerClient

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient
			c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)

			gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, "compactor-1")
			if tc.expectedFinalStatus == compactorschedulerpb.REASSIGN {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
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
	var IDs = [][]byte{testBlockID1.Bytes(), testBlockID2.Bytes()}

	tests := map[string]struct {
		setupMock           func(*mockCompactorSchedulerClient)
		expectedLeaseCalls  int
		expectedUpdateCalls int
		planJob             bool
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
						Key: &compactorschedulerpb.JobKey{Id: "user-1"},
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
			planJob:             true,
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
			cfg.SchedulerEndpoint = "localhost:9095"
			cfg.PlanningMode = planningModeScheduler
			// set cfg.SchedulerUpdateInterval long, only testing initial progress update
			cfg.SchedulerUpdateInterval = 1 * time.Hour

			bucketClient := &bucket.ClientMock{}
			if tc.planJob {
				bucketClient.MockIter("user-1/", []string{}, nil)
			}
			bucketClient.MockIter("user-1/markers/", []string{}, nil)
			bucketClient.MockGet(fmt.Sprintf("user-1/%s/meta.json", testBlockID1), "", block.ErrorSyncMetaNotFound)
			bucketClient.MockGet(fmt.Sprintf("user-1/%s/meta.json", testBlockID2), "", block.ErrorSyncMetaNotFound)

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
			c.bucketClient = bucketClient

			c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)

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
	cfg.SchedulerEndpoint = "localhost:9095"
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
	cfg.SchedulerEndpoint = "unreachable-scheduler:9095"
	cfg.PlanningMode = planningModeScheduler

	c, _, _, _, _ := prepare(t, cfg, inmem)

	// Starting should succeed if a valid cfg.SchedulerEndpoint string is passed, do not Dial w. block
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
				Key:  &compactorschedulerpb.JobKey{Id: "test-tenant"},
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

	mcp := newMockConfigProvider()
	mcp.maxPerBlockUploadConcurrency = map[string]int{"test-tenant": 1}
	c, _, _, _, _ := prepareWithConfigProvider(t, cfg, &bucket.ClientMock{}, mcp)

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("test-tenant/", []string{}, nil)
	bucketClient.MockIter("test-tenant/markers/", []string{}, nil)
	c.bucketClient = bucketClient

	c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)

	gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, "compactor-1")
	require.NoError(t, err, "should eventually succeed with plannedJobs retry policy")
	require.True(t, gotWork)
	require.Equal(t, failuresBeforeSuccess+1, callCount)
}

func TestSchedulerExecutor_NoGoRoutineLeak(t *testing.T) {
	initialGoroutines := goleak.IgnoreCurrent()
	defer testutil.VerifyNoLeak(t, initialGoroutines)

	mockSchedulerClient := &mockCompactorSchedulerClient{
		LeaseJobFunc: func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
			return &compactorschedulerpb.LeaseJobResponse{
				Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
				Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testBlockID1.Bytes()}}, JobType: compactorschedulerpb.COMPACTION},
			}, nil
		},
		UpdateJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")
	cfg.SchedulerUpdateInterval = 10 * time.Millisecond // Short interval to trigger the updater quickly

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("test-tenant/markers/", []string{}, nil)
	bucketClient.MockGet(fmt.Sprintf("test-tenant/%s/meta.json", testBlockID1), "", block.ErrorSyncMetaNotFound)

	schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
	require.NoError(t, err)
	schedulerExec.schedulerClient = mockSchedulerClient

	c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bucketClient, newMockConfigProvider())
	c.bucketClient = bucketClient

	c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)

	ctx, cancel := context.WithCancel(context.Background())
	gotWork, err := schedulerExec.leaseAndExecuteJob(ctx, c, "compactor-1")
	require.Error(t, err) // expect an error since bucket has no test block
	require.True(t, gotWork)

	cancel()

	if schedulerExec.schedulerConn != nil {
		require.NoError(t, schedulerExec.schedulerConn.Close())
	}
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

	// New mock compactor w. minimal metrics for testing
	mockCompactor := &MultitenantCompactor{
		schedulerLastContact: promauto.With(nil).NewGauge(prometheus.GaugeOpts{Name: "test_last_contact"}),
	}

	go schedulerExec.startJobStatusUpdater(ctx, mockCompactor, jobKey, jobSpec, jobCancelFunc)

	// Wait for the job to cancel after NOT_FOUND
	select {
	case <-jobCanceled:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("job was not canceled")
	}

	// check that exactly 2 updates were sent. First recv OK, next recv NOT_FOUND to trigge cancel
	require.Equal(t, 2, updateCallCount, "should have sent exactly 2 updates: one successful, then one NOT_FOUND")
}

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
					BlockIds: [][]byte{[]byte("invalid")},
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
						testBlockID1.Bytes(),
						[]byte("invalid"),
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

			key := &compactorschedulerpb.JobKey{Id: "test-job-id"}
			status, err := schedulerExec.executeCompactionJob(context.Background(), c, key, tc.spec)

			require.Error(t, err)
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}

func TestSchedulerExecutor_ExecuteCompactionJob_Compaction(t *testing.T) {
	const splitShards = 4

	type setupResult struct {
		blockIDsToCompact []ulid.ULID
		compactedBlocks   []ulid.ULID
		uncompactedBlocks []ulid.ULID
	}

	tests := map[string]struct {
		setupBucket             func(*testing.T, objstore.Bucket) setupResult
		split                   bool
		expectedStatus          compactorschedulerpb.UpdateType
		expectNewBlocksCount    int
		expectUncompactedBlocks int
		expectError             bool
	}{
		"compacts_single_block": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1},
					compactedBlocks:   []ulid.ULID{block1},
					uncompactedBlocks: nil,
				}
			},
			split:                   false,
			expectedStatus:          compactorschedulerpb.COMPLETE,
			expectNewBlocksCount:    1,
			expectUncompactedBlocks: 0,
			expectError:             false,
		},
		"compacts_multiple_blocks": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
				block2 := createTSDBBlock(t, bkt, "test-tenant", 20, 30, 2, nil)
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1, block2},
					compactedBlocks:   []ulid.ULID{block1, block2},
					uncompactedBlocks: nil,
				}
			},
			split:                   false,
			expectedStatus:          compactorschedulerpb.COMPLETE,
			expectNewBlocksCount:    1,
			expectUncompactedBlocks: 0,
		},
		"compacts_subset_of_blocks": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
				block2 := createTSDBBlock(t, bkt, "test-tenant", 20, 30, 2, nil)
				block3 := createTSDBBlock(t, bkt, "test-tenant", 30, 40, 2, nil)
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1, block2},
					compactedBlocks:   []ulid.ULID{block1, block2},
					uncompactedBlocks: []ulid.ULID{block3},
				}
			},
			split:                   false,
			expectedStatus:          compactorschedulerpb.COMPLETE,
			expectNewBlocksCount:    1,
			expectUncompactedBlocks: 1,
		},
		"compacts_single_block_with_split": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 50, 32, nil)
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1},
					compactedBlocks:   []ulid.ULID{block1},
					uncompactedBlocks: nil,
				}
			},
			split:                   true,
			expectedStatus:          compactorschedulerpb.COMPLETE,
			expectNewBlocksCount:    splitShards,
			expectUncompactedBlocks: 0,
		},
		"reassign_when_requested_blocks_not_in_obj_storage": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
				block2 := ulid.MustNew(ulid.Timestamp(time.Unix(1600000002, 0)), rand.New(rand.NewSource(2)))
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1, block2},
					compactedBlocks:   nil,
					uncompactedBlocks: []ulid.ULID{block1},
				}
			},
			split:                   false,
			expectedStatus:          compactorschedulerpb.REASSIGN,
			expectNewBlocksCount:    0,
			expectUncompactedBlocks: 1,
			expectError:             true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(planningModeScheduler, "localhost:9095")

			bkt := objstore.NewInMemBucket()

			setup := tc.setupBucket(t, bkt)

			blocksAfterSetup := countBlocksInBucket(t, bkt, "test-tenant")

			schedulerExec, err := newSchedulerExecutor(cfg, log.NewNopLogger(), nil)
			require.NoError(t, err)

			mockCfg := newMockConfigProvider()
			if tc.split {
				mockCfg.splitAndMergeShards = map[string]int{"test-tenant": splitShards}
			}
			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bkt, mockCfg)
			c.bucketClient = bkt
			c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)

			compactor, planner, err := splitAndMergeCompactorFactory(context.Background(), cfg, log.NewNopLogger(), prometheus.NewRegistry())
			require.NoError(t, err)
			c.blocksCompactor = compactor
			c.blocksPlanner = planner

			blockIDBytes := make([][]byte, len(setup.blockIDsToCompact))
			for i, id := range setup.blockIDsToCompact {
				blockIDBytes[i] = id.Bytes()
			}

			spec := &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: blockIDBytes,
					Split:    tc.split,
				},
				JobType: compactorschedulerpb.COMPACTION,
			}

			key := &compactorschedulerpb.JobKey{Id: "test-job-id"}
			status, err := schedulerExec.executeCompactionJob(context.Background(), c, key, spec)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.expectedStatus, status)

			blocksAfterCompaction := countBlocksInBucket(t, bkt, "test-tenant")
			newBlocksCreated := blocksAfterCompaction - blocksAfterSetup
			assert.Equal(t, tc.expectNewBlocksCount, newBlocksCreated, "expected %d new blocks, had %d after setup, got %d after compaction", tc.expectNewBlocksCount, blocksAfterSetup, blocksAfterCompaction)

			for _, blockID := range setup.compactedBlocks {
				marked, err := bkt.Exists(context.Background(), fmt.Sprintf("test-tenant/%s/deletion-mark.json", blockID.String()))
				require.NoError(t, err)
				assert.True(t, marked, "compacted block %s should be marked for deletion after compaction", blockID.String())
			}

			assert.Equal(t, len(setup.uncompactedBlocks), tc.expectUncompactedBlocks, "expected %d uncompacted blocks", tc.expectUncompactedBlocks)
			for _, blockID := range setup.uncompactedBlocks {
				exists, err := bkt.Exists(context.Background(), fmt.Sprintf("test-tenant/%s/meta.json", blockID.String()))
				require.NoError(t, err)
				assert.True(t, exists, "uncompacted block %s should still exist after compaction", blockID.String())
			}
		})
	}
}

func countBlocksInBucket(t *testing.T, bkt objstore.Bucket, userID string) int {
	count := 0
	err := bkt.Iter(context.Background(), userID+"/", func(s string) error {
		if _, ok := block.IsBlockDir(s); ok {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

func TestEmptyCompactionDir(t *testing.T) {
	tests := map[string]struct {
		setupDir func(t *testing.T, dir string)
	}{
		"creates_directory_if_not_exists": {
			setupDir: func(t *testing.T, dir string) {},
		},
		"removes_files_and_subdirectories": {
			setupDir: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(dir, 0750))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "file1.txt"), []byte("content"), 0644))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "file2.txt"), []byte("content"), 0644))
				subdir := filepath.Join(dir, "subdir")
				require.NoError(t, os.MkdirAll(subdir, 0750))
				require.NoError(t, os.WriteFile(filepath.Join(subdir, "file.txt"), []byte("content"), 0644))
			},
		},
		"handles_empty_directory": {
			setupDir: func(t *testing.T, dir string) {
				require.NoError(t, os.MkdirAll(dir, 0750))
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			tmpDir := t.TempDir()
			compactDir := filepath.Join(tmpDir, "compact")

			tc.setupDir(t, compactDir)

			err := emptyCompactionDir(compactDir)
			require.NoError(t, err)

			info, err := os.Stat(compactDir)
			require.NoError(t, err)
			require.True(t, info.IsDir(), "directory should still exist after cleanup")

			// Verify directory contents
			entries, err := os.ReadDir(compactDir)
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestBuildCompactionJobFromMetas(t *testing.T) {
	makeMeta := func(minTime, maxTime int64, seed int64) *block.Meta {
		return &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    ulid.MustNew(uint64(minTime), rand.New(rand.NewSource(seed))),
				MinTime: minTime,
				MaxTime: maxTime,
			},
			Thanos: block.ThanosMeta{Labels: map[string]string{"tenant": "test"}},
		}
	}

	meta1, meta2, meta3 := makeMeta(100, 200, 1), makeMeta(200, 300, 2), makeMeta(50, 150, 3)

	tests := map[string]struct {
		metas          []*block.Meta
		split          bool
		splitNumShards uint32
		expectError    bool
		expectMinTime  int64
		expectMaxTime  int64
	}{
		"empty_metas_returns_error": {
			metas:       []*block.Meta{},
			expectError: true,
		},
		"single_block_no_split": {
			metas:         []*block.Meta{meta1},
			expectMinTime: 100,
			expectMaxTime: 200,
		},
		"multiple_blocks_with_split": {
			metas:          []*block.Meta{meta1, meta2},
			split:          true,
			splitNumShards: 4,
			expectMinTime:  100,
			expectMaxTime:  300,
		},
		"blocks_sorted_by_min_time": {
			metas:         []*block.Meta{meta2, meta3, meta1},
			expectMinTime: 50,
			expectMaxTime: 300,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			spec := &compactorschedulerpb.JobSpec{Job: &compactorschedulerpb.CompactionJob{Split: tc.split}}
			job, err := buildCompactionJobFromMetas("test-tenant", "test-group-key", tc.metas, spec, tc.splitNumShards)

			if tc.expectError {
				require.Error(t, err)
				return
			}

			require.NotNil(t, job)
			assert.Equal(t, "test-tenant", job.userID)
			assert.Equal(t, len(tc.metas), len(job.metasByMinTime))
			assert.Equal(t, tc.split, job.useSplitting)
			assert.Equal(t, tc.splitNumShards, job.splitNumShards)

			assert.True(t, slices.IsSortedFunc(job.metasByMinTime, func(a, b *block.Meta) int {
				return cmp.Compare(a.MinTime, b.MinTime)
			}), "blocks should be sorted by MinTime")

			if len(job.metasByMinTime) > 0 {
				assert.Equal(t, tc.expectMinTime, job.metasByMinTime[0].MinTime)
				assert.Equal(t, tc.expectMaxTime, job.metasByMinTime[len(job.metasByMinTime)-1].MaxTime)
			}
		})
	}
}
