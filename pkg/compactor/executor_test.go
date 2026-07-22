// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
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
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

var (
	testBlockID1 = ulid.MustNew(1, nil)
	testBlockID2 = ulid.MustNew(2, nil)
)

func makeTestCompactorConfig(t *testing.T) Config {
	t.Helper()
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerClientConfig.Enabled = true
	cfg.SchedulerClientConfig.SchedulerEndpoint = "localhost:9095"
	cfg.DataDir = t.TempDir()
	cfg.SparseIndexHeadersSamplingRate = 32
	return cfg
}

func makeSchedulerTestConfig(t *testing.T) Config {
	t.Helper()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	cfg := prepareConfig(t)
	cfg.ShardingRing.Common.InstanceID = "compactor-1"
	cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.Common.KVStore.Mock = ringStore
	cfg.SchedulerClientConfig.SchedulerEndpoint = "localhost:9095"
	cfg.SchedulerClientConfig.Enabled = true
	cfg.SchedulerClientConfig.Lanes = flagext.StringSliceCSV{"compact+plan"}
	return cfg
}

// mockCompactorSchedulerClient implements CompactorSchedulerClient
type mockCompactorSchedulerClient struct {
	mu                   sync.Mutex
	leaseJobCallCount    int
	updateJobCallCount   int
	firstUpdate          compactorschedulerpb.UpdateType
	lastUpdate           compactorschedulerpb.UpdateType
	recvPlannedReq       bool
	LeaseJobFunc         func(ctx context.Context, in *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error)
	UpdateJobFunc        func(ctx context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error)
	UpdateCleanupJobFunc func(ctx context.Context, in *compactorschedulerpb.UpdateCleanupJobRequest) (*compactorschedulerpb.UpdateJobResponse, error)
	PlannedJobsFunc      func(ctx context.Context, in *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error)
	UpdatePlanJobFunc    func(ctx context.Context, in *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error)
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

func (m *mockCompactorSchedulerClient) UpdateCleanupJob(ctx context.Context, in *compactorschedulerpb.UpdateCleanupJobRequest, opts ...grpc.CallOption) (*compactorschedulerpb.UpdateJobResponse, error) {
	m.mu.Lock()
	if m.updateJobCallCount == 0 {
		m.firstUpdate = in.Update
	}
	m.lastUpdate = in.Update
	m.updateJobCallCount++
	m.mu.Unlock()
	if m.UpdateCleanupJobFunc != nil {
		return m.UpdateCleanupJobFunc(ctx, in)
	}
	return &compactorschedulerpb.UpdateJobResponse{}, nil
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

func (m *mockCompactorSchedulerClient) GetLeaseJobCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.leaseJobCallCount
}

func newTestSchedulerExecutor(t *testing.T, cfg Config, client compactorschedulerpb.CompactorSchedulerClient) *schedulerExecutor {
	t.Helper()
	exec, err := newSchedulerExecutor(cfg.SchedulerClientConfig, log.NewNopLogger(), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { exec.schedulerConn.Close() })
	exec.schedulerClient = client
	return exec
}

func prepareCompactorForExecutorTest(t *testing.T, cfg Config, bkt objstore.Bucket, cfgProvider ConfigProvider) *MultitenantCompactor {
	t.Helper()
	c, _, _, _, _ := prepareWithConfigProvider(t, cfg, bkt, cfgProvider)
	c.bucketClient = bkt
	c.shardingStrategy = newSplitAndMergeShardingStrategy(nil, nil, nil, c.cfgProvider)
	return c
}

func testLeaseJobRequest() *compactorschedulerpb.LeaseJobRequest {
	return &compactorschedulerpb.LeaseJobRequest{
		WorkerId: "test-compactor",
		LaneRequests: []*compactorschedulerpb.LaneRequest{
			{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
			{JobType: compactorschedulerpb.JOB_TYPE_PLANNING},
		},
	}
}

func TestParseLaneRequests(t *testing.T) {
	compaction := compactorschedulerpb.JOB_TYPE_COMPACTION
	planning := compactorschedulerpb.JOB_TYPE_PLANNING
	cleanup := compactorschedulerpb.JOB_TYPE_CLEANUP

	jobTypes := func(requests []*compactorschedulerpb.LaneRequest) []compactorschedulerpb.JobType {
		types := make([]compactorschedulerpb.JobType, len(requests))
		for i, r := range requests {
			types[i] = r.JobType
		}
		return types
	}

	cases := map[string]struct {
		input   flagext.StringSliceCSV
		wantErr bool
		workers [][]compactorschedulerpb.JobType
	}{
		"no lanes rejected": {
			input:   flagext.StringSliceCSV{},
			wantErr: true,
		},
		"empty lane rejected": {
			input:   flagext.StringSliceCSV{""},
			wantErr: true,
		},
		"single worker compaction then planning": {
			input:   flagext.StringSliceCSV{"compact+plan"},
			workers: [][]compactorschedulerpb.JobType{{compaction, planning}},
		},
		"single worker planning only": {
			input:   flagext.StringSliceCSV{"plan"},
			workers: [][]compactorschedulerpb.JobType{{planning}},
		},
		"two workers split by lane": {
			input: flagext.StringSliceCSV{"compact", "plan"},
			workers: [][]compactorschedulerpb.JobType{
				{compaction},
				{planning},
			},
		},
		"compaction across multiple workers allowed": {
			input: flagext.StringSliceCSV{"compact", "compact"},
			workers: [][]compactorschedulerpb.JobType{
				{compaction},
				{compaction},
			},
		},
		"duplicate compaction within single worker rejected": {
			input:   flagext.StringSliceCSV{"compact+compact"},
			wantErr: true,
		},
		"duplicate planning within single worker rejected": {
			input:   flagext.StringSliceCSV{"plan+plan"},
			wantErr: true,
		},
		"single worker cleanup only": {
			input:   flagext.StringSliceCSV{"cleanup"},
			workers: [][]compactorschedulerpb.JobType{{cleanup}},
		},
		"single worker compaction then cleanup": {
			input:   flagext.StringSliceCSV{"compact+cleanup"},
			workers: [][]compactorschedulerpb.JobType{{compaction, cleanup}},
		},
		"cleanup across dedicated worker": {
			input: flagext.StringSliceCSV{"plan+compact", "cleanup"},
			workers: [][]compactorschedulerpb.JobType{
				{planning, compaction},
				{cleanup},
			},
		},
		"duplicate cleanup within single worker rejected": {
			input:   flagext.StringSliceCSV{"cleanup+cleanup"},
			wantErr: true,
		},
		"unknown job type rejected": {
			input:   flagext.StringSliceCSV{"unknown"},
			wantErr: true,
		},
		"count suffix repeats cleanup across goroutines": {
			input: flagext.StringSliceCSV{"cleanup:4"},
			workers: [][]compactorschedulerpb.JobType{
				{cleanup},
				{cleanup},
				{cleanup},
				{cleanup},
			},
		},
		"count suffix applies per entry not to whole value": {
			input: flagext.StringSliceCSV{"compact+plan", "plan", "cleanup:3"},
			workers: [][]compactorschedulerpb.JobType{
				{compaction, planning},
				{planning},
				{cleanup},
				{cleanup},
				{cleanup},
			},
		},
		"count suffix on multi-type entry repeats whole entry": {
			input: flagext.StringSliceCSV{"compact+plan:2"},
			workers: [][]compactorschedulerpb.JobType{
				{compaction, planning},
				{compaction, planning},
			},
		},
		"count of one is a single goroutine": {
			input:   flagext.StringSliceCSV{"cleanup:1"},
			workers: [][]compactorschedulerpb.JobType{{cleanup}},
		},
		"zero count rejected": {
			input:   flagext.StringSliceCSV{"cleanup:0"},
			wantErr: true,
		},
		"negative count rejected": {
			input:   flagext.StringSliceCSV{"cleanup:-1"},
			wantErr: true,
		},
		"non-numeric count rejected": {
			input:   flagext.StringSliceCSV{"cleanup:abc"},
			wantErr: true,
		},
		"empty count rejected": {
			input:   flagext.StringSliceCSV{"cleanup:"},
			wantErr: true,
		},
		"count without lane spec rejected": {
			input:   flagext.StringSliceCSV{":4"},
			wantErr: true,
		},
		"multiple colons rejected": {
			input:   flagext.StringSliceCSV{"cleanup:4:2"},
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := parseLaneRequests(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, result, len(tc.workers))
			for i, worker := range tc.workers {
				require.Equal(t, worker, jobTypes(result[i]))
			}
		})
	}
}

func TestSchedulerExecutor_JobStatusUpdates(t *testing.T) {
	testCases := map[string]struct {
		setupMock            func(*mockCompactorSchedulerClient)
		makeBucket           func() objstore.Bucket // nil = objstore.NewInMemBucket()
		expectError          bool
		expectUpdateCount    int
		expectLastUpdate     compactorschedulerpb.UpdateType
		expectPlannedRequest bool
	}{
		"compaction_job_abandons_when_block_not_found": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testBlockID1.Bytes()}}, JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectError:       true,
			expectUpdateCount: 1,
			expectLastUpdate:  compactorschedulerpb.UPDATE_TYPE_ABANDON,
		},
		"successful_planning_job_no_status_update": {
			expectUpdateCount: 0,
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "planning-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.JOB_TYPE_PLANNING},
					}, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, in *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, in *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
					return &compactorschedulerpb.PlannedJobsResponse{}, nil
				}
			},
			expectPlannedRequest: true,
		},
		"planning_job_transient_failure_sends_reassign": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "planning-job"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.JOB_TYPE_PLANNING},
					}, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			makeBucket: func() objstore.Bucket {
				bkt := &bucket.ClientMock{}
				bkt.MockIter("test-tenant/", []string{}, errors.New("bucket unavailable"))
				bkt.MockIter("test-tenant/markers/", []string{}, nil)
				return bkt
			},
			expectError:       true,
			expectUpdateCount: 1,
			expectLastUpdate:  compactorschedulerpb.UPDATE_TYPE_REASSIGN,
		},
	}

	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			tc.setupMock(mockSchedulerClient)

			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.UpdateInterval = 1 * time.Hour
			cfg.CompactionConcurrency = 1

			var bucketClient objstore.Bucket = objstore.NewInMemBucket()
			if tc.makeBucket != nil {
				bucketClient = tc.makeBucket()
			}

			schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)
			c := prepareCompactorForExecutorTest(t, cfg, bucketClient, newMockConfigProvider())

			gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, t.TempDir(), testLeaseJobRequest())
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, gotWork, "should have gotten work")

			assert.Equal(t, tc.expectUpdateCount, mockSchedulerClient.GetUpdateJobCallCount())
			assert.Equal(t, tc.expectPlannedRequest, mockSchedulerClient.ReceivedPlannedRequest())
			if tc.expectUpdateCount > 0 {
				assert.Equal(t, tc.expectLastUpdate.String(), mockSchedulerClient.GetLastUpdate().String())
			}
		})
	}
}

func TestSchedulerExecutor_BackoffBehavior(t *testing.T) {
	var IDs = [][]byte{testBlockID1.Bytes(), testBlockID2.Bytes()}

	tests := map[string]struct {
		setupMock          func(*mockCompactorSchedulerClient)
		expectGrowingDelay bool
	}{
		"scheduler_errors_should_trigger_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return nil, errors.New("error")
				}
			},
			expectGrowingDelay: true,
		},
		"no_work_available_should_trigger_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					// i.e. no work available
					return &compactorschedulerpb.LeaseJobResponse{}, nil
				}
			},
			expectGrowingDelay: true,
		},
		"leased_compaction_job_that_fails_should_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key: &compactorschedulerpb.JobKey{Id: "compaction-job"},
						Spec: &compactorschedulerpb.JobSpec{
							Tenant:  "user-1",
							Job:     &compactorschedulerpb.CompactionJob{Split: true, BlockIds: IDs}, // blocks are missing
							JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
						},
					}, nil
				}
				mock.UpdateJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
			},
			expectGrowingDelay: true,
		},
		"leased_planning_job_should_not_backoff": {
			setupMock: func(mock *mockCompactorSchedulerClient) {
				mock.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key: &compactorschedulerpb.JobKey{Id: "user-1"},
						Spec: &compactorschedulerpb.JobSpec{
							Tenant:  "user-1",
							JobType: compactorschedulerpb.JOB_TYPE_PLANNING,
						},
					}, nil
				}
				mock.UpdatePlanJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				}
				mock.PlannedJobsFunc = func(_ context.Context, _ *compactorschedulerpb.PlannedJobsRequest) (*compactorschedulerpb.PlannedJobsResponse, error) {
					return &compactorschedulerpb.PlannedJobsResponse{}, nil
				}
			},
			expectGrowingDelay: false,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			tc.setupMock(mockSchedulerClient)

			cfg := makeSchedulerTestConfig(t)
			cfg.SchedulerClientConfig.UpdateInterval = 1 * time.Hour
			cfg.SchedulerClientConfig.LeasingMinBackoff = 100 * time.Millisecond
			cfg.SchedulerClientConfig.LeasingMaxBackoff = 400 * time.Millisecond

			bucketClient := &bucket.ClientMock{}
			bucketClient.MockIter("user-1/", []string{}, nil)
			bucketClient.MockIter("user-1/markers/", []string{}, nil)
			bucketClient.MockGet(fmt.Sprintf("user-1/%s/meta.json", testBlockID1), "", block.ErrorSyncMetaNotFound)
			bucketClient.MockGet(fmt.Sprintf("user-1/%s/meta.json", testBlockID2), "", block.ErrorSyncMetaNotFound)

			c := prepareCompactorForExecutorTest(t, cfg, bucketClient, newMockConfigProvider())

			reg := prometheus.NewPedanticRegistry()
			var err error
			c.ring, c.ringLifecycler, err = newRingAndLifecycler(cfg.ShardingRing, ringName, ringKey, log.NewNopLogger(), reg)
			require.NoError(t, err)

			schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)

			synctest.Test(t, func(t *testing.T) {
				runCtx, runCancel := context.WithCancel(context.Background())
				t.Cleanup(runCancel)

				errCh := make(chan error, 1)
				go func() {
					errCh <- schedulerExec.run(runCtx, c)
				}()

				// Iteration 1 should fire immediately
				waitTimeout := 2 * cfg.SchedulerClientConfig.LeasingMaxBackoff
				sleepUntil(t, func() bool { return mockSchedulerClient.GetLeaseJobCallCount() == 1 }, waitTimeout)
				delay1 := sleepUntil(t, func() bool { return mockSchedulerClient.GetLeaseJobCallCount() == 2 }, waitTimeout)
				delay2 := sleepUntil(t, func() bool { return mockSchedulerClient.GetLeaseJobCallCount() == 3 }, waitTimeout)
				runCancel()
				synctest.Wait()
				require.NoError(t, <-errCh, "executor should exit without error")

				// if it grows, back-off grows its jitter window as follows:
				// [start,start*2) -> [start*2, start*4) -> [start*4, start*8) -> ... until max is reached]
				maxFirstDelay := 2 * cfg.SchedulerClientConfig.LeasingMinBackoff
				assert.LessOrEqual(t, delay1, maxFirstDelay)
				if tc.expectGrowingDelay {
					assert.GreaterOrEqual(t, delay2, maxFirstDelay, "second delay should grow")
				} else {
					assert.LessOrEqual(t, delay2, maxFirstDelay, "second delay should not grow")
				}
			})
		})
	}
}

// sleepUntil advances fake time in 1ms steps until cond returns true or timeout elapses.
// Returns the elapsed fake time with millisecond precision. Calls t.Fatal if timeout
// elapses before cond returns true. Must be called from within a synctest bubble.
func sleepUntil(t *testing.T, cond func() bool, timeout time.Duration) time.Duration {
	t.Helper()
	from := time.Now()
	for !cond() {
		if time.Since(from) >= timeout {
			t.Fatalf("sleepUntil: condition not met within %v", timeout)
		}
		time.Sleep(time.Millisecond)
	}
	return time.Since(from)
}

func TestSchedulerExecutor_ServicesLifecycle(t *testing.T) {
	cfg := makeSchedulerTestConfig(t)
	c, _, _, _, _ := prepare(t, cfg, objstore.NewInMemBucket())

	assert.Nil(t, c.executor, "executor should be nil before service start")
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), c))

	require.NotNil(t, c.executor, "executor should be initialized after starting")

	schedulerExec, ok := c.executor.(*schedulerExecutor)

	require.True(t, ok, "executor should be a schedulerExecutor in scheduler mode")
	assert.NotNil(t, schedulerExec.schedulerClient, "scheduler client should be initialized within executor")

	assert.True(t, c.ringSubservices.IsHealthy())
	require.NoError(t, c.blocksCleaner.AwaitRunning(context.Background()))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), c))
	assert.Equal(t, connectivity.Shutdown, schedulerExec.schedulerConn.GetState())
}

func TestSchedulerExecutor_UnreachableScheduler(t *testing.T) {
	cfg := makeSchedulerTestConfig(t)
	cfg.SchedulerClientConfig.SchedulerEndpoint = "unreachable-scheduler:9095"
	c, _, _, _, _ := prepare(t, cfg, objstore.NewInMemBucket())

	// Starting should succeed if a valid scheduler endpoint is passed, do not Dial w. block
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
				Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{}, JobType: compactorschedulerpb.JOB_TYPE_PLANNING},
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

	cfg := makeTestCompactorConfig(t)

	schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)

	mcp := newMockConfigProvider()
	mcp.maxPerBlockUploadConcurrency = map[string]int{"test-tenant": 1}

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("test-tenant/", []string{}, nil)
	bucketClient.MockIter("test-tenant/markers/", []string{}, nil)
	c := prepareCompactorForExecutorTest(t, cfg, bucketClient, mcp)

	// Wrap with synctest to avoid sleeping in real time during retries
	synctest.Test(t, func(t *testing.T) {
		gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, t.TempDir(), testLeaseJobRequest())
		require.NoError(t, err, "should eventually succeed with plannedJobs retry policy")
		require.True(t, gotWork)
		require.Equal(t, failuresBeforeSuccess+1, callCount)
	})
}

func TestSchedulerExecutor_NoGoRoutineLeak(t *testing.T) {
	mockSchedulerClient := &mockCompactorSchedulerClient{
		LeaseJobFunc: func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
			return &compactorschedulerpb.LeaseJobResponse{
				Key:  &compactorschedulerpb.JobKey{Id: "compaction-job"},
				Spec: &compactorschedulerpb.JobSpec{Tenant: "test-tenant", Job: &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testBlockID1.Bytes()}}, JobType: compactorschedulerpb.JOB_TYPE_COMPACTION},
			}, nil
		},
		UpdateJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(t)
	cfg.SchedulerClientConfig.UpdateInterval = 10 * time.Millisecond // Short interval to trigger the updater quickly

	bucketClient := &bucket.ClientMock{}
	bucketClient.MockIter("test-tenant/markers/", []string{}, nil)
	bucketClient.MockGet(fmt.Sprintf("test-tenant/%s/meta.json", testBlockID1), "", block.ErrorSyncMetaNotFound)

	// newTestSchedulerExecutor dials the scheduler; snapshot goroutines after that so
	// gRPC-internal goroutines are excluded from the leak check.
	schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)
	initialGoroutines := goleak.IgnoreCurrent()
	defer testutil.VerifyNoLeak(t, initialGoroutines)

	c := prepareCompactorForExecutorTest(t, cfg, bucketClient, newMockConfigProvider())

	gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, t.TempDir(), testLeaseJobRequest())
	require.Error(t, err) // expect an error since bucket has no test block
	require.True(t, gotWork)
}

func TestSchedulerExecutor_JobCancellationOn_NotFoundResponse(t *testing.T) {
	var mockSchedulerClient *mockCompactorSchedulerClient
	mockSchedulerClient = &mockCompactorSchedulerClient{
		UpdateJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
			// Return NOT_FOUND after first heartbeat to simulate job cancellation by scheduler
			if mockSchedulerClient.GetUpdateJobCallCount() > 1 {
				return nil, status.Error(codes.NotFound, "job not found")
			}
			return &compactorschedulerpb.UpdateJobResponse{}, nil
		},
	}

	cfg := makeTestCompactorConfig(t)
	cfg.SchedulerClientConfig.UpdateInterval = 10 * time.Millisecond

	schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)

	// Test the startJobStatusUpdater directly
	jobKey := &compactorschedulerpb.JobKey{Id: "test-job"}
	jobSpec := &compactorschedulerpb.JobSpec{Tenant: "test-tenant", JobType: compactorschedulerpb.JOB_TYPE_COMPACTION}

	// New mock compactor w. minimal metrics for testing
	mockCompactor := &MultitenantCompactor{
		schedulerLastContact: promauto.With(nil).NewGauge(prometheus.GaugeOpts{Name: "test_last_contact"}),
	}

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)

		go schedulerExec.startJobStatusUpdater(ctx, mockCompactor, jobKey, jobSpec, cancel)

		// we expect the job to be canceled after on the second update attempt
		sleepUntil(t, func() bool { return mockSchedulerClient.GetUpdateJobCallCount() == 2 }, 3*cfg.SchedulerClientConfig.UpdateInterval)
		synctest.Wait()
		require.Error(t, ctx.Err(), "job context should have been canceled after NOT_FOUND response")
	})

	// check that exactly 2 updates were sent. First recv OK, next recv NOT_FOUND to trigger cancel
	require.Equal(t, 2, mockSchedulerClient.GetUpdateJobCallCount(), "should have sent exactly 2 updates: one successful, then one NOT_FOUND")
}

func TestSchedulerExecutor_TerminatingFinalJobStatus(t *testing.T) {
	key := &compactorschedulerpb.JobKey{Id: "test-job"}
	spec := &compactorschedulerpb.JobSpec{
		Tenant:  "tenant",
		JobType: compactorschedulerpb.JOB_TYPE_PLANNING,
	}

	t.Run("canceled_context_uses_fresh_context_for_update", func(t *testing.T) {
		mock := &mockCompactorSchedulerClient{
			UpdatePlanJobFunc: func(ctx context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
				assert.NoError(t, ctx.Err())
				return &compactorschedulerpb.UpdateJobResponse{}, nil
			},
		}
		exec := newTestSchedulerExecutor(t, makeTestCompactorConfig(t), mock)

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		exec.sendFinalJobStatus(canceledCtx, key, spec, compactorschedulerpb.UPDATE_TYPE_REASSIGN, nil)
		require.Equal(t, 1, mock.GetUpdateJobCallCount())
	})

	t.Run("canceled_context_returns_after_final_status_timeout", func(t *testing.T) {
		mock := &mockCompactorSchedulerClient{
			UpdatePlanJobFunc: func(ctx context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		cfg := makeTestCompactorConfig(t)
		cfg.SchedulerClientConfig.TerminatingFinalStatusTimeout = time.Millisecond // arbitrary

		exec := newTestSchedulerExecutor(t, cfg, mock)
		cancelledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		synctest.Test(t, func(t *testing.T) {
			done := make(chan struct{})
			go func() {
				exec.sendFinalJobStatus(cancelledCtx, key, spec, compactorschedulerpb.UPDATE_TYPE_REASSIGN, nil)
				close(done) // unblock select
			}()

			// Advance synctest time past timeout
			time.Sleep(cfg.SchedulerClientConfig.TerminatingFinalStatusTimeout + time.Millisecond)
			synctest.Wait()

			select {
			case <-done:
			default:
				t.Fatal("sendFinalJobStatus should have returned after FinalStatusTimeout elapsed")
			}
		})
	})
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
				JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
		},
		"empty_block_ids_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{},
					Split:    false,
				},
				JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
		},
		"invalid_block_id_returns_abandon": {
			spec: &compactorschedulerpb.JobSpec{
				Tenant: "test-tenant",
				Job: &compactorschedulerpb.CompactionJob{
					BlockIds: [][]byte{[]byte("invalid")},
					Split:    false,
				},
				JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
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
				JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)
			schedulerExec := newTestSchedulerExecutor(t, cfg, nil)

			c, _, _, _, _ := prepareWithConfigProvider(t, cfg, &bucket.ClientMock{}, newMockConfigProvider())

			key := &compactorschedulerpb.JobKey{Id: "test-job-id"}
			status, err := schedulerExec.executeCompactionJob(context.Background(), c, t.TempDir(), key, tc.spec)

			require.Error(t, err)
			assert.Equal(t, tc.expectedStatus, status)
		})
	}
}

// newTestBlocksCleaner builds a BlocksCleaner suitable for driving per-job cleanup directly, without
// running the periodic cleanup service.
func newTestBlocksCleaner(t *testing.T, c *MultitenantCompactor, bkt objstore.Bucket) *BlocksCleaner {
	t.Helper()
	return NewBlocksCleaner(BlocksCleanerConfig{
		DeletionDelay:                 time.Hour,
		CleanupConcurrency:            1,
		DeleteBlocksConcurrency:       1,
		GetDeletionMarkersConcurrency: 1,
		UpdateBlocksConcurrency:       1,
		SchedulerCleanupEnabled:       true,
	}, bkt, mimir_tsdb.AllUsers, c.cfgProvider, log.NewNopLogger(), nil)
}

func TestSchedulerExecutor_ExecuteCleanupJob(t *testing.T) {
	const tenant = "test-tenant"

	tests := map[string]struct {
		tenant         string
		setupBucket    func(t *testing.T, bkt objstore.Bucket)
		makeBucket     func() objstore.Bucket // overrides the default in-memory bucket
		expectedStatus compactorschedulerpb.UpdateType
		expectedErrIs  error
	}{
		"empty tenant abandons": {
			tenant:         "",
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectedErrIs:  errCleanupJobHasNoTenant,
		},
		"cleans active tenant": {
			tenant: tenant,
			setupBucket: func(t *testing.T, bkt objstore.Bucket) {
				createTSDBBlock(t, bkt, tenant, 10, 20, 2, nil)
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		},
		"deletes tenant marked for deletion": {
			tenant: tenant,
			setupBucket: func(t *testing.T, bkt objstore.Bucket) {
				createTSDBBlock(t, bkt, tenant, 10, 20, 2, nil)
				require.NoError(t, mimir_tsdb.WriteTenantDeletionMark(context.Background(), bkt, tenant, nil, mimir_tsdb.NewTenantDeletionMark(time.Now())))
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		},
		"reassigns when deletion mark read fails": {
			tenant: tenant,
			makeBucket: func() objstore.Bucket {
				bkt := &bucket.ClientMock{}
				bkt.On("SupportedIterOptions").Return()
				// Non-not-found error while reading the deletion mark forces a reassign.
				bkt.MockGet(path.Join(tenant, mimir_tsdb.TenantDeletionMarkPath), "unused", errors.New("bucket unavailable"))
				return bkt
			},
			expectedStatus: compactorschedulerpb.UPDATE_TYPE_REASSIGN,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)
			schedulerExec := newTestSchedulerExecutor(t, cfg, nil)

			var bkt objstore.Bucket = objstore.NewInMemBucket()
			if tc.makeBucket != nil {
				bkt = tc.makeBucket()
			}
			if tc.setupBucket != nil {
				tc.setupBucket(t, bkt)
			}

			c := prepareCompactorForExecutorTest(t, cfg, bkt, newMockConfigProvider())
			c.blocksCleaner = newTestBlocksCleaner(t, c, bkt)

			spec := &compactorschedulerpb.JobSpec{Tenant: tc.tenant, JobType: compactorschedulerpb.JOB_TYPE_CLEANUP}
			status, _, err := schedulerExec.executeCleanupJob(context.Background(), c, spec)

			assert.Equal(t, tc.expectedStatus, status)
			switch {
			case tc.expectedErrIs != nil:
				require.ErrorIs(t, err, tc.expectedErrIs)
			case tc.expectedStatus == compactorschedulerpb.UPDATE_TYPE_COMPLETE:
				require.NoError(t, err)
			default:
				require.Error(t, err)
			}
		})
	}
}

// TestSchedulerExecutor_CleanupJobStatusRouting verifies that cleanup jobs route their final status
// updates to UpdateCleanupJob rather than the compaction or plan endpoints.
func TestSchedulerExecutor_CleanupJobStatusRouting(t *testing.T) {
	const tenant = "test-tenant"

	tests := map[string]struct {
		tenant           string
		expectedUpdate   compactorschedulerpb.UpdateType
		expectExecuteErr bool
	}{
		"successful cleanup routes complete": {
			tenant:         tenant,
			expectedUpdate: compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		},
		"missing tenant routes abandon": {
			tenant:           "",
			expectedUpdate:   compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectExecuteErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var gotUpdate compactorschedulerpb.UpdateType
			cleanupCalled := false
			mockClient := &mockCompactorSchedulerClient{
				LeaseJobFunc: func(context.Context, *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
					return &compactorschedulerpb.LeaseJobResponse{
						Key:  &compactorschedulerpb.JobKey{Id: "c"},
						Spec: &compactorschedulerpb.JobSpec{Tenant: tc.tenant, JobType: compactorschedulerpb.JOB_TYPE_CLEANUP},
					}, nil
				},
				UpdateCleanupJobFunc: func(_ context.Context, in *compactorschedulerpb.UpdateCleanupJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					cleanupCalled = true
					gotUpdate = in.Update
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				},
			}

			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.UpdateInterval = time.Hour // avoid periodic in-progress updates
			schedulerExec := newTestSchedulerExecutor(t, cfg, mockClient)

			bkt := objstore.NewInMemBucket()
			if tc.tenant != "" {
				createTSDBBlock(t, bkt, tc.tenant, 10, 20, 2, nil)
			}
			c := prepareCompactorForExecutorTest(t, cfg, bkt, newMockConfigProvider())
			c.blocksCleaner = newTestBlocksCleaner(t, c, bkt)

			gotWork, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, t.TempDir(), testLeaseJobRequest())
			require.True(t, gotWork)
			if tc.expectExecuteErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.True(t, cleanupCalled, "cleanup status should route to UpdateCleanupJob")
			assert.Equal(t, tc.expectedUpdate.String(), gotUpdate.String())
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
		setupBucket          func(*testing.T, objstore.Bucket) setupResult
		split                bool
		expectedStatus       compactorschedulerpb.UpdateType
		expectNewBlocksCount int
		expectError          bool
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
			expectedStatus:       compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectNewBlocksCount: 1,
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
			expectedStatus:       compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectNewBlocksCount: 1,
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
			expectedStatus:       compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectNewBlocksCount: 1,
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
			split:                true,
			expectedStatus:       compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			expectNewBlocksCount: splitShards,
		},
		"abandon_when_requested_blocks_not_in_obj_storage": {
			setupBucket: func(t *testing.T, bkt objstore.Bucket) setupResult {
				block1 := createTSDBBlock(t, bkt, "test-tenant", 10, 20, 2, nil)
				block2 := testBlockID1
				return setupResult{
					blockIDsToCompact: []ulid.ULID{block1, block2},
					compactedBlocks:   nil,
					uncompactedBlocks: []ulid.ULID{block1},
				}
			},
			expectedStatus:       compactorschedulerpb.UPDATE_TYPE_ABANDON,
			expectNewBlocksCount: 0,
			expectError:          true,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := makeTestCompactorConfig(t)

			bkt := objstore.NewInMemBucket()

			setup := tc.setupBucket(t, bkt)

			blocksAfterSetup := countBlocksInBucket(t, bkt, "test-tenant")

			mockCfg := newMockConfigProvider()
			if tc.split {
				mockCfg.splitAndMergeShards = map[string]int{"test-tenant": splitShards}
			}
			schedulerExec := newTestSchedulerExecutor(t, cfg, nil)
			c := prepareCompactorForExecutorTest(t, cfg, bkt, mockCfg)

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
				JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
			}

			key := &compactorschedulerpb.JobKey{Id: "test-job-id"}
			status, err := schedulerExec.executeCompactionJob(context.Background(), c, t.TempDir(), key, spec)

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

	meta1, meta2, meta3, meta4 := makeMeta(100, 200, 1), makeMeta(200, 300, 2), makeMeta(50, 150, 3), makeMeta(50, 300, 4)

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
		"overlapping_time_ranges": {
			// meta4 overlaps meta1: earlier MinTime but later MaxTime.
			metas:         []*block.Meta{meta1, meta4},
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
				assert.Equal(t, tc.expectMinTime, job.MinTime())
				assert.Equal(t, tc.expectMaxTime, job.MaxTime())
			}
		})
	}
}

type blockingBucket struct {
	objstore.Bucket
}

func (b *blockingBucket) Get(ctx context.Context, _ string) (io.ReadCloser, error) {
	// Blocks compaction jobs
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *blockingBucket) Iter(ctx context.Context, _ string, _ func(string) error, _ ...objstore.IterOption) error {
	// Blocks planning jobs
	<-ctx.Done()
	return ctx.Err()
}

func TestSchedulerExecutor_SchedulerCancellation_SkipsFinalStatus(t *testing.T) {
	tests := map[string]struct {
		leaseResponse *compactorschedulerpb.LeaseJobResponse
		setupUpdate   func(*mockCompactorSchedulerClient)
	}{
		"compaction": {
			leaseResponse: &compactorschedulerpb.LeaseJobResponse{
				Key: &compactorschedulerpb.JobKey{Id: "compaction"},
				Spec: &compactorschedulerpb.JobSpec{
					Tenant:  "tenant",
					Job:     &compactorschedulerpb.CompactionJob{BlockIds: [][]byte{testBlockID1.Bytes()}},
					JobType: compactorschedulerpb.JOB_TYPE_COMPACTION,
				},
			},
			setupUpdate: func(m *mockCompactorSchedulerClient) {
				m.UpdateJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return nil, status.Error(codes.NotFound, "not found")
				}
			},
		},
		"planning": {
			leaseResponse: &compactorschedulerpb.LeaseJobResponse{
				Key: &compactorschedulerpb.JobKey{Id: "planning"},
				Spec: &compactorschedulerpb.JobSpec{
					Tenant:  "tenant",
					JobType: compactorschedulerpb.JOB_TYPE_PLANNING,
				},
			},
			setupUpdate: func(m *mockCompactorSchedulerClient) {
				m.UpdatePlanJobFunc = func(_ context.Context, _ *compactorschedulerpb.UpdatePlanJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return nil, status.Error(codes.NotFound, "not found")
				}
			},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			mockSchedulerClient := &mockCompactorSchedulerClient{}
			mockSchedulerClient.LeaseJobFunc = func(_ context.Context, _ *compactorschedulerpb.LeaseJobRequest) (*compactorschedulerpb.LeaseJobResponse, error) {
				return tc.leaseResponse, nil
			}
			tc.setupUpdate(mockSchedulerClient)

			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.UpdateInterval = 1 * time.Millisecond // arbitrary

			schedulerExec := newTestSchedulerExecutor(t, cfg, mockSchedulerClient)
			c := prepareCompactorForExecutorTest(t, cfg, &blockingBucket{Bucket: objstore.NewInMemBucket()}, newMockConfigProvider())

			synctest.Test(t, func(t *testing.T) {
				errCh := make(chan error, 1)
				go func() {
					_, err := schedulerExec.leaseAndExecuteJob(context.Background(), c, t.TempDir(), testLeaseJobRequest())
					errCh <- err
				}()

				// Wait until both goroutines get caught:
				// 1. The job is blocked in a bucket call
				// 2. The heartbeat is waiting on its ticker
				synctest.Wait()

				// Advance synctest time so a heartbeat is sent.
				// The scheduler will return NotFound in response, which cancels the job context,
				// which then frees the goroutine from the blocking bucket.
				time.Sleep(cfg.SchedulerClientConfig.UpdateInterval + time.Millisecond)
				synctest.Wait()

				require.Error(t, <-errCh)
				require.Equal(t, 1, mockSchedulerClient.GetUpdateJobCallCount()) // no final status sent
			})
		})
	}
}

func TestSchedulerExecutor_SendFinalJobStatus_Interrupted(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		interrupted               bool
		enableInterruptedReassign bool
		status                    compactorschedulerpb.UpdateType
		want                      compactorschedulerpb.UpdateType
	}{
		{
			name:                      "interrupted reassign with enableInterruptedReassign enabled",
			interrupted:               true,
			enableInterruptedReassign: true,
			status:                    compactorschedulerpb.UPDATE_TYPE_REASSIGN,
			want:                      compactorschedulerpb.UPDATE_TYPE_INTERRUPTED_REASSIGN,
		},
		{
			name:        "interrupted reassign with enableInterruptedReassign disabled",
			interrupted: true,
			status:      compactorschedulerpb.UPDATE_TYPE_REASSIGN,
			want:        compactorschedulerpb.UPDATE_TYPE_REASSIGN,
		},
		{
			name:                      "different update type not translated",
			interrupted:               true,
			enableInterruptedReassign: true,
			status:                    compactorschedulerpb.UPDATE_TYPE_COMPLETE,
			want:                      compactorschedulerpb.UPDATE_TYPE_COMPLETE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockCompactorSchedulerClient{
				UpdateJobFunc: func(context.Context, *compactorschedulerpb.UpdateCompactionJobRequest) (*compactorschedulerpb.UpdateJobResponse, error) {
					return &compactorschedulerpb.UpdateJobResponse{}, nil
				},
			}
			cfg := makeTestCompactorConfig(t)
			cfg.SchedulerClientConfig.EnableInterruptedReassign = tc.enableInterruptedReassign
			exec := newTestSchedulerExecutor(t, cfg, mock)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.interrupted {
				cancel()
			}

			exec.sendFinalJobStatus(ctx,
				&compactorschedulerpb.JobKey{Id: "job-1"},
				&compactorschedulerpb.JobSpec{JobType: compactorschedulerpb.JOB_TYPE_COMPACTION, Tenant: "test-tenant"},
				tc.status,
				nil,
			)
			require.Equal(t, tc.want.String(), mock.GetLastUpdate().String())
		})
	}
}
