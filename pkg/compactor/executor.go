// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/retrypolicy"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/timeutil"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	errCompactionJobHasNoBlocks = errors.New("compaction job has no blocks")
	errNoBlockMetadataProvided  = errors.New("no block metadata provided")
)

// compactionExecutor defines how compaction work is executed.
type compactionExecutor interface {
	run(ctx context.Context, compactor *MultitenantCompactor) error
	stop() error
}

// standaloneExecutor runs compaction on a timer without external scheduling.
type standaloneExecutor struct{}

func (e *standaloneExecutor) run(ctx context.Context, c *MultitenantCompactor) error {
	// Run an initial compaction before starting the interval.
	c.compactUsers(ctx)

	// Apply 0-100% jitter to the first tick to spread compactions when multiple
	// compactors start at the same time and the initial run completes quickly.
	firstInterval := util.DurationWithNegativeJitter(c.compactorCfg.CompactionInterval, 1.0) + 1
	standardInterval := util.DurationWithJitter(c.compactorCfg.CompactionInterval, 0.05)
	stopTicker, tickerChan := timeutil.NewVariableTicker(firstInterval, standardInterval)
	defer stopTicker()

	for {
		select {
		case <-tickerChan:
			c.compactUsers(ctx)
		case <-ctx.Done():
			return nil
		case err := <-c.ringSubservicesWatcher.Chan():
			return fmt.Errorf("compactor subservice failed: %w", err)
		}
	}
}

func (e *standaloneExecutor) stop() error {
	return nil
}

// schedulerExecutor requests compaction jobs from an external scheduler.
type schedulerExecutor struct {
	cfg                      Config
	logger                   log.Logger
	schedulerClient          compactorschedulerpb.CompactorSchedulerClient
	schedulerConn            *grpc.ClientConn
	invalidClusterValidation *prometheus.CounterVec
	retryable                failsafe.Executor[any]
	lastCleanupTime          time.Time
}

func newSchedulerExecutor(cfg Config, logger log.Logger, invalidClusterValidation *prometheus.CounterVec) (*schedulerExecutor, error) {
	executor := &schedulerExecutor{
		cfg:                      cfg,
		logger:                   logger,
		invalidClusterValidation: invalidClusterValidation,
	}

	executor.retryable = failsafe.With(retrypolicy.NewBuilder[any]().
		HandleIf(func(_ any, err error) bool {
			if errStatus, ok := grpcutil.ErrorToStatus(err); ok {
				switch errStatus.Code() {
				case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
					// client will generate UNAVAILABLE if some data transmitted (e.g., request metadata written
					// to TCP connection) before connection breaks.
					return true
				}
			}
			return false
		}).
		WithBackoff(cfg.ExecutorRetryMinBackoff, cfg.ExecutorRetryMaxBackoff).
		WithJitter(500 * time.Millisecond).
		WithMaxAttempts(-1).
		Build())

	// Initialize scheduler client. This call will succeed even if scheduler is unreachable
	// since grpc.Dial() creates the connection immediately and connects lazily.
	var err error
	executor.schedulerClient, executor.schedulerConn, err = executor.makeSchedulerClient()
	if err != nil {
		return nil, err
	}

	return executor, nil
}

func (e *schedulerExecutor) run(ctx context.Context, c *MultitenantCompactor) error {
	workerID := fmt.Sprintf("compactor-%s", c.ringLifecycler.GetInstanceID())
	level.Info(e.logger).Log("msg", "compactor running in scheduler mode", "scheduler_endpoint", e.cfg.SchedulerEndpoint, "worker_id", workerID)

	compactDir := filepath.Join(c.compactorCfg.DataDir, "compact")

	b := backoff.New(ctx, backoff.Config{
		MinBackoff: e.cfg.SchedulerMinLeasingBackoff,
		MaxBackoff: e.cfg.SchedulerMaxLeasingBackoff,
	})

	for {
		// Clean up the compaction directory before leasing work if interval is configured.
		if e.cfg.CompactionDirCleanupInterval > 0 {
			if err := e.cleanupCompactionDir(compactDir); err != nil {
				level.Warn(e.logger).Log("msg", "failed to cleanup compaction directory", "path", compactDir, "err", err)
			}
		}

		ok, err := e.leaseAndExecuteJob(ctx, c, workerID)
		if err != nil {
			if grpcutil.ErrorToStatusCode(err) != codes.NotFound || b.NumRetries() > 10 {
				level.Warn(e.logger).Log("msg", "failed to lease or execute job", "err", err)
			}
		}
		if ok {
			b.Reset()
		}

		select {
		case <-time.After(b.NextDelay()):
			continue
		case <-ctx.Done():
			return nil
		}
	}
}

func (e *schedulerExecutor) stop() error {
	if e.schedulerConn != nil {
		return e.schedulerConn.Close()
	}
	return nil
}

// emptyCompactionDir removes all contents from the compaction directory without deleting the directory itself.
// If the directory does not exist, it will be created.
func emptyCompactionDir(compactDir string) error {
	// Ensure directory exists first
	if err := os.MkdirAll(compactDir, 0750); err != nil {
		return errors.Wrap(err, "failed to create compaction directory")
	}

	// Read all entries in the directory
	entries, err := os.ReadDir(compactDir)
	if err != nil {
		return errors.Wrap(err, "failed to read compaction directory")
	}

	// Remove each entry
	for _, entry := range entries {
		path := filepath.Join(compactDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			return errors.Wrapf(err, "failed to remove %s", path)
		}
	}
	return nil
}

// cleanupCompactionDir cleans up the compaction directory if the configured
// cleanup interval has elapsed since the last cleanup.
func (e *schedulerExecutor) cleanupCompactionDir(compactDir string) error {
	elapsed := time.Since(e.lastCleanupTime)
	shouldCleanup := elapsed >= e.cfg.CompactionDirCleanupInterval

	if !shouldCleanup {
		return nil
	}

	if err := emptyCompactionDir(compactDir); err != nil {
		return err
	}

	e.lastCleanupTime = time.Now()
	return nil
}

// startJobStatusUpdater starts a goroutine that sends periodic IN_PROGRESS keep-alive updates
func (e *schedulerExecutor) startJobStatusUpdater(ctx context.Context, c *MultitenantCompactor, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, cancelJob context.CancelCauseFunc) {
	ticker := time.NewTicker(e.cfg.SchedulerUpdateInterval)
	defer ticker.Stop()

	jobId := key.Id
	jobTenant := spec.Tenant

	for {
		select {
		case <-ticker.C:
			if err := e.updateJobStatus(ctx, key, spec, compactorschedulerpb.IN_PROGRESS); err != nil {
				// Check if the job was canceled from the scheduler side (not found response)
				if grpcutil.ErrorToStatusCode(err) == codes.NotFound {
					level.Info(e.logger).Log("msg", "job canceled by scheduler, stopping work", "job_id", jobId, "tenant", jobTenant)
					cancelJob(err) // Cancel the job context to stop the main work
					return
				}
				level.Warn(e.logger).Log("msg", "failed to send keep-alive update", "job_id", jobId, "tenant", jobTenant, "err", err)
			} else {
				// Update scheduler contact timestamp on successful heartbeat
				c.schedulerLastContact.SetToCurrentTime()
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendFinalJobStatus sends a final status update to the scheduler with retry policy.
// Compaction jobs send final statuses on completion, planning jobs only on failure for reassignment.
func (e *schedulerExecutor) sendFinalJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, status compactorschedulerpb.UpdateType) {
	jobId := key.Id
	jobTenant := spec.Tenant

	var err error
	switch spec.JobType {
	case compactorschedulerpb.COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: status}
		err = e.retryable.WithContext(ctx).Run(func() error {
			_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
			return err
		})
	case compactorschedulerpb.PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Update: status}
		err = e.retryable.WithContext(ctx).Run(func() error {
			_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
			return err
		})
	default:
		err = fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", spec.JobType.String())
	}

	if err != nil {
		level.Error(e.logger).Log("msg", "failed to send final status update", "job_id", jobId, "tenant", jobTenant, "status", status, "err", err)
	}
}

func (e *schedulerExecutor) makeSchedulerClient() (compactorschedulerpb.CompactorSchedulerClient, *grpc.ClientConn, error) {
	invalidClusterReporter := util.NewInvalidClusterValidationReporter(e.cfg.GRPCClientConfig.ClusterValidation.Label, e.invalidClusterValidation, e.logger)
	opts, err := e.cfg.GRPCClientConfig.DialOption(nil, nil, invalidClusterReporter)
	if err != nil {
		return nil, nil, err
	}

	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(e.cfg.SchedulerEndpoint, opts...)
	if err != nil {
		return nil, nil, err
	}

	client := compactorschedulerpb.NewCompactorSchedulerClient(conn)
	return client, conn, nil
}

func (e *schedulerExecutor) leaseAndExecuteJob(ctx context.Context, c *MultitenantCompactor, workerID string) (bool, error) {
	req := &compactorschedulerpb.LeaseJobRequest{
		WorkerId: workerID,
	}

	resp, err := e.schedulerClient.LeaseJob(ctx, req)
	if err != nil {
		// Leasing a job counts as successful contact if the scheduler returns a job to the compactor or if the scheduler
		// returns an error indicating no work is available. All other errors are assumed to be a scheduler-side error.
		if grpcutil.ErrorToStatusCode(err) == codes.NotFound {
			c.schedulerLastContact.SetToCurrentTime()
		}
		return false, err
	}

	c.schedulerLastContact.SetToCurrentTime()

	jobID := resp.Key.Id
	jobTenant := resp.Spec.Tenant
	jobType := resp.Spec.JobType

	// Create a cancellable context for this job that can be canceled if the scheduler cancels the job
	jobCtx, cancelJob := context.WithCancelCause(ctx)
	wg := sync.WaitGroup{}
	wg.Add(1)
	// Start async keep-alive updater for periodic IN_PROGRESS messages
	go func() {
		e.startJobStatusUpdater(jobCtx, c, resp.Key, resp.Spec, cancelJob)
		wg.Done()
	}()

	switch jobType {
	case compactorschedulerpb.COMPACTION:
		status, err := e.executeCompactionJob(jobCtx, c, resp.Key, resp.Spec)
		cancelJob(err)
		wg.Wait()
		if err != nil {
			level.Warn(e.logger).Log("msg", "failed to execute job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", err)
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
			return true, err
		}
		e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, status)
		return true, nil
	case compactorschedulerpb.PLANNING:
		plannedJobs, planErr := e.executePlanningJob(jobCtx, c, resp.Key.Id)
		cancelJob(planErr)
		wg.Wait()
		if planErr != nil {
			level.Warn(e.logger).Log("msg", "failed to execute planning job", "job_id", jobID, "tenant", jobTenant, "job_type", jobType, "err", planErr)
			// Planning jobs only send final status updates on failure
			e.sendFinalJobStatus(ctx, resp.Key, resp.Spec, compactorschedulerpb.REASSIGN)
			return true, planErr
		}

		// For planning jobs, no final status update is sent - results are communicated via PlannedJobs
		if err := e.sendPlannedJobs(ctx, resp.Key, plannedJobs); err != nil {
			level.Warn(e.logger).Log("msg", "failed to send planned jobs", "job_id", jobID, "tenant", jobTenant, "num_jobs", len(plannedJobs), "err", err)
			return true, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", jobType.String())
	}
}

func (e *schedulerExecutor) updateJobStatus(ctx context.Context, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec, updType compactorschedulerpb.UpdateType) error {
	switch spec.JobType {
	case compactorschedulerpb.COMPACTION:
		req := &compactorschedulerpb.UpdateCompactionJobRequest{Key: key, Tenant: spec.Tenant, Update: updType}
		_, err := e.schedulerClient.UpdateCompactionJob(ctx, req)
		return err
	case compactorschedulerpb.PLANNING:
		req := &compactorschedulerpb.UpdatePlanJobRequest{Key: key, Update: updType}
		_, err := e.schedulerClient.UpdatePlanJob(ctx, req)
		return err
	default:
		return fmt.Errorf("unsupported job type %q, only COMPACTION and PLANNING are supported", spec.JobType.String())
	}
}

func (e *schedulerExecutor) executeCompactionJob(ctx context.Context, c *MultitenantCompactor, key *compactorschedulerpb.JobKey, spec *compactorschedulerpb.JobSpec) (compactorschedulerpb.UpdateType, error) {
	if spec.Job == nil || len(spec.Job.BlockIds) == 0 {
		level.Error(e.logger).Log("msg", "invalid compaction plan, abandoning job", "tenant", spec.Tenant)
		return compactorschedulerpb.ABANDON, errCompactionJobHasNoBlocks
	}

	userID := spec.Tenant
	userLogger := util_log.WithUserID(userID, e.logger)

	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg, userLogger)

	userBucket := bucket.NewUserBucketClient(userID, c.bucketClient, c.cfgProvider)

	// Omit ShardAwareDeduplicateFilter. Duplicates are cleaned up during planning via GarbageCollect().
	// Keep LabelRemoverFilter to remove deprecated labels if applicable.
	fetcherFilters := []block.MetadataFilter{
		NewLabelRemoverFilter(compactionIgnoredLabels),
	}

	var maxLookback = c.cfgProvider.CompactorMaxLookback(userID)
	if c.cfgProvider.CompactorBlockUploadEnabled(userID) {
		maxLookback = 0
	}

	syncDir := c.metaSyncDirForUser(userID)
	fetcher, err := block.NewMetaFetcher(userLogger, c.compactorCfg.MetaSyncConcurrency, userBucket, syncDir, reg, fetcherFilters, maxLookback)
	if err != nil {
		return compactorschedulerpb.REASSIGN, errors.Wrap(err, "failed to create meta fetcher")
	}

	syncer, err := newMetaSyncer(userLogger, reg, userBucket, fetcher, nil, c.blocksMarkedForDeletion)
	if err != nil {
		return compactorschedulerpb.REASSIGN, errors.Wrap(err, "failed to create syncer")
	}

	blockIDs := make([]ulid.ULID, len(spec.Job.BlockIds))
	for i, id := range spec.Job.BlockIds {
		if err := blockIDs[i].UnmarshalBinary(id); err != nil {
			level.Error(userLogger).Log("msg", "invalid block ID, abandoning job", "tenant", userID, "err", err)
			return compactorschedulerpb.ABANDON, errors.Wrapf(err, "failed to parse block ID")
		}
	}

	// TODO: When requested blocks aren't found in object storage, the job should ideally
	// be abandoned rather than reassigned. Consider checking userBucket.IsObjNotFoundErr(err)
	// when fetching metadata to detect this condition and return ABANDON instead of REASSIGN.
	if err := syncer.SyncRequestedMetas(ctx, blockIDs); err != nil {
		return compactorschedulerpb.REASSIGN, errors.Wrap(err, "failed to sync metas")
	}

	jobMetas, err := getJobMetas(syncer, blockIDs)
	if err != nil {
		level.Error(userLogger).Log("msg", "blocks not found after sync, abandoning job", "err", err)
		return compactorschedulerpb.ABANDON, err
	}

	var splitNumShards uint32
	if spec.Job.Split {
		splitNumShards = uint32(c.cfgProvider.CompactorSplitAndMergeShards(userID))
	}

	job, err := buildCompactionJobFromMetas(userID, key.Id, jobMetas, spec, splitNumShards)
	if err != nil {
		return compactorschedulerpb.REASSIGN, err
	}

	compactor, err := c.newBucketCompactor(ctx, userID, userLogger, userBucket, syncer, reg)
	if err != nil {
		return compactorschedulerpb.REASSIGN, errors.Wrap(err, "failed to create bucket compactor")
	}

	// Track that a compaction job has started
	compactor.metrics.groupCompactionRunsStarted.Inc()

	level.Info(userLogger).Log("msg", "executing compaction job from scheduler", "tenant", userID, "blocks", len(blockIDs), "split", spec.Job.Split)
	_, compactedBlockIDs, err := compactor.runCompactionJob(ctx, job)
	if err == nil {
		compactor.metrics.groupCompactionRunsCompleted.Inc()
		if hasNonZeroULIDs(compactedBlockIDs) {
			compactor.metrics.groupCompactions.Inc()
		}
		compactor.metrics.groupCompactionsLastSuccess.SetToCurrentTime()
		level.Info(userLogger).Log("msg", "compaction job completed", "tenant", userID, "compacted_blocks", len(compactedBlockIDs))
		return compactorschedulerpb.COMPLETE, nil
	}

	// At this point the compaction has failed. Track the failure.
	compactor.metrics.groupCompactionRunsFailed.Inc()

	if errors.Is(err, syscall.ENOSPC) {
		c.outOfSpace.Inc()
	}

	if handleErr := compactor.handleKnownCompactionErrors(ctx, job, err); handleErr == nil {
		return compactorschedulerpb.ABANDON, err
	}

	// All other errors should be reassigned
	level.Warn(userLogger).Log("msg", "compaction job failed with unhandled error, reassigning", "err", err)
	return compactorschedulerpb.REASSIGN, err
}

func (e *schedulerExecutor) executePlanningJob(ctx context.Context, c *MultitenantCompactor, tenant string) ([]*compactorschedulerpb.PlannedCompactionJob, error) {
	userBucket := bucket.NewUserBucketClient(tenant, c.bucketClient, c.cfgProvider)
	userLogger := log.With(e.logger, "user", tenant)

	reg := prometheus.NewRegistry()
	defer c.syncerMetrics.gatherThanosSyncerMetrics(reg, userLogger)

	syncer, err := c.createMetaSyncerForUser(tenant, userBucket, userLogger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create meta syncer")
	}

	bucketCompactor, err := c.newBucketCompactor(ctx, tenant, userLogger, userBucket, syncer, reg)
	if err != nil {
		return nil, errors.Wrap(err, "creating bucket compactor")
	}

	level.Info(userLogger).Log("msg", "start sync of metas")
	if err := syncer.SyncMetas(ctx); err != nil {
		return nil, errors.Wrap(err, "sync")
	}

	level.Info(userLogger).Log("msg", "start of GC")
	if err := syncer.GarbageCollect(ctx); err != nil {
		return nil, errors.Wrap(err, "blocks garbage collect")
	}

	grouper := c.blocksGrouperFactory(ctx, c.compactorCfg, c.cfgProvider, tenant, userLogger, reg)
	jobs, err := grouper.Groups(syncer.Metas())
	if err != nil {
		return nil, errors.Wrap(err, "group compaction jobs")
	}

	if c.jobsOrder != nil {
		jobs = c.jobsOrder(jobs)
	} else {
		level.Info(userLogger).Log("msg", "unknown sorting, jobs will be unsorted")
	}

	jobs = bucketCompactor.filterJobsByWaitPeriod(ctx, jobs)

	now := time.Now()
	for _, delta := range bucketCompactor.blockMaxTimeDeltas(now, jobs) {
		bucketCompactor.metrics.blocksMaxTimeDelta.Observe(delta)
	}

	plannedJobs := make([]*compactorschedulerpb.PlannedCompactionJob, 0, len(jobs))
	for _, job := range jobs {
		toCompact, err := c.blocksPlanner.Plan(ctx, job.metasByMinTime)
		if err != nil {
			// Planning is pass-through for the split-merge compactor besides a range double check. If one group of blocks fails it will continue
			// to fail without intervention. In order to not stop all planning if that somehow occurs warn and continue
			level.Warn(userLogger).Log("msg", "failed to plan job", "err", err)
			continue
		}
		if len(toCompact) == 0 {
			continue
		}

		plannedJob := &compactorschedulerpb.PlannedCompactionJob{
			Id: job.key,
			Job: &compactorschedulerpb.CompactionJob{
				Split:    job.useSplitting,
				BlockIds: serializeBlockIds(toCompact),
			},
		}
		plannedJobs = append(plannedJobs, plannedJob)
	}

	level.Info(userLogger).Log("msg", "job planning completed", "num_jobs", len(plannedJobs))
	return plannedJobs, nil
}

func serializeBlockIds(metas []*block.Meta) [][]byte {
	ids := make([][]byte, 0, len(metas))
	for _, meta := range metas {
		ids = append(ids, meta.ULID.Bytes())
	}
	return ids
}

// sendPlannedJobs sends the planned compaction jobs back to the scheduler with retries.
func (e *schedulerExecutor) sendPlannedJobs(ctx context.Context, key *compactorschedulerpb.JobKey, plannedJobs []*compactorschedulerpb.PlannedCompactionJob) error {
	req := &compactorschedulerpb.PlannedJobsRequest{
		Key:  key,
		Jobs: plannedJobs,
	}

	return e.retryable.WithContext(ctx).Run(func() error {
		_, err := e.schedulerClient.PlannedJobs(ctx, req)
		return err
	})
}

func getJobMetas(syncer *metaSyncer, blockIDs []ulid.ULID) ([]*block.Meta, error) {
	allMetas := syncer.Metas()
	jobMetas := make([]*block.Meta, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		meta, ok := allMetas[blockID]
		if !ok {
			return nil, fmt.Errorf("block %s not found in synced metadata", blockID.String())
		}
		jobMetas = append(jobMetas, meta)
	}
	return jobMetas, nil
}

func buildCompactionJobFromMetas(userID string, groupKey string, jobMetas []*block.Meta, spec *compactorschedulerpb.JobSpec, splitNumShards uint32) (*Job, error) {
	if len(jobMetas) == 0 {
		return nil, errNoBlockMetadataProvided
	}

	// Blocks are already sorted by MinTime from planning
	var rangeStart, rangeEnd = int64(math.MaxInt64), int64(math.MinInt64)
	for _, meta := range jobMetas {
		if meta.MinTime < rangeStart {
			rangeStart = meta.MinTime
		}
		if meta.MaxTime > rangeEnd {
			rangeEnd = meta.MaxTime
		}
	}

	resolution := jobMetas[0].Thanos.Downsample.Resolution
	externalLabels := labels.FromMap(jobMetas[0].Thanos.Labels)

	stage := stageMerge
	if spec.Job.Split {
		stage = stageSplit
	}

	var shardID string
	if jobMetas[0].Thanos.Labels != nil {
		shardID = jobMetas[0].Thanos.Labels[block.CompactorShardIDExternalLabel]
	}

	// groupKey is provided by the scheduler and is already constructed
	shardingKey := fmt.Sprintf("%s-%s-%d-%d-%s", userID, stage, rangeStart, rangeEnd, shardID)

	job := newJob(userID, groupKey, externalLabels, resolution, spec.Job.Split, splitNumShards, shardingKey)
	for _, meta := range jobMetas {
		if err := job.AppendMeta(meta); err != nil {
			return nil, errors.Wrap(err, "failed to append block metadata to job")
		}
	}

	return job, nil
}
