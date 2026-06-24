// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient

import (
	"context"
	"flag"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/trackerop"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// limitsProvider provides access to user limits.
type limitsProvider interface {
	MaxActiveOrGlobalSeriesPerUser(userID string) int
}

type UsageTrackerRejectionObserver interface {
	ObserveAsyncUsageTrackerRejection(userID string)
}

type Config struct {
	IgnoreRejectedSeries bool `yaml:"ignore_rejected_series" category:"experimental"`
	IgnoreErrors         bool `yaml:"ignore_errors" category:"experimental"`

	GRPCClientConfig ClientConfig `yaml:"grpc"`

	PreferAvailabilityZone string        `yaml:"prefer_availability_zone"`
	RequestsHedgingDelay   time.Duration `yaml:"requests_hedging_delay" category:"advanced"`
	ReusableWorkers        int           `yaml:"reusable_workers" category:"advanced"`

	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`

	UsersCloseToLimitPollInterval        time.Duration `yaml:"users_close_to_limit_poll_interval" category:"advanced"`
	UsersCloseToLimitCacheStartupRetries int           `yaml:"users_close_to_limit_cache_startup_retries" category:"advanced"`

	MaxTimeToWaitForAsyncTrackingResponseAfterIngestion time.Duration `yaml:"max_time_to_wait_for_async_tracking_response_after_ingestion" category:"advanced"`

	// MinSeriesLimitForAsyncTracking is the minimum series limit for a user to be eligible for async tracking.
	// Users with a series limit below this threshold will always be tracked synchronously.
	// Set to 0 to disable this check (all users eligible for async tracking based on proximity to limit).
	MinSeriesLimitForAsyncTracking int `yaml:"min_series_limit_for_async_tracking" category:"advanced"`

	UseBatchedTracking      bool          `yaml:"use_batched_tracking" category:"experimental"`
	BatchDelay              time.Duration `yaml:"batch_delay" category:"advanced"`
	MaxBatchSeries          int           `yaml:"max_batch_series" category:"advanced"`
	TrackSeriesBatchTimeout time.Duration `yaml:"track_series_batch_timeout" category:"advanced"`

	// UseSyncBatchedTracking makes the synchronous TrackSeries transparently batch calls per partition,
	// lingering for up to SyncBatchDelay so that calls from multiple concurrent callers are sent together
	// in a single TrackSeriesBatch RPC. The caller still blocks and still receives its own rejected series.
	UseSyncBatchedTracking bool          `yaml:"use_sync_batched_tracking" category:"experimental"`
	SyncBatchDelay         time.Duration `yaml:"sync_batch_delay" category:"advanced"`

	// Allow to inject custom client factory in tests.
	ClientFactory client.PoolFactory `yaml:"-"`
}

type ClientConfig struct {
	grpcclient.Config      `yaml:",inline"`
	HealthCheckGracePeriod time.Duration `yaml:"health_check_grace_period" category:"experimental"`
}

func (cfg *ClientConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.Config.RegisterFlagsWithPrefix(prefix, f)
	f.DurationVar(&cfg.HealthCheckGracePeriod, prefix+".health-check-grace-period", 0, "The grace period for health checks. If a usage-tracker connection consistently fails health checks for this period, any open connections are closed. The usage-tracker will attempt to reconnect to that usage-tracker if a subsequent request is made to that usage-tracker. Set to 0 to immediately remove usage-tracker connections on the first health check failure.")
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.IgnoreRejectedSeries, prefix+"ignore-rejected-series", false, "Ignore rejected series when tracking series in usage-tracker. If enabled, the client will not return the list of rejected series, but it will still track them in usage-tracker. This is useful to validate the rollout process of this service.")
	f.BoolVar(&cfg.IgnoreErrors, prefix+"ignore-errors", false, "Ignore failed requests when tracking series in usage-tracker. If enabled, the client will not return any errors to the caller, assuming all series were accepted.")

	f.StringVar(&cfg.PreferAvailabilityZone, prefix+"prefer-availability-zone", "", "Preferred availability zone to query usage-trackers.")
	f.DurationVar(&cfg.RequestsHedgingDelay, prefix+"requests-hedging-delay", 100*time.Millisecond, "Delay before initiating requests to further usage-trackers (e.g. in other zones).")
	f.IntVar(&cfg.ReusableWorkers, prefix+"reusable-workers", 500, "Number of pre-allocated workers used to send requests to usage-trackers. If 0, no workers pool will be used and a new goroutine will be spawned for each request.")
	f.DurationVar(&cfg.UsersCloseToLimitPollInterval, prefix+"users-close-to-limit-poll-interval", time.Second, "Interval to poll usage-tracker instances for the list of users close to their series limit. This list is used to determine whether to track series synchronously or asynchronously.")
	f.IntVar(&cfg.UsersCloseToLimitCacheStartupRetries, prefix+"users-close-to-limit-cache-startup-retries", 3, "Number of retries to populate the users close to limit cache at startup. If all retries fail, the client will start with an empty cache.")

	f.DurationVar(&cfg.MaxTimeToWaitForAsyncTrackingResponseAfterIngestion, prefix+"max-time-to-wait-for-async-tracking-response-after-ingestion", 250*time.Millisecond, "Maximum time to wait for an asynchronous tracking response after ingestion request is completed.")
	f.IntVar(&cfg.MinSeriesLimitForAsyncTracking, prefix+"min-series-limit-for-async-tracking", 0, "Minimum series limit for a user to be eligible for async tracking. Users with a series limit below this threshold will always be tracked synchronously. Set to 0 to disable this check.")

	f.BoolVar(&cfg.UseBatchedTracking, prefix+"use-batched-tracking", false, "Use batched tracking for series. If enabled, the client will track series in batches to reduce RPC traffic.")
	f.DurationVar(&cfg.BatchDelay, prefix+"batch-delay", 750*time.Millisecond, "How long to accumulate a batch before sending the request.")
	f.IntVar(&cfg.MaxBatchSeries, prefix+"max-batch-series", 1_000_000, "Maximum number of series to track in a single batch. If 0, no maximum is used.")
	f.DurationVar(&cfg.TrackSeriesBatchTimeout, prefix+"track-series-batch-timeout", 2*time.Second, "Timeout for tracking series in a batch.")

	f.BoolVar(&cfg.UseSyncBatchedTracking, prefix+"use-sync-batched-tracking", false, "Use synchronous batched tracking for series. If enabled, synchronous TrackSeries calls linger for up to -sync-batch-delay per partition and are sent together in a single batch request to reduce RPC traffic, while still returning the rejected series to each caller.")
	f.DurationVar(&cfg.SyncBatchDelay, prefix+"sync-batch-delay", 50*time.Millisecond, "How long to accumulate a synchronous batch per partition before sending the request, when -use-sync-batched-tracking is enabled.")

	cfg.GRPCClientConfig.RegisterFlagsWithPrefix(prefix+"grpc-client-config", f)
}

type UsageTrackerClient struct {
	services.Service

	cfg    Config
	logger log.Logger
	limits limitsProvider

	partitionRing *ring.MultiPartitionInstanceRing

	clientsPool *client.Pool

	// trackSeriesWorkersPool is the pool of workers used to send requests to usage-tracker instances.
	trackSeriesWorkersPool *concurrency.ReusableGoroutinesPool

	// batcher manages batch per-partition series tracking.
	batcher *batcher

	// syncBatcher manages synchronous batched per-partition series tracking.
	syncBatcher *syncBatcher

	// Cache for users close to their limits.
	usersCloseToLimitsMtx   sync.RWMutex
	usersCloseToLimit       []string
	usersCloseToLimitLoaded bool

	// Observer for usage tracker series rejections.
	rejectionObserver UsageTrackerRejectionObserver

	// Metrics.
	trackSeriesDuration                 *prometheus.HistogramVec
	usersCloseToLimitCount              prometheus.Gauge
	usersCloseToLimitLastUpdateSeconds  prometheus.Gauge
	usersCloseToLimitUpdateFailures     prometheus.Counter
	batchTrackingFlushedOnSizeThreshold prometheus.Counter

	syncBatchFlushes        *prometheus.CounterVec
	syncBatchSeriesPerFlush prometheus.Histogram
}

func NewUsageTrackerClient(clientName string, clientCfg Config, partitionRing *ring.MultiPartitionInstanceRing, instanceRing ring.ReadRing, limits limitsProvider, logger log.Logger, registerer prometheus.Registerer, rejectionObserver UsageTrackerRejectionObserver) *UsageTrackerClient {
	clientsPool := newUsageTrackerClientPool(client.NewRingServiceDiscovery(instanceRing), clientName, clientCfg, logger, registerer)

	c := &UsageTrackerClient{
		cfg:               clientCfg,
		logger:            logger,
		limits:            limits,
		partitionRing:     partitionRing,
		clientsPool:       clientsPool,
		rejectionObserver: rejectionObserver,

		trackSeriesWorkersPool: concurrency.NewReusableGoroutinesPool(clientCfg.ReusableWorkers),
		trackSeriesDuration: promauto.With(registerer).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_usage_tracker_client_track_series_duration_seconds",
			Help:                            "Time taken to track all series in remote write request, eventually sharding the tracking among multiple usage-tracker instances.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"status_code"}),
		usersCloseToLimitCount: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_usage_tracker_client_users_close_to_limit_count",
			Help: "Number of users that are close to their series limit.",
		}),
		usersCloseToLimitLastUpdateSeconds: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_usage_tracker_client_users_close_to_limit_last_update_timestamp_seconds",
			Help: "Unix timestamp of the last update to the users close to limit cache.",
		}),
		usersCloseToLimitUpdateFailures: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_client_users_close_to_limit_update_failures_total",
			Help: "Total number of failed attempts to update the users close to limit cache.",
		}),
		batchTrackingFlushedOnSizeThreshold: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_client_batch_tracking_flushed_on_size_threshold_total",
			Help: "Total number of times the batch tracking client flushed a batch due to exceeding the size threshold.",
		}),
		syncBatchFlushes: promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_usage_tracker_client_sync_batch_flushes_total",
			Help: "Total number of synchronous tracking batch flushes, by the trigger that caused the flush.",
		}, []string{"reason"}),
		syncBatchSeriesPerFlush: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_usage_tracker_client_sync_batch_series_per_flush",
			Help:                            "Number of series sent in a single synchronous tracking batch flush.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}),
	}

	// Pre-initialize both label values so the series exist before the first flush.
	c.syncBatchFlushes.WithLabelValues(syncFlushReasonSize)
	c.syncBatchFlushes.WithLabelValues(syncFlushReasonLinger)

	c.batcher = newBatcher(clientCfg.MaxBatchSeries, clientCfg.BatchDelay, logger, c)
	c.syncBatcher = newSyncBatcher(clientCfg.MaxBatchSeries, clientCfg.SyncBatchDelay, logger, c)
	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c
}

// starting implements services.StartingFn.
func (c *UsageTrackerClient) starting(ctx context.Context) error {
	// Try to populate the cache at startup with retries.
	// If all retries fail, we still start with an empty cache.
	maxRetries := c.cfg.UsersCloseToLimitCacheStartupRetries
	if maxRetries <= 0 {
		maxRetries = 1 // At least try once
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			level.Warn(c.logger).Log("msg", "retrying users close to limit cache population at startup", "attempt", attempt+1, "max_retries", maxRetries)
		}

		c.updateUsersCloseToLimitCache(ctx)

		// Check if the cache was successfully populated.
		c.usersCloseToLimitsMtx.Lock()
		count := len(c.usersCloseToLimit)
		loaded := c.usersCloseToLimitLoaded
		c.usersCloseToLimitsMtx.Unlock()
		if loaded {
			level.Info(c.logger).Log("msg", "successfully populated users close to limit cache at startup", "user_count", count, "attempt", attempt+1)
			return nil
		}
	}

	// All retries failed, but we still start with an empty cache.
	level.Warn(c.logger).Log("msg", "failed to populate users close to limit cache at startup after all retries, starting with empty cache", "max_retries", maxRetries)
	return nil
}

// running implements services.RunningFn.
func (c *UsageTrackerClient) running(ctx context.Context) error {
	if c.cfg.UseBatchedTracking {
		go c.batcher.flusher()
	}

	ticker := time.NewTicker(c.cfg.UsersCloseToLimitPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.updateUsersCloseToLimitCache(ctx)
		}
	}
}

// stopping implements services.StoppingFn.
func (c *UsageTrackerClient) stopping(_ error) error {
	c.trackSeriesWorkersPool.Close()
	c.batcher.stop()
	c.syncBatcher.stop()
	return nil
}

func (c *UsageTrackerClient) TrackSeries(ctx context.Context, userID string, series []uint64) (_ []uint64, returnErr error) {
	// Nothing to do if there are no series to track.
	if len(series) == 0 {
		return nil, nil
	}

	var (
		batchOptions = ring.DoBatchOptions{
			Cleanup:       nil,
			IsClientError: func(error) bool { return false },
			Go:            c.trackSeriesWorkersPool.Go,
		}

		startTime  = time.Now()
		rejectedMx sync.Mutex
		rejected   []uint64
	)

	defer func() {
		statusCode := "OK"
		if returnErr != nil {
			statusCode = "error"
		}
		c.trackSeriesDuration.WithLabelValues(statusCode).Observe(time.Since(startTime).Seconds())
	}()

	// Create the partition ring view as late as possible, because we want to get the most updated
	// snapshot of the ring.
	partitionBatchRing := ring.NewActivePartitionBatchRing(c.partitionRing.PartitionRing())

	// Series hashes are 64bit but the hash ring tokens are 32bit, so we truncate
	// hashes to 32bit to get keys to lookup in the ring.
	keys := make([]uint32, len(series))
	for i, hash := range series {
		keys[i] = uint32(hash)
	}

	if c.cfg.UseSyncBatchedTracking {
		return c.trackSeriesSyncBatched(ctx, userID, series, keys, partitionBatchRing, batchOptions)
	}

	err := ring.DoBatchWithOptions(ctx, trackerop.TrackSeriesOp, partitionBatchRing, keys,
		func(partition ring.InstanceDesc, indexes []int) error {
			// The partition ID is stored in the ring.InstanceDesc.Id.
			partitionID, err := strconv.ParseUint(partition.Id, 10, 31)
			if err != nil {
				return err
			}

			// Build the list of series hashes that belong to this partition.
			partitionSeries := make([]uint64, len(indexes))
			for i, idx := range indexes {
				partitionSeries[i] = series[idx]
			}

			// Track the series for this partition.
			partitionRejected, err := c.trackSeriesPerPartition(ctx, userID, int32(partitionID), partitionSeries)
			if err != nil {
				return errors.Wrapf(err, "partition %d", partitionID)
			}

			if len(partitionRejected) > 0 {
				rejectedMx.Lock()
				rejected = append(rejected, partitionRejected...)
				rejectedMx.Unlock()
			}

			return nil
		}, batchOptions,
	)

	if err != nil {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, err
	}

	if c.cfg.IgnoreRejectedSeries {
		// If the client is configured to ignore rejected series, we return an empty slice.
		return nil, nil
	}

	// It should never happen that a response arrives at this point, but better to protect
	// from bugs that could cause panics.
	rejectedMx.Lock()
	rejectedCopy := rejected
	rejectedMx.Unlock()

	return rejectedCopy, nil
}

// trackSeriesSyncBatched implements the synchronous, batched variant of TrackSeries. It shards the
// series to partitions and enqueues each partition's series into the per-partition sync batcher,
// then blocks until every partition's batch has been flushed and its response received. The series
// rejected for this specific caller are recovered from the per-user rejection set returned by the
// batch RPC.
func (c *UsageTrackerClient) trackSeriesSyncBatched(ctx context.Context, userID string, series []uint64, keys []uint32, partitionBatchRing *ring.ActivePartitionBatchRing, batchOptions ring.DoBatchOptions) ([]uint64, error) {
	var (
		waitMx sync.Mutex
		waits  []chan syncTrackResult
	)

	err := ring.DoBatchWithOptions(ctx, trackerop.TrackSeriesOp, partitionBatchRing, keys,
		func(partition ring.InstanceDesc, indexes []int) error {
			// The partition ID is stored in the ring.InstanceDesc.Id.
			partitionID, err := strconv.ParseUint(partition.Id, 10, 31)
			if err != nil {
				return err
			}

			// Build the list of series hashes that belong to this partition.
			partitionSeries := make([]uint64, len(indexes))
			for i, idx := range indexes {
				partitionSeries[i] = series[idx]
			}

			// Enqueue the series for this partition. The actual RPC happens when the batch is flushed.
			ch := c.syncBatcher.trackSeries(int32(partitionID), userID, partitionSeries)

			waitMx.Lock()
			waits = append(waits, ch)
			waitMx.Unlock()

			return nil
		}, batchOptions,
	)
	if err != nil {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, err
	}

	// Wait for every partition's batch to be flushed and its response delivered.
	var rejected []uint64
	for _, ch := range waits {
		select {
		case <-ctx.Done():
			if c.cfg.IgnoreErrors {
				return nil, nil
			}
			return nil, ctx.Err()
		case res := <-ch:
			if res.err != nil {
				if c.cfg.IgnoreErrors {
					return nil, nil
				}
				return nil, res.err
			}
			rejected = append(rejected, res.rejected...)
		}
	}

	if c.cfg.IgnoreRejectedSeries {
		return nil, nil
	}

	return rejected, nil
}

func (c *UsageTrackerClient) trackSeriesPerPartition(ctx context.Context, userID string, partitionID int32, series []uint64) ([]uint64, error) {
	// Get the usage-tracker instances for the input partition.
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, trackerop.TrackSeriesOp)
	if err != nil {
		return nil, err
	}

	// Prepare the request.
	req := &usagetrackerpb.TrackSeriesRequest{
		UserID:       userID,
		Partition:    partitionID,
		SeriesHashes: series,
	}

	cfg := ring.DoUntilQuorumConfig{
		Logger: spanlogger.FromContext(ctx, log.With(c.logger, "component", "usage-tracker-client", "op", "track-series-per-partition", "partition", partitionID)),

		MinimizeRequests: true,
		HedgingDelay:     c.cfg.RequestsHedgingDelay,

		// Give precedence to the client's zone.
		ZoneSorter: c.sortZones,

		// No error is a terminal error, and a failing request should be retried on another usage-tracker
		// replica for the same partition (if available).
		IsTerminalError: func(_ error) bool { return false },
	}

	res, err := ring.DoUntilQuorum(ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) ([]uint64, error) {
		poolClient, err := c.clientsPool.GetClientForInstance(*instance)
		if err != nil {
			return nil, errors.Errorf("usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}

		trackerClient := poolClient.(usagetrackerpb.UsageTrackerClient)
		trackerRes, err := trackerClient.TrackSeries(ctx, req)
		if err != nil {
			return nil, errors.Wrapf(err, "usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}

		return trackerRes.RejectedSeriesHashes, nil
	}, func(_ []uint64) {
		// No cleanup.
	})

	if err != nil {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, errors.Errorf("unexpected no responses from usage-tracker for partition %d", partitionID)
	}

	return res[0], nil
}

// TrackSeriesAsync tracks series asynchronously. It will batch the series by partition and user
// and flush the batches when the batch size or batch delay is reached.
func (c *UsageTrackerClient) TrackSeriesAsync(ctx context.Context, userID string, series []uint64) (returnErr error) {
	// Nothing to do if there are no series to track.
	if len(series) == 0 {
		return nil
	}

	var (
		batchOptions = ring.DoBatchOptions{
			Cleanup:       nil,
			IsClientError: func(error) bool { return false },
			Go:            c.trackSeriesWorkersPool.Go,
		}
	)

	partitionBatchRing := ring.NewActivePartitionBatchRing(c.partitionRing.PartitionRing())

	// Series hashes are 64bit but the hash ring tokens are 32bit, so we truncate
	// hashes to 32bit to get keys to lookup in the ring.
	keys := make([]uint32, len(series))
	for i, hash := range series {
		keys[i] = uint32(hash)
	}

	return ring.DoBatchWithOptions(ctx, trackerop.TrackSeriesOp, partitionBatchRing, keys,
		func(partition ring.InstanceDesc, indexes []int) error {
			// The partition ID is stored in the ring.InstanceDesc.Id.
			partitionID, err := strconv.ParseUint(partition.Id, 10, 31)
			if err != nil {
				return err
			}

			// Build the list of series hashes that belong to this partition.
			partitionSeries := make([]uint64, len(indexes))
			for i, idx := range indexes {
				partitionSeries[i] = series[idx]
			}

			c.batcher.trackSeries(int32(partitionID), userID, partitionSeries)
			return nil
		}, batchOptions,
	)
}

// trackSeriesPerPartitionBatch tracks series per partition batch. It is called
// to track an accumulated batch of series. It will return the list of rejections
// for each user.
func (c *UsageTrackerClient) trackSeriesPerPartitionBatch(ctx context.Context, partitionID int32, users []*usagetrackerpb.TrackSeriesBatchUser) ([]*usagetrackerpb.TrackSeriesBatchRejection, error) {
	// Get the usage-tracker instances for the input partition.
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, trackerop.TrackSeriesOp)
	if err != nil {
		return nil, err
	}

	req := &usagetrackerpb.TrackSeriesBatchRequest{
		Partitions: []*usagetrackerpb.TrackSeriesBatchPartition{
			{Partition: partitionID, Users: users},
		},
	}

	cfg := ring.DoUntilQuorumConfig{
		Logger: spanlogger.FromContext(ctx, log.With(c.logger, "component", "usage-tracker-client", "op", "track-series-per-partition-batch", "partition", partitionID)),

		MinimizeRequests: true,
		HedgingDelay:     c.cfg.RequestsHedgingDelay,

		// Give precedence to the client's zone.
		ZoneSorter: c.sortZones,

		// No error is a terminal error, and a failing request should be retried on another usage-tracker
		// replica for the same partition (if available).
		IsTerminalError: func(_ error) bool { return false },
	}

	res, err := ring.DoUntilQuorum(ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) ([]*usagetrackerpb.TrackSeriesBatchRejection, error) {
		poolClient, err := c.clientsPool.GetClientForInstance(*instance)
		if err != nil {
			return nil, errors.Errorf("usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}

		callCtx, cancel := context.WithTimeout(ctx, c.cfg.TrackSeriesBatchTimeout)
		defer cancel()

		trackerClient := poolClient.(usagetrackerpb.UsageTrackerClient)
		trackerRes, err := trackerClient.TrackSeriesBatch(callCtx, req)
		if err != nil {
			return nil, errors.Wrapf(err, "usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}

		return trackerRes.Rejections, nil
	}, func(_ []*usagetrackerpb.TrackSeriesBatchRejection) {
		// No cleanup.
	})

	if err != nil {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, err
	}
	if len(res) == 0 {
		if c.cfg.IgnoreErrors {
			return nil, nil
		}
		return nil, errors.Errorf("unexpected no responses from usage-tracker for partition %d", partitionID)
	}

	return res[0], nil
}

func (c *UsageTrackerClient) sortZones(zones []string) []string {
	// Shuffle the zones to distribute load evenly.
	if len(zones) > 2 || (c.cfg.PreferAvailabilityZone == "" && len(zones) > 1) {
		rand.Shuffle(len(zones), func(i, j int) {
			zones[i], zones[j] = zones[j], zones[i]
		})
	}

	if c.cfg.PreferAvailabilityZone != "" {
		// Give priority to the preferred zone.
		for i, z := range zones {
			if z == c.cfg.PreferAvailabilityZone {
				zones[0], zones[i] = zones[i], zones[0]
				break
			}
		}
	}

	return zones
}

// updateUsersCloseToLimitCache polls a random usage-tracker partition for the list of users
// close to their series limit and updates the local cache.
func (c *UsageTrackerClient) updateUsersCloseToLimitCache(ctx context.Context) (ok bool) {
	partitionID, set, ok := c.selectRandomPartition()
	if !ok {
		c.usersCloseToLimitUpdateFailures.Inc()
		return false
	}

	cfg := ring.DoUntilQuorumConfig{
		Logger: spanlogger.FromContext(ctx, log.With(c.logger, "component", "usage-tracker-client", "op", "get-users-close-to-limit")),

		MinimizeRequests: true,
		HedgingDelay:     c.cfg.RequestsHedgingDelay,

		// Give precedence to the client's zone.
		ZoneSorter: c.sortZones,

		// No error is a terminal error, and a failing request should be retried on another usage-tracker
		// replica for the same partition (if available).
		IsTerminalError: func(_ error) bool { return false },
	}

	_, err := ring.DoUntilQuorum[[]string](ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) ([]string, error) {
		poolClient, err := c.clientsPool.GetClientForInstance(*instance)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get client for usage-tracker instance %s (%s)", instance.Id, instance.Addr)
		}

		trackerClient := poolClient.(usagetrackerpb.UsageTrackerClient)
		resp, err := trackerClient.GetUsersCloseToLimit(ctx, &usagetrackerpb.GetUsersCloseToLimitRequest{Partition: partitionID})
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get users close to limit from partition %d", partitionID)
		}

		c.usersCloseToLimitsMtx.Lock()
		first := !c.usersCloseToLimitLoaded
		c.usersCloseToLimit = slices.Clone(resp.SortedUserIds)
		c.usersCloseToLimitLoaded = true
		c.usersCloseToLimitsMtx.Unlock()

		// Update metrics.
		c.usersCloseToLimitCount.Set(float64(len(resp.SortedUserIds)))
		c.usersCloseToLimitLastUpdateSeconds.Set(float64(time.Now().Unix()))

		lvl := level.Debug
		if first {
			lvl = level.Info
		}
		lvl(c.logger).Log(
			"component", "usage-tracker-client",
			"msg", "updated users close to limit cache",
			"partition", resp.Partition,
			"user_count", len(resp.SortedUserIds),
		)
		return nil, nil
	}, func([]string) {})

	if err != nil {
		c.usersCloseToLimitUpdateFailures.Inc()
		level.Error(c.logger).Log(
			"component", "usage-tracker-client",
			"msg", "failed to get users close to limit from usage-tracker",
			"err", err,
		)
		return false
	}

	return true
}

func (c *UsageTrackerClient) selectRandomPartition() (int32, ring.ReplicationSet, bool) {
	partitions := c.partitionRing.PartitionRing().ActivePartitionIDs()
	if len(partitions) == 0 {
		level.Error(c.logger).Log(
			"component", "usage-tracker-client",
			"op", "select-random-partition",
			"msg", "no partitions available in ring for users close to limit poll",
		)
		return 0, ring.ReplicationSet{}, false
	}
	partitionID := partitions[rand.IntN(len(partitions))]
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, trackerop.TrackSeriesOp)
	if err != nil {
		level.Error(c.logger).Log(
			"component", "usage-tracker-client",
			"op", "select-random-partition",
			"msg", "failed to get replication set for partition",
			"partition", partitionID,
			"err", err,
		)
		return 0, ring.ReplicationSet{}, false
	}

	return partitionID, set, true
}

// CanTrackAsync returns true if the user can be tracked asynchronously.
// A user can be tracked async if:
// 1. The user is NOT in the cache of users close to their limit, AND
// 2. The user's series limit is >= MinSeriesLimitForAsyncTracking (if configured)
func (c *UsageTrackerClient) CanTrackAsync(userID string) bool {
	// Check if user's limit is below the minimum threshold for async tracking.
	if c.cfg.MinSeriesLimitForAsyncTracking > 0 {
		userLimit := c.limits.MaxActiveOrGlobalSeriesPerUser(userID)
		if userLimit > 0 && userLimit < c.cfg.MinSeriesLimitForAsyncTracking {
			// User's limit is too low, must track synchronously.
			return false
		}
	}

	// Check if user is close to their limit.
	c.usersCloseToLimitsMtx.RLock()
	defer c.usersCloseToLimitsMtx.RUnlock()
	if !c.usersCloseToLimitLoaded {
		// Can't do async if we don't really know.
		return false
	}

	// Can track async if it's not in the list of users close to their limit.
	_, found := slices.BinarySearch(c.usersCloseToLimit, userID)
	return !found
}

type batcher struct {
	maxSeriesPerBatch int
	batchDelay        time.Duration

	batchersMtx sync.RWMutex
	batchers    []*partitionBatcher

	trackerClient *UsageTrackerClient
	stoppingChan  chan struct{}
	logger        log.Logger
	onFlushDone   func() // if set, called after each partition flush completes
}

func newBatcher(maxSeriesPerBatch int, batchDelay time.Duration, logger log.Logger, trackerClient *UsageTrackerClient) *batcher {
	const defaultPartitions = 64

	return &batcher{
		maxSeriesPerBatch: maxSeriesPerBatch,
		batchDelay:        batchDelay,

		batchers: make([]*partitionBatcher, defaultPartitions),

		trackerClient: trackerClient,
		stoppingChan:  make(chan struct{}),
		logger:        logger,
	}
}

func (c *batcher) flusher() {
	// We flush all partitions at the same time to improve the batch-reading
	// economics on the usage-tracker server.
	t := time.NewTimer(util.DurationWithJitter(c.batchDelay, 0.1))
	for {
		select {
		case <-t.C:
			c.signalAll()
			t.Reset(util.DurationWithJitter(c.batchDelay, 0.1))
		case <-c.stoppingChan:
			return
		}
	}
}

// trackSeries tracks some series for a user in a partition. It will be batched and flushed asynchronously.
func (c *batcher) trackSeries(partition int32, userID string, series []uint64) {
	// Since c.batchers doesn't change much, prefer to fetch it with a shared
	// read lock, falling back to a write lock only if needed.

	c.batchersMtx.RLock()
	b, _ := c.getBatcher(partition)
	c.batchersMtx.RUnlock()

	if b == nil {
		c.batchersMtx.Lock()
		var grow bool

		// Re-check, since multiple readers may have entered the "not found" block.
		if b, grow = c.getBatcher(partition); b == nil {
			if grow {
				c.growBatchers(partition)
			}
			b = newPartitionBatcher(partition, c.maxSeriesPerBatch, c.logger, c.trackerClient, c.stoppingChan, c.onFlushDone)
			c.batchers[partition] = b
			go b.flushWorker()
		}

		c.batchersMtx.Unlock()
	}

	b.trackSeries(userID, series)
}

// getBatcher returns the batcher for the given partition, along with whether it needs to be grown.
// assumes a suitable lock is held.
func (c *batcher) getBatcher(partition int32) (*partitionBatcher, bool) {
	if int(partition) < len(c.batchers) {
		return c.batchers[partition], false
	}
	return nil, true
}

// growBatchers grows the batchers slice to accommodate the given partition. The exclusive lock must be held.
func (c *batcher) growBatchers(partition int32) {
	// round to next pow 2 and reallocate/copy if necessary.
	lenRequired := int(partition) + 1 // translate between zero-based partition ID and required slice length.
	if newLen := math.NextPowerTwo(lenRequired); newLen > len(c.batchers) {
		newBatchers := make([]*partitionBatcher, newLen)
		copy(newBatchers, c.batchers)
		c.batchers = newBatchers
	}
}

func (c *batcher) stop() {
	close(c.stoppingChan)

	// Wait for all partition-batchers to finish stopping.
	var wg sync.WaitGroup

	func() {
		c.batchersMtx.RLock()
		defer c.batchersMtx.RUnlock()
		for _, b := range c.batchers {
			if b != nil {
				wg.Go(b.stop)
			}
		}
	}()

	wg.Wait()
}

// signalAll signals all partition-batchers to perform a flush.
func (c *batcher) signalAll() {
	c.batchersMtx.RLock()
	defer c.batchersMtx.RUnlock()
	for _, b := range c.batchers {
		if b != nil {
			b.signalFlush()
		}
	}
}

// testFlush synchronously flushes all batchers. It's for tests.
func (c *batcher) testFlush() {
	c.batchersMtx.RLock()
	defer c.batchersMtx.RUnlock()
	for _, b := range c.batchers {
		if b != nil {
			b.flushBatch(true)
		}
	}
}

// partitionBatcher batches series hashes by partition and user. It will be
// flushed when it either reaches a size threshold or when the per-partition
// batch delay is reached.
type partitionBatcher struct {
	partition int32

	usersMtx    sync.Mutex
	userSeries  []*usagetrackerpb.TrackSeriesBatchUser
	seriesCount int

	flushChan    chan struct{}
	stoppingChan <-chan struct{}

	trackerClient *UsageTrackerClient
	workersPool   *concurrency.ReusableGoroutinesPool

	maxSeriesPerBatch     int
	logger                log.Logger
	onFlushDone           func()       // if set, called after each flush completes. For testing.
	sizeThresholdExceeded atomic.Int64 // how many times we've exceeded the size threshold. Also for testing.
}

func newPartitionBatcher(partition int32, maxSeriesPerBatch int, logger log.Logger, trackerClient *UsageTrackerClient, stopping <-chan struct{}, onFlushDone func()) *partitionBatcher {
	return &partitionBatcher{
		partition: partition,

		userSeries:  nil,
		seriesCount: 0,

		flushChan:    make(chan struct{}),
		stoppingChan: stopping,

		trackerClient: trackerClient,
		workersPool:   concurrency.NewReusableGoroutinesPool(2),

		maxSeriesPerBatch: maxSeriesPerBatch,
		logger:            log.With(logger, "partition", partition),
		onFlushDone:       onFlushDone,
	}
}

// trackSeries adds a user and their series to this partition's current batch,
// flushing it if it exceeds the size threshold.
func (b *partitionBatcher) trackSeries(userID string, series []uint64) {
	select {
	case <-b.stoppingChan:
		return
	default:
	}

	b.usersMtx.Lock()
	b.userSeries = append(b.userSeries, &usagetrackerpb.TrackSeriesBatchUser{
		UserID:       userID,
		SeriesHashes: series,
	})
	b.seriesCount += len(series)
	needsFlush := b.maxSeriesPerBatch > 0 && b.seriesCount >= b.maxSeriesPerBatch
	b.usersMtx.Unlock()

	if needsFlush {
		b.sizeThresholdExceeded.Add(1)
		b.signalFlush()
		b.trackerClient.batchTrackingFlushedOnSizeThreshold.Inc()
	}
}

func (b *partitionBatcher) signalFlush() {
	select {
	case b.flushChan <- struct{}{}:
	case <-b.stoppingChan:
		return
	default: // flusher is busy. no need to block.
	}
}

func (b *partitionBatcher) flushWorker() {
	for {
		select {
		case <-b.flushChan:
			b.flushBatch(false)
		case <-b.stoppingChan:
			b.workersPool.Close()
			return
		}
	}
}

// stop stops the partition batcher, waiting for any outstanding batches to be flushed.
func (b *partitionBatcher) stop() {
	b.flushBatch(true)
}

func (b *partitionBatcher) flushBatch(synchronous bool) {
	b.usersMtx.Lock()
	users := b.userSeries
	b.userSeries = nil
	b.seriesCount = 0
	b.usersMtx.Unlock()

	if len(users) > 0 {
		if synchronous {
			b.flush(users)
		} else {
			b.workersPool.Go(func() {
				b.flush(users)
			})
		}
	}
}

func (b *partitionBatcher) flush(users []*usagetrackerpb.TrackSeriesBatchUser) {
	defer func() {
		if b.onFlushDone != nil {
			b.onFlushDone()
		}
	}()

	rejections, err := b.trackerClient.trackSeriesPerPartitionBatch(context.Background(), b.partition, users)
	if err != nil {
		level.Error(b.logger).Log("msg", "failed to track series in partition batch", "err", err)
		return
	}

	if len(rejections) > 0 {
		count := 0

		for _, rejection := range rejections {
			for _, user := range rejection.Users {
				b.trackerClient.rejectionObserver.ObserveAsyncUsageTrackerRejection(user.UserID)
				count += len(user.RejectedSeriesHashes)
			}
		}

		level.Warn(b.logger).Log("msg", "ingested some series that should have been rejected, because they were batch-tracked asynchronously", "count", count)
	}
}

// Reasons a synchronous batch flush was triggered, used as the "reason" label of the
// cortex_usage_tracker_client_sync_batch_flushes_total metric.
const (
	syncFlushReasonSize   = "size"
	syncFlushReasonLinger = "linger"
)

// syncTrackRequest is a single caller's request accumulated in a synchronous batch. The flush
// delivers its result on the result channel exactly once.
type syncTrackRequest struct {
	userID string
	series []uint64
	result chan syncTrackResult
}

type syncTrackResult struct {
	rejected []uint64
	err      error
}

// syncBatcher batches synchronous per-partition series tracking across concurrent callers. Unlike
// the asynchronous batcher, callers block on the result of their request, which carries the series
// rejected for that caller. There's no global flusher: each partition lingers for batchDelay after
// it receives its first pending request (see syncPartitionBatcher.trackSeries).
type syncBatcher struct {
	maxSeriesPerBatch int
	batchDelay        time.Duration

	batchersMtx sync.RWMutex
	batchers    []*syncPartitionBatcher

	trackerClient *UsageTrackerClient
	stoppingChan  chan struct{}
	logger        log.Logger
}

func newSyncBatcher(maxSeriesPerBatch int, batchDelay time.Duration, logger log.Logger, trackerClient *UsageTrackerClient) *syncBatcher {
	const defaultPartitions = 64

	return &syncBatcher{
		maxSeriesPerBatch: maxSeriesPerBatch,
		batchDelay:        batchDelay,

		batchers: make([]*syncPartitionBatcher, defaultPartitions),

		trackerClient: trackerClient,
		stoppingChan:  make(chan struct{}),
		logger:        logger,
	}
}

// trackSeries enqueues the series for a user in a partition and returns a channel that will receive
// exactly one result once the partition's batch is flushed.
func (c *syncBatcher) trackSeries(partition int32, userID string, series []uint64) chan syncTrackResult {
	// Since c.batchers doesn't change much, prefer to fetch it with a shared
	// read lock, falling back to a write lock only if needed.

	c.batchersMtx.RLock()
	b, _ := c.getBatcher(partition)
	c.batchersMtx.RUnlock()

	if b == nil {
		c.batchersMtx.Lock()
		var grow bool

		// Re-check, since multiple readers may have entered the "not found" block.
		if b, grow = c.getBatcher(partition); b == nil {
			if grow {
				c.growBatchers(partition)
			}
			b = newSyncPartitionBatcher(partition, c.maxSeriesPerBatch, c.batchDelay, c.logger, c.trackerClient, c.stoppingChan)
			c.batchers[partition] = b
			go b.flushWorker()
		}

		c.batchersMtx.Unlock()
	}

	return b.trackSeries(userID, series)
}

// getBatcher returns the batcher for the given partition, along with whether it needs to be grown.
// assumes a suitable lock is held.
func (c *syncBatcher) getBatcher(partition int32) (*syncPartitionBatcher, bool) {
	if int(partition) < len(c.batchers) {
		return c.batchers[partition], false
	}
	return nil, true
}

// growBatchers grows the batchers slice to accommodate the given partition. The exclusive lock must be held.
func (c *syncBatcher) growBatchers(partition int32) {
	// round to next pow 2 and reallocate/copy if necessary.
	lenRequired := int(partition) + 1 // translate between zero-based partition ID and required slice length.
	if newLen := math.NextPowerTwo(lenRequired); newLen > len(c.batchers) {
		newBatchers := make([]*syncPartitionBatcher, newLen)
		copy(newBatchers, c.batchers)
		c.batchers = newBatchers
	}
}

func (c *syncBatcher) stop() {
	close(c.stoppingChan)

	// Wait for all partition-batchers to finish stopping.
	var wg sync.WaitGroup

	func() {
		c.batchersMtx.RLock()
		defer c.batchersMtx.RUnlock()
		for _, b := range c.batchers {
			if b != nil {
				wg.Go(b.stop)
			}
		}
	}()

	wg.Wait()
}

// testFlush synchronously flushes all batchers. It's for tests.
func (c *syncBatcher) testFlush() {
	c.batchersMtx.RLock()
	defer c.batchersMtx.RUnlock()
	for _, b := range c.batchers {
		if b != nil {
			b.flushBatch(true)
		}
	}
}

// pendingCount returns the number of requests currently accumulated for the given partition. It's for tests.
func (c *syncBatcher) pendingCount(partition int32) int {
	c.batchersMtx.RLock()
	defer c.batchersMtx.RUnlock()
	b, _ := c.getBatcher(partition)
	if b == nil {
		return 0
	}
	b.requestsMtx.Lock()
	defer b.requestsMtx.Unlock()
	return len(b.requests)
}

// syncPartitionBatcher accumulates synchronous tracking requests for a single partition. It flushes
// either when the accumulated series count reaches the size threshold or when the linger window
// (batchDelay) elapses since the batch became non-empty.
type syncPartitionBatcher struct {
	partition int32

	requestsMtx          sync.Mutex
	requests             []*syncTrackRequest
	seriesCount          int
	stopped              bool
	sizeThresholdPending bool

	flushChan    chan struct{}
	stoppingChan <-chan struct{}

	trackerClient *UsageTrackerClient
	workersPool   *concurrency.ReusableGoroutinesPool

	maxSeriesPerBatch int
	batchDelay        time.Duration
	logger            log.Logger
}

func newSyncPartitionBatcher(partition int32, maxSeriesPerBatch int, batchDelay time.Duration, logger log.Logger, trackerClient *UsageTrackerClient, stopping <-chan struct{}) *syncPartitionBatcher {
	return &syncPartitionBatcher{
		partition: partition,

		requests:    nil,
		seriesCount: 0,

		// Buffered so a flush signal is never lost: the synchronous batcher has no periodic flusher
		// to retry a dropped signal (unlike the asynchronous batcher), and a lost signal would leave
		// callers blocked forever. A buffered slot guarantees at least one pending flush is queued.
		flushChan:    make(chan struct{}, 1),
		stoppingChan: stopping,

		trackerClient: trackerClient,
		workersPool:   concurrency.NewReusableGoroutinesPool(2),

		maxSeriesPerBatch: maxSeriesPerBatch,
		batchDelay:        batchDelay,
		logger:            log.With(logger, "partition", partition),
	}
}

// trackSeries adds a user and their series to this partition's current batch and returns a channel
// that will receive exactly one result once the batch is flushed. The linger timer is armed when the
// batch transitions from empty to non-empty.
func (b *syncPartitionBatcher) trackSeries(userID string, series []uint64) chan syncTrackResult {
	req := &syncTrackRequest{
		userID: userID,
		series: series,
		result: make(chan syncTrackResult, 1),
	}

	b.requestsMtx.Lock()
	if b.stopped {
		// We're stopping: deliver immediately so the caller never blocks.
		b.requestsMtx.Unlock()
		if b.trackerClient.cfg.IgnoreErrors {
			req.result <- syncTrackResult{}
		} else {
			req.result <- syncTrackResult{err: errors.New("usage-tracker client is stopping")}
		}
		return req.result
	}
	wasEmpty := len(b.requests) == 0
	b.requests = append(b.requests, req)
	b.seriesCount += len(series)
	needsFlush := b.maxSeriesPerBatch > 0 && b.seriesCount >= b.maxSeriesPerBatch
	if needsFlush {
		// Record the trigger under the lock so the flush is attributed to the size threshold even if
		// it coalesces with a linger signal. The actual flush counter is incremented at flush time.
		b.sizeThresholdPending = true
	}
	b.requestsMtx.Unlock()

	switch {
	case needsFlush:
		b.signalFlush()
	case wasEmpty:
		// Arm the linger window: flush this partition once batchDelay elapses.
		time.AfterFunc(b.batchDelay, b.signalFlush)
	}

	return req.result
}

func (b *syncPartitionBatcher) signalFlush() {
	select {
	case b.flushChan <- struct{}{}:
	case <-b.stoppingChan:
		return
	default: // flusher is busy. no need to block.
	}
}

func (b *syncPartitionBatcher) flushWorker() {
	for {
		select {
		case <-b.flushChan:
			b.flushBatch(false)
		case <-b.stoppingChan:
			// Drain any pending requests before exiting so their callers don't block forever.
			b.drain()
			b.workersPool.Close()
			return
		}
	}
}

// stop marks the batcher as stopped and drains any outstanding requests synchronously, so that no
// caller is left blocked waiting for a result. It's safe to call concurrently with the flushWorker
// reacting to the stopping signal: the second drain finds an empty batch.
func (b *syncPartitionBatcher) stop() {
	b.drain()
}

// takeBatch snapshots and resets the pending batch under the lock, returning the accumulated
// requests and the reason the flush was triggered (size threshold vs linger window). When
// markStopped is set, it also marks the batcher stopped so no further requests are accepted; this,
// together with the stopped check in trackSeries, guarantees every accepted request is either in
// this snapshot or rejected up front.
func (b *syncPartitionBatcher) takeBatch(markStopped bool) ([]*syncTrackRequest, string) {
	b.requestsMtx.Lock()
	defer b.requestsMtx.Unlock()

	if markStopped {
		b.stopped = true
	}
	requests := b.requests
	reason := syncFlushReasonLinger
	if b.sizeThresholdPending {
		reason = syncFlushReasonSize
	}
	b.requests = nil
	b.seriesCount = 0
	b.sizeThresholdPending = false
	return requests, reason
}

// drain marks the batcher as stopped and flushes whatever is pending.
func (b *syncPartitionBatcher) drain() {
	requests, reason := b.takeBatch(true)
	if len(requests) > 0 {
		b.trackerClient.syncBatchFlushes.WithLabelValues(reason).Inc()
		b.flush(requests)
	}
}

func (b *syncPartitionBatcher) flushBatch(synchronous bool) {
	requests, reason := b.takeBatch(false)
	if len(requests) == 0 {
		return
	}

	b.trackerClient.syncBatchFlushes.WithLabelValues(reason).Inc()
	if synchronous {
		b.flush(requests)
	} else {
		b.workersPool.Go(func() {
			b.flush(requests)
		})
	}
}

func (b *syncPartitionBatcher) flush(requests []*syncTrackRequest) {
	users := make([]*usagetrackerpb.TrackSeriesBatchUser, len(requests))
	seriesCount := 0
	for i, req := range requests {
		seriesCount += len(req.series)
		users[i] = &usagetrackerpb.TrackSeriesBatchUser{
			UserID:       req.userID,
			SeriesHashes: req.series,
		}
	}
	b.trackerClient.syncBatchSeriesPerFlush.Observe(float64(seriesCount))

	// Use a background context so that one caller cancelling doesn't kill the shared batch for the
	// others. The RPC is bounded by Config.TrackSeriesBatchTimeout inside trackSeriesPerPartitionBatch.
	rejections, err := b.trackerClient.trackSeriesPerPartitionBatch(context.Background(), b.partition, users)
	if err != nil {
		level.Error(b.logger).Log("msg", "failed to track series in synchronous partition batch", "err", err)
		for _, req := range requests {
			req.result <- syncTrackResult{err: err}
		}
		return
	}

	// Build the set of rejected series hashes per user, then hand each caller back the subset of its
	// own series that were rejected.
	rejectedByUser := make(map[string]map[uint64]struct{})
	for _, rejection := range rejections {
		for _, user := range rejection.Users {
			set := rejectedByUser[user.UserID]
			if set == nil {
				set = make(map[uint64]struct{}, len(user.RejectedSeriesHashes))
				rejectedByUser[user.UserID] = set
			}
			for _, h := range user.RejectedSeriesHashes {
				set[h] = struct{}{}
			}
		}
	}

	for _, req := range requests {
		var rejected []uint64
		if set := rejectedByUser[req.userID]; set != nil {
			for _, h := range req.series {
				if _, ok := set[h]; ok {
					rejected = append(rejected, h)
				}
			}
		}
		req.result <- syncTrackResult{rejected: rejected}
	}
}
