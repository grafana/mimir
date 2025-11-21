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

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	// The ring operation used to track series.
	TrackSeriesOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

// limitsProvider provides access to user limits.
type limitsProvider interface {
	MaxActiveSeriesPerUser(userID string) int
}

type Config struct {
	IgnoreRejectedSeries bool `yaml:"ignore_rejected_series" category:"experimental"`
	IgnoreErrors         bool `yaml:"ignore_errors" category:"experimental"`

	GRPCClientConfig grpcclient.Config `yaml:"grpc"`

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

	// Allow to inject custom client factory in tests.
	ClientFactory client.PoolFactory `yaml:"-"`
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

	// Cache for users close to their limits.
	usersCloseToLimitsMtx   sync.RWMutex
	usersCloseToLimit       []string
	usersCloseToLimitLoaded bool

	// Metrics.
	trackSeriesDuration                *prometheus.HistogramVec
	usersCloseToLimitCount             prometheus.Gauge
	usersCloseToLimitLastUpdateSeconds prometheus.Gauge
	usersCloseToLimitUpdateFailures    prometheus.Counter
}

func NewUsageTrackerClient(clientName string, clientCfg Config, partitionRing *ring.MultiPartitionInstanceRing, instanceRing ring.ReadRing, limits limitsProvider, logger log.Logger, registerer prometheus.Registerer) *UsageTrackerClient {
	c := &UsageTrackerClient{
		cfg:                    clientCfg,
		logger:                 logger,
		limits:                 limits,
		partitionRing:          partitionRing,
		clientsPool:            newUsageTrackerClientPool(client.NewRingServiceDiscovery(instanceRing), clientName, clientCfg, logger, registerer),
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
	}

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

	err := ring.DoBatchWithOptions(ctx, TrackSeriesOp, partitionBatchRing, keys,
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

func (c *UsageTrackerClient) trackSeriesPerPartition(ctx context.Context, userID string, partitionID int32, series []uint64) ([]uint64, error) {
	// Get the usage-tracker instances for the input partition.
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, TrackSeriesOp)
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

	res, err := ring.DoUntilQuorum[[]uint64](ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) ([]uint64, error) {
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
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, TrackSeriesOp)
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
		userLimit := c.limits.MaxActiveSeriesPerUser(userID)
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
