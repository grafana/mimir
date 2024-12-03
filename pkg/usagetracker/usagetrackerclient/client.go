// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient

import (
	"context"
	"flag"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var (
	// The ring operation used to track series.
	trackSeriesOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
)

type Config struct {
	PreferAvailabilityZone string        `yaml:"prefer_availability_zone"`
	RequestsHedgingDelay   time.Duration `yaml:"requests_hedging_delay" category:"advanced"`
	ReusableWorkers        int           `yaml:"reusable_workers" category:"advanced"`

	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`

	// Allow to inject custom client factory in tests.
	clientFactory client.PoolFactory `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.PreferAvailabilityZone, prefix+".prefer-availability-zone", "", "Preferred availability zone to query usage-trackers.")
	f.DurationVar(&cfg.RequestsHedgingDelay, prefix+".requests-hedging-delay", 100*time.Millisecond, "Delay before initiating requests to further usage-trackers (e.g. in other zones).")
	f.IntVar(&cfg.ReusableWorkers, prefix+".reusable-workers", 500, "Number of pre-allocated workers used to send requests to usage-trackers. If 0, no workers pool will be used and a new goroutine will be spawned for each request.")

	f.BoolVar(&cfg.TLSEnabled, prefix+".tls-enabled", cfg.TLSEnabled, "Enable TLS for gRPC client connecting to usage-tracker.")
	cfg.TLS.RegisterFlagsWithPrefix(prefix, f)
}

type UsageTrackerClient struct {
	services.Service

	cfg    Config
	logger log.Logger

	partitionRing      *ring.PartitionInstanceRing
	partitionBatchRing *ring.ActivePartitionBatchRing

	clientsPool *client.Pool

	// trackSeriesWorkersPool is the pool of workers used to send requests to usage-tracker instances.
	trackSeriesWorkersPool *concurrency.ReusableGoroutinesPool
}

func NewUsageTrackerClient(clientName string, clientCfg Config, partitionRing *ring.PartitionInstanceRing, instanceRing *ring.Ring, logger log.Logger, registerer prometheus.Registerer) *UsageTrackerClient {
	c := &UsageTrackerClient{
		cfg:                    clientCfg,
		logger:                 logger,
		partitionRing:          partitionRing,
		partitionBatchRing:     ring.NewActivePartitionBatchRing(partitionRing.PartitionRing()),
		clientsPool:            newUsageTrackerClientPool(client.NewRingServiceDiscovery(instanceRing), clientName, clientCfg, logger, registerer),
		trackSeriesWorkersPool: concurrency.NewReusableGoroutinesPool(clientCfg.ReusableWorkers),
	}

	c.Service = services.NewIdleService(nil, c.stop)
	return c
}

// stop implements services.StoppingFn.
func (c *UsageTrackerClient) stop(_ error) error {
	c.trackSeriesWorkersPool.Close()

	return nil
}

func (c *UsageTrackerClient) TrackSeries(ctx context.Context, userID string, series []uint64) ([]uint64, error) {
	var (
		batchOptions = ring.DoBatchOptions{
			Cleanup:       nil,
			IsClientError: func(error) bool { return false },
			Go:            c.trackSeriesWorkersPool.Go,
		}

		rejectedMx sync.Mutex
		rejected   []uint64
	)

	// Series hashes are 64bit but the hash ring tokens are 32bit, so we truncate
	// hashes to 32bit to get keys to lookup in the ring.
	keys := make([]uint32, len(series))
	for i, hash := range series {
		keys[i] = uint32(hash)
	}

	err := ring.DoBatchWithOptions(ctx, trackSeriesOp, c.partitionBatchRing, keys,
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
		return nil, err
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
	set, err := c.partitionRing.GetReplicationSetForPartitionAndOperation(partitionID, trackSeriesOp)
	if err != nil {
		return nil, err
	}

	// Prepare the request.
	req := &usagetrackerpb.TrackSeriesRequest{
		UserID:       userID,
		SeriesHashes: series,
	}

	cfg := ring.DoUntilQuorumConfig{
		Logger: spanlogger.FromContext(ctx, c.logger),

		MinimizeRequests: true,
		HedgingDelay:     c.cfg.RequestsHedgingDelay,

		// Give precedence to the client's zone.
		ZoneSorter: c.sortZones,

		// No error is a terminal error, and a failing request should be retried on another usage-tracker
		// replica for the same partition (if available).
		IsTerminalError: func(_ error) bool { return false },
	}

	res, err := ring.DoUntilQuorum[[]uint64](ctx, set, cfg, func(ctx context.Context, instance *ring.InstanceDesc) ([]uint64, error) {
		if instance == nil {
			// This should never happen.
			return nil, errors.New("instance is nil")
		}

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
		return nil, err
	}
	if len(res) == 0 {
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
