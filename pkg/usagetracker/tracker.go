// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Config struct {
	InstanceRing  InstanceRingConfig  `yaml:"ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
}

type UsageTracker struct {
	services.Service

	store *trackerStore

	overrides *validation.Overrides
	logger    log.Logger

	partitionID          int32
	partitionLifecycler  *ring.PartitionInstanceLifecycler
	partitionWatcher     *ring.PartitionRingWatcher
	partitionPageHandler *ring.PartitionRingPageHandler

	instanceLifecycler *ring.BasicLifecycler

	// Dependencies.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewUsageTracker(cfg Config, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	t := &UsageTracker{
		overrides: overrides,
		logger:    logger,
	}

	// Get the partition ID.
	var err error
	t.partitionID, err = partitionIDFromInstanceID(cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating usage-tracker partition ID")
	}

	// Init instance ring.
	t.instanceLifecycler, err = NewInstanceRingLifecycler(cfg.InstanceRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Init the partition ring.
	partitionKVClient, err := NewPartitionRingKVClient(cfg.PartitionRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, t.partitionID, cfg.InstanceRing.InstanceID, partitionKVClient, logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionWatcher = ring.NewPartitionRingWatcher(partitionRingName, partitionRingKey, partitionKVClient, logger, prometheus.WrapRegistererWithPrefix("cortex_", registerer))
	t.partitionPageHandler = ring.NewPartitionRingPageHandler(t.partitionWatcher, ring.NewPartitionRingEditor(partitionRingKey, partitionKVClient))

	t.store = newTrackerStore(logger, t)

	t.Service = services.NewBasicService(t.start, t.run, t.stop)

	return t, nil
}

// start implements services.StartingFn.
func (t *UsageTracker) start(ctx context.Context) error {
	var err error

	// Start dependencies.
	if t.subservices, err = services.NewManager(t.instanceLifecycler, t.partitionLifecycler, t.partitionWatcher); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker dependencies")
	}

	t.subservicesWatcher = services.NewFailureWatcher()
	t.subservicesWatcher.WatchManager(t.subservices)

	if err = services.StartManagerAndAwaitHealthy(ctx, t.subservices); err != nil {
		return errors.Wrap(err, "unable to start usage-tracker subservices")
	}

	return nil
}

// stop implements services.StoppingFn.
func (t *UsageTracker) stop(_ error) error {
	// Stop dependencies.
	if t.subservices != nil {
		_ = services.StopManagerAndAwaitStopped(context.Background(), t.subservices)
	}

	return nil
}

// run implements services.RunningFn.
func (t *UsageTracker) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	return t.store.trackSeries(context.Background(), req, time.Now())
}

func (t *UsageTracker) InstanceRingHandler(w http.ResponseWriter, req *http.Request) {
	t.instanceLifecycler.ServeHTTP(w, req)
}

func (t *UsageTracker) PartitionRingHandler(w http.ResponseWriter, req *http.Request) {
	t.partitionPageHandler.ServeHTTP(w, req)
}

func (t *UsageTracker) localSeriesLimit(userID string) uint64 {
	globalLimit := t.overrides.MaxGlobalSeriesPerUser(userID) // TODO: use a new active series limit.
	if globalLimit <= 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return uint64(float64(globalLimit) / float64(t.partitionWatcher.PartitionRing().ActivePartitionsCount()))
}
