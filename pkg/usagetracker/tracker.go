// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type Config struct {
	Enabled       bool                `yaml:"enabled"`
	InstanceRing  InstanceRingConfig  `yaml:"ring"`
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`

	IdleTimeout time.Duration `yaml:"idle_timeout"`
}

func (c *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	f.BoolVar(&c.Enabled, "usage-tracker.enabled", false, "True to enable the usage-tracker.")

	c.InstanceRing.RegisterFlags(f, logger)
	c.PartitionRing.RegisterFlags(f)
	f.DurationVar(&c.IdleTimeout, "usage-tracker.idle-timeout", 20*time.Minute, "The time after which series are considered idle and not active anymore. Must be greater than 0 and less than 1 hour.")
}

type UsageTracker struct {
	services.Service

	store *trackerStore

	overrides *validation.Overrides
	logger    log.Logger

	partitionID         int32
	partitionLifecycler *ring.PartitionInstanceLifecycler
	partitionRing       *ring.PartitionInstanceRing

	instanceLifecycler *ring.BasicLifecycler

	// Dependencies.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func NewUsageTracker(cfg Config, partitionRing *ring.PartitionInstanceRing, overrides *validation.Overrides, logger log.Logger, registerer prometheus.Registerer) (*UsageTracker, error) {
	if cfg.IdleTimeout <= 0 || cfg.IdleTimeout > time.Hour {
		return nil, fmt.Errorf("invalid idle timeout %q, should be greater than 0 and less than 1 hour", cfg.IdleTimeout)
	}

	t := &UsageTracker{
		partitionRing: partitionRing,
		overrides:     overrides,
		logger:        logger,
	}

	// Get the partition ID.
	var err error
	t.partitionID, err = partitionIDFromInstanceID(cfg.InstanceRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating usage-tracker partition ID")
	}

	// Init instance ring lifecycler.
	t.instanceLifecycler, err = NewInstanceRingLifecycler(cfg.InstanceRing, logger, registerer)
	if err != nil {
		return nil, err
	}

	// Init the partition ring lifecycler.
	partitionKVClient, err := NewPartitionRingKVClient(cfg.PartitionRing, "lifecycler", logger, registerer)
	if err != nil {
		return nil, err
	}

	t.partitionLifecycler, err = NewPartitionRingLifecycler(cfg.PartitionRing, t.partitionID, cfg.InstanceRing.InstanceID, partitionKVClient, logger, registerer)
	if err != nil {
		return nil, err
	}

	t.store = newTrackerStore(cfg.IdleTimeout, logger, t, notImplementedEventsPublisher{logger: logger})
	t.Service = services.NewBasicService(t.start, t.run, t.stop)

	return t, nil
}

// start implements services.StartingFn.
func (t *UsageTracker) start(ctx context.Context) error {
	var err error

	// Start dependencies.
	if t.subservices, err = services.NewManager(t.instanceLifecycler, t.partitionLifecycler); err != nil {
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
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case now := <-ticker.C:
			t.store.cleanup(now)
		case <-ctx.Done():
			return nil
		case err := <-t.subservicesWatcher.Chan():
			return errors.Wrap(err, "usage-tracker dependency failed")
		}
	}
}

// TrackSeries implements usagetrackerpb.UsageTrackerServer.
func (t *UsageTracker) TrackSeries(_ context.Context, req *usagetrackerpb.TrackSeriesRequest) (*usagetrackerpb.TrackSeriesResponse, error) {
	rejected, err := t.store.trackSeries(context.Background(), req.UserID, req.SeriesHashes, time.Now())
	if err != nil {
		return nil, err
	}
	return &usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: rejected}, nil
}

func (t *UsageTracker) localSeriesLimit(userID string) uint64 {
	globalLimit := t.overrides.MaxGlobalSeriesPerUser(userID) // TODO: use a new active series limit.
	if globalLimit <= 0 {
		return 0
	}

	// Global limit is equally distributed among all active partitions.
	return uint64(float64(globalLimit) / float64(t.partitionRing.PartitionRing().ActivePartitionsCount()))
}

type notImplementedEventsPublisher struct {
	logger log.Logger
}

func (ev notImplementedEventsPublisher) publishCreatedSeries(_ context.Context, userID string, series []uint64, _ time.Time) error {
	level.Info(ev.logger).Log("msg", "publishCreatedSeries not implemented", "userID", userID, "series", len(series))
	return nil
}
