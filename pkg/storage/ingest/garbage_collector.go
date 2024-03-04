// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

type GarbageCollector struct {
	services.Service

	cfg           GarbageCollectorConfig
	logger        log.Logger
	metadataStore *MetadataStore
	segmentStore  *SegmentStorage

	// Metrics.
	runsStarted          prometheus.Counter
	runsCompleted        prometheus.Counter
	runsFailed           prometheus.Counter
	segmentsCleanedTotal prometheus.Counter
	segmentsFailedTotal  prometheus.Counter
}

func NewGarbageCollector(cfg GarbageCollectorConfig, metadataStore *MetadataStore, segmentStore *SegmentStorage, logger log.Logger, reg prometheus.Registerer) *GarbageCollector {
	c := &GarbageCollector{
		cfg:           cfg,
		logger:        logger,
		metadataStore: metadataStore,
		segmentStore:  segmentStore,

		runsStarted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_garbage_collection_started_total",
			Help: "Total number of garbage collection runs started.",
		}),
		runsCompleted: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_garbage_collection_completed_total",
			Help: "Total number of garbage collection runs successfully completed.",
		}),
		runsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_garbage_collection_failed_total",
			Help: "Total number of garbage collection runs failed.",
		}),
		segmentsCleanedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_segments_cleaned_total",
			Help: "Total number of segments deleted.",
		}),
		segmentsFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_segments_cleanup_failures_total",
			Help: "Total number of segments failed to be deleted.",
		}),
	}

	c.Service = services.NewTimerService(cfg.CleanupInterval, nil, c.onTimerTick, nil)
	return c
}

func (c *GarbageCollector) onTimerTick(ctx context.Context) error {
	c.runsStarted.Inc()

	if err := c.cleanupSegments(ctx); err != nil && !errors.Is(err, context.Canceled) {
		level.Warn(c.logger).Log("msg", "failed to cleanup segments", "err", err)
		c.runsFailed.Inc()
	} else {
		c.runsCompleted.Inc()
	}

	// Never return error otherwise the service will terminate.
	return nil
}

func (c *GarbageCollector) cleanupSegments(ctx context.Context) error {
	for ctx.Err() == nil {
		var (
			limit        = c.cfg.DeleteConcurrency * 50
			deleteBefore = time.Now().Add(-c.cfg.RetentionPeriod)
		)

		// Find segments created before the retention period.
		refs, err := c.metadataStore.GetSegmentsCreatedBefore(ctx, deleteBefore, limit)
		if err != nil {
			return errors.Wrap(err, "failed to list segments to delete")
		}

		if len(refs) == 0 {
			level.Info(c.logger).Log("msg", "garbage collector found no segments to delete")
			return nil
		}

		level.Info(c.logger).Log("msg", "garbage collector found segments to delete", "num", len(refs))

		// Concurrently delete segments.
		err = concurrency.ForEachJob(ctx, len(refs), c.cfg.DeleteConcurrency, func(ctx context.Context, idx int) error {
			ref := refs[idx]

			if err := c.segmentStore.DeleteSegment(ctx, ref); err != nil {
				level.Warn(c.logger).Log("msg", "failed to delete segment", "segment_ref", ref.String(), "err", err)
				c.segmentsFailedTotal.Inc()
			} else {
				c.segmentsCleanedTotal.Inc()
			}

			// Never return error because we don't want a transient error to interrupt other deletions.
			return nil
		})

		if err != nil {
			// We should an error here only if context was canceled.
			return err
		}
	}

	return ctx.Err()
}

// StandaloneGarbageCollector embed GarbageCollector and all required dependencies.
type StandaloneGarbageCollector struct {
	services.Service

	cfg    Config
	logger log.Logger
	reg    prometheus.Registerer

	services        *services.Manager
	servicesWatcher *services.FailureWatcher
}

func NewStandaloneGarbageCollector(cfg Config, logger log.Logger, reg prometheus.Registerer) *StandaloneGarbageCollector {
	c := &StandaloneGarbageCollector{
		cfg:    cfg,
		logger: logger,
		reg:    reg,
	}

	c.Service = services.NewBasicService(c.starting, c.running, c.stopping)
	return c
}

func (c *StandaloneGarbageCollector) starting(ctx context.Context) error {
	bucket, err := bucket.NewClient(ctx, c.cfg.Bucket, "garbage-collector", c.logger, c.reg)
	if err != nil {
		return errors.Wrap(err, "failed to create bucket client")
	}

	// Create all services.
	var (
		metadataDB    = NewMetadataStorePostgresql(c.cfg.PostgresConfig)
		metadataStore = NewMetadataStore(metadataDB, c.logger)
		segmentStore  = NewSegmentStorage(bucket, metadataStore, c.reg)
		collector     = NewGarbageCollector(c.cfg.GarbageCollectorConfig, metadataStore, segmentStore, c.logger, c.reg)
	)

	// Start all services.
	c.services, err = services.NewManager(metadataStore, collector)
	if err != nil {
		return errors.Wrap(err, "failed to create services manager")
	}
	if err := services.StartManagerAndAwaitHealthy(ctx, c.services); err != nil {
		return errors.Wrap(err, "failed to start services")
	}

	c.servicesWatcher = services.NewFailureWatcher()
	c.servicesWatcher.WatchManager(c.services)

	return nil
}

func (c *StandaloneGarbageCollector) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-c.servicesWatcher.Chan():
		return errors.Wrap(err, "service failed")
	}
}

func (c *StandaloneGarbageCollector) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), c.services)
}
