// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// partitionPusher is the Pusher handed to a per-partition
// ingest.PartitionReader. It captures the partition ID so samples can
// be routed to the right (tenant, partition) TSDB on the readcache.
type partitionPusher struct {
	rc          *Readcache
	partitionID int32
}

// PushToStorageAndReleaseRequest implements ingest.Pusher.
//
// Note on string lifetimes: PushToStorageAndReleaseRequest is invoked
// by the Kafka consumer with a request whose string fields may alias
// the shared decompression buffer (the yoloString pattern documented
// in CLAUDE.md). The Appender in Prometheus copies labels into the
// head's interning pool synchronously, so by the time we return from
// Append the strings are deep-copied. We do not retain any references
// to req after this call.
func (p *partitionPusher) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	defer func() {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
	}()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return errors.Wrap(err, "extracting tenant from kafka record context")
	}

	db, err := p.rc.getOrOpenTSDB(userID, p.partitionID)
	if err != nil {
		return errors.Wrap(err, "opening partition TSDB")
	}
	if db == nil {
		// We may have just released ownership of this partition. The
		// reader will be torn down momentarily; safe to drop the
		// batch.
		return nil
	}

	// Serialize with CompactHead and ApplyConfig on this partition TSDB.
	// Kafka ingest may use parallel pusher shards (ingestion-concurrency-max).
	db.tsdbMut.Lock()
	defer db.tsdbMut.Unlock()

	app := db.Appender(ctx).(ingester.ExtendedAppender)
	res := ingester.PushWriteRequestTimeseries(ctx, ingester.WriteRequestTimeseriesPush{
		UserID:             userID,
		Timeseries:         req.Timeseries,
		Source:             req.Source,
		Limits:             p.rc.limits,
		Logger:             p.rc.logger,
		Samplers:           p.rc.pushErrSamplers,
		Discard:            nil,
		MaxSeriesPerUser:   func() int { return p.rc.limits.MaxGlobalSeriesPerUser(userID) },
		MaxSeriesPerMetric: func() int { return p.rc.limits.MaxGlobalSeriesPerMetric(userID) },
		ActiveSeries:       nil,
		App:                app,
		Head:               db.Head(),
	})
	if res.Err != nil {
		return ingester.MapPushErrorToErrorWithStatus(res.Err)
	}
	if res.FirstPartialErr != nil {
		return ingester.MapPushErrorToErrorWithStatus(res.FirstPartialErr)
	}
	return nil
}

// NotifyPreCommit implements ingest.PreCommitNotifier. Readcache has
// nothing to do here today (no per-batch flushing semantics tied to
// offset commits); the hook exists so it can grow into one (e.g.
// flushing exemplar batches that bypass the appender).
func (p *partitionPusher) NotifyPreCommit(_ context.Context) error {
	return nil
}

// startKafkaReader spins up the per-partition ingest.PartitionReader
// for partitionID. Idempotent: calling twice on the same partition is
// a no-op.
//
// The offset file is colocated with the per-partition TSDB so that
// data and offset move together when ownership is reassigned.
func (r *Readcache) startKafkaReader(ctx context.Context, p *partitionState) error {
	if p.reader != nil {
		return nil
	}

	offsetFilePath := filepath.Join(r.cfg.DataDir, fmt.Sprintf("partition-%d.offset.json", p.partitionID))

	kafkaCfg := r.cfg.Kafka
	if r.cfg.KafkaTopic != "" {
		kafkaCfg.Topic = r.cfg.KafkaTopic
	}

	pusher := &partitionPusher{rc: r, partitionID: p.partitionID}

	// Use an isolated per-partition Registry rather than wrapping
	// r.reg directly. Wrapping r.reg with a constant label only
	// distinguishes series — the underlying metric *names* still
	// collide on the global registry. That bites whenever the
	// rebalancer moves a partition off this pod and back on later:
	// the second startKafkaReader call panics with "duplicate
	// metrics collector registration attempted". By giving each
	// reader its own Registry and registering that Registry as a
	// Collector on the main one, we get scrape coverage without
	// global-namespace conflicts; removePartition then drops the
	// Registry to free the namespace for the next ownership cycle.
	var perPartitionReg prometheus.Registerer
	var partitionReg *prometheus.Registry
	if r.reg != nil {
		partitionReg = prometheus.NewRegistry()
		// The "reader_partition" constant label keeps scrape output
		// distinguishable between partitions on the same pod. We
		// pick this name rather than "partition" because some of
		// the inner reader metrics already use a "partition"
		// variable label, and Prometheus rejects a constant label
		// that collides with a variable label.
		perPartitionReg = prometheus.WrapRegistererWith(prometheus.Labels{
			"reader_partition": strconv.Itoa(int(p.partitionID)),
		}, partitionReg)
	}

	reader, err := ingest.NewPartitionReaderForPusher(
		kafkaCfg,
		p.partitionID,
		r.cfg.InstanceID,
		offsetFilePath,
		pusher,
		log.With(r.logger, "component", "readcache_reader", "partition", p.partitionID),
		perPartitionReg,
	)
	if err != nil {
		return errors.Wrapf(err, "creating partition reader for partition %d", p.partitionID)
	}

	if err := services.StartAndAwaitRunning(ctx, reader); err != nil {
		stopCtx := context.Background()
		if stopErr := services.StopAndAwaitTerminated(stopCtx, reader); stopErr != nil {
			level.Warn(r.logger).Log("msg", "stopping partition reader after failed start",
				"partition", p.partitionID, "err", stopErr)
		}
		return errors.Wrapf(err, "starting partition reader for partition %d", p.partitionID)
	}

	// Only expose per-partition metrics on the main registerer once the
	// reader is running. Registering earlier leaks a collector when startup
	// fails after metrics have been created on the isolated registry.
	if partitionReg != nil {
		if err := r.reg.Register(partitionReg); err != nil {
			stopCtx := context.Background()
			if stopErr := services.StopAndAwaitTerminated(stopCtx, reader); stopErr != nil {
				level.Warn(r.logger).Log("msg", "stopping partition reader after metric registration failure",
					"partition", p.partitionID, "err", stopErr)
			}
			return fmt.Errorf("registering per-partition metric registry: %w", err)
		}
	}

	p.reader = reader
	p.readerMetrics = partitionReg
	return nil
}

// unregisterPartitionMetricsRegistry removes a per-partition metric
// registry from the main registerer. partitionID is only used for logs.
func (r *Readcache) unregisterPartitionMetricsRegistry(partitionReg *prometheus.Registry, partitionID int32) {
	if partitionReg == nil || r.reg == nil {
		return
	}
	if !r.reg.Unregister(partitionReg) {
		level.Warn(r.logger).Log("msg", "readcache: per-partition metric registry not found in registerer",
			"partition", partitionID)
	}
}

// stopKafkaReaderLocked stops the partition reader if present. Caller
// must hold p's owning lock so the reader pointer isn't observed mid-
// transition.
//
// In addition to stopping the reader's services, this releases the
// per-partition metric registry. Skipping the Unregister here means
// the next addPartition call for the same partition ID would
// panic on a duplicate Registry collector registration; this is
// the readcache slicer's "move partition X off and back onto the
// same pod" path, which we want to support cleanly.
func (r *Readcache) stopKafkaReaderLocked(p *partitionState) error {
	if p.reader == nil {
		return nil
	}
	ctx := context.Background()
	if err := services.StopAndAwaitTerminated(ctx, p.reader); err != nil {
		return errors.Wrapf(err, "stopping partition reader for partition %d", p.partitionID)
	}
	p.reader = nil

	if p.readerMetrics != nil {
		r.unregisterPartitionMetricsRegistry(p.readerMetrics, p.partitionID)
		p.readerMetrics = nil
	}
	return nil
}
