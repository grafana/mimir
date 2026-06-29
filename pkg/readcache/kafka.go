// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// partitionPusher is the Pusher handed to a per-partition
// ingest.PartitionReader. It captures the partition ID so samples can
// be routed to the right (tenant, partition) TSDB on the readcache.
type partitionPusher struct {
	rc          *Readcache
	partitionID int32
	// ranges is a direct reference to the partitionState.ranges
	// owned by this partition. Capturing it on the pusher avoids an
	// extra partitionMu RLock per push just to look the
	// partitionState back up; the partitionState (and therefore
	// ranges) is guaranteed to outlive the pusher because the
	// reader stop path runs before partitionState is dropped from
	// r.partitions (see Readcache.removePartition for the ordering
	// invariant).
	ranges *partitionRanges
	// samplesIngested is the pre-resolved CounterVec child for this
	// partition. Resolving WithLabelValues once at construction time
	// avoids a map lookup on every Kafka batch on the hot ingest
	// path.
	samplesIngested prometheus.Counter
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
	// Count what landed in the head (float samples + native
	// histograms). PushWriteRequestTimeseries internally rejects
	// individual sample-level errors via FirstPartialErr; res.Err
	// (handled above) is the only hard-failure path, and on that
	// path the batch is retried by the partition reader, so
	// counting here avoids double-attribution on retry. Counting
	// what arrived from Kafka — including the few samples that
	// triggered a FirstPartialErr — is the right "ingest rate"
	// signal: it tells you how much the Kafka topic asked this
	// partition to handle, not how many individual samples passed
	// validation.
	if p.samplesIngested != nil {
		var n int
		for _, ts := range req.Timeseries {
			n += len(ts.Samples) + len(ts.Histograms)
		}
		if n > 0 {
			p.samplesIngested.Add(float64(n))
		}
	}
	// Mirror of cortex_ingester_ingested_samples_total: only samples
	// that survived soft-error validation and were committed. The
	// ingester increments after the hard-failure return and
	// regardless of FirstPartialErr (the commit completed); same
	// ordering here, so the two counters line up 1:1 per user.
	if p.rc.ingestedSamples != nil {
		p.rc.ingestedSamples.WithLabelValues(userID).Add(float64(res.Stats.SucceededSamplesCount))
	}
	// Attribute the batch to its per-range EwmaRate so the
	// rebalancer can balance by ingest throughput. recordSampleBatch
	// is safe to call after Append: it only reads ts.Labels (deep
	// copied into the head's intern pool by Append) and counts —
	// no references are retained after the call returns.
	if p.ranges != nil {
		p.ranges.recordSampleBatch(userID, req.Timeseries)
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
	// Always adopt new partitions at the live edge: skip
	// PartitionReader's startup catch-up loop entirely. addPartition
	// is on the critical path of every readcache assignment change
	// (see Readcache.applyAssignment), and the rebalancer serializes
	// adds within one snapshot; if start() blocks for catch-up on
	// each partition, a pod that just received N partitions takes
	// O(N * MaxConsumerLagAtStartup) wall-clock to finish
	// reconciling. By forcing the consume position to the partition
	// end, getStartOffset returns kafkaOffsetEnd, which
	// PartitionReader.start() uses to short-circuit
	// processNextFetchesUntilTargetOrMaxLagHonored.
	//
	// What we trade for it: any records produced before the reader
	// joins are not consumed by this readcache. That's acceptable
	// because readcache is a query-locality optimisation, not a
	// durability path; the head will refill from the produce stream
	// over the following minutes, and ingester/blockbuilder remain
	// the canonical sources for anything older.
	//
	// The literal "end" is the user-facing flag value validated by
	// ingest.KafkaConfig.Validate, so it's stable wire surface; the
	// underlying constant lives in pkg/storage/ingest/config.go.
	kafkaCfg.ConsumeFromPositionAtStartup = "end"

	pusher := &partitionPusher{rc: r, partitionID: p.partitionID, ranges: p.ranges}
	if r.samplesIngestedTotal != nil {
		pusher.samplesIngested = r.samplesIngestedTotal.WithLabelValues(strconv.Itoa(int(p.partitionID)))
	}

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

	reader, err := ingest.NewSingleClusterPartitionReader(
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
	//
	// We wrap partitionReg in a frozen-descriptor collector before
	// registering. The reason is subtle: the outer Registry identifies
	// each registered Collector by the XOR of its descriptor IDs (see
	// vendor/github.com/prometheus/client_golang/prometheus/registry.go:
	// Register and Unregister both call c.Describe and hash the IDs).
	// PartitionReader registers some metrics lazily during operation
	// (e.g. WithLabelValues for new label combinations), so by the time
	// stopKafkaReaderLocked calls Unregister, partitionReg.Describe
	// returns a *different* set of descs than at Register time, and the
	// outer Registry's Unregister silently returns false because the
	// recomputed collectorID doesn't match anything. The stale entry
	// then collides on the *next* addPartition for the same partition
	// ID with "duplicate metrics collector registration attempted",
	// because the freshly-recreated partitionReg's initial desc set
	// happens to match the old (still-registered) collectorID.
	//
	// Snapshotting descs at Register time and reusing them at
	// Unregister time gives the wrapper a stable collectorID identity
	// across the partition's lifecycle. Collect still delegates to the
	// live partitionReg, so all metrics — including those added after
	// the snapshot — still appear in scrape output (the default,
	// non-pedantic Registry does not enforce desc/metric consistency
	// at Gather time).
	var partitionCollector prometheus.Collector
	if partitionReg != nil {
		partitionCollector = newFrozenDescCollector(partitionReg)
		if err := r.reg.Register(partitionCollector); err != nil {
			stopCtx := context.Background()
			if stopErr := services.StopAndAwaitTerminated(stopCtx, reader); stopErr != nil {
				level.Warn(r.logger).Log("msg", "stopping partition reader after metric registration failure",
					"partition", p.partitionID, "err", stopErr)
			}
			return fmt.Errorf("registering per-partition metric registry: %w", err)
		}
	}

	p.reader = reader
	p.readerMetrics = partitionCollector
	// Capture the offset we joined at and when. The reader started at
	// the live edge (ConsumeFromPositionAtStartup = "end") and has
	// finished starting, so LastSeenOffsets is the partition's
	// high-water offset at acquisition. Both are kept for the admin
	// page's TSDB listing; the current end offset is read live from
	// the reader.
	p.startOffset.Store(reader.LastSeenOffsets().ForKafkaCluster(0))
	p.startedConsumingAt.Store(time.Now().UnixMilli())
	return nil
}

// unregisterPartitionMetricsRegistry removes a per-partition metric
// collector from the main registerer. The Collector must be the
// exact value that was passed to Register (a *frozenDescCollector
// wrapping the per-partition prometheus.Registry); the wrapper's
// stable Describe output is what lets the outer Registry's
// collector-ID hash match the original registration even though the
// inner Registry has accumulated new descriptors since.
// partitionID is only used for logs.
func (r *Readcache) unregisterPartitionMetricsRegistry(partitionReg prometheus.Collector, partitionID int32) {
	if partitionReg == nil || r.reg == nil {
		return
	}
	if !r.reg.Unregister(partitionReg) {
		level.Warn(r.logger).Log("msg", "readcache: per-partition metric registry not found in registerer",
			"partition", partitionID)
	}
}

// stopKafkaReaderLocked stops the partition reader if present.
//
// The "Locked" suffix is historical: this function reads and writes
// p.reader without taking a mutex, so the caller is responsible for
// preventing concurrent access. Both call sites (stopPartition via
// stopping() and removePartition) satisfy this by ensuring p has
// already been removed from r.partitions, so no other goroutine can
// reach this partitionState. addPartition is the only producer of
// p.reader and runs at most once per (partition, partitionState)
// pair.
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
