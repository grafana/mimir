// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
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

	app := db.Appender(ctx)
	for _, ts := range req.Timeseries {
		lbls := mimirpb.FromLabelAdaptersToLabels(ts.Labels)
		var ref storage.SeriesRef
		for _, s := range ts.Samples {
			r, err := app.Append(ref, lbls, s.TimestampMs, s.Value)
			if err != nil {
				return errors.Wrapf(err, "appending sample for series %s", lbls.String())
			}
			ref = r
		}
		for j := range ts.Histograms {
			h := &ts.Histograms[j]
			if h.IsFloatHistogram() {
				ffh := mimirpb.FromFloatHistogramProtoToFloatHistogram(h)
				if _, err := app.AppendHistogram(ref, lbls, h.Timestamp, nil, ffh); err != nil {
					return errors.Wrap(err, "appending float histogram")
				}
			} else {
				fh := mimirpb.FromHistogramProtoToHistogram(h)
				if _, err := app.AppendHistogram(ref, lbls, h.Timestamp, fh, nil); err != nil {
					return errors.Wrap(err, "appending histogram")
				}
			}
		}
		for _, ex := range ts.Exemplars {
			exLbls := mimirpb.FromLabelAdaptersToLabels(ex.Labels)
			_, _ = app.AppendExemplar(ref, lbls, exemplar.Exemplar{
				Labels: exLbls,
				Value:  ex.Value,
				Ts:     ex.TimestampMs,
				HasTs:  true,
			})
		}
	}
	if err := app.Commit(); err != nil {
		return errors.Wrap(err, "committing partition appender")
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

	// Wrap the registerer with a per-partition constant label so that
	// each PartitionReader's metric registrations don't collide on
	// the shared mimir-wide registry. Without this wrapper, two
	// readers calling promauto.NewHistogram with the same name would
	// panic.
	var perPartitionReg prometheus.Registerer
	if r.reg != nil {
		// We use the label name "reader_partition" instead of
		// "partition" because some of the inner reader metrics
		// already use a "partition" variable label, and a constant
		// label that collides with a variable label is rejected by
		// the Prometheus client library.
		perPartitionReg = prometheus.WrapRegistererWith(prometheus.Labels{
			"reader_partition": strconv.Itoa(int(p.partitionID)),
		}, r.reg)
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
		return errors.Wrapf(err, "starting partition reader for partition %d", p.partitionID)
	}

	p.reader = reader
	return nil
}

// stopKafkaReaderLocked stops the partition reader if present. Caller
// must hold p's owning lock so the reader pointer isn't observed mid-
// transition.
func (r *Readcache) stopKafkaReaderLocked(p *partitionState) error {
	if p.reader == nil {
		return nil
	}
	ctx := context.Background()
	if err := services.StopAndAwaitTerminated(ctx, p.reader); err != nil {
		return errors.Wrapf(err, "stopping partition reader for partition %d", p.partitionID)
	}
	p.reader = nil
	return nil
}
