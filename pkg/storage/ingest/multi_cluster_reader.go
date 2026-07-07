// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
)

// MultiClusterPartitionReader consumes the same partition from multiple Kafka clusters, pushing every
// cluster's records into the same Pusher. In the compartments architecture there is one Kafka cluster
// per write compartment.
//
// Each per-cluster reader has its own Kafka connection, consumer group, and offset file; offsets are not
// shared across clusters because each cluster has an independent offset space.
//
// Cross-cluster ordering depends on whether ordered consumption is enabled:
//   - When disabled, each cluster's records are pushed independently, so there's no cross-cluster ordering
//     guarantee.
//   - When enabled, every cluster's records are funneled through a shared HeapMerger that orders them by
//     Kafka record timestamp (best effort) before forwarding to the Pusher. A per-cluster reader only
//     commits its offset once the merger has pushed its records, preserving at-least-once delivery.
//
// MultiClusterPartitionReader presents itself as a single services.Service so it can be managed like a
// SingleClusterPartitionReader.
type MultiClusterPartitionReader struct {
	services.Service

	logger         log.Logger
	readers        []*SingleClusterPartitionReader
	merger         *HeapMerger // nil when heap merging is disabled
	readersManager *services.Manager
	watcher        *services.FailureWatcher
}

// NewMultiClusterPartitionReader creates a MultiClusterPartitionReader that consumes the given partition
// from every configured Kafka cluster. clusterConfigs holds one KafkaConfig per Kafka cluster, already
// resolved with that cluster's topic, address, and SASL credentials.
//
// offsetFilePath is the path of each cluster's offset file and must contain the
// compartments.WriteCompartmentIDPlaceholder, which is replaced with the Kafka cluster ID so each cluster
// tracks offsets in its own file. instanceID is the base for each reader's consumer group, suffixed per
// Kafka cluster so each cluster tracks offsets independently.
//
// When orderedCfg.Enabled is set, records from every cluster are consumed in best-effort Kafka-record-timestamp
// order before being pushed; otherwise each cluster pushes independently, so there's no cross-cluster
// ordering guarantee.
func NewMultiClusterPartitionReader(
	clusterConfigs []KafkaConfig,
	orderedCfg OrderedConsumptionConfig,
	partitionID int32,
	instanceID string,
	offsetFilePath string,
	pusher Pusher,
	logger log.Logger,
	reg prometheus.Registerer,
) (*MultiClusterPartitionReader, error) {
	if len(clusterConfigs) < 1 {
		return nil, fmt.Errorf("at least one Kafka cluster must be configured, got %d", len(clusterConfigs))
	}
	if !strings.Contains(offsetFilePath, compartments.WriteCompartmentIDPlaceholder) {
		return nil, fmt.Errorf("the offset file path %q must contain the %q placeholder", offsetFilePath, compartments.WriteCompartmentIDPlaceholder)
	}

	// When ordered consumption is enabled, all clusters feed a shared HeapMerger whose downstream consumer
	// is a PusherConsumer that turns the merged record stream into per-tenant WriteRequests. When disabled,
	// each per-cluster reader gets its own PusherConsumer and pushes independently (no cross-cluster
	// ordering guarantee).
	//
	// Merging is skipped when there's a single Kafka cluster: the heap only ever holds that one cluster's
	// records so it can't reorder anything, and routing them through the merger would add its batching
	// latency and single-goroutine serialization for no ordering benefit.
	var merger *HeapMerger
	if orderedCfg.Enabled && len(clusterConfigs) > 1 {
		mergerLogger := log.With(logger, "component", "heap_merger")
		// The downstream push path is shared across all clusters, so its PusherConsumer metrics are
		// registered once on the base registerer rather than per cluster.
		pusherMetrics := NewPusherConsumerMetrics(reg)
		pusherFactory := consumerFactoryFunc(func() RecordConsumer {
			// clusterConfigs[0] only carries the Kafka client tunables consulted by PusherConsumer
			// (e.g. FallbackClientErrorSampleRate); those are identical across clusters.
			return NewPusherConsumer(pusher, clusterConfigs[0], pusherMetrics, mergerLogger)
		})
		merger = NewHeapMerger(orderedCfg, pusherFactory, NewHeapMergerMetrics(reg), mergerLogger)
	}

	readers := make([]*SingleClusterPartitionReader, len(clusterConfigs))
	// There is one Kafka cluster per write compartment, so the Kafka cluster ID is also the write
	// compartment ID. The offset file name, consumer group suffix, and metric label keep the
	// "write compartment" naming because that's the operator-facing concept.
	for kafkaClusterID, clusterCfg := range clusterConfigs {
		clusterOffsetFilePath := compartments.ReplaceWriteCompartment(offsetFilePath, kafkaClusterID)
		readerInstanceID := fmt.Sprintf("%s-wc-%d", instanceID, kafkaClusterID)
		clusterReg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(kafkaClusterID)}, reg)
		clusterLogger := log.With(logger, "write_compartment", kafkaClusterID)

		var reader *SingleClusterPartitionReader
		var err error
		if merger != nil {
			// The reader streams its records into the shared merger, which pushes them (in merged order)
			// via the shared PusherConsumer. The Pusher is still passed as the PreCommitNotifier so
			// offset commits notify it directly.
			consumerFactory := consumerFactoryFunc(func() RecordConsumer {
				return merger.NewSubmittingConsumer(kafkaClusterID)
			})
			reader, err = newSingleClusterPartitionReader(clusterCfg, partitionID, readerInstanceID, clusterOffsetFilePath, consumerFactory, pusher, clusterLogger, clusterReg)
		} else {
			reader, err = NewSingleClusterPartitionReader(clusterCfg, partitionID, readerInstanceID, clusterOffsetFilePath, pusher, clusterLogger, clusterReg)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for write compartment %d", kafkaClusterID)
		}
		readers[kafkaClusterID] = reader
	}

	r := &MultiClusterPartitionReader{
		logger:  logger,
		readers: readers,
		merger:  merger,
	}
	r.Service = services.NewBasicService(r.starting, r.running, r.stopping).WithName("multi-cluster-partition-reader")
	return r, nil
}

func (r *MultiClusterPartitionReader) starting(ctx context.Context) error {
	// The merger is started before the readers and stopped after them (see stopping()), so it stays
	// available to flush and ack a reader's in-flight consume, which continues on a non-cancellable context.
	if r.merger != nil {
		if err := services.StartAndAwaitRunning(ctx, r.merger); err != nil {
			return errors.Wrap(err, "starting heap merger")
		}
	}

	svcs := make([]services.Service, len(r.readers))
	for i, reader := range r.readers {
		svcs[i] = reader
	}

	var err error
	if r.readersManager, err = services.NewManager(svcs...); err != nil {
		r.stopMerger()
		return errors.Wrap(err, "creating partition readers service manager")
	}

	// If any per-cluster reader fails to start, the whole reader fails to start. This is required to
	// guarantee that the caller doesn't end up with partial consumption if a subset of readers fails to start.
	if err := services.StartManagerAndAwaitHealthy(ctx, r.readersManager); err != nil {
		// Stop the readers that did start, then the merger, so we don't leak them when starting() fails.
		r.readersManager.StopAsync()
		_ = r.readersManager.AwaitStopped(context.Background())
		r.stopMerger()
		return errors.Wrap(err, "starting per-cluster partition readers")
	}

	// Only start watching for failures once everything is running: the watcher's goroutine blocks on the
	// failure channel, which is drained by running() and closed by stopping(), neither of which runs if
	// starting() fails.
	r.watcher = services.NewFailureWatcher()
	r.watcher.WatchManager(r.readersManager)
	if r.merger != nil {
		r.watcher.WatchService(r.merger)
	}

	return nil
}

func (r *MultiClusterPartitionReader) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.watcher.Chan():
		return errors.Wrap(err, "a per-cluster partition reader failed")
	}
}

func (r *MultiClusterPartitionReader) stopping(_ error) error {
	// stopping() only runs if starting() returned without error, so the readers manager and the failure
	// watcher are always set here.
	r.watcher.Close()

	// Stop and fully drain the readers before the merger, so the merger can still flush and ack their
	// in-flight consumes (which run on a non-cancellable context) until every reader has stopped.
	r.readersManager.StopAsync()
	err := r.readersManager.AwaitStopped(context.Background())

	r.stopMerger()
	return err
}

func (r *MultiClusterPartitionReader) stopMerger() {
	if r.merger == nil {
		return
	}
	r.merger.StopAsync()
	_ = r.merger.AwaitTerminated(context.Background())
}

// LastSeenOffsets returns the highest record offset seen by each Kafka cluster's reader, indexed by
// Kafka cluster ID.
func (r *MultiClusterPartitionReader) LastSeenOffsets() kmeta.PartitionOffsets {
	offsets := make([]int64, len(r.readers))
	for kafkaClusterID, reader := range r.readers {
		offsets[kafkaClusterID] = reader.LastSeenOffsets().ForKafkaCluster(0)
	}

	return kmeta.NewMultiClusterPartitionOffsets(offsets)
}

// EnforceReadMaxDelay returns an error if any Kafka cluster's reader is lagging behind more than
// maxDelay. The ingester is caught up only when all Kafka clusters are.
func (r *MultiClusterPartitionReader) EnforceReadMaxDelay(maxDelay time.Duration) error {
	var errs multierror.MultiError
	for kafkaClusterID, reader := range r.readers {
		if err := reader.EnforceReadMaxDelay(maxDelay); err != nil {
			errs.Add(errors.Wrapf(err, "write compartment %d", kafkaClusterID))
		}
	}
	return errs.Err()
}

// WaitReadConsistencyUntilOffsets waits, for every Kafka cluster in parallel, until that cluster's reader
// has consumed up to its own offset. The offsets must cover exactly one offset per Kafka cluster; a
// mismatch is an invariant violation by the caller. Each per-cluster offset is forwarded to that cluster's
// reader as-is, so a negative offset is handled identically to the single-cluster reader (an empty
// partition returns immediately).
func (r *MultiClusterPartitionReader) WaitReadConsistencyUntilOffsets(ctx context.Context, offsets kmeta.PartitionOffsets) error {
	if offsets.NumKafkaClusters() != len(r.readers) {
		return fmt.Errorf("the multi-cluster partition reader consumes from %d Kafka clusters but was given read consistency offsets for %d", len(r.readers), offsets.NumKafkaClusters())
	}

	// errgroup.WithContext cancels gctx on the first error, and each per-cluster wait honors gctx, so a
	// failure in one Kafka cluster unblocks the others immediately instead of waiting for their own
	// timeouts.
	g, gctx := errgroup.WithContext(ctx)
	for kafkaClusterID, reader := range r.readers {
		g.Go(func() error {
			offset := offsets.ForKafkaCluster(kafkaClusterID)
			err := reader.WaitReadConsistencyUntilOffsets(gctx, kmeta.NewSingleClusterPartitionOffsets(offset))
			return errors.Wrapf(err, "write compartment %d", kafkaClusterID)
		})
	}

	return g.Wait()
}

// WaitReadConsistencyUntilLastProducedOffset waits, for every Kafka cluster in parallel, until that
// cluster's reader has consumed up to its last-produced offset.
func (r *MultiClusterPartitionReader) WaitReadConsistencyUntilLastProducedOffset(ctx context.Context) error {
	// errgroup.WithContext cancels gctx on the first error, and each per-cluster wait honors gctx, so a
	// failure in one Kafka cluster unblocks the others immediately instead of waiting for their own
	// timeouts.
	g, gctx := errgroup.WithContext(ctx)
	for kafkaClusterID, reader := range r.readers {
		g.Go(func() error {
			return errors.Wrapf(reader.WaitReadConsistencyUntilLastProducedOffset(gctx), "write compartment %d", kafkaClusterID)
		})
	}
	return g.Wait()
}
