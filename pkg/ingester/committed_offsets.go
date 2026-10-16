// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
)

// committedOffsetsWatcher periodically fetches the committed offsets of the configured consumer
// group — the one the block-builder consumes with — for the ingester's partition, from every Kafka
// cluster the group commits to. The latest observed offsets gate the deletion of compacted blocks:
// a block is deleted only once the consumer group consumed the records the block's data came from
// (see userTSDB.blocksToDelete).
type committedOffsetsWatcher struct {
	// kafkaCfgs holds one Kafka config per cluster the consumer group commits to, indexed by
	// Kafka cluster ID (see committedOffsetsKafkaConfigs).
	kafkaCfgs []ingest.KafkaConfig

	consumerGroup string
	partitionID   int32
	interval      time.Duration
	logger        log.Logger
	metrics       *committedOffsetsMetrics

	// committed holds the latest observed offsets, indexed by Kafka cluster ID. It is nil
	// until the consumer group committed to at least one cluster.
	committed atomic.Pointer[kmeta.PartitionOffsets]
}

type committedOffsetsMetrics struct {
	committedOffset *prometheus.GaugeVec
}

func newCommittedOffsetsMetrics(reg prometheus.Registerer) *committedOffsetsMetrics {
	return &committedOffsetsMetrics{
		committedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_committed_offset",
			Help: "The last committed offset of the configured consumer group observed for the ingester's partition, per Kafka cluster (write compartment).",
		}, []string{"write_compartment"}),
	}
}

func newCommittedOffsetsWatcher(kafkaCfgs []ingest.KafkaConfig, consumerGroup string, partitionID int32, interval time.Duration, logger log.Logger, reg prometheus.Registerer) *committedOffsetsWatcher {
	return &committedOffsetsWatcher{
		kafkaCfgs:     kafkaCfgs,
		consumerGroup: consumerGroup,
		partitionID:   partitionID,
		interval:      interval,
		logger:        logger,
		metrics:       newCommittedOffsetsMetrics(reg),
	}
}

// Committed returns the latest observed committed offsets, indexed by Kafka cluster ID, and
// whether the consumer group committed to at least one cluster yet.
func (w *committedOffsetsWatcher) Committed() (kmeta.PartitionOffsets, bool) {
	if offsets := w.committed.Load(); offsets != nil {
		return *offsets, true
	}
	return nil, false
}

// poll fetches the committed offsets from every cluster and refreshes the cached snapshot.
//
// poll never returns an error: it is the iteration of a timer service, whose failure would fail
// the whole ingester. A failed fetch keeps the previously cached snapshot, which only delays
// block deletion, never enables it early.
func (w *committedOffsetsWatcher) poll(ctx context.Context) error {
	offsets, anyCommitted, err := ingest.FetchCommittedOffsets(ctx, w.kafkaCfgs, w.consumerGroup, w.partitionID, w.logger)
	if err != nil {
		level.Warn(w.logger).Log("msg", "failed to fetch committed offsets, keeping the previously observed ones", "err", err)
		return nil
	}

	for clusterID, offset := range offsets {
		w.metrics.committedOffset.WithLabelValues(strconv.Itoa(clusterID)).Set(float64(offset))
	}

	if !anyCommitted {
		// The consumer group did not commit to any cluster yet, e.g. it does not exist because
		// the block-builder did not commit yet. The offsets stay unknown, which keeps block
		// deletion disabled.
		level.Debug(w.logger).Log("msg", "consumer group has not committed any offset yet")
		return nil
	}

	if w.committed.Load() == nil {
		level.Info(w.logger).Log("msg", "observed the first committed offsets of the consumer group", "partition", w.partitionID, "offsets", offsets)
	} else {
		level.Debug(w.logger).Log("msg", "observed committed offsets", "offsets", offsets)
	}
	w.committed.Store(&offsets)
	return nil
}

// service returns the service that polls the committed offsets. Starting the service fetches the
// offsets once. The poll opens and closes its own Kafka clients, so stopping the service has
// nothing to release.
func (w *committedOffsetsWatcher) service() *services.BasicService {
	return services.NewTimerService(w.interval, w.poll, w.poll, nil).WithName("ingester-committed-offsets")
}
