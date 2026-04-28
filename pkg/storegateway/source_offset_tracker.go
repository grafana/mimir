// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// sourceOffsetTracker tracks the highest Kafka source offset seen per partition
// across all blocks owned by a store gateway. It is a high-watermark tracker:
// offsets only increase, never decrease.
type sourceOffsetTracker struct {
	mu      sync.Mutex
	offsets map[int32]int64

	metric *prometheus.GaugeVec
}

func newSourceOffsetTracker(reg prometheus.Registerer) *sourceOffsetTracker {
	t := &sourceOffsetTracker{
		offsets: make(map[int32]int64),
		metric: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_bucket_store_highest_seen_source_offset",
			Help: "The highest Kafka source offset seen across all blocks for each partition.",
		}, []string{"partition"}),
	}
	return t
}

// update merges the given offsets into the tracker, keeping the maximum
// offset per partition and updating the metric accordingly.
func (t *sourceOffsetTracker) update(offsets map[int32]int64) {
	if len(offsets) == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for partition, offset := range offsets {
		if existing, ok := t.offsets[partition]; !ok || offset > existing {
			t.offsets[partition] = offset
			t.metric.WithLabelValues(strconv.Itoa(int(partition))).Set(float64(offset))
		}
	}
}
