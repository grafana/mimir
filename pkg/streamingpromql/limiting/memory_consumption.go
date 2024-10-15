// SPDX-License-Identifier: AGPL-3.0-only

package limiting

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryConsumptionTracker tracks the current memory utilisation of a single query, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
//
// It is not safe to use this type from multiple goroutines simultaneously.
type MemoryConsumptionTracker struct {
	MaxEstimatedMemoryConsumptionBytes     uint64
	CurrentEstimatedMemoryConsumptionBytes uint64
	PeakEstimatedMemoryConsumptionBytes    uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection bool
}

func NewMemoryConsumptionTracker(maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter) *MemoryConsumptionTracker {
	return &MemoryConsumptionTracker{
		MaxEstimatedMemoryConsumptionBytes: maxEstimatedMemoryConsumptionBytes,

		rejectionCount: rejectionCount,
	}
}

// IncreaseMemoryConsumption attempts to increase the current memory consumption by b bytes.
//
// It returns an error if the query would exceed the maximum memory consumption limit.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumption(b uint64) error {
	if l.MaxEstimatedMemoryConsumptionBytes > 0 && l.CurrentEstimatedMemoryConsumptionBytes+b > l.MaxEstimatedMemoryConsumptionBytes {
		if !l.haveRecordedRejection {
			l.haveRecordedRejection = true
			l.rejectionCount.Inc()
		}

		return limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(l.MaxEstimatedMemoryConsumptionBytes)
	}

	l.CurrentEstimatedMemoryConsumptionBytes += b
	l.PeakEstimatedMemoryConsumptionBytes = max(l.PeakEstimatedMemoryConsumptionBytes, l.CurrentEstimatedMemoryConsumptionBytes)

	return nil
}

// DecreaseMemoryConsumption decreases the current memory consumption by b bytes.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumption(b uint64) {
	if b > l.CurrentEstimatedMemoryConsumptionBytes {
		panic("Estimated memory consumption of this query is negative. This indicates something has been returned to a pool more than once, which is a bug.")
	}
	l.CurrentEstimatedMemoryConsumptionBytes -= b
}
