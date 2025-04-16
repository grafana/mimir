// SPDX-License-Identifier: AGPL-3.0-only

package limiting

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryConsumptionTracker tracks the current memory utilisation of a single query, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
//
// It is safe to use this type from multiple goroutines simultaneously.
type MemoryConsumptionTracker struct {
	MaxEstimatedMemoryConsumptionBytes     uint64
	CurrentEstimatedMemoryConsumptionBytes atomic.Uint64
	PeakEstimatedMemoryConsumptionBytes    atomic.Uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection atomic.Bool
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
	newEstimate := l.CurrentEstimatedMemoryConsumptionBytes.Add(b)

	if l.MaxEstimatedMemoryConsumptionBytes > 0 && newEstimate > l.MaxEstimatedMemoryConsumptionBytes {
		if !l.haveRecordedRejection.Swap(true) {
			l.rejectionCount.Inc()
		}

		// Restore the previous estimate.
		l.CurrentEstimatedMemoryConsumptionBytes.Sub(b)

		return limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(l.MaxEstimatedMemoryConsumptionBytes)
	}

	// TODO: how to handle peak consumption reporting? Surely there's a better way
	// Keep trying to update the peak until we succeed or the peak is updated to be higher than the new estimate elsewhere.
	for {
		currentPeak := l.PeakEstimatedMemoryConsumptionBytes.Load()

		if newEstimate <= currentPeak {
			return nil
		}

		if l.PeakEstimatedMemoryConsumptionBytes.CompareAndSwap(currentPeak, newEstimate) {
			return nil
		}
	}
}

// DecreaseMemoryConsumption decreases the current memory consumption by b bytes.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumption(b uint64) {
	// TODO: surely there's a better way to do this
	for {
		current := l.CurrentEstimatedMemoryConsumptionBytes.Load()

		if current < b {
			panic("Estimated memory consumption of this query is negative. This indicates something has been returned to a pool more than once, which is a bug.")
		}

		if l.CurrentEstimatedMemoryConsumptionBytes.CompareAndSwap(current, current-b) {
			return
		}
	}
}
