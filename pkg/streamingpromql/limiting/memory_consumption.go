// SPDX-License-Identifier: AGPL-3.0-only

package limiting

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/util/limiter"
)

type contextKey int

const (
	memoryConsumptionTracker contextKey = 0
)

// FromContextWithFallback returns a MemoryConsumptionTracker that has been added to this
// context. If there is no MemoryConsumptionTracker in this context, a new no-op tracker that
// does not enforce any limits is returned.
func FromContextWithFallback(ctx context.Context) *MemoryConsumptionTracker {
	tracker, ok := ctx.Value(memoryConsumptionTracker).(*MemoryConsumptionTracker)
	if !ok {
		return NewMemoryConsumptionTracker(0, nil)
	}

	return tracker
}

// AddToContext adds a MemoryConsumptionTracker to this context. This is used to propagate
// per-query memory consumption tracking to parts of the read path that cannot be modified
// to accept extra parameters.
func AddToContext(ctx context.Context, tracker *MemoryConsumptionTracker) context.Context {
	return context.WithValue(ctx, interface{}(memoryConsumptionTracker), tracker)
}

// MemoryConsumptionTracker tracks the current memory utilisation of a single query, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
type MemoryConsumptionTracker struct {
	maxEstimatedMemoryConsumptionBytes     uint64
	currentEstimatedMemoryConsumptionBytes uint64
	peakEstimatedMemoryConsumptionBytes    uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection bool

	// mtx protects all mutable state of the memory consumption tracker. We use a mutex
	// rather than atomics because we only want to adjust the memory used after checking
	// that it would not exceed the limit.
	mtx sync.Mutex
}

func NewMemoryConsumptionTracker(maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter) *MemoryConsumptionTracker {
	return &MemoryConsumptionTracker{
		maxEstimatedMemoryConsumptionBytes: maxEstimatedMemoryConsumptionBytes,

		rejectionCount: rejectionCount,
	}
}

// IncreaseMemoryConsumption attempts to increase the current memory consumption by b bytes.
//
// It returns an error if the query would exceed the maximum memory consumption limit.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumption(b uint64) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if l.maxEstimatedMemoryConsumptionBytes > 0 && l.currentEstimatedMemoryConsumptionBytes+b > l.maxEstimatedMemoryConsumptionBytes {
		if !l.haveRecordedRejection {
			l.haveRecordedRejection = true
			l.rejectionCount.Inc()
		}

		return limiter.NewMaxEstimatedMemoryConsumptionPerQueryLimitError(l.maxEstimatedMemoryConsumptionBytes)
	}

	l.currentEstimatedMemoryConsumptionBytes += b
	l.peakEstimatedMemoryConsumptionBytes = max(l.peakEstimatedMemoryConsumptionBytes, l.currentEstimatedMemoryConsumptionBytes)

	return nil
}

// DecreaseMemoryConsumption decreases the current memory consumption by b bytes.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumption(b uint64) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if b > l.currentEstimatedMemoryConsumptionBytes {
		panic("Estimated memory consumption of this query is negative. This indicates something has been returned to a pool more than once, which is a bug.")
	}
	l.currentEstimatedMemoryConsumptionBytes -= b
}

// PeakEstimatedMemoryConsumptionBytes returns the peak memory consumption in bytes.
func (l *MemoryConsumptionTracker) PeakEstimatedMemoryConsumptionBytes() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.peakEstimatedMemoryConsumptionBytes
}

// CurrentEstimatedMemoryConsumptionBytes returns the current memory consumption in bytes.
func (l *MemoryConsumptionTracker) CurrentEstimatedMemoryConsumptionBytes() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.currentEstimatedMemoryConsumptionBytes
}
