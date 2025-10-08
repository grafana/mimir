// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryTrackingSeriesSet is storage.SeriesSet that tracks the wrapped SeriesSet memory consumption.
type MemoryTrackingSeriesSet struct {
	inner                    storage.SeriesSet
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentSeries            storage.Series
	memoryDecreased          bool
}

func NewMemoryTrackingSeriesSet(inner storage.SeriesSet, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) storage.SeriesSet {
	return &MemoryTrackingSeriesSet{
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (m *MemoryTrackingSeriesSet) Next() bool {
	// Reset tracking state when moving to next series
	m.currentSeries = nil
	m.memoryDecreased = false
	return m.inner.Next()
}

func (m *MemoryTrackingSeriesSet) At() storage.Series {
	at := m.inner.At()

	// Only decrease memory once per series position
	if m.memoryConsumptionTracker != nil {
		if m.currentSeries != at {
			// New series at this position
			m.currentSeries = at
			m.memoryDecreased = true
			defer m.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(at.Labels())
		}
	}
	return at
}

func (m *MemoryTrackingSeriesSet) Err() error {
	return m.inner.Err()
}

func (m *MemoryTrackingSeriesSet) Warnings() annotations.Annotations {
	return m.inner.Warnings()
}
