// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// MemoryTrackingSeriesSet is storage.SeriesSet that tracks the wrapped SeriesSet memory consumption.
type MemoryTrackingSeriesSet struct {
	storage.SeriesSet
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	currentSeries            storage.Series
	memoryDecreased          bool
}

func NewMemoryTrackingSeriesSet(embed storage.SeriesSet, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) storage.SeriesSet {
	return &MemoryTrackingSeriesSet{
		SeriesSet:                embed,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (m *MemoryTrackingSeriesSet) Next() bool {
	// Reset tracking state when moving to next series
	m.currentSeries = nil
	m.memoryDecreased = false
	return m.SeriesSet.Next()
}

func (m *MemoryTrackingSeriesSet) At() storage.Series {
	at := m.SeriesSet.At()

	// Only decrease memory once per series position
	if m.memoryConsumptionTracker != nil && !m.memoryDecreased {
		if m.currentSeries != at {
			// New series at this position
			m.currentSeries = at
			m.memoryDecreased = true
			defer m.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(at.Labels())
		}
	}
	return at
}
