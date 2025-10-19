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
	// This flag prevents multiple decrements for the same series position when At() is called multiple times.
	memoryDecreased bool
}

func NewMemoryTrackingSeriesSet(inner storage.SeriesSet, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) storage.SeriesSet {
	return &MemoryTrackingSeriesSet{
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (m *MemoryTrackingSeriesSet) Next() bool {
	m.memoryDecreased = false
	return m.inner.Next()
}

func (m *MemoryTrackingSeriesSet) At() storage.Series {
	at := m.inner.At()

	// Only decrease memory once per each single Next() call and multiple At() call
	if !m.memoryDecreased {
		// New series at this position
		m.memoryDecreased = true
		// The caller should IncreaseMemoryConsumption for labels if they want to retain the labels.
		// In the future we should avoid calling decrease here and make it the caller responsibility.
		m.memoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(at.Labels())
	}
	return at
}

func (m *MemoryTrackingSeriesSet) Err() error {
	return m.inner.Err()
}

func (m *MemoryTrackingSeriesSet) Warnings() annotations.Annotations {
	return m.inner.Warnings()
}
