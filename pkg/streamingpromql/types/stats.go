// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/stats/query_stats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// QueryStats tracks statistics about the execution of a single query.
//
// It is not safe to use this type from multiple goroutines simultaneously.
type QueryStats struct {
	// The total number of samples processed during the query.
	//
	// In the case of range vector selectors, each sample is counted once for each time step it appears in.
	// For example, if a query is running with a step of 30s with a range vector selector with range 45s,
	// then samples in the overlapping 15s are counted twice.
	TotalSamples int64

	// TotalSamplesPerStep represents the total number of samples scanned
	// per step while evaluating a query. Each step should be identical to the
	// TotalSamples when a step is run as an instant query, which means
	// we intentionally do not account for optimizations that happen inside the
	// range query engine that reduce the actual work that happens.
	TotalSamplesPerStep []int64

	EnablePerStepStats bool
	timeRange          QueryTimeRange

	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func NewQueryStats(timeRange QueryTimeRange, enablePerStepStats bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*QueryStats, error) {
	qs := &QueryStats{
		EnablePerStepStats:       enablePerStepStats,
		timeRange:                timeRange,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
	if enablePerStepStats {
		s, err := Int64SlicePool.Get(qs.timeRange.StepCount, qs.memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
		qs.TotalSamplesPerStep = s[:qs.timeRange.StepCount]
	}
	return qs, nil
}

// IncrementSamplesAtStep increments the total samples count. Use this if you know the step index.
func (qs *QueryStats) IncrementSamplesAtStep(i int, samples int64) {
	if qs == nil {
		return
	}
	qs.TotalSamples += samples

	if qs.TotalSamplesPerStep != nil {
		qs.TotalSamplesPerStep[i] += samples
	}
}

// IncrementSamplesAtTimestamp increments the total samples count. Use this if you only have the corresponding step
// timestamp.
func (qs *QueryStats) IncrementSamplesAtTimestamp(t, samples int64) {
	if qs == nil {
		return
	}
	qs.TotalSamples += samples

	if qs.TotalSamplesPerStep != nil {
		qs.TotalSamplesPerStep[qs.timeRange.PointIndex(t)] += samples
	}
}

// Clear resets the TotalSamples counter to 0 and zeroes out all entries in the
// TotalSamplesPerStep slice, preserving its length and capacity for reuse.
func (qs *QueryStats) Clear() {
	qs.TotalSamples = 0
	clear(qs.TotalSamplesPerStep)
}

func (qs *QueryStats) Close() {
	if qs.TotalSamplesPerStep != nil {
		Int64SlicePool.Put(&qs.TotalSamplesPerStep, qs.memoryConsumptionTracker)
		qs.TotalSamplesPerStep = nil
	}
}

const timestampFieldSize = int64(unsafe.Sizeof(int64(0)))

func EquivalentFloatSampleCount(h *histogram.FloatHistogram) int64 {
	return (int64(h.Size()) + timestampFieldSize) / int64(FPointSize)
}
