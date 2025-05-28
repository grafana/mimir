// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/stats/query_stats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
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

	TotalSamplesPerStep []int64

	EnablePerStepStats bool
	startTimestamp     int64
	interval           int64
}

type stepStat struct {
	T int64
	V int64
}

const timestampFieldSize = int64(unsafe.Sizeof(int64(0)))

func (qs *QueryStats) InitStepTracking(start, end, interval int64) {
	if !qs.EnablePerStepStats {
		return
	}

	numSteps := int((end-start)/interval) + 1
	qs.TotalSamplesPerStep = make([]int64, numSteps)
	qs.startTimestamp = start
	qs.interval = interval
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
		i := int((t - qs.startTimestamp) / qs.interval)
		qs.TotalSamplesPerStep[i] += samples
	}
}

// TODO: ikonstantinov: think about better name
func (qs *QueryStats) NewChild(enablePerStepStats bool) *QueryStats {
	return &QueryStats{
		EnablePerStepStats: enablePerStepStats,
	}
}

func (qs *QueryStats) TotalSamplesPerStepPoints() []stepStat {
	if !qs.EnablePerStepStats {
		return nil
	}

	ts := make([]stepStat, len(qs.TotalSamplesPerStep))
	for i, c := range qs.TotalSamplesPerStep {
		ts[i] = stepStat{T: qs.startTimestamp + int64(i)*qs.interval, V: c}
	}
	return ts
}

func (qs *QueryStats) Release() {
	qs.TotalSamples = 0
	if qs.TotalSamplesPerStep != nil {
		clear(qs.TotalSamplesPerStep)
	}
}

func EquivalentFloatSampleCount(h *histogram.FloatHistogram) int64 {
	return (int64(h.Size()) + timestampFieldSize) / int64(FPointSize)
}
