// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitklimitratio

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type quietCloser interface {
	close()
}

type stepCounter interface {
	quietCloser
	inc(step int)
	count(step int) int
}

type stepCounterNoOp struct{}

func (s *stepCounterNoOp) inc(_ int)       {}
func (s *stepCounterNoOp) close()          {}
func (s *stepCounterNoOp) count(_ int) int { return 0 }

type stepCounterImpl struct {
	// index by step --> number of series (which have a point at this step) we have included for the group
	seriesIncludedAtStep     []int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (s *stepCounterImpl) close() {
	types.IntSlicePool.Put(&s.seriesIncludedAtStep, s.memoryConsumptionTracker)
}

func (s *stepCounterImpl) inc(step int) {
	s.seriesIncludedAtStep[step]++
}

func (s *stepCounterImpl) count(step int) int {
	return s.seriesIncludedAtStep[step]
}

func newStepCounterImpl(size int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (stepCounter, error) {
	intSlice, err := types.IntSlicePool.Get(size, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	intSlice = intSlice[:size]
	return &stepCounterImpl{
		memoryConsumptionTracker: memoryConsumptionTracker,
		seriesIncludedAtStep:     intSlice,
	}, nil
}

type queryGroup struct {
	seriesCount              int            // Total number of series that contribute to this group.
	filled                   bool           // Flag to indicate we have identified k series for this group and no more series need to be accumulated
	accumulatedSeries        []*querySeries // A place to store the series we will return for this group
	stepCounter              stepCounter    // A utility to track the number of series points for each step across the group
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (q *queryGroup) initAccumulatedSeries(size int) {
	q.accumulatedSeries = make([]*querySeries, 0, size)
}

func (q *queryGroup) incStepCounter(step int) {
	q.stepCounter.inc(step)
}

func (q *queryGroup) countAtStep(step int) int {
	return q.stepCounter.count(step)
}

func (q *queryGroup) close() {
	q.stepCounter.close()
}

type querySeries struct {
	metadata types.SeriesMetadata
	value    types.InstantVectorSeriesData
}

func newQueryGroup(size int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, needsStepCounter bool) (*queryGroup, error) {
	var counter stepCounter
	var err error

	// The step counter is only needed for the limitk query internals implementation.
	// Keeping the counter here avoids having to maintain a separate map of group --> counter references.
	// Although the counter could be safely used for limit_ratio queries, it would be a waste to allocate the counter slice for each group and not need it.
	// The noop counter is used to avoid having a nil counter which needs a conditionally checked for each possible counter increment.
	if needsStepCounter {
		counter, err = newStepCounterImpl(size, memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
	} else {
		counter = &stepCounterNoOp{}
	}

	return &queryGroup{
		memoryConsumptionTracker: memoryConsumptionTracker,
		stepCounter:              counter,
	}, nil
}
