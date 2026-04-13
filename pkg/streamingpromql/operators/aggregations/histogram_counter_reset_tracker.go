// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type histogramCounterResetTracker struct {
	resetHints               []histogram.CounterResetHint
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newHistogramCounterResetTracker(size int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*histogramCounterResetTracker, error) {
	g := &histogramCounterResetTracker{
		memoryConsumptionTracker: memoryConsumptionTracker,
	}

	var err error
	g.resetHints, err = types.CounterResetHintSlicePool.Get(size, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	g.resetHints = g.resetHints[:size]

	return g, nil
}

func (c *histogramCounterResetTracker) close() {
	types.CounterResetHintSlicePool.Put(&c.resetHints, c.memoryConsumptionTracker)
}

func (c *histogramCounterResetTracker) init(outputIdx int64, hint histogram.CounterResetHint) {
	c.resetHints[outputIdx] = hint
}

// checkCounterResetConflicts will return true if we see a histogram.NotCounterReset and histogram.CounterReset for histograms in the same group.
func (c *histogramCounterResetTracker) checkCounterResetConflicts(outputIdx int64, hint histogram.CounterResetHint) bool {

	existing := c.resetHints[outputIdx]
	if existing == hint {
		return false
	}
	switch hint {
	case histogram.NotCounterReset:
		if existing == histogram.CounterReset {
			return true
		}
		c.resetHints[outputIdx] = hint
	case histogram.CounterReset:
		if existing == histogram.NotCounterReset {
			return true
		}
		c.resetHints[outputIdx] = hint
	}
	return false
}
