package aggregations

import (
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type histogramCounterResetTracker struct {
	histogramCounterReset    []histogram.CounterResetHint
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func newHistogramCounterResetTracker(size int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*histogramCounterResetTracker, error) {
	g := &histogramCounterResetTracker{
		memoryConsumptionTracker: memoryConsumptionTracker,
	}

	byteSlice, err := types.ByteSlicePool.Get(size, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	g.histogramCounterReset = *(*[]histogram.CounterResetHint)(unsafe.Pointer(&byteSlice))
	g.histogramCounterReset = g.histogramCounterReset[:size]

	return g, nil
}

func (c *histogramCounterResetTracker) close() {
	types.ByteSlicePool.Put((*[]byte)(unsafe.Pointer(&c.histogramCounterReset)), c.memoryConsumptionTracker)
}

func (c *histogramCounterResetTracker) init(outputIdx int64, hint histogram.CounterResetHint) {
	c.histogramCounterReset[outputIdx] = hint
}

// checkCounterResetConflicts will return true if we see a histogram.NotCounterReset and histogram.CounterReset for histograms in the same group.
func (c *histogramCounterResetTracker) checkCounterResetConflicts(outputIdx int64, hint histogram.CounterResetHint) bool {

	existing := c.histogramCounterReset[outputIdx]
	if existing == hint {
		return false
	}
	switch hint {
	case histogram.NotCounterReset:
		if existing == histogram.CounterReset {
			return true
		}
		c.histogramCounterReset[outputIdx] = hint
	case histogram.CounterReset:
		if existing == histogram.NotCounterReset {
			return true
		}
		c.histogramCounterReset[outputIdx] = hint
	}
	return false
}
