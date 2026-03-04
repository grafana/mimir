// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// buildXORChunk creates an XOR chunk loaded with numSamples float samples at the given
// interval starting from t0 (milliseconds).  The chunk itself is returned so that
// fresh iterators can be created cheaply inside the benchmark loop.
func buildXORChunk(t0 int64, interval time.Duration, numSamples int) *chunkenc.XORChunk {
	c := chunkenc.NewXORChunk()
	app, err := c.Appender()
	if err != nil {
		panic(err)
	}
	for i := 0; i < numSamples; i++ {
		t := t0 + int64(i)*interval.Milliseconds()
		app.Append(0, t, float64(i+1))
	}
	return c
}

// BenchmarkFillBufferMicro_WithSkip exercises fillBuffer where the iterator must skip a
// large number of pre-window samples on its first invocation. This is the primary
// beneficiary of the Seek optimisation: a single Seek(rangeStart+1) replaces N calls to
// Next()+check.
//
// Storage, selector, and query-plumbing are bypassed — only the fillBuffer loop is measured.
func BenchmarkFillBufferMicro_WithSkip(b *testing.B) {
	const (
		interval      = 15 * time.Second
		totalDuration = 60 * time.Minute
		rangeDuration = 5 * time.Minute
	)
	numSamples := int(totalDuration/interval) + 1 // 241 samples; ~220 before the window

	t0 := int64(0)
	rangeEnd := t0 + totalDuration.Milliseconds()
	rangeStart := rangeEnd - rangeDuration.Milliseconds()

	// Build chunk once; create a new iterator each iteration from the same chunk data.
	chunk := buildXORChunk(t0, interval, numSamples)

	ctx := context.Background()
	mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	floatBuf := types.NewFPointRingBuffer(mc)
	histBuf := types.NewHPointRingBuffer(mc)
	defer floatBuf.Close()
	defer histBuf.Close()

	sel := &Selector{
		Range:                    rangeDuration,
		LookbackDelta:            rangeDuration,
		MemoryConsumptionTracker: mc,
	}
	rv := &RangeVectorSelector{
		Selector:          sel,
		Stats:             types.NewQueryStats(),
		floats:            floatBuf,
		histograms:        histBuf,
		rangeMilliseconds: rangeDuration.Milliseconds(),
	}

	// Reuse a single iterator instance to avoid per-iteration allocation.
	var iterReuse chunkenc.Iterator

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset iterator to the beginning of the chunk.
		iterReuse = chunk.Iterator(iterReuse)
		rv.chunkIterator = iterReuse
		floatBuf.Reset()

		_, err := rv.fillBuffer(floatBuf, histBuf, rangeStart, rangeEnd)
		require.NoError(b, err)
	}
}

// BenchmarkFillBufferMicro_NoSkip exercises fillBuffer where all samples are within the
// range window — the pure inner-append loop with no pre-window samples to skip.
func BenchmarkFillBufferMicro_NoSkip(b *testing.B) {
	const (
		interval      = 15 * time.Second
		rangeDuration = 5 * time.Minute
	)
	numSamples := int(rangeDuration/interval) + 1 // 21 samples

	t0 := int64(0)
	rangeStart := t0 - 1
	rangeEnd := t0 + rangeDuration.Milliseconds()

	chunk := buildXORChunk(t0, interval, numSamples)

	ctx := context.Background()
	mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	floatBuf := types.NewFPointRingBuffer(mc)
	histBuf := types.NewHPointRingBuffer(mc)
	defer floatBuf.Close()
	defer histBuf.Close()

	sel := &Selector{
		Range:                    rangeDuration,
		LookbackDelta:            rangeDuration,
		MemoryConsumptionTracker: mc,
	}
	rv := &RangeVectorSelector{
		Selector:          sel,
		Stats:             types.NewQueryStats(),
		floats:            floatBuf,
		histograms:        histBuf,
		rangeMilliseconds: rangeDuration.Milliseconds(),
	}

	var iterReuse chunkenc.Iterator

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iterReuse = chunk.Iterator(iterReuse)
		rv.chunkIterator = iterReuse
		floatBuf.Reset()

		_, err := rv.fillBuffer(floatBuf, histBuf, rangeStart, rangeEnd)
		require.NoError(b, err)
	}
}

// BenchmarkFillBufferMicro_SteadyState simulates the steady-state scenario: the buffer
// already has most samples cached and fillBuffer only appends a handful of new samples
// per step. We call fillBuffer multiple times on the same iterator, simulating a sliding
// window query.
func BenchmarkFillBufferMicro_SteadyState(b *testing.B) {
	const (
		stepDur       = time.Minute
		rangeDuration = 5 * time.Minute
		totalDuration = 30 * time.Minute
		interval      = 15 * time.Second
	)
	numSamples := int(totalDuration/interval) + 1 // 121 samples
	numSteps := int(totalDuration / stepDur)       // 30 steps

	t0 := int64(0)
	chunk := buildXORChunk(t0, interval, numSamples)

	ctx := context.Background()
	mc := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

	floatBuf := types.NewFPointRingBuffer(mc)
	histBuf := types.NewHPointRingBuffer(mc)
	defer floatBuf.Close()
	defer histBuf.Close()

	sel := &Selector{
		Range:                    rangeDuration,
		LookbackDelta:            rangeDuration,
		MemoryConsumptionTracker: mc,
	}
	rv := &RangeVectorSelector{
		Selector:          sel,
		Stats:             types.NewQueryStats(),
		floats:            floatBuf,
		histograms:        histBuf,
		rangeMilliseconds: rangeDuration.Milliseconds(),
	}

	var iterReuse chunkenc.Iterator

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iterReuse = chunk.Iterator(iterReuse)
		rv.chunkIterator = iterReuse
		floatBuf.Reset()
		histBuf.Reset()

		for s := 0; s <= numSteps; s++ {
			stepEndT := t0 + int64(s)*stepDur.Milliseconds()
			rangeEnd := stepEndT
			rangeStart := rangeEnd - rangeDuration.Milliseconds()

			floatBuf.DiscardPointsAtOrBefore(rangeStart)

			_, err := rv.fillBuffer(floatBuf, histBuf, rangeStart, rangeEnd)
			require.NoError(b, err)
		}
	}
}
