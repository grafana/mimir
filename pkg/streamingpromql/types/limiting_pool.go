// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"slices"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// There's not too much science behind this number: 100,000 points allows for a point per minute for just under 70 days.
	// Then we use the next power of two, given the pools always return slices with capacity equal to a power of two.
	MaxExpectedPointsPerSeries = 131_072

	// When allocating a slice of HPoints include an estimate of the size of the FloatHistogram pointed to by each HPoint
	// for bookkeeping purposes. The FloatHistogram is allocated separately from the slice of HPoints but it's easier to
	// track their memory usage as part of the allocation of the slice. The size, 288 bytes, is an estimate without too
	// much science behind it. The minimum size of a FloatHistogram is 168 bytes + 10 buckets (10 * 8 bytes) + 5 spans
	// (5 * 8 bytes). Some FloatHistograms will be bigger than this and some will be smaller.
	nativeHistogramEstimatedSize = 288

	FPointSize           = uint64(unsafe.Sizeof(promql.FPoint{}))
	HPointSize           = uint64(unsafe.Sizeof(promql.HPoint{}) + nativeHistogramEstimatedSize)
	VectorSampleSize     = uint64(unsafe.Sizeof(promql.Sample{})) // This assumes each sample is a float sample, not a histogram.
	Float64Size          = uint64(unsafe.Sizeof(float64(0)))
	IntSize              = uint64(unsafe.Sizeof(int(0)))
	CounterResetHintSize = uint64(unsafe.Sizeof(histogram.CounterResetHint(0)))
	Int64Size            = uint64(unsafe.Sizeof(int64(0)))
	BoolSize             = uint64(unsafe.Sizeof(false))
	HistogramPointerSize = uint64(unsafe.Sizeof((*histogram.FloatHistogram)(nil)))
	SeriesMetadataSize   = uint64(unsafe.Sizeof(SeriesMetadata{}))
)

var (
	// EnableManglingReturnedSlices enables mangling values in slices returned to pool to aid in detecting use-after-return bugs.
	// Only used in tests.
	EnableManglingReturnedSlices = false

	FPointSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []promql.FPoint {
			return make([]promql.FPoint, 0, size)
		}),
		limiter.FPointSlices,
		FPointSize,
		false,
		nil,
		nil,
	)

	HPointSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []promql.HPoint {
			return make([]promql.HPoint, 0, size)
		}),
		limiter.HPointSlices,
		HPointSize,
		false,
		func(point promql.HPoint) promql.HPoint {
			point.H = mangleHistogram(point.H)
			return point
		},
		nil,
	)

	VectorPool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) promql.Vector {
			return make(promql.Vector, 0, size)
		}),
		limiter.Vectors,
		VectorSampleSize,
		false,
		nil,
		nil,
	)

	Float64SlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []float64 {
			return make([]float64, 0, size)
		}),
		limiter.Float64Slices,
		Float64Size,
		true,
		nil,
		nil,
	)

	IntSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []int {
			return make([]int, 0, size)
		}),
		limiter.IntSlices,
		IntSize,
		true,
		nil,
		nil,
	)

	CounterResetHintSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []histogram.CounterResetHint {
			return make([]histogram.CounterResetHint, 0, size)
		}),
		limiter.CounterResetHintSlices,
		CounterResetHintSize,
		true,
		nil,
		nil,
	)

	Int64SlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []int64 {
			return make([]int64, 0, size)
		}),
		limiter.Int64Slices,
		Int64Size,
		true,
		nil,
		nil,
	)

	BoolSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []bool {
			return make([]bool, 0, size)
		}),
		limiter.BoolSlices,
		BoolSize,
		true,
		nil,
		nil,
	)

	HistogramSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedPointsPerSeries, func(size int) []*histogram.FloatHistogram {
			return make([]*histogram.FloatHistogram, 0, size)
		}),
		limiter.HistogramPointerSlices,
		HistogramPointerSize,
		true,
		mangleHistogram,
		nil,
	)

	SeriesMetadataSlicePool = NewLimitingBucketedPool(
		pool.NewBucketedPool(MaxExpectedSeriesPerResult, func(size int) []SeriesMetadata {
			return make([]SeriesMetadata, 0, size)
		}),
		limiter.SeriesMetadataSlices,
		SeriesMetadataSize,
		true,
		nil,
		func(sms []SeriesMetadata, tracker *limiter.MemoryConsumptionTracker) {
			// When putting SeriesMetadata slices back to the pool, we decrease the memory consumption for each label in the metadata.
			for _, sm := range sms {
				tracker.DecreaseMemoryConsumptionForLabels(sm.Labels)
			}
		},
	)
)

func mangleHistogram(h *histogram.FloatHistogram) *histogram.FloatHistogram {
	if h == nil {
		return nil
	}

	h.ZeroCount = 12345678
	h.Count = 12345678
	h.Sum = 12345678

	for i := range h.NegativeBuckets {
		h.NegativeBuckets[i] = 12345678
	}

	for i := range h.PositiveBuckets {
		h.PositiveBuckets[i] = 12345678
	}

	// As of https://github.com/prometheus/prometheus/pull/16565, CustomValues slices are treated as immutable,
	// so we replace the slice with a new slice rather than mutating the existing slice.
	h.CustomValues = slices.Repeat([]float64{12345678}, len(h.CustomValues))

	return h
}

// LimitingBucketedPool pools slices across multiple query evaluations, and applies any max in-memory bytes limit.
//
// LimitingBucketedPool only estimates the in-memory size of the slices it returns. For example, it ignores the overhead of slice headers,
// assumes all native histograms are the same size, and assumes all elements of a promql.Vector are float samples.
type LimitingBucketedPool[S ~[]E, E any] struct {
	inner       *pool.BucketedPool[S, E]
	source      limiter.MemoryConsumptionSource
	elementSize uint64
	clearOnGet  bool
	mangle      func(E) E
	onPutHook   func(S, *limiter.MemoryConsumptionTracker)
}

func NewLimitingBucketedPool[S ~[]E, E any](inner *pool.BucketedPool[S, E], source limiter.MemoryConsumptionSource, elementSize uint64, clearOnGet bool, mangle func(E) E, onPutHook func(S, *limiter.MemoryConsumptionTracker)) *LimitingBucketedPool[S, E] {
	return &LimitingBucketedPool[S, E]{
		inner:       inner,
		source:      source,
		elementSize: elementSize,
		clearOnGet:  clearOnGet,
		mangle:      mangle,
		onPutHook:   onPutHook,
	}
}

// Get returns a slice of E of length 0 and capacity greater than or equal to size.
//
// If the capacity of the returned slice would cause the max memory consumption limit to be exceeded, then an error is returned.
//
// Note that the capacity of the returned slice may be significantly larger than size, depending on the configuration of the underlying bucketed pool.
func (p *LimitingBucketedPool[S, E]) Get(size int, tracker *limiter.MemoryConsumptionTracker) (S, error) {
	// We don't bother checking the limit before we get the slice for a couple of reasons:
	// - we prefer to enforce the limit based on the capacity of the returned slices, not the requested size, to more accurately capture the true memory utilisation
	// - we expect that the vast majority of the time, the limit won't be hit, so the extra caution just slows things down
	// - we assume that allocating a single slice won't consume an enormous amount of memory and therefore risk this process OOMing.
	s := p.inner.Get(size)

	// We use the capacity of the slice, not 'size', for two reasons:
	// - it more accurately reflects the true memory utilisation, as BucketedPool will always round up to the next nearest bucket, to make reuse of slices easier
	// - there's no guarantee the slice will have size 'size' when it's returned to us in putWithElementSize, so using 'size' would make the accounting below impossible
	estimatedBytes := uint64(cap(s)) * p.elementSize

	if err := tracker.IncreaseMemoryConsumption(estimatedBytes, p.source); err != nil {
		p.inner.Put(s)
		return nil, err
	}

	if p.clearOnGet {
		clear(s[:size])
	}

	return s, nil
}

// Put returns a slice of E to the pool and updates the current memory consumption.
func (p *LimitingBucketedPool[S, E]) Put(s *S, tracker *limiter.MemoryConsumptionTracker) {
	if s == nil {
		return
	}

	if EnableManglingReturnedSlices && p.mangle != nil {
		for i, e := range *s {
			(*s)[i] = p.mangle(e)
		}
	}

	tracker.DecreaseMemoryConsumption(uint64(cap(*s))*p.elementSize, p.source)
	if p.onPutHook != nil {
		p.onPutHook(*s, tracker)
	}
	p.inner.Put(*s)

	*s = nil
}

// AppendToSlice appends items to a slice retrieved from the pool and tracks any slice capacity growth.
// If the capacity is insufficient, it gets a new slice from the pool and returns the old one.
// On error, the input slice s is returned to the pool and should not continue to be used.
func (p *LimitingBucketedPool[S, E]) AppendToSlice(s S, tracker *limiter.MemoryConsumptionTracker, items ...E) (S, error) {
	requiredLen := len(s) + len(items)

	if cap(s) >= requiredLen {
		return append(s, items...), nil
	}

	newSlice, err := p.Get(requiredLen, tracker)
	if err != nil {
		p.Put(&s, tracker)
		return nil, err
	}

	newSlice = newSlice[:len(s)]
	copy(newSlice, s)

	// The elements have been copied to the new slice but are still present in the old slice and could be inadvertently
	// mutated (e.g. places that use slices of FloatHistogram instances reuse those instances if they're in the slice).
	// Therefore the old slice needs to be cleared.
	clear(s)
	p.Put(&s, tracker)

	return append(newSlice, items...), nil
}

// PutInstantVectorSeriesData is equivalent to calling FPointSlicePool.Put(d.Floats) and HPointSlicePool.Put(d.Histograms).
func PutInstantVectorSeriesData(d InstantVectorSeriesData, tracker *limiter.MemoryConsumptionTracker) {
	FPointSlicePool.Put(&d.Floats, tracker)
	HPointSlicePool.Put(&d.Histograms, tracker)
}
