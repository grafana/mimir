// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"fmt"
	"sync"
	"unsafe"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

var StringSize = uint64(unsafe.Sizeof(""))

type contextKey int

const (
	memoryConsumptionTracker contextKey = 0
)

// MemoryTrackerFromContextWithFallback returns a MemoryConsumptionTracker that has been added to this
// context. If there is no MemoryConsumptionTracker in this context, a new no-op tracker that
// does not enforce any limits is returned.
func MemoryTrackerFromContextWithFallback(ctx context.Context) *MemoryConsumptionTracker {
	tracker, ok := ctx.Value(memoryConsumptionTracker).(*MemoryConsumptionTracker)
	if !ok {
		return NewMemoryConsumptionTracker(ctx, 0, nil, "")
	}

	return tracker
}

// AddMemoryTrackerToContext adds a MemoryConsumptionTracker to this context. This is used to propagate
// per-query memory consumption tracking to parts of the read path that cannot be modified
// to accept extra parameters.
func AddMemoryTrackerToContext(ctx context.Context, tracker *MemoryConsumptionTracker) context.Context {
	return context.WithValue(ctx, interface{}(memoryConsumptionTracker), tracker)
}

type MemoryConsumptionSource int

const (
	IngesterChunks MemoryConsumptionSource = iota
	StoreGatewayChunks
	FPointSlices
	HPointSlices
	Vectors
	Float64Slices
	IntSlices
	IntSliceSlice
	Int64Slices
	BoolSlices
	HistogramPointerSlices
	SeriesMetadataSlices
	BucketSlices
	BucketsSlices
	QuantileGroupSlices
	TopKBottomKInstantQuerySeriesSlices
	TopKBottomKRangeQuerySeriesSlices
	SeriesMetadataLabels

	memoryConsumptionSourceCount = SeriesMetadataLabels + 1
)

const (
	unknownMemorySource = "unknown memory source"
)

func (s MemoryConsumptionSource) String() string {
	switch s {
	case IngesterChunks:
		return "ingester chunks"
	case StoreGatewayChunks:
		return "store-gateway chunks"
	case FPointSlices:
		return "[]promql.FPoint"
	case HPointSlices:
		return "[]promql.HPoint"
	case Vectors:
		return "promql.Vector"
	case Float64Slices:
		return "[]float64"
	case IntSlices:
		return "[]int"
	case IntSliceSlice:
		return "[][]int"
	case Int64Slices:
		return "[]int64"
	case BoolSlices:
		return "[]bool"
	case HistogramPointerSlices:
		return "[]*histogram.FloatHistogram"
	case SeriesMetadataSlices:
		return "[]SeriesMetadata"
	case BucketSlices:
		return "[]promql.Bucket"
	case BucketsSlices:
		return "[]promql.Buckets"
	case QuantileGroupSlices:
		return "[]aggregations.qGroup"
	case TopKBottomKInstantQuerySeriesSlices:
		return "[]topkbottom.instantQuerySeries"
	case TopKBottomKRangeQuerySeriesSlices:
		return "[]topkbottom.rangeQuerySeries"
	case SeriesMetadataLabels:
		return "labels.Labels"
	default:
		return unknownMemorySource
	}
}

// MemoryConsumptionTracker tracks the current memory utilisation of a single query, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
type MemoryConsumptionTracker struct {
	maxEstimatedMemoryConsumptionBytes     uint64
	currentEstimatedMemoryConsumptionBytes uint64
	peakEstimatedMemoryConsumptionBytes    uint64

	currentEstimatedMemoryConsumptionBySource [memoryConsumptionSourceCount]uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection bool
	queryDescription      string

	ctx context.Context // Used to retrieve trace ID to include in panic messages.

	// mtx protects all mutable state of the memory consumption tracker. We use a mutex
	// rather than atomics because we only want to adjust the memory used after checking
	// that it would not exceed the limit.
	mtx sync.Mutex
}

func NewMemoryConsumptionTracker(ctx context.Context, maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter, queryDescription string) *MemoryConsumptionTracker {
	return &MemoryConsumptionTracker{
		maxEstimatedMemoryConsumptionBytes: maxEstimatedMemoryConsumptionBytes,

		rejectionCount:   rejectionCount,
		queryDescription: queryDescription,
		ctx:              ctx,
	}
}

// IncreaseMemoryConsumption attempts to increase the current memory consumption by b bytes.
//
// It returns an error if the query would exceed the maximum memory consumption limit.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumption(b uint64, source MemoryConsumptionSource) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if l.maxEstimatedMemoryConsumptionBytes > 0 && l.currentEstimatedMemoryConsumptionBytes+b > l.maxEstimatedMemoryConsumptionBytes {
		if !l.haveRecordedRejection {
			l.haveRecordedRejection = true
			l.rejectionCount.Inc()
		}

		return NewMaxEstimatedMemoryConsumptionPerQueryLimitError(l.maxEstimatedMemoryConsumptionBytes)
	}

	l.currentEstimatedMemoryConsumptionBySource[source] += b
	l.currentEstimatedMemoryConsumptionBytes += b
	l.peakEstimatedMemoryConsumptionBytes = max(l.peakEstimatedMemoryConsumptionBytes, l.currentEstimatedMemoryConsumptionBytes)

	return nil
}

// DecreaseMemoryConsumption decreases the current memory consumption by b bytes.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumption(b uint64, source MemoryConsumptionSource) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if b > l.currentEstimatedMemoryConsumptionBySource[source] {
		traceID, ok := tracing.ExtractTraceID(l.ctx)
		traceDescription := ""

		if ok {
			traceDescription = fmt.Sprintf(" (trace ID: %v)", traceID)
		}

		panic(fmt.Sprintf("Estimated memory consumption of all instances of %s in this query is negative. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: %v%v", source, l.queryDescription, traceDescription))
	}

	l.currentEstimatedMemoryConsumptionBytes -= b
	l.currentEstimatedMemoryConsumptionBySource[source] -= b
}

// PeakEstimatedMemoryConsumptionBytes returns the peak memory consumption in bytes.
func (l *MemoryConsumptionTracker) PeakEstimatedMemoryConsumptionBytes() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.peakEstimatedMemoryConsumptionBytes
}

// CurrentEstimatedMemoryConsumptionBytes returns the current memory consumption in bytes.
func (l *MemoryConsumptionTracker) CurrentEstimatedMemoryConsumptionBytes() uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.currentEstimatedMemoryConsumptionBytes
}

func (l *MemoryConsumptionTracker) IncreaseMemoryConsumptionForLabels(lbs labels.Labels) error {
	for _, lb := range lbs {
		// TODO: Update labels.Labels to get size of bytes directly instead of calculating it here.
		if err := l.IncreaseMemoryConsumption(uint64(len(lb.Name)+len(lb.Value))*StringSize, SeriesMetadataLabels); err != nil {
			return err
		}
	}
	return nil
}

func (l *MemoryConsumptionTracker) DecreaseMemoryConsumptionForLabels(lbs labels.Labels) {
	for _, lb := range lbs {
		// TODO: Update labels.Labels to get size of bytes directly instead of calculating it here.
		l.DecreaseMemoryConsumption(uint64(len(lb.Name)+len(lb.Value))*StringSize, SeriesMetadataLabels)
	}
}
