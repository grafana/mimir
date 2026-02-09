// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

type contextKey int

const (
	memoryConsumptionTracker contextKey = 0
)

var errNoMemoryConsumptionTrackerInContext = errors.New("no memory consumption tracker in context")

// MemoryConsumptionTrackerFromContext returns a MemoryConsumptionTracker that has been added to this
// context. If there is no MemoryConsumptionTracker in this context, return errNoMemoryConsumptionTrackerInContext.
func MemoryConsumptionTrackerFromContext(ctx context.Context) (*MemoryConsumptionTracker, error) {
	tracker, ok := ctx.Value(memoryConsumptionTracker).(*MemoryConsumptionTracker)
	if !ok {
		return nil, errNoMemoryConsumptionTrackerInContext
	}

	return tracker, nil
}

// AddMemoryTrackerToContext adds a MemoryConsumptionTracker to this context. This is used to propagate
// per-query memory consumption tracking to parts of the read path that cannot be modified
// to accept extra parameters.
func AddMemoryTrackerToContext(ctx context.Context, tracker *MemoryConsumptionTracker) context.Context {
	return context.WithValue(ctx, interface{}(memoryConsumptionTracker), tracker)
}

// ContextWithNewUnlimitedMemoryConsumptionTracker creates an unlimited MemoryConsumptionTracker and add it to the context and return
// the context. This can be used in places where we don't want to limit memory consumption, although tracking will still happen.
func ContextWithNewUnlimitedMemoryConsumptionTracker(ctx context.Context) context.Context {
	memoryTracker := NewUnlimitedMemoryConsumptionTracker(ctx)
	return AddMemoryTrackerToContext(ctx, memoryTracker)
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
	Labels
	CounterResetHintSlices
	memoryConsumptionSourceCount = CounterResetHintSlices + 1
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
	case CounterResetHintSlices:
		return "[]CounterResetHint"
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
	case Labels:
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

// NewUnlimitedMemoryConsumptionTracker creates a new MemoryConsumptionTracker that track memory consumption but
// will not enforce memory limit.
func NewUnlimitedMemoryConsumptionTracker(ctx context.Context) *MemoryConsumptionTracker {
	return NewMemoryConsumptionTracker(ctx, 0, nil, "")
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

// IncreaseMemoryConsumptionForLabels attempts to increase the current memory consumption based on labels.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumptionForLabels(lbls labels.Labels) error {
	if err := l.IncreaseMemoryConsumption(uint64(lbls.ByteSize()), Labels); err != nil {
		return err
	}
	return nil
}

// DecreaseMemoryConsumptionForLabels decreases the current memory consumption based on labels.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumptionForLabels(lbls labels.Labels) {
	l.DecreaseMemoryConsumption(uint64(lbls.ByteSize()), Labels)
}

// ResetMemoryConsumption resets all memory consumption tracking to zero.
func (l *MemoryConsumptionTracker) ResetMemoryConsumption() {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	l.currentEstimatedMemoryConsumptionBytes = 0
	l.currentEstimatedMemoryConsumptionBySource = [memoryConsumptionSourceCount]uint64{}
}

func (l *MemoryConsumptionTracker) DescribeCurrentMemoryConsumption() string {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	b := &strings.Builder{}

	for source, value := range l.currentEstimatedMemoryConsumptionBySource {
		b.WriteString(MemoryConsumptionSource(source).String())
		b.WriteString(": ")
		b.WriteString(strconv.FormatUint(value, 10))
		b.WriteString(" B \n")
	}

	return b.String()
}
