// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"weak"

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
	SeriesGroupPairSlices
	BucketGroupPointerSlices
	GroupPointerSlices
	AggregationGroup
	BufferedQuerierResponses
	memoryConsumptionSourceCount = BufferedQuerierResponses + 1
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
	case SeriesGroupPairSlices:
		return "[]functions.seriesGroupPair"
	case BucketGroupPointerSlices:
		return "[]*functions.bucketGroup"
	case GroupPointerSlices:
		return "[]*aggregations.group"
	case AggregationGroup:
		return "aggregation.AggregationGroup"
	case BufferedQuerierResponses:
		return "buffered querier responses"
	default:
		return unknownMemorySource
	}
}

type inflightCleanupArg struct {
	tracker *MemoryConsumptionTrackerTracker
	id      uint64
}

// MemoryConsumptionTrackerTracker exposes metrics related to the cumulative in-flight MemoryConsumptionTrackers.
type MemoryConsumptionTrackerTracker struct {
	inflight                               sync.Map // map[uint64]weak.Pointer[MemoryConsumptionTracker]
	nextID                                 atomic.Uint64
	maxEstimatedMemoryConsumptionBytes     prometheus.Gauge
	currentEstimatedMemoryConsumptionBytes prometheus.Gauge
	peakEstimatedMemoryConsumptionBytes    prometheus.Gauge
	sampled                                prometheus.Gauge
}

func NewMemoryConsumptionTrackerTracker(reg prometheus.Registerer) *MemoryConsumptionTrackerTracker {
	t := &MemoryConsumptionTrackerTracker{
		maxEstimatedMemoryConsumptionBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_querier_inflight_query_max_estimated_memory_consumption_bytes",
			Help: "Total of the max estimated memory consumption limit across all in-flight queries.",
		}),
		currentEstimatedMemoryConsumptionBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_querier_inflight_query_current_estimated_memory_consumption_bytes",
			Help: "Total current estimated memory consumption across all in-flight queries.",
		}),
		peakEstimatedMemoryConsumptionBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes",
			Help: "Total peak estimated memory consumption across all in-flight queries.",
		}),
		sampled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_querier_inflight_query_sampled_total",
			Help: "Number of in-flight queries sampled during the last metrics collection.",
		}),
	}
	reg.MustRegister(t)
	return t
}

// NewMemoryConsumptionTracker returns a new MemoryConsumptionTracker the same as if limiter.MemoryConsumptionTracker() was called. However this new tracker will be included in the accumulated metrics managed by this MemoryConsumptionTrackerTracker.
func (t *MemoryConsumptionTrackerTracker) NewMemoryConsumptionTracker(ctx context.Context, maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter, queryDescription string) *MemoryConsumptionTracker {
	tracker := NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionBytes, rejectionCount, queryDescription)
	t.track(tracker)
	return tracker
}

// track will include this tracker in the accumulated metrics exposed by this MemoryConsumptionTrackerTracker.
// Note that the tracker is stored as a weak reference and in a sync.Map is used to provides lock-free concurrent access.
func (t *MemoryConsumptionTrackerTracker) track(tracker *MemoryConsumptionTracker) {
	id := t.nextID.Add(1)
	t.inflight.Store(id, weak.Make(tracker))
	runtime.AddCleanup(tracker, func(arg inflightCleanupArg) {
		arg.tracker.inflight.Delete(arg.id)
	}, inflightCleanupArg{tracker: t, id: id})
}

// Describe implements prometheus.Collector.
func (t *MemoryConsumptionTrackerTracker) Describe(ch chan<- *prometheus.Desc) {
	t.maxEstimatedMemoryConsumptionBytes.Describe(ch)
	t.currentEstimatedMemoryConsumptionBytes.Describe(ch)
	t.peakEstimatedMemoryConsumptionBytes.Describe(ch)
	t.sampled.Describe(ch)
}

// Collect implements prometheus.Collector. It aggregates memory consumption across all in-flight
// queries and emits the gauge values. Dead weak pointers are removed opportunistically.
func (t *MemoryConsumptionTrackerTracker) Collect(ch chan<- prometheus.Metric) {
	var maxBytes, currentBytes, peakBytes float64
	sampled := 0
	t.inflight.Range(func(key, value any) bool {
		tracker := value.(weak.Pointer[MemoryConsumptionTracker]).Value()
		if tracker == nil {
			t.inflight.Delete(key)
			return true
		}
		maxBytes += float64(tracker.maxEstimatedMemoryConsumptionBytes)
		currentBytes += float64(tracker.CurrentEstimatedMemoryConsumptionBytes())
		peakBytes += float64(tracker.PeakEstimatedMemoryConsumptionBytes())
		sampled++
		return true
	})

	t.maxEstimatedMemoryConsumptionBytes.Set(maxBytes)
	t.currentEstimatedMemoryConsumptionBytes.Set(currentBytes)
	t.peakEstimatedMemoryConsumptionBytes.Set(peakBytes)
	t.sampled.Set(float64(sampled))

	t.maxEstimatedMemoryConsumptionBytes.Collect(ch)
	t.currentEstimatedMemoryConsumptionBytes.Collect(ch)
	t.peakEstimatedMemoryConsumptionBytes.Collect(ch)
	t.sampled.Collect(ch)
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

		panic(fmt.Sprintf(
			"Estimated memory consumption of all instances of %s in this query is %d bytes when trying to return %d bytes. This indicates something has been returned to a pool more than once, which is a bug. The affected query is: %v%v",
			source,
			l.currentEstimatedMemoryConsumptionBySource[source],
			b,
			l.queryDescription,
			traceDescription,
		))
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

// CurrentEstimatedMemoryConsumptionBytesBySource returns the current memory consumption for a source in bytes.
func (l *MemoryConsumptionTracker) CurrentEstimatedMemoryConsumptionBytesBySource(s MemoryConsumptionSource) uint64 {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.currentEstimatedMemoryConsumptionBySource[s]
}

// IncreaseMemoryConsumptionForLabels attempts to increase the current memory consumption based on labels.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumptionForLabels(lbls labels.Labels) error {
	return l.IncreaseMemoryConsumption(lbls.ByteSize(), Labels)
}

// DecreaseMemoryConsumptionForLabels decreases the current memory consumption based on labels.
func (l *MemoryConsumptionTracker) DecreaseMemoryConsumptionForLabels(lbls labels.Labels) {
	l.DecreaseMemoryConsumption(lbls.ByteSize(), Labels)
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
