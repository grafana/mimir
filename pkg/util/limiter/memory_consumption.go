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
	"go.uber.org/atomic"
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
	SplitMiddlewareCachedResponses
	memoryConsumptionSourceCount = SplitMiddlewareCachedResponses + 1
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
	case SplitMiddlewareCachedResponses:
		return "split middleware cached responses"
	default:
		return unknownMemorySource
	}
}

// InflightMemoryConsumptionTracker exposes metrics related to the cumulative in-flight MemoryConsumptionTrackers.
type InflightMemoryConsumptionTracker struct {
	inflight sync.Map // map[uint64]*MemoryConsumptionTracker
	nextID   atomic.Uint64

	maxDesc     *prometheus.Desc
	currentDesc *prometheus.Desc
	peakDesc    *prometheus.Desc
	sampledDesc *prometheus.Desc

	// This is an optional counter which is passed to each MemoryConsumptionTracker instance.
	// This metric is not registered by this tracker. MemoryConsumptionTrackers may increment this.
	queriesRejectedDueToPeakMemoryConsumption prometheus.Counter

	// When true, all produced MemoryConsumptionTrackers will have unlimited allowed bytes
	forceUnlimited bool
}

// NewUnlimintedInflightMemoryConsumptionTracker returns a new InflightMemoryConsumptionTracker. There should only be one instance of this per container.
// All MemoryConsumptionTrackers returned from this instance will be unlimited memory consumption trackers.
func NewUnlimintedInflightMemoryConsumptionTracker(reg prometheus.Registerer) *InflightMemoryConsumptionTracker {
	t := NewInflightMemoryConsumptionTracker(reg, nil)
	t.forceUnlimited = true
	return t
}

// NewInflightMemoryConsumptionTracker returns a new InflightMemoryConsumptionTracker. There should only be one instance of this per container.
// The InflightMemoryConsumptionTracker provides metrics related to the cumulative in-flight MemoryConsumptionTracker statistics.
// It is also a factory for producing MemoryConsumptionTracker instances.
//
// Note that both reg and queriesRejectedDueToPeakMemoryConsumption params are optional, but are expected when used for production code paths.
// reg is required to register our Prometheus metrics
// queriesRejectedDueToPeakMemoryConsumption is shared with the query engine and will be incremented when queries are canceled due to the memory tracker being exceeded.
func NewInflightMemoryConsumptionTracker(reg prometheus.Registerer, queriesRejectedDueToPeakMemoryConsumption prometheus.Counter) *InflightMemoryConsumptionTracker {
	t := &InflightMemoryConsumptionTracker{
		maxDesc: prometheus.NewDesc(
			"cortex_querier_inflight_query_max_estimated_memory_consumption_limit_bytes",
			"Total of the max estimated memory consumption limit across all in-flight queries.",
			nil, nil,
		),
		currentDesc: prometheus.NewDesc(
			"cortex_querier_inflight_query_current_estimated_memory_consumption_bytes",
			"Total current estimated memory consumption across all in-flight queries.",
			nil, nil,
		),
		peakDesc: prometheus.NewDesc(
			"cortex_querier_inflight_query_peak_estimated_memory_consumption_bytes",
			"Total peak estimated memory consumption across all in-flight queries.",
			nil, nil,
		),
		sampledDesc: prometheus.NewDesc(
			"cortex_querier_inflight_query_sampled_count",
			"Number of in-flight memory consumption trackers accumulated during the last metrics collection.",
			nil, nil,
		),
		// Note that we do not register this counter. We just keep a reference to this counter so the memory consumption trackers can update it.
		queriesRejectedDueToPeakMemoryConsumption: queriesRejectedDueToPeakMemoryConsumption,
	}

	// reg may be nil when used in unit tests, and we do not wish to register these metrics
	if reg != nil {
		reg.MustRegister(t)
	}

	return t
}

// NewMemoryConsumptionTracker returns a new MemoryConsumptionTracker the same as if limiter.MemoryConsumptionTracker() was called.
// However this new tracker will be included in the accumulated metrics managed by this InflightMemoryConsumptionTracker.
// A new instantiation will set the internal reference count to 1. There is no need to call IncreaseReferenceCount() after construction.
// Ensure that you invoke DecrementReferenceCount(tracker) once the tracker is no longer required.
func (t *InflightMemoryConsumptionTracker) NewMemoryConsumptionTracker(ctx context.Context, maxEstimatedMemoryConsumptionBytes uint64, queryDescription string) *MemoryConsumptionTracker {
	if t.forceUnlimited {
		maxEstimatedMemoryConsumptionBytes = 0
	}
	tracker := NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionBytes, t.queriesRejectedDueToPeakMemoryConsumption, queryDescription)
	id := t.nextID.Add(1)
	tracker.trackingId = id
	t.inflight.Store(id, tracker)
	tracker.refCount.Store(1)
	return tracker
}

func (t *InflightMemoryConsumptionTracker) IncreaseReferenceCount(tracker *MemoryConsumptionTracker) {
	tracker.refCount.Inc()
}

// DecrementReferenceCount removes the tracking of this tracker.
func (t *InflightMemoryConsumptionTracker) DecrementReferenceCount(tracker *MemoryConsumptionTracker) {
	if tracker.trackingId == 0 {
		panic("cannot decrement a reference count on a tracker not created via the InflightMemoryConsumptionTracker")
	}
	if tracker.refCount.Dec() < 1 {
		t.inflight.Delete(tracker.trackingId)
	}
}

// IsRegistered returns true if the given tracker is currently registered with this InflightMemoryConsumptionTracker
// Only a registered tracker can have its reference decreased via DecrementReferenceCount()
func (t *InflightMemoryConsumptionTracker) IsRegistered(tracker *MemoryConsumptionTracker) bool {
	if tracker.trackingId == 0 {
		return false
	}
	_, ok := t.inflight.Load(tracker.trackingId)
	return ok
}

// Describe implements prometheus.Collector.
func (t *InflightMemoryConsumptionTracker) Describe(ch chan<- *prometheus.Desc) {
	ch <- t.maxDesc
	ch <- t.currentDesc
	ch <- t.peakDesc
	ch <- t.sampledDesc
}

// Collect implements prometheus.Collector. It aggregates memory consumption across all in-flight
// queries and emits the gauge values.
func (t *InflightMemoryConsumptionTracker) Collect(ch chan<- prometheus.Metric) {
	var maxBytes, currentBytes, peakBytes float64
	sampled := 0
	t.inflight.Range(func(key, value any) bool {
		tracker := value.(*MemoryConsumptionTracker)
		maxBytes += float64(tracker.maxEstimatedMemoryConsumptionBytes)
		currentBytes += float64(tracker.CurrentEstimatedMemoryConsumptionBytes())
		peakBytes += float64(tracker.PeakEstimatedMemoryConsumptionBytes())
		sampled++
		return true
	})

	ch <- prometheus.MustNewConstMetric(t.maxDesc, prometheus.GaugeValue, maxBytes)
	ch <- prometheus.MustNewConstMetric(t.currentDesc, prometheus.GaugeValue, currentBytes)
	ch <- prometheus.MustNewConstMetric(t.peakDesc, prometheus.GaugeValue, peakBytes)
	ch <- prometheus.MustNewConstMetric(t.sampledDesc, prometheus.GaugeValue, float64(sampled))
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

	trackingId uint64
	refCount   atomic.Int32
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
		refCount:         atomic.Int32{},
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
			// This may be nil in unit tests
			if l.rejectionCount != nil {
				l.rejectionCount.Inc()
			}
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
