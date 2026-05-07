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
	inflight sync.Map // map[*MemoryConsumptionTracker]struct{}

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

// NewUnlimintedInflightMemoryConsumptionTracker returns a new InflightMemoryConsumptionTracker. There should only be one instance of this per process.
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
// Ensure that you invoke Deregister(tracker) once the tracker is no longer required.
func (t *InflightMemoryConsumptionTracker) NewMemoryConsumptionTracker(ctx context.Context, maxEstimatedMemoryConsumptionBytes uint64, queryDescription string) *MemoryConsumptionTracker {
	if t.forceUnlimited {
		maxEstimatedMemoryConsumptionBytes = 0
	}
	tracker := NewMemoryConsumptionTracker(ctx, maxEstimatedMemoryConsumptionBytes, t.queriesRejectedDueToPeakMemoryConsumption, queryDescription)
	tracker.producer = t
	t.inflight.Store(tracker, struct{}{})
	return tracker
}

// Deregister will remove the tracker from being reported in the accumulated metrics.
func (t *InflightMemoryConsumptionTracker) Deregister(tracker *MemoryConsumptionTracker) {
	// It is intentional to return silently in we are given a non managed tracker.
	// There are still a range of test code paths which inject unlimited memory consumption trackers into the context which would cause this to fail.
	// It is not a problem if a non managed tracker is requested to be deregistered.
	if tracker.parent != nil || tracker.producer == nil {
		return
	}

	// This should never happen - as we expect there is only a single instance of a InflightMemoryConsumptionTracker per container.
	// This will catch misconfigurations which could occur in unit tests.
	if tracker.producer != t {
		panic("cannot deregister inflight memory consumption tracker - the given tracker was allocated by another InflightMemoryConsumptionTracker")
	}

	t.inflight.Delete(tracker)
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
	t.inflight.Range(func(key, _ any) bool {
		tracker := key.(*MemoryConsumptionTracker)
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

// IsTracking returns true if the given tracker is being actively tracked by this InflightMemoryConsumptionTracker.
// Note that this function is only used by unit tests and will only return true on managed trackers.
// Unmanaged and nested trackers will always return false.
func (t *InflightMemoryConsumptionTracker) IsTracking(tracker *MemoryConsumptionTracker) bool {
	if tracker.producer != t {
		return false
	}
	_, ok := t.inflight.Load(tracker)
	return ok
}

// MemoryConsumptionTracker tracks the current memory utilisation of a single query, and applies any max in-memory bytes limit.
//
// It also tracks the peak number of in-memory bytes for use in query statistics.
//
// Note that there are three types of trackers.
//
// Trackers allocated via NewMemoryConsumptionTracker() - these are unmanaged trackers and are not included in the InflightMemoryConsumptionTracker accumulated metrics.
// It is invalid to use these trackers with the InflightMemoryConsumptionTracker. Although it is noted that is safe to call Deregister() with a non managed tracker.
//
// Trackers allocated via InflightMemoryConsumptionTracker.NewMemoryConsumptionTracker() - these are managed trackers and their values are included in the InflightMemoryConsumptionTracker accumulated metrics.
// It is important that these trackers are deregistered with the InflightMemoryConsumptionTracker when their lifecycle is complete.
// These trackers will have a producer set.
//
// Trackers allocated via InflightMemoryConsumptionTracker.NewNestedMemoryConsumptionTracker() - these trackers wrap a managed tracker. Any memory changes are first passed through the parent tracker.
// These trackers do not need to be deregistered, but it is safe to call Deregister() with a nested tracker argument.
// These trackers will have a parent set, but do not themselves have a producer set.
type MemoryConsumptionTracker struct {
	maxEstimatedMemoryConsumptionBytes     uint64
	currentEstimatedMemoryConsumptionBytes uint64
	peakEstimatedMemoryConsumptionBytes    uint64

	currentEstimatedMemoryConsumptionBySource [memoryConsumptionSourceCount]uint64

	rejectionCount        prometheus.Counter
	haveRecordedRejection bool
	queryDescription      string

	traceID string // Used to include in panic messages. Empty if the query didn't carry a tracing ID.

	// mtx protects all mutable state of the memory consumption tracker. We use a mutex
	// rather than atomics because we only want to adjust the memory used after checking
	// that it would not exceed the limit.
	mtx sync.Mutex

	// producer is InflightMemoryConsumptionTracker which created this tracker
	producer *InflightMemoryConsumptionTracker

	parent *MemoryConsumptionTracker
}

// NewUnlimitedMemoryConsumptionTracker creates a new MemoryConsumptionTracker that track memory consumption but
// will not enforce memory limit.
func NewUnlimitedMemoryConsumptionTracker(ctx context.Context) *MemoryConsumptionTracker {
	return NewMemoryConsumptionTracker(ctx, 0, nil, "")
}

func NewMemoryConsumptionTracker(ctx context.Context, maxEstimatedMemoryConsumptionBytes uint64, rejectionCount prometheus.Counter, queryDescription string) *MemoryConsumptionTracker {
	traceID, _ := tracing.ExtractTraceID(ctx)
	return &MemoryConsumptionTracker{
		maxEstimatedMemoryConsumptionBytes: maxEstimatedMemoryConsumptionBytes,

		rejectionCount:   rejectionCount,
		queryDescription: queryDescription,

		traceID: traceID,
	}
}

// IncreaseMemoryConsumption attempts to increase the current memory consumption by b bytes.
//
// It returns an error if the query would exceed the maximum memory consumption limit.
func (l *MemoryConsumptionTracker) IncreaseMemoryConsumption(b uint64, source MemoryConsumptionSource) error {
	if l.parent != nil {
		if err := l.parent.IncreaseMemoryConsumption(b, source); err != nil {
			return err
		}
	}

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
	if l.parent != nil {
		l.parent.DecreaseMemoryConsumption(b, source)
	}

	l.mtx.Lock()
	defer l.mtx.Unlock()

	if b > l.currentEstimatedMemoryConsumptionBySource[source] {
		traceDescription := ""
		if l.traceID != "" {
			traceDescription = fmt.Sprintf(" (trace ID: %v)", l.traceID)
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

// NewNestedMemoryConsumptionTracker returns a MemoryConsumptionTracker (nested) which is backed by this MemoryConsumptionTracker (parent).
// Any increment or decrement in memory is first passed through the parent before the nested tracker is updated.
// Any functions for requesting the nested tracker values - such as PeakEstimatedMemoryConsumptionBytes() - returns only the nested tracker's values.
//
// Note that the accumulated metrics reported by the InflightMemoryConsumptionTracker will not include the nested MemoryConsumptionTracker. Only
// the metrics on the parent trackers are included in the InflightMemoryConsumptionTracker accumulations.
func (l *MemoryConsumptionTracker) NewNestedMemoryConsumptionTracker(ctx context.Context, queryDescription string) *MemoryConsumptionTracker {
	if l.producer == nil {
		panic("cannot nest a tracker not created via a InflightMemoryConsumptionTracker")
	}
	tracker := NewMemoryConsumptionTracker(ctx, l.maxEstimatedMemoryConsumptionBytes, l.producer.queriesRejectedDueToPeakMemoryConsumption, queryDescription)
	tracker.parent = l
	return tracker
}
