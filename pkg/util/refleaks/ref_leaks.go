// SPDX-License-Identifier: AGPL-3.0-only

// Package refleaks provides a way to detect references to objects that should
// not have any references anymore.
package refleaks

import (
	"flag"
	"fmt"
	"math"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"
)

type InstrumentConfig struct {
	Percentage                   float64       `yaml:"percentage" category:"experimental"`
	BeforeReusePeriod            time.Duration `yaml:"before_reuse_period" category:"experimental"`
	MaxInflightInstrumentedBytes uint64        `yaml:"max_inflight_instrumented_bytes" category:"experimental"`
}

func (c *InstrumentConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Float64Var(&c.Percentage, prefix+"percentage", 0, `Percentage [0-100] of request or message buffers to instrument for reference leaks. Set to 0 to disable.`)
	f.DurationVar(&c.BeforeReusePeriod, prefix+"before-reuse-period", 2*time.Minute, `Period after a buffer instrumented for referenced leaks is nominally freed until the buffer is uninstrumented and effectively freed to be reused. After this period, any lingering references to the buffer may potentially be dereferenced again with no detection.`)
	f.Uint64Var(&c.MaxInflightInstrumentedBytes, prefix+"max-inflight-instrumented-bytes", 0, `Maximum sum of length of buffers instrumented at any given time, in bytes. When surpassed, incoming buffers will not be instrumented, regardless of the configured percentage. Zero means no limit.`)
}

func (c InstrumentConfig) Validate() error {
	if c.Percentage < 0 || c.Percentage > 100 {
		return fmt.Errorf("percentage must be in [0-100], got: %v", c.Percentage)
	}
	if c.BeforeReusePeriod < 0 {
		return fmt.Errorf("before-reuse-period must be positive, got: %v", c.BeforeReusePeriod)
	}
	return nil
}

func (c InstrumentConfig) Tracker(reg prometheus.Registerer) *Tracker {
	if c.Percentage <= 0 {
		return nil
	}

	var t Tracker
	t.instrumentOneIn = uint64(math.Trunc(100 / c.Percentage))
	t.waitBeforeReuse = c.BeforeReusePeriod
	t.maxInflightInstrumentedBytes = c.MaxInflightInstrumentedBytes
	if t.maxInflightInstrumentedBytes == 0 {
		t.maxInflightInstrumentedBytes = math.MaxUint64
	}

	t.InflightInstrumentedBytesMetric = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "mimir_reference_leaks_inflight_instrumented_bytes",
		Help: "Total bytes of buffers instrumented for reference leak detection.",
	})
	t.InstrumentedBuffersTotalMetric = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "mimir_reference_leaks_instrumented_buffers_total",
		Help: "Total number of buffers instrumented for reference leak detection.",
	})

	return &t
}

type Tracker struct {
	instrumentOneIn              uint64
	waitBeforeReuse              time.Duration
	maxInflightInstrumentedBytes uint64

	unmarshaledWithBufferRefCount atomic.Uint64
	inflightInstrumentedBytes     atomic.Uint64

	InflightInstrumentedBytesMetric prometheus.Gauge
	InstrumentedBuffersTotalMetric  prometheus.Counter

	unmapQueue chan unmapTask
	freeing    atomic.Bool
}

// MaybeInstrument may allocate and instrument a slice of T. If it does, it will
// return the slice and a mem.Buffer wrapping the underlying raw memory.
//
// When the mem.Buffer's reference count reaches zero, and enough time (configurable
// with [InstrumentConfig.BeforeReusePeriod]) has passed, the underlying raw
// memory will be unaccessible, and trying to access it will cause a segfault.
func MaybeInstrument[T any](t *Tracker, len, cap int) (_ []T, _ mem.Buffer, instrumented bool) {
	should := t != nil && cap > 0 && t.instrumentOneIn > 0 && t.unmarshaledWithBufferRefCount.Add(1)%t.instrumentOneIn == 0
	if !should {
		return nil, nil, false
	}

	var zero T
	typeSize := int(unsafe.Sizeof(zero))
	bytesCap := cap * typeSize
	pageAlignedCap := roundUpToMultiple(bytesCap, pageSize)

	inflight := t.inflightInstrumentedBytes.Add(uint64(pageAlignedCap))
	if inflight > t.maxInflightInstrumentedBytes {
		t.inflightInstrumentedBytes.Sub(uint64(pageAlignedCap))
		return nil, nil, false
	}

	t.InstrumentedBuffersTotalMetric.Inc()
	t.InflightInstrumentedBytesMetric.Set(float64(inflight))

	// Allocate separate pages for this buffer. We'll detect ref leaks by
	// munmaping the pages on Free, after which trying to access them will
	// segfault.
	b, err := syscall.Mmap(-1, 0, pageAlignedCap, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		panic(fmt.Errorf("mmap: %w", err))
	}

	// Restrict the raw bytes slice to the size we actually need, without the
	// page-aligning padding. Otherwise, calling instrumentedBuf.ReaOnlyData()
	// will return uninitialized memory.
	b = b[:bytesCap:bytesCap]

	// []byte (bytesCap, bytesCap) -> []T (len, cap)
	bp := unsafe.Pointer(unsafe.SliceData(b))
	sp := (*T)(bp)
	s := unsafe.Slice(sp, cap)
	s = s[:len]

	buf := mem.NewBuffer(&b, nil)
	instrumentedBuf := &instrumentLeaksBuf{
		Buffer:          buf,
		waitBeforeReuse: t.waitBeforeReuse,
		tracker:         t,
	}
	instrumentedBuf.refCount.Inc()

	return s, instrumentedBuf, true
}

var pageSize = syscall.Getpagesize()

type instrumentLeaksBuf struct {
	mem.Buffer
	refCount        atomic.Int64
	waitBeforeReuse time.Duration
	tracker         *Tracker
}

func (b *instrumentLeaksBuf) Ref() {
	b.Buffer.Ref()
	b.refCount.Inc()
}

func (b *instrumentLeaksBuf) Free() {
	b.Buffer.Free()

	refCount := b.refCount.Dec()
	switch {
	case refCount == 0:
		buf := b.ReadOnlyData()
		ptr := unsafe.SliceData(buf)
		allPages := unsafe.Slice(ptr, roundUpToMultiple(len(buf), pageSize))
		if b.waitBeforeReuse > 0 {
			err := syscall.Mprotect(allPages, syscall.PROT_NONE)
			if err != nil {
				panic(fmt.Errorf("mprotect: %w", err))
			}
			select {
			case b.tracker.unmapQueue <- unmapTask{buf: allPages, at: time.Now().Add(b.waitBeforeReuse), tracker: b.tracker}:
				b.tracker.maybeStartFreeingInstrumentedBuffers()
				return
			default:
				// Queue is full, munmap right away.
			}
		}
		b.tracker.unmap(allPages)
	case refCount < 0:
		panic("instrumentLeaksBuf reference count below zero")
	}
}

type unmapTask struct {
	buf     []byte
	at      time.Time
	tracker *Tracker
}

var unmapQueue chan unmapTask

func (t *Tracker) maybeStartFreeingInstrumentedBuffers() {
	if t.freeing.CompareAndSwap(false, true) {
		go func() {
			for {
				select {
				case t := <-unmapQueue:
					time.Sleep(time.Until(t.at))
					t.tracker.unmap(t.buf)
				case <-time.After(t.waitBeforeReuse):
					t.freeing.Store(false)
					return
				}
			}
		}()
	}
}

func (t *Tracker) unmap(buf []byte) {
	newInflight := t.inflightInstrumentedBytes.Sub(uint64(len(buf)))
	t.InflightInstrumentedBytesMetric.Set(float64(newInflight))

	err := syscall.Munmap(buf)
	if err != nil {
		panic(fmt.Errorf("munmap: %w", err))
	}
}

func roundUpToMultiple(n, of int) int {
	return ((n + of - 1) / of) * of
}

var GlobalTracker *Tracker

func init() {
	var config InstrumentConfig
	var reg prometheus.Registerer
	if testing.Testing() {
		// Instrument all buffers when testing.
		config.Percentage = 100
		config.BeforeReusePeriod = 0
		config.MaxInflightInstrumentedBytes = 0
		reg = prometheus.NewRegistry()
	}
	GlobalTracker = config.Tracker(reg)
}
