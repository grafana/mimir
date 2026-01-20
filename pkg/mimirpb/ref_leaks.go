// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"flag"
	"fmt"
	"math"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"
)

type InstrumentRefLeaksConfig struct {
	Percentage                   float64       `yaml:"percentage" category:"experimental"`
	BeforeReusePeriod            time.Duration `yaml:"before_reuse_period" category:"experimental"`
	MaxInflightInstrumentedBytes uint64        `yaml:"max_inflight_instrumented_bytes" category:"experimental"`
}

func (c *InstrumentRefLeaksConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.Float64Var(&c.Percentage, prefix+"percentage", 0, `Percentage [0-100] of request or message buffers to instrument for reference leaks. Set to 0 to disable.`)
	f.DurationVar(&c.BeforeReusePeriod, prefix+"before-reuse-period", 2*time.Minute, `Period after a buffer instrumented for referenced leaks is nominally freed until the buffer is uninstrumented and effectively freed to be reused. After this period, any lingering references to the buffer may potentially be dereferenced again with no detection.`)
	f.Uint64Var(&c.MaxInflightInstrumentedBytes, prefix+"max-inflight-instrumented-bytes", 0, `Maximum sum of length of buffers instrumented at any given time, in bytes. When surpassed, incoming buffers will not be instrumented, regardless of the configured percentage. Zero means no limit.`)
}

func (c InstrumentRefLeaksConfig) Validate() error {
	if c.Percentage < 0 || c.Percentage > 100 {
		return fmt.Errorf("percentage must be in [0-100], got: %v", c.Percentage)
	}
	if c.BeforeReusePeriod < 0 {
		return fmt.Errorf("before-reuse-period must be positive, got: %v", c.BeforeReusePeriod)
	}
	return nil
}

func (c InstrumentRefLeaksConfig) tracker(reg prometheus.Registerer) refLeaksTracker {
	if c.Percentage <= 0 {
		return refLeaksTracker{}
	}

	var t refLeaksTracker
	t.instrumentOneIn = uint64(math.Trunc(100 / c.Percentage))
	t.waitBeforeReuse = c.BeforeReusePeriod
	t.maxInflightInstrumentedBytes = c.MaxInflightInstrumentedBytes
	if t.maxInflightInstrumentedBytes == 0 {
		t.maxInflightInstrumentedBytes = math.MaxUint64
	}

	t.inflightInstrumentedBytesMetric = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "mimir_reference_leaks_inflight_instrumented_bytes",
		Help: "Total bytes of buffers instrumented for reference leak detection.",
	})
	t.instrumentedBuffersTotalMetric = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "mimir_reference_leaks_instrumented_buffers_total",
		Help: "Total number of buffers instrumented for reference leak detection.",
	})

	return t
}

type refLeaksTracker struct {
	instrumentOneIn              uint64
	waitBeforeReuse              time.Duration
	maxInflightInstrumentedBytes uint64

	unmarshaledWithBufferRefCount atomic.Uint64
	inflightInstrumentedBytes     atomic.Uint64

	inflightInstrumentedBytesMetric prometheus.Gauge
	instrumentedBuffersTotalMetric  prometheus.Counter
}

func (t *refLeaksTracker) maybeInstrumentRefLeaks(data mem.BufferSlice) mem.Buffer {
	should := data.Len() > 0 && t.instrumentOneIn > 0 && t.unmarshaledWithBufferRefCount.Add(1)%t.instrumentOneIn == 0
	if !should {
		return nil
	}

	pageAlignedLen := roundUpToMultiple(data.Len(), pageSize)

	inflight := t.inflightInstrumentedBytes.Add(uint64(pageAlignedLen))
	if inflight > t.maxInflightInstrumentedBytes {
		t.inflightInstrumentedBytes.Sub(uint64(pageAlignedLen))
		return nil
	}

	t.instrumentedBuffersTotalMetric.Inc()
	t.inflightInstrumentedBytesMetric.Set(float64(inflight))

	// Allocate separate pages for this buffer. We'll detect ref leaks by
	// munmaping the pages on Free, after which trying to access them will
	// segfault.
	b, err := syscall.Mmap(-1, 0, pageAlignedLen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
	if err != nil {
		panic(fmt.Errorf("mmap: %w", err))
	}
	b = b[:data.Len()]
	data.CopyTo(b)
	buf := mem.NewBuffer(&b, nil)
	instrumentedBuf := &instrumentLeaksBuf{
		Buffer:          buf,
		waitBeforeReuse: t.waitBeforeReuse,
		tracker:         t,
	}
	instrumentedBuf.refCount.Inc()
	return instrumentedBuf
}

type instrumentLeaksBuf struct {
	mem.Buffer
	refCount        atomic.Int64
	waitBeforeReuse time.Duration
	tracker         *refLeaksTracker
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
			case unmapQueue <- unmapTask{buf: allPages, at: time.Now().Add(b.waitBeforeReuse), tracker: b.tracker}:
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
	tracker *refLeaksTracker
}

var unmapQueue chan unmapTask

func (c InstrumentRefLeaksConfig) maybeStartFreeingInstrumentedBuffers() {
	if c.Percentage > 0 && c.BeforeReusePeriod > 0 {
		startFreeingInstrumentedBuffers()
	}
}

var startFreeingInstrumentedBuffers = sync.OnceFunc(func() {
	unmapQueue = make(chan unmapTask, 1000)
	go func() {
		for t := range unmapQueue {
			time.Sleep(time.Until(t.at))
			t.tracker.unmap(t.buf)
		}
	}()
})

func (t *refLeaksTracker) unmap(buf []byte) {
	newInflight := t.inflightInstrumentedBytes.Sub(uint64(len(buf)))
	t.inflightInstrumentedBytesMetric.Set(float64(newInflight))

	err := syscall.Munmap(buf)
	if err != nil {
		panic(fmt.Errorf("munmap: %w", err))
	}
}

func roundUpToMultiple(n, of int) int {
	return ((n + of - 1) / of) * of
}
