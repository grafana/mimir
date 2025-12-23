// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"math"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"go.uber.org/atomic"
	"google.golang.org/grpc/mem"
)

type InstrumentRefLeaksConfig struct {
	Percentage                   float64
	BeforeReusePeriod            time.Duration
	MaxInflightInstrumentedBytes uint64
}

func (cfg InstrumentRefLeaksConfig) tracker() refLeaksTracker {
	if cfg.Percentage <= 0 {
		return refLeaksTracker{}
	}

	var t refLeaksTracker
	t.instrumentOneIn = uint64(math.Trunc(100 / cfg.Percentage))
	t.waitBeforeReuse = cfg.BeforeReusePeriod
	t.maxInflightInstrumentedBytes = cfg.MaxInflightInstrumentedBytes
	if t.maxInflightInstrumentedBytes == 0 {
		t.maxInflightInstrumentedBytes = math.MaxUint64
	}
	return t
}

type refLeaksTracker struct {
	instrumentOneIn              uint64
	waitBeforeReuse              time.Duration
	maxInflightInstrumentedBytes uint64

	unmarshaledWithBufferRefCount atomic.Uint64
	inflightInstrumentedBytes     atomic.Uint64
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
		Buffer:                    buf,
		waitBeforeReuse:           t.waitBeforeReuse,
		inflightInstrumentedBytes: &t.inflightInstrumentedBytes,
	}
	instrumentedBuf.refCount.Inc()
	return instrumentedBuf
}

type instrumentLeaksBuf struct {
	mem.Buffer
	refCount                  atomic.Int64
	waitBeforeReuse           time.Duration
	inflightInstrumentedBytes *atomic.Uint64
}

func (b *instrumentLeaksBuf) Ref() {
	b.Buffer.Ref()
	b.refCount.Inc()
}

func (b *instrumentLeaksBuf) Free() {
	b.Buffer.Free()

	if b.refCount.Dec() == 0 {
		buf := b.ReadOnlyData()
		ptr := unsafe.SliceData(buf)
		allPages := unsafe.Slice(ptr, roundUpToMultiple(len(buf), pageSize))
		if b.waitBeforeReuse > 0 {
			err := syscall.Mprotect(allPages, syscall.PROT_NONE)
			if err != nil {
				panic(fmt.Errorf("mprotect: %w", err))
			}
			select {
			case unmapQueue <- unmapTask{buf: allPages, at: time.Now().Add(b.waitBeforeReuse), inflightInstrumentedBytes: b.inflightInstrumentedBytes}:
				return
			default:
				// Queue is full, munmap right away.
			}
		}
		unmap(allPages, b.inflightInstrumentedBytes)
	}
}

type unmapTask struct {
	buf                       []byte
	at                        time.Time
	inflightInstrumentedBytes *atomic.Uint64
}

var unmapQueue chan unmapTask

func (cfg InstrumentRefLeaksConfig) maybeStartFreeingInstrumentedBuffers() {
	if cfg.Percentage > 0 && cfg.BeforeReusePeriod > 0 {
		startFreeingInstrumentedBuffers()
	}
}

var startFreeingInstrumentedBuffers = sync.OnceFunc(func() {
	unmapQueue = make(chan unmapTask, 1000)
	go func() {
		for t := range unmapQueue {
			time.Sleep(time.Until(t.at))
			unmap(t.buf, t.inflightInstrumentedBytes)
		}
	}()
})

func unmap(buf []byte, inflightInstrumentedBytes *atomic.Uint64) {
	inflightInstrumentedBytes.Sub(uint64(len(buf)))
	err := syscall.Munmap(buf)
	if err != nil {
		panic(fmt.Errorf("munmap: %w", err))
	}
}

func roundUpToMultiple(n, of int) int {
	return ((n + of - 1) / of) * of
}
