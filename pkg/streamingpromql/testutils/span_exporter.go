// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// NewFixedInMemoryExporter creates a SpanExporter that keeps a fixed number of
// exported spans in memory. Useful for tests that assert that particular spans
// were emitted.
func NewFixedInMemoryExporter(cap int) *FixedInMemoryExporter {
	return &FixedInMemoryExporter{
		stubs: make([]tracetest.SpanStub, cap),
		idx:   0,
	}
}

type FixedInMemoryExporter struct {
	mtx   sync.Mutex
	stubs []tracetest.SpanStub
	idx   int
}

func (e *FixedInMemoryExporter) ExportSpans(_ context.Context, spans []trace.ReadOnlySpan) error {
	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)

	e.mtx.Lock()
	defer e.mtx.Unlock()

	l := cap(e.stubs)
	for _, s := range stubs {
		e.stubs[e.idx%l] = s
		e.idx++
	}

	return nil
}

func (e *FixedInMemoryExporter) Shutdown(context.Context) error {
	e.Reset()
	return nil
}

func (e *FixedInMemoryExporter) Reset() {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	e.idx = 0
}

func (e *FixedInMemoryExporter) GetSpans() tracetest.SpanStubs {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	var ret tracetest.SpanStubs
	l := cap(e.stubs)

	// We keep track of the number of items written. If we've written more
	// than the capacity of the underlying slice, return every element. If
	// we haven't written more than the capacity of the underlying slice,
	// return from index 0 (start) up to the index that we've written to.
	start := 0
	if e.idx > l {
		start = e.idx - l
	}

	for i := start; i < e.idx; i++ {
		ret = append(ret, e.stubs[i%l])
	}

	return ret
}
