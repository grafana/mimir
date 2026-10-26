// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	spanExporterOnce sync.Once
	spanExporter     *tracetest.InMemoryExporter
)

// testSpanExporter returns the exporter of the one tracer provider the
// package's tests install, emptied. The package's tracer, from otel.Tracer,
// delegates to the first provider set in the process and keeps it, so tests
// share one instead of each setting and restoring their own. Tests that use
// it must not run in parallel.
func testSpanExporter(*testing.T) *tracetest.InMemoryExporter {
	spanExporterOnce.Do(func() {
		spanExporter = tracetest.NewInMemoryExporter()
		otel.SetTracerProvider(tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(tracesdk.NewSimpleSpanProcessor(spanExporter))))
	})
	spanExporter.Reset()
	return spanExporter
}
