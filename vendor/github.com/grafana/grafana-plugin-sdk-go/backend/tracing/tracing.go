package tracing

import (
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Opts contains settings for the trace provider and tracer setup that can be configured by the plugin developer.
type Opts struct {
	// CustomAttributes contains custom key value attributes used for the default OpenTelemetry trace provider.
	CustomAttributes []attribute.KeyValue
}

// defaultTracerName is the name for the default tracer that is set up if InitDefaultTracer is never called.
const defaultTracerName = "github.com/grafana/grafana-plugin-sdk-go"

var (
	defaultTracer         trace.Tracer
	defaultTracerInitOnce sync.Once
)

// DefaultTracer returns the default tracer that has been set with InitDefaultTracer.
// If InitDefaultTracer has never been called, the returned default tracer is an OTEL tracer
// with its name set to a generic name (`defaultTracerName`)
func DefaultTracer() trace.Tracer {
	defaultTracerInitOnce.Do(func() {
		// Use a non-nil default tracer if it's not set, for the first call.
		if defaultTracer == nil {
			defaultTracer = &contextualTracer{tracer: otel.Tracer(defaultTracerName)}
		}
	})
	return defaultTracer
}

// InitDefaultTracer sets the default tracer to the specified value.
// This method should only be called once during the plugin's initialization, and it's not safe for concurrent use.
func InitDefaultTracer(tracer trace.Tracer) {
	defaultTracer = &contextualTracer{tracer: tracer}
}
