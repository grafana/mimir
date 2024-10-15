// SPDX-License-Identifier: AGPL-3.0-only

package tracing

import (
	"context"
	"encoding/binary"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/embedded"
)

type OpenTelemetryProviderBridge struct {
	// See https://pkg.go.dev/go.opentelemetry.io/otel/trace#hdr-API_Implementations.
	embedded.TracerProvider

	tracer opentracing.Tracer
}

func NewOpenTelemetryProviderBridge(tracer opentracing.Tracer) *OpenTelemetryProviderBridge {
	return &OpenTelemetryProviderBridge{
		tracer: tracer,
	}
}

// BridgeOpenTracingToOtel wraps ctx so that Otel-using code can link to an OpenTracing span.
func BridgeOpenTracingToOtel(ctx context.Context) context.Context {
	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return ctx
	}
	tracerBridge, ok := otel.Tracer("").(*OpenTelemetryTracerBridge)
	if !ok {
		return ctx

	}
	ctx, _ = tracerBridge.openTracingToOtel(ctx, span)
	return ctx
}

// Tracer creates an implementation of the Tracer interface.
// The instrumentationName must be the name of the library providing
// instrumentation. This name may be the same as the instrumented code
// only if that code provides built-in instrumentation. If the
// instrumentationName is empty, then a implementation defined default
// name will be used instead.
//
// This method must be concurrency safe.
func (p *OpenTelemetryProviderBridge) Tracer(_ string, _ ...trace.TracerOption) trace.Tracer {
	return NewOpenTelemetryTracerBridge(p.tracer, p)
}

type OpenTelemetryTracerBridge struct {
	// See https://pkg.go.dev/go.opentelemetry.io/otel/trace#hdr-API_Implementations.
	embedded.Tracer

	tracer   opentracing.Tracer
	provider trace.TracerProvider
}

func NewOpenTelemetryTracerBridge(tracer opentracing.Tracer, provider trace.TracerProvider) *OpenTelemetryTracerBridge {
	return &OpenTelemetryTracerBridge{
		tracer:   tracer,
		provider: provider,
	}
}

// Start creates a span and a context.Context containing the newly-created span.
//
// If the context.Context provided in `ctx` contains a Span then the newly-created
// Span will be a child of that span, otherwise it will be a root span. This behavior
// can be overridden by providing `WithNewRoot()` as a SpanOption, causing the
// newly-created Span to be a root span even if `ctx` contains a Span.
//
// When creating a Span it is recommended to provide all known span attributes using
// the `WithAttributes()` SpanOption as samplers will only have access to the
// attributes provided when a Span is created.
//
// Any Span that is created MUST also be ended. This is the responsibility of the user.
// Implementations of this API may leak memory or other resources if Spans are not ended.
func (t *OpenTelemetryTracerBridge) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	var mappedOptions []opentracing.StartSpanOption

	// Map supported options.
	if len(opts) > 0 {
		mappedOptions = make([]opentracing.StartSpanOption, 0, len(opts))
		cfg := trace.NewSpanStartConfig(opts...)

		if !cfg.Timestamp().IsZero() {
			mappedOptions = append(mappedOptions, opentracing.StartTime(cfg.Timestamp()))
		}
		if len(cfg.Attributes()) > 0 {
			tags := make(map[string]any, len(cfg.Attributes()))

			for _, attr := range cfg.Attributes() {
				if !attr.Valid() {
					continue
				}

				tags[string(attr.Key)] = attr.Value.AsInterface()
			}

			mappedOptions = append(mappedOptions, opentracing.Tags(tags))
		}
	}

	// If the context contains a valid *remote* OTel span, and there isn't yet an OpenTelemetry span there,
	// we manually link this OTel span to the OT child we are about to start before.
	// This allows preserving a minimal parent-child relationship with OTel spans, which were extracted
	// via OTel propagators. Otherwise, the relation gets lost.
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() && sc.IsRemote() {
		if parentSpan := opentracing.SpanFromContext(ctx); parentSpan == nil {
			// Create a new Jaeger span context with minimum required options.
			jsc := jaeger.NewSpanContext(
				jaegerFromOTelTraceID(sc.TraceID()),
				jaegerFromOTelSpanID(sc.SpanID()),
				0,
				sc.IsSampled(),
				nil,
			)
			mappedOptions = append(mappedOptions, opentracing.ChildOf(jsc))
		}
	}

	span, ctx := opentracing.StartSpanFromContextWithTracer(ctx, t.tracer, spanName, mappedOptions...)
	return t.openTracingToOtel(ctx, span)
}

func (t *OpenTelemetryTracerBridge) openTracingToOtel(ctx context.Context, span opentracing.Span) (context.Context, trace.Span) {
	otelSpan := NewOpenTelemetrySpanBridge(span, t.provider)
	return trace.ContextWithSpan(ctx, otelSpan), otelSpan
}

type OpenTelemetrySpanBridge struct {
	// See https://pkg.go.dev/go.opentelemetry.io/otel/trace#hdr-API_Implementations.
	embedded.Span

	span     opentracing.Span
	provider trace.TracerProvider
}

func NewOpenTelemetrySpanBridge(span opentracing.Span, provider trace.TracerProvider) *OpenTelemetrySpanBridge {
	return &OpenTelemetrySpanBridge{
		span:     span,
		provider: provider,
	}
}

// End completes the Span. The Span is considered complete and ready to be
// delivered through the rest of the telemetry pipeline after this method
// is called. Therefore, updates to the Span are not allowed after this
// method has been called.
func (s *OpenTelemetrySpanBridge) End(options ...trace.SpanEndOption) {
	if len(options) == 0 {
		s.span.Finish()
		return
	}

	cfg := trace.NewSpanEndConfig(options...)
	s.span.FinishWithOptions(opentracing.FinishOptions{
		FinishTime: cfg.Timestamp(),
	})
}

// AddEvent adds an event with the provided name and options.
func (s *OpenTelemetrySpanBridge) AddEvent(name string, options ...trace.EventOption) {
	cfg := trace.NewEventConfig(options...)
	s.addEvent(name, cfg.Attributes())
}

func (s *OpenTelemetrySpanBridge) addEvent(name string, attributes []attribute.KeyValue) {
	s.logFieldWithAttributes(log.Event(name), attributes)
}

// AddLink implements trace.Span.
func (s *OpenTelemetrySpanBridge) AddLink(_ trace.Link) {
	// Ignored: OpenTracing only lets you link spans together at creation time.
}

// IsRecording returns the recording state of the Span. It will return
// true if the Span is active and events can be recorded.
func (s *OpenTelemetrySpanBridge) IsRecording() bool {
	return true
}

// RecordError will record err as an exception span event for this span. An
// additional call to SetStatus is required if the Status of the Span should
// be set to Error, as this method does not change the Span status. If this
// span is not being recorded or err is nil then this method does nothing.
func (s *OpenTelemetrySpanBridge) RecordError(err error, options ...trace.EventOption) {
	cfg := trace.NewEventConfig(options...)
	s.recordError(err, cfg.Attributes())
}

func (s *OpenTelemetrySpanBridge) recordError(err error, attributes []attribute.KeyValue) {
	s.logFieldWithAttributes(log.Error(err), attributes)
}

// SpanContext returns the SpanContext of the Span. The returned SpanContext
// is usable even after the End method has been called for the Span.
func (s *OpenTelemetrySpanBridge) SpanContext() trace.SpanContext {
	// We only support Jaeger span context.
	sctx, ok := s.span.Context().(jaeger.SpanContext)
	if !ok {
		return trace.SpanContext{}
	}

	var flags trace.TraceFlags
	flags = flags.WithSampled(sctx.IsSampled())

	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    jaegerToOTelTraceID(sctx.TraceID()),
		SpanID:     jaegerToOTelSpanID(sctx.SpanID()),
		TraceFlags: flags,

		// Unsupported because we can't read it from the Jaeger span context.
		Remote: false,
	})
}

// SetStatus sets the status of the Span in the form of a code and a
// description, overriding previous values set. The description is only
// included in a status when the code is for an error.
func (s *OpenTelemetrySpanBridge) SetStatus(code codes.Code, description string) {
	// We use a log instead of setting tags to have it more prominent in the tracing UI.
	s.span.LogFields(log.Uint32("code", uint32(code)), log.String("description", description))
}

// SetName sets the Span name.
func (s *OpenTelemetrySpanBridge) SetName(name string) {
	s.span.SetOperationName(name)
}

// SetAttributes sets kv as attributes of the Span. If a key from kv
// already exists for an attribute of the Span it will be overwritten with
// the value contained in kv.
func (s *OpenTelemetrySpanBridge) SetAttributes(kv ...attribute.KeyValue) {
	for _, attr := range kv {
		if !attr.Valid() {
			continue
		}

		s.span.SetTag(string(attr.Key), attr.Value.AsInterface())
	}
}

// TracerProvider returns a TracerProvider that can be used to generate
// additional Spans on the same telemetry pipeline as the current Span.
func (s *OpenTelemetrySpanBridge) TracerProvider() trace.TracerProvider {
	return s.provider
}

func (s *OpenTelemetrySpanBridge) logFieldWithAttributes(field log.Field, attributes []attribute.KeyValue) {
	if len(attributes) == 0 {
		s.span.LogFields(field)
		return
	}

	fields := make([]log.Field, 0, 1+len(attributes))
	fields = append(fields, field)

	for _, attr := range attributes {
		if attr.Valid() {
			fields = append(fields, log.Object(string(attr.Key), attr.Value.AsInterface()))
		}
	}

	s.span.LogFields(fields...)
}

func jaegerToOTelTraceID(input jaeger.TraceID) trace.TraceID {
	var traceID trace.TraceID
	binary.BigEndian.PutUint64(traceID[0:8], input.High)
	binary.BigEndian.PutUint64(traceID[8:16], input.Low)
	return traceID
}

func jaegerToOTelSpanID(input jaeger.SpanID) trace.SpanID {
	var spanID trace.SpanID
	binary.BigEndian.PutUint64(spanID[0:8], uint64(input))
	return spanID
}

func jaegerFromOTelTraceID(input trace.TraceID) jaeger.TraceID {
	var traceID jaeger.TraceID
	traceID.High = binary.BigEndian.Uint64(input[0:8])
	traceID.Low = binary.BigEndian.Uint64(input[8:16])
	return traceID
}

func jaegerFromOTelSpanID(input trace.SpanID) jaeger.SpanID {
	return jaeger.SpanID(binary.BigEndian.Uint64(input[0:8]))
}
