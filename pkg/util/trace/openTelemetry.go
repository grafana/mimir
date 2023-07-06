package trace

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type OpenTelemetry struct {
	serviceName    string
	tracerProvider *tracesdk.TracerProvider
	logger         log.Logger
}

type OpentelemetrySpan struct {
	span trace.Span
}

func (ote *OpenTelemetry) Inject(ctx context.Context, header http.Header, _ Span) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(header))
}

func (ote *OpenTelemetry) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span) {
	ctx, span := ote.tracerProvider.Tracer("").Start(ctx, spanName)
	opentelemetrySpan := OpentelemetrySpan{
		span: span,
	}
	return ctx, opentelemetrySpan
}

func (ote *OpenTelemetry) initTracerProvider() error {
	// here we need to Integrate the PR in weaveworks/common here: https://github.com/weaveworks/common/pull/291/files
	// or just copy this code for now and wait for people to review it
	panic("implement me")
}

func (s OpentelemetrySpan) AddEvents(keys []string, values []EventValue) {
	for i, v := range values {
		// TODO: add support for other types
		if v.Num != 0 {
			s.span.AddEvent(keys[i], trace.WithAttributes(attribute.Key(keys[i]).String(v.Str)))
		}
		if v.Str != "" {
			s.span.AddEvent(keys[i], trace.WithAttributes(attribute.Key(keys[i]).Int64(v.Num)))
		}
	}
}

func (s OpentelemetrySpan) End() {
	s.span.End()
}

func (s OpentelemetrySpan) RecordError(err error, options ...trace.EventOption) {
	for _, o := range options {
		s.span.RecordError(err, o)
	}
}

func (s OpentelemetrySpan) SetAttributes(key string, value interface{}, kv attribute.KeyValue) {
	s.span.SetAttributes(kv)
}

func (s OpentelemetrySpan) SetName(name string) {
	s.span.SetName(name)
}

func (s OpentelemetrySpan) SetStatus(code codes.Code, description string) {
	s.span.SetStatus(code, description)
}
