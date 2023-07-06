package trace

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otl "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/weaveworks/common/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OpenTracing struct {
	serviceName string
	logger      log.Logger
}

type OpenTracingSpan struct {
	span opentracing.Span
}

func (ot *OpenTracing) initTracerProvider() error {
	if trace, err := tracing.NewFromEnv(ot.serviceName); err != nil {
		return errors.Wrap(err, "failed to setup opentracing tracer")
	} else {
		defer trace.Close()
	}
	return nil
}

func (ts *OpenTracing) Inject(ctx context.Context, header http.Header, span Span) {
	opentracingSpan, ok := span.(OpenTracingSpan)
	if !ok {
		level.Error(ts.logger).Log("msg", "Failed to cast opentracing span")
	}
	err := opentracing.GlobalTracer().Inject(
		opentracingSpan.span.Context(),
		opentracing.HTTPHeaders,
		opentracing.HTTPHeadersCarrier(header))

	if err != nil {
		level.Error(ts.logger).Log("msg", "Failed to inject span context instance", "err", err)
	}
}

func (ts *OpenTracing) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span) {
	span, ctx := opentracing.StartSpanFromContext(ctx, spanName)
	opentracingSpan := OpenTracingSpan{span: span}
	return ctx, opentracingSpan
}

func (s OpenTracingSpan) AddEvents(keys []string, values []EventValue) {
	fields := []otl.Field{}
	for i, v := range values {
		if v.Str != "" {
			field := otl.String(keys[i], v.Str)
			fields = append(fields, field)
		}
		if v.Num != 0 {
			field := otl.Int64(keys[i], v.Num)
			fields = append(fields, field)
		}
	}
	s.span.LogFields(fields...)
}

func (s OpenTracingSpan) End() {
	s.span.Finish()
}

func (s OpenTracingSpan) RecordError(err error, options ...trace.EventOption) {
	ext.Error.Set(s.span, true)
}

func (s OpenTracingSpan) SetAttributes(key string, value interface{}, kv attribute.KeyValue) {
	s.span.SetTag(key, value)
}

func (s OpenTracingSpan) SetName(name string) {
	s.span.SetOperationName(name)
}

func (s OpenTracingSpan) SetStatus(code codes.Code, description string) {
	ext.Error.Set(s.span, true)
}
