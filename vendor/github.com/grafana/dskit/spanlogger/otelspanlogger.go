package spanlogger

import (
	"context"
	"fmt"
	"reflect"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type OtelSpanLogger struct {
	log.Logger
	trace.Span
	sampled bool
}

func OtelNew(ctx context.Context, tr trace.Tracer, logger log.Logger, method string, resolver TenantResolver, kvps ...interface{}) (*OtelSpanLogger, context.Context) {
	ctx, sp := tr.Start(ctx, method)
	defer sp.End()
	if ids, err := resolver.TenantIDs(ctx); err == nil && len(ids) > 0 {
		sp.SetAttributes(attribute.StringSlice(TenantIDsTagName, ids))
	}
	lwc, sampled := withContext(ctx, logger, resolver, true)
	l := &OtelSpanLogger{
		Logger:  log.With(lwc, "method", method),
		Span:    sp,
		sampled: sampled,
	}
	if len(kvps) > 0 {
		level.Debug(l).Log(kvps...)
	}

	ctx = context.WithValue(ctx, loggerCtxKey, logger)
	return l, ctx
}

func OtelFromContext(ctx context.Context, fallback log.Logger, resolver TenantResolver) *OtelSpanLogger {
	logger, ok := ctx.Value(loggerCtxKey).(log.Logger)
	if !ok {
		logger = fallback
	}
	sp := trace.SpanFromContext(ctx)
	lwc, sampled := withContext(ctx, logger, resolver, true)
	return &OtelSpanLogger{
		Logger:  lwc,
		Span:    sp,
		sampled: sampled,
	}
}

func (s *OtelSpanLogger) Log(kvps ...interface{}) error {
	s.Logger.Log(kvps...)
	if !s.sampled {
		return nil
	}
	fields, err := convertKVToAttributes(kvps...)
	if err != nil {
		return err
	}
	s.AddEvent("log", trace.WithAttributes(fields...))
	return nil
}

func (s *OtelSpanLogger) Error(err error) error {
	if err == nil || !s.sampled {
		return err
	}
	s.Span.SetStatus(codes.Error, "")
	s.Span.RecordError(err)
	return err
}

// convertKVToAttributes converts keyValues to a slice of attribute.KeyValue
func convertKVToAttributes(keyValues ...interface{}) ([]attribute.KeyValue, error) {
	if len(keyValues)%2 != 0 {
		return nil, fmt.Errorf("non-even keyValues len: %d", len(keyValues))
	}
	fields := make([]attribute.KeyValue, len(keyValues)/2)
	for i := 0; i*2 < len(keyValues); i++ {
		key, ok := keyValues[i*2].(string)
		if !ok {
			return nil, fmt.Errorf("non-string key (pair #%d): %T", i, keyValues[i*2])
		}
		value := keyValues[i*2+1]
		typedVal := reflect.ValueOf(value)

		switch typedVal.Kind() {
		case reflect.Bool:
			fields[i] = attribute.Bool(key, typedVal.Bool())
		case reflect.String:
			fields[i] = attribute.String(key, typedVal.String())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fields[i] = attribute.Int(key, int(typedVal.Int()))
		case reflect.Float32, reflect.Float64:
			fields[i] = attribute.Float64(key, typedVal.Float())
		default:
			if typedVal.Kind() == reflect.Ptr && typedVal.IsNil() {
				fields[i] = attribute.String(key, "nil")
				continue
			}
			// When in doubt, coerce to a string
			fields[i] = attribute.String(key, fmt.Sprintf("%v", value))
		}
	}
	return fields, nil
}
