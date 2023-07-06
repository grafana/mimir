package trace

import (
	"context"
	"net/http"
	"os"
	"strconv"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var mimirTracer Tracer

const (
	otelTracer  = "OtelTracer"
	serviceName = "mimir"
)

type EventValue struct {
	Str string
	Num int64
}

// Tracer defines the service used to create new spans.
type Tracer interface {
	// Start creates a new [Span] and places trace metadata on the
	// [context.Context] passed to the method.
	// Chose a low cardinality spanName and use [Span.SetAttributes]
	// or [Span.AddEvents] for high cardinality data.
	Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, Span)
	// Inject adds identifying information for the span to the
	// headers defined in [http.Header] map (this mutates http.Header).
	//
	// Implementation quirk: Where OpenTelemetry is used, the [Span] is
	// picked up from [context.Context] and for OpenTracing the
	// information passed as [Span] is preferred.
	Inject(context.Context, http.Header, Span)

	initTracerProvider() error
}

type Span interface {
	// End finalizes the Span and adds its end timestamp.
	// Any further operations on the Span are not permitted after
	// End has been called.
	End()
	// SetAttributes adds additional data to a span.
	// SetAttributes repeats the key value pair with [string] and [any]
	// used for OpenTracing and [attribute.KeyValue] used for
	// OpenTelemetry.
	SetAttributes(key string, value interface{}, kv attribute.KeyValue)
	// SetName renames the span.
	SetName(name string)
	// SetStatus can be used to indicate whether the span was
	// successfully or unsuccessfully executed.
	//
	// Only useful for OpenTelemetry.
	SetStatus(code codes.Code, description string)
	// RecordError adds an error to the span.
	//
	// Only useful for OpenTelemetry.
	RecordError(err error, options ...trace.EventOption)
	// AddEvents adds additional data with a temporal dimension to the
	// span.
	//
	// Panics if the length of keys is shorter than the length of values.
	AddEvents(keys []string, values []EventValue)
}

// Opentracing is the default setting, but can be overridden by setting
func InitTracingService(logger log.Logger) error {
	if e := os.Getenv(otelTracer); e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			if value {
				mimirTracer = &OpenTelemetry{
					serviceName: serviceName,
					logger:      logger,
				}
				return mimirTracer.initTracerProvider()
			}
		} else {
			return errors.Wrapf(err, "cannot parse env var %s=%s", otelTracer, e)
		}
	}
	mimirTracer = &OpenTracing{
		serviceName: serviceName,
		logger:      logger,
	}
	return mimirTracer.initTracerProvider()
}

// GetTracer returns the Tracer used by the service.
func GetTracer() Tracer {
	return mimirTracer
}
