package httpclient

import (
	"fmt"
	"net/http"
	"net/http/httptrace"
	"strconv"

	"go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/grafana-plugin-sdk-go/backend/tracing"
)

const (
	TracingMiddlewareName   = "Tracing"
	httpContentLengthTagKey = "http.content_length"
)

// TracingMiddleware is a middleware that creates spans for each outgoing request, tracking the url, method and response
// code as span attributes. If tracer is nil, it will use tracing.DefaultTracer().
func TracingMiddleware(tracer trace.Tracer) Middleware {
	return NamedMiddlewareFunc(TracingMiddlewareName, func(opts Options, next http.RoundTripper) http.RoundTripper {
		t := tracer
		if t == nil {
			t = tracing.DefaultTracer()
		}
		return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
			ctx, span := t.Start(req.Context(), "HTTP Outgoing Request", trace.WithSpanKind(trace.SpanKindClient))
			defer span.End()

			ctx = httptrace.WithClientTrace(
				ctx,
				otelhttptrace.NewClientTrace(ctx, otelhttptrace.WithoutSubSpans(), otelhttptrace.WithoutHeaders()),
			)

			req = req.WithContext(ctx)
			for k, v := range opts.Labels {
				span.SetAttributes(attribute.Key(k).String(v))
			}

			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))
			res, err := next.RoundTrip(req)

			span.SetAttributes(attribute.String("http.url", req.URL.String()))
			span.SetAttributes(attribute.String("http.method", req.Method))

			if err != nil {
				span.RecordError(err)
				return res, err
			}

			if res != nil {
				// we avoid measuring contentlength less than zero because it indicates
				// that the content size is unknown. https://godoc.org/github.com/badu/http#Response
				if res.ContentLength > 0 {
					span.SetAttributes(attribute.Key(httpContentLengthTagKey).Int64(res.ContentLength))
				}

				span.SetAttributes(attribute.Int("http.status_code", res.StatusCode))
				if res.StatusCode >= 400 {
					span.SetStatus(codes.Error, fmt.Sprintf("error with HTTP status code %s", strconv.Itoa(res.StatusCode)))
				}
			}

			return res, err
		})
	})
}
