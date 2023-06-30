package tracing

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/samplers/jaegerremote"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
	jaegerotel "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func NewFromEnv(serviceName string) (*tracesdk.TracerProvider, error) {
	cfg, err := parseTracingConfig()
	if err != nil {
		return nil, errors.Wrap(err, "could not load jaeger tracer configuration")
	}
	if cfg.samplerRemoteURL == "" && cfg.host == "" && cfg.collectorURL == "" {
		return nil, ErrBlankTraceConfiguration
	}
	return cfg.initJaegerTracerProvider(serviceName)
}

func parseAttributes(sTags string) ([]attribute.KeyValue, error) {
	pairs := strings.Split(sTags, ",")
	res := []attribute.KeyValue{}
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) > 1 {
			k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
			if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
				ed := strings.SplitN(v[2:len(v)-1], ":", 2)
				e, d := ed[0], ed[1]
				v = os.Getenv(e)
				if v == "" && d != "" {
					v = d
				}
			}
			res = append(res, attribute.String(k, v))
		} else if p != "" {
			return nil, errors.Errorf("invalid tag %q, expected key=value", p)
		}
	}
	return res, nil
}

type TracingConfig struct {
	host             string
	collectorURL     string
	port             string
	samplerType      string
	samplerRemoteURL string
	samplerParam     float64
	customAttributes []attribute.KeyValue
}

func parseTracingConfig() (*TracingConfig, error) {
	cfg := TracingConfig{
		host:             os.Getenv("JAEGER_AGENT_HOST"),
		collectorURL:     os.Getenv("JAEGER_ENDPOINT"),
		port:             os.Getenv("JAEGER_AGENT_PORT"),
		samplerType:      os.Getenv("JAEGER_SAMPLER_TYPE"),
		samplerRemoteURL: os.Getenv("JAEGER_SAMPLING_ENDPOINT"),
	}
	var err error
	cfg.samplerParam, err = strconv.ParseFloat(os.Getenv("JAEGER_SAMPLER_PARAM"), 64)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse JAEGER_SAMPLER_PARAM")
	}
	cfg.customAttributes, err = parseAttributes(os.Getenv("JAEGER_TAGS"))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse JAEGER_TAGS")
	}
	return cfg, nil
}

func (cfg TracingConfig) initJaegerTracerProvider(serviceName string) (*tracesdk.TracerProvider, error) {
	// Read environment variables to configure Jaeger
	var ep jaegerotel.EndpointOption
	// Create the jaeger exporter: address can be either agent address (host:port) or collector URL
	if cfg.host != "" {
		ep = jaegerotel.WithAgentEndpoint(jaegerotel.WithAgentHost(cfg.host), jaegerotel.WithAgentPort(cfg.port))
	} else {
		ep = jaegerotel.WithCollectorEndpoint(jaegerotel.WithEndpoint(cfg.collectorURL))
	}
	exp, err := jaegerotel.New(ep)
	if err != nil {
		return nil, err
	}

	res, err := NewResource(serviceName, cfg.customAttributes)
	if err != nil {
		return nil, err
	}

	// Configure sampling strategy
	sampler := tracesdk.AlwaysSample()
	if cfg.samplerType == "const" || cfg.samplerType == "probabilistic" {
		sampler = tracesdk.TraceIDRatioBased(cfg.samplerParam)
	} else if cfg.samplerType == "remote" {
		sampler = jaegerremote.New(serviceName, jaegerremote.WithSamplingServerURL(cfg.samplerRemoteURL),
			jaegerremote.WithInitialSampler(tracesdk.TraceIDRatioBased(cfg.samplerParam)))
	} else if cfg.samplerType != "" {
		return nil, errors.Errorf("unknown sampler type %q", cfg.samplerType)
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(res),
		tracesdk.WithSampler(sampler),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// OpenTracing <=> OpenTelemetry bridge
	otTracer, _ := bridge.NewTracerPair(tp.Tracer(serviceName))
	opentracing.SetGlobalTracer(otTracer)

	return tp, nil
}

// ExtractTraceID extracts the trace id, if any from the context.
func ExtractTraceID(ctx context.Context) (string, bool) {
	traceId := trace.SpanContextFromContext(ctx).TraceID().String()
	return traceId, traceId != ""
}

func ExtractSampledTraceID(ctx context.Context) (string, bool) {
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	if traceID != "" {
		return traceID, trace.SpanContextFromContext(ctx).IsSampled()
	}
	return "", false
}
