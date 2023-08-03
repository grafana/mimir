package tracing

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/uber/jaeger-client-go"
	jaegerpropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/contrib/samplers/jaegerremote"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	bridge "go.opentelemetry.io/otel/bridge/opentracing"
	jaegerotel "go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

// ErrInvalidConfiguration is an error to notify client to provide valid trace report agent or config server
var (
	ErrBlankTraceConfiguration = errors.New("no trace report agent, config server, or collector endpoint specified")
)

const (
	envSamplerParam           = "JAEGER_SAMPLER_PARAM"
	envEndpoint               = "JAEGER_ENDPOINT"
	envJaegerAgentHost        = "JAEGER_AGENT_HOST"
	envJaegerAgentPort        = "JAEGER_AGENT_PORT"
	envSamplerType            = "JAEGER_SAMPLER_TYPE"
	envSamplerRemoteURL       = "JAEGER_SAMPLING_ENDPOINT"
	DefaultSamplingServerPort = 5778
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

// Parse Jeager tags from env var JAEGER_TAGS, example of TAGs format: key1=value1,key2=value2
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
	port             int
	samplerType      string
	samplerRemoteURL string
	samplerParam     float64
	customAttributes []attribute.KeyValue
}

func parseTracingConfig() (*TracingConfig, error) {
	cfg := TracingConfig{}
	var err error

	// We first parse the collector endpoint, if it is not exist then we parse the agent endpoint, if it is not exist, we default to localhost:6831
	if e := os.Getenv(envEndpoint); e != "" {
		u, err := url.ParseRequestURI(e)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", envEndpoint, e)
		}
		cfg.collectorURL = u.String()
	} else {
		if e := os.Getenv(envJaegerAgentHost); e != "" {
			cfg.host = e
		}
		if e := os.Getenv(envJaegerAgentPort); e != "" {
			if value, err := strconv.ParseInt(e, 10, 0); err == nil {
				cfg.port = int(value)
			} else {
				return nil, errors.Wrapf(err, "cannot parse env var %s=%s", envJaegerAgentPort, e)
			}
		}
	}

	// Then we parse the sampler type, if it is not exist then we default to const
	if e := os.Getenv(envSamplerType); e != "" {
		cfg.samplerType = e
	}

	if e := os.Getenv(envSamplerParam); e != "" {
		if value, err := strconv.ParseFloat(e, 64); err == nil {
			cfg.samplerParam = value
		} else {
			return nil, errors.Wrapf(err, "cannot parse env var %s=%s", envSamplerParam, e)
		}
	}

	if e := os.Getenv(envSamplerRemoteURL); e != "" {
		cfg.samplerRemoteURL = e
	} else if e := os.Getenv(envJaegerAgentHost); e != "" {
		// Fallback if we know the agent host - try the sampling endpoint there
		cfg.samplerRemoteURL = fmt.Sprintf("http://%s:%d/sampling", e, DefaultSamplingServerPort)
	}

	cfg.customAttributes, err = parseAttributes(os.Getenv("JAEGER_TAGS"))
	if err != nil {
		return nil, errors.Wrap(err, "could not parse JAEGER_TAGS")
	}
	return &cfg, nil
}

func (cfg TracingConfig) initJaegerTracerProvider(serviceName string) (*tracesdk.TracerProvider, error) {
	// Read environment variables to configure Jaeger
	var ep jaegerotel.EndpointOption
	// Create the jaeger exporter: address can be either agent address (host:port) or collector URL
	if cfg.host != "" {
		ep = jaegerotel.WithAgentEndpoint(jaegerotel.WithAgentHost(cfg.host), jaegerotel.WithAgentPort(strconv.Itoa(cfg.port)))
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
	propagators := []propagation.TextMapPropagator{}
	// w3c Propagator is the default propagator
	propagators = append(propagators, propagation.TraceContext{}, propagation.Baggage{})
	// jaeger Propagator is for backwards compatibility
	propagators = append(propagators, jaegerpropagator.Jaeger{})
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagators...))

	// OpenTracing <=> OpenTelemetry bridge
	// The tracer name is empty so that the bridge uses the default tracer.
	otTracer, _ := bridge.NewTracerPair(tp.Tracer(""))
	opentracing.SetGlobalTracer(otTracer)

	return tp, nil
}

// ExtractTraceID extracts the trace id, if any from the context.
func ExtractTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}
	sctx, ok := sp.Context().(jaeger.SpanContext)
	if !ok {
		return "", false
	}

	return sctx.TraceID().String(), true
}

func ExtractSampledTraceID(ctx context.Context) (string, bool) {
	sp := opentracing.SpanFromContext(ctx)
	if sp == nil {
		return "", false
	}
	sctx, ok := sp.Context().(jaeger.SpanContext)
	if !ok {
		return "", false
	}

	return sctx.TraceID().String(), sctx.IsSampled()
}
