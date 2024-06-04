package backend

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/backend/tracing"
	"github.com/grafana/grafana-plugin-sdk-go/build"
	"github.com/grafana/grafana-plugin-sdk-go/internal/tracerprovider"
)

const (
	// PluginProfilerEnvDeprecated is a deprecated constant for the GF_PLUGINS_PROFILER environment variable used to enable pprof.
	PluginProfilerEnvDeprecated = "GF_PLUGINS_PROFILER"
	// PluginProfilingEnabledEnv is a constant for the GF_PLUGIN_PROFILING_ENABLED environment variable used to enable pprof.
	PluginProfilingEnabledEnv = "GF_PLUGIN_PROFILING_ENABLED"

	// PluginProfilerPortEnvDeprecated is a constant for the GF_PLUGINS_PROFILER_PORT environment variable use to specify a pprof port (default 6060).
	PluginProfilerPortEnvDeprecated = "GF_PLUGINS_PROFILER_PORT" // nolint:gosec
	// PluginProfilingPortEnv is a constant for the GF_PLUGIN_PROFILING_PORT environment variable use to specify a pprof port (default 6060).
	PluginProfilingPortEnv = "GF_PLUGIN_PROFILING_PORT" // nolint:gosec

	// PluginTracingOpenTelemetryOTLPAddressEnv is a constant for the GF_INSTANCE_OTLP_ADDRESS
	// environment variable used to specify the OTLP address.
	PluginTracingOpenTelemetryOTLPAddressEnv = "GF_INSTANCE_OTLP_ADDRESS" // nolint:gosec
	// PluginTracingOpenTelemetryOTLPPropagationEnv is a constant for the GF_INSTANCE_OTLP_PROPAGATION
	// environment variable used to specify the OTLP propagation format.
	PluginTracingOpenTelemetryOTLPPropagationEnv = "GF_INSTANCE_OTLP_PROPAGATION"

	// PluginTracingSamplerTypeEnv is a constant for the GF_INSTANCE_OTLP_SAMPLER_TYPE
	// environment variable used to specify the OTLP sampler type.
	PluginTracingSamplerTypeEnv = "GF_INSTANCE_OTLP_SAMPLER_TYPE"

	// PluginTracingSamplerParamEnv is a constant for the GF_INSTANCE_OTLP_SAMPLER_PARAM
	// environment variable used to specify an additional float parameter used by the OTLP sampler,
	// depending on the type.
	PluginTracingSamplerParamEnv = "GF_INSTANCE_OTLP_SAMPLER_PARAM"

	// PluginTracingSamplerRemoteURL is a constant for the GF_INSTANCE_OTLP_SAMPLER_REMOTE_URL
	// environment variable used to specify the remote url for the sampler type. This is relevant
	// only when GF_INSTANCE_OTLP_SAMPLER_TYPE is "remote".
	PluginTracingSamplerRemoteURL = "GF_INSTANCE_OTLP_SAMPLER_REMOTE_URL"

	// PluginVersionEnv is a constant for the GF_PLUGIN_VERSION environment variable containing the plugin's version.
	// Deprecated: Use build.GetBuildInfo().Version instead.
	PluginVersionEnv = "GF_PLUGIN_VERSION"

	// defaultRemoteSamplerServiceName is the default service name passed to the remote sampler when it cannot be
	// determined from the build info.
	defaultRemoteSamplerServiceName = "grafana-plugin"
)

// SetupPluginEnvironment will read the environment variables and apply the
// standard environment behavior.
//
// As the SDK evolves, this will likely change.
//
// Currently, this function enables and configures profiling with pprof.
func SetupPluginEnvironment(pluginID string) {
	setupProfiler(pluginID)
}

func setupProfiler(pluginID string) {
	// Enable profiler
	profilerEnabled := false
	if value, ok := os.LookupEnv(PluginProfilerEnvDeprecated); ok {
		// compare value to plugin name
		if value == pluginID {
			profilerEnabled = true
		}
	} else if value, ok = os.LookupEnv(PluginProfilingEnabledEnv); ok {
		if value == "true" {
			profilerEnabled = true
		}
	}

	Logger.Debug("Profiler", "enabled", profilerEnabled)
	if profilerEnabled {
		profilerPort := "6060"
		for _, env := range []string{PluginProfilerPortEnvDeprecated, PluginProfilingPortEnv} {
			if value, ok := os.LookupEnv(env); ok {
				profilerPort = value
				break
			}
		}
		Logger.Info("Profiler", "port", profilerPort)
		portConfig := fmt.Sprintf(":%s", profilerPort)

		r := http.NewServeMux()
		r.HandleFunc("/debug/pprof/", pprof.Index)
		r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		r.HandleFunc("/debug/pprof/profile", pprof.Profile)
		r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		r.HandleFunc("/debug/pprof/trace", pprof.Trace)

		go func() {
			//nolint:gosec
			if err := http.ListenAndServe(portConfig, r); err != nil {
				Logger.Error("Error Running profiler", "error", err)
			}
		}()
	}
}

func getTracerCustomAttributes(pluginID string) []attribute.KeyValue {
	var customAttributes []attribute.KeyValue
	// Add plugin id and version to custom attributes
	// Try to get plugin version from build info
	// If not available, fallback to environment variable
	var pluginVersion string
	buildInfo, err := build.GetBuildInfo()
	if err != nil {
		Logger.Debug("Failed to get build info", "error", err)
	} else {
		pluginVersion = buildInfo.Version
	}
	if pluginVersion == "" {
		if pv, ok := os.LookupEnv(PluginVersionEnv); ok {
			pluginVersion = pv
		}
	}
	customAttributes = []attribute.KeyValue{
		semconv.ServiceNameKey.String(pluginID),
		semconv.ServiceVersionKey.String(pluginVersion),
	}
	return customAttributes
}

// SetupTracer sets up the global OTEL trace provider and tracer.
func SetupTracer(pluginID string, tracingOpts tracing.Opts) error {
	// Set up tracing
	tracingCfg := getTracingConfig(build.GetBuildInfo)
	if tracingCfg.isEnabled() {
		// Append custom attributes to the default ones
		tracingOpts.CustomAttributes = append(getTracerCustomAttributes(pluginID), tracingOpts.CustomAttributes...)

		// Initialize global tracer provider
		tp, err := tracerprovider.NewTracerProvider(tracingCfg.address, tracingCfg.sampler, tracingOpts)
		if err != nil {
			return fmt.Errorf("new trace provider: %w", err)
		}
		pf, err := tracerprovider.NewTextMapPropagator(tracingCfg.propagation)
		if err != nil {
			return fmt.Errorf("new propagator format: %w", err)
		}
		tracerprovider.InitGlobalTracerProvider(tp, pf)

		// Initialize global tracer for plugin developer usage
		tracing.InitDefaultTracer(otel.Tracer(pluginID))
	}

	enabled := tracingCfg.isEnabled()
	Logger.Debug("Tracing", "enabled", enabled)
	if enabled {
		Logger.Debug(
			"Tracing configuration",
			"propagation", tracingCfg.propagation,
			"samplerType", tracingCfg.sampler.SamplerType,
			"samplerParam", tracingCfg.sampler.Param,
			"samplerRemoteURL", tracingCfg.sampler.Remote.URL,
			"samplerRemoteServiceName", tracingCfg.sampler.Remote.ServiceName,
		)
	}
	return nil
}

// tracingConfig contains the configuration for OTEL tracing.
type tracingConfig struct {
	address     string
	propagation string

	sampler tracerprovider.SamplerOptions
}

// isEnabled returns true if OTEL tracing is enabled.
func (c tracingConfig) isEnabled() bool {
	return c.address != ""
}

// getTracingConfig returns a new tracingConfig based on the current environment variables.
func getTracingConfig(buildInfoGetter build.InfoGetter) tracingConfig {
	var otelAddr, otelPropagation, samplerRemoteURL, samplerParamString string
	var samplerType tracerprovider.SamplerType
	var samplerParam float64
	otelAddr, ok := os.LookupEnv(PluginTracingOpenTelemetryOTLPAddressEnv)
	if ok {
		// Additional OTEL config
		otelPropagation = os.Getenv(PluginTracingOpenTelemetryOTLPPropagationEnv)

		// Sampling config
		samplerType = tracerprovider.SamplerType(os.Getenv(PluginTracingSamplerTypeEnv))
		samplerRemoteURL = os.Getenv(PluginTracingSamplerRemoteURL)
		samplerParamString = os.Getenv(PluginTracingSamplerParamEnv)
		var err error
		samplerParam, err = strconv.ParseFloat(samplerParamString, 64)
		if err != nil {
			// Default value if invalid float is provided is 1.0 (AlwaysSample)
			log.DefaultLogger.Warn(
				"Could not parse sampler param to float, defaulting to 1.0",
				"samplerParam", samplerParamString, "error", err,
			)
			samplerParam = 1.0
		}
	}
	var serviceName string
	if samplerType == tracerprovider.SamplerTypeRemote {
		serviceName = remoteSamplerServiceName(buildInfoGetter)
	}
	return tracingConfig{
		address:     otelAddr,
		propagation: otelPropagation,
		sampler: tracerprovider.SamplerOptions{
			SamplerType: samplerType,
			Param:       samplerParam,
			Remote: tracerprovider.RemoteSamplerOptions{
				URL:         samplerRemoteURL,
				ServiceName: serviceName,
			},
		},
	}
}

// remoteSamplerServiceName returns the service name for the remote tracing sampler.
// It attempts to get it from the provided buildinfo getter. If unsuccessful or empty,
// defaultRemoteSamplerServiceName is returned instead.
func remoteSamplerServiceName(buildInfoGetter build.InfoGetter) string {
	// Use plugin id as service name, if possible. Otherwise, use a generic default value.
	bi, err := buildInfoGetter.GetInfo()
	if err != nil {
		log.DefaultLogger.Warn("Could not get build info for remote sampler service name", "error", err)
		return defaultRemoteSamplerServiceName
	}
	if bi.PluginID == "" {
		return defaultRemoteSamplerServiceName
	}
	return bi.PluginID
}
