package querytee

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/grafana/dskit/flagext"
)

type BackendConfig struct {
	RequestHeaders http.Header `json:"request_headers" yaml:"request_headers"`
}

type YAMLConfig struct {
	ServerConfig     ServerConfig     `yaml:"server"`
	BackendsConfig   BackendsConfig   `yaml:"backends"`
	ProxyConfig      YAMLProxyConfig  `yaml:"proxy"`
	ComparisonConfig ComparisonConfig `yaml:"comparison"`
}

type ServerConfig struct {
	HTTPService             HTTPServiceConfig `yaml:"http_service"`
	GRPCService             GRPCServiceConfig `yaml:"grpc_service"`
	GracefulShutdownTimeout time.Duration     `yaml:"graceful_shutdown_timeout"`
	PathPrefix              string            `yaml:"path_prefix"`
}

type HTTPServiceConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

type GRPCServiceConfig struct {
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

type BackendsConfig struct {
	Endpoints []BackendEndpoint `yaml:"endpoints"`
}

type BackendEndpoint struct {
	Name              string        `yaml:"name"`
	URL               string        `yaml:"url"`
	Preferred         bool          `yaml:"preferred"`
	Timeout           time.Duration `yaml:"timeout"`
	SkipTLSVerify     bool          `yaml:"skip_tls_verify"`
	RequestProportion float64       `yaml:"request_proportion"`
	RequestHeaders    http.Header   `yaml:"request_headers"`
}

type YAMLProxyConfig struct {
	CompareResponses                        bool          `yaml:"compare_responses"`
	PassthroughNonRegisteredRoutes          bool          `yaml:"passthrough_non_registered_routes"`
	AddMissingTimeParameterToInstantQueries bool          `yaml:"add_missing_time_parameter_to_instant_queries"`
	LogSlowQueryResponseThreshold           time.Duration `yaml:"log_slow_query_response_threshold"`
	SkipPreferredBackendFailures            bool          `yaml:"skip_preferred_backend_failures"`
}

type ComparisonConfig struct {
	ValueTolerance         float64       `yaml:"value_tolerance"`
	UseRelativeError       bool          `yaml:"use_relative_error"`
	SkipRecentSamples      time.Duration `yaml:"skip_recent_samples"`
	SkipSamplesBefore      flagext.Time  `yaml:"skip_samples_before"`
	RequireExactErrorMatch bool          `yaml:"require_exact_error_match"`
}

func (cfg *YAMLConfig) Validate() error {
	return cfg.BackendsConfig.Validate()
}

func (cfg *BackendsConfig) Validate() error {
	if len(cfg.Endpoints) < 2 {
		return fmt.Errorf("at least 2 backends required, got %d", len(cfg.Endpoints))
	}

	var preferredCount int
	backendNames := make(map[string]bool)

	for i, backend := range cfg.Endpoints {
		if backend.Name == "" {
			return fmt.Errorf("backend[%d]: name is required", i)
		}
		if backend.URL == "" {
			return fmt.Errorf("backend[%d] (%s): url is required", i, backend.Name)
		}

		if backendNames[backend.Name] {
			return fmt.Errorf("backend[%d]: duplicate backend name '%s'", i, backend.Name)
		}
		backendNames[backend.Name] = true

		if _, err := url.Parse(backend.URL); err != nil {
			return fmt.Errorf("backend[%d] (%s): invalid url '%s': %w", i, backend.Name, backend.URL, err)
		}

		if backend.RequestProportion < 0.0 || backend.RequestProportion > 1.0 {
			return fmt.Errorf("backend[%d] (%s): request_proportion must be between 0.0 and 1.0, got %f",
				i, backend.Name, backend.RequestProportion)
		}

		if backend.Preferred {
			preferredCount++
		}

		if backend.Timeout == 0 {
			cfg.Endpoints[i].Timeout = 150 * time.Second
		}

		if !backend.Preferred && backend.RequestProportion == 0.0 {
			cfg.Endpoints[i].RequestProportion = 1.0
		}
	}

	if preferredCount != 1 {
		return fmt.Errorf("exactly 1 preferred backend required, got %d", preferredCount)
	}

	return nil
}

func (cfg *ProxyConfig) ApplyYAMLConfig(yamlCfg *YAMLConfig) error {
	if err := yamlCfg.Validate(); err != nil {
		return err
	}

	cfg.parsedYAMLConfig = yamlCfg

	cfg.BackendConfigs = make(map[string]*BackendConfig)
	for _, endpoint := range yamlCfg.BackendsConfig.Endpoints {
		if endpoint.Preferred {
			cfg.PreferredBackend = endpoint.Name
		}
		cfg.BackendConfigs[endpoint.Name] = &BackendConfig{
			RequestHeaders: endpoint.RequestHeaders,
		}
	}

	if yamlCfg.ServerConfig.HTTPService.Address != "" {
		cfg.ServerHTTPServiceAddress = yamlCfg.ServerConfig.HTTPService.Address
	}
	if yamlCfg.ServerConfig.HTTPService.Port != 0 {
		cfg.ServerHTTPServicePort = yamlCfg.ServerConfig.HTTPService.Port
	}
	if yamlCfg.ServerConfig.GRPCService.Address != "" {
		cfg.ServerGRPCServiceAddress = yamlCfg.ServerConfig.GRPCService.Address
	}
	if yamlCfg.ServerConfig.GRPCService.Port != 0 {
		cfg.ServerGRPCServicePort = yamlCfg.ServerConfig.GRPCService.Port
	}
	if yamlCfg.ServerConfig.GracefulShutdownTimeout != 0 {
		cfg.ServerGracefulShutdownTimeout = yamlCfg.ServerConfig.GracefulShutdownTimeout
	}

	cfg.CompareResponses = yamlCfg.ProxyConfig.CompareResponses
	cfg.PassThroughNonRegisteredRoutes = yamlCfg.ProxyConfig.PassthroughNonRegisteredRoutes
	cfg.AddMissingTimeParamToInstantQueries = yamlCfg.ProxyConfig.AddMissingTimeParameterToInstantQueries
	if yamlCfg.ProxyConfig.LogSlowQueryResponseThreshold != 0 {
		cfg.LogSlowQueryResponseThreshold = yamlCfg.ProxyConfig.LogSlowQueryResponseThreshold
	}
	cfg.SkipPreferredBackendFailures = yamlCfg.ProxyConfig.SkipPreferredBackendFailures

	if yamlCfg.ComparisonConfig.ValueTolerance != 0 {
		cfg.ValueComparisonTolerance = yamlCfg.ComparisonConfig.ValueTolerance
	}
	cfg.UseRelativeError = yamlCfg.ComparisonConfig.UseRelativeError
	if yamlCfg.ComparisonConfig.SkipRecentSamples != 0 {
		cfg.SkipRecentSamples = yamlCfg.ComparisonConfig.SkipRecentSamples
	}
	if !time.Time(yamlCfg.ComparisonConfig.SkipSamplesBefore).IsZero() {
		cfg.SkipSamplesBefore = yamlCfg.ComparisonConfig.SkipSamplesBefore
	}
	cfg.RequireExactErrorMatch = yamlCfg.ComparisonConfig.RequireExactErrorMatch

	return nil
}
