package querytee

import (
	"net/http"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestYAMLConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      YAMLConfig
		expectError string
	}{
		{
			name: "valid config with minimum requirements",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "preferred",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:      "secondary",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "",
		},
		{
			name: "valid config with all options",
			config: YAMLConfig{
				ServerConfig: ServerConfig{
					HTTPService: HTTPServiceConfig{
						Address: "127.0.0.1",
						Port:    8080,
					},
					GRPCService: GRPCServiceConfig{
						Address: "127.0.0.1",
						Port:    9095,
					},
					GracefulShutdownTimeout: 30 * time.Second,
					PathPrefix:              "/mimir",
				},
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:              "preferred",
							URL:               "http://localhost:9090",
							Preferred:         true,
							Timeout:           120 * time.Second,
							SkipTLSVerify:     false,
							RequestProportion: 1.0,
							RequestHeaders: http.Header{
								"X-Custom-Header": []string{"value1", "value2"},
							},
						},
						{
							Name:              "secondary",
							URL:               "http://localhost:9091",
							Preferred:         false,
							Timeout:           150 * time.Second,
							SkipTLSVerify:     true,
							RequestProportion: 0.8,
						},
					},
				},
				ProxyConfig: YAMLProxyConfig{
					CompareResponses:                        true,
					PassthroughNonRegisteredRoutes:          true,
					AddMissingTimeParameterToInstantQueries: false,
					LogSlowQueryResponseThreshold:           5 * time.Second,
					SkipPreferredBackendFailures:            true,
				},
				ComparisonConfig: ComparisonConfig{
					ValueTolerance:         0.001,
					UseRelativeError:       true,
					SkipRecentSamples:      5 * time.Minute,
					SkipSamplesBefore:      flagext.Time{},
					RequireExactErrorMatch: false,
				},
			},
			expectError: "",
		},
		{
			name: "only one backend",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "only-one",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
					},
				},
			},
			expectError: "at least 2 backends required, got 1",
		},
		{
			name: "no preferred backend",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "http://localhost:9090",
							Preferred: false,
						},
						{
							Name:      "backend2",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "exactly 1 preferred backend required, got 0",
		},
		{
			name: "multiple preferred backends",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:      "backend2",
							URL:       "http://localhost:9091",
							Preferred: true,
						},
					},
				},
			},
			expectError: "exactly 1 preferred backend required, got 2",
		},
		{
			name: "missing backend name",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:      "backend2",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "backend[0]: name is required",
		},
		{
			name: "missing backend URL",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "",
							Preferred: true,
						},
						{
							Name:      "backend2",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "backend[0] (backend1): url is required",
		},
		{
			name: "duplicate backend names",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "duplicate",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:      "duplicate",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "backend[1]: duplicate backend name 'duplicate'",
		},
		{
			name: "invalid URL",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "://invalid-url",
							Preferred: true,
						},
						{
							Name:      "backend2",
							URL:       "http://localhost:9091",
							Preferred: false,
						},
					},
				},
			},
			expectError: "backend[0] (backend1): invalid url '://invalid-url'",
		},
		{
			name: "invalid request proportion - negative",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:              "backend2",
							URL:               "http://localhost:9091",
							Preferred:         false,
							RequestProportion: -0.1,
						},
					},
				},
			},
			expectError: "backend[1] (backend2): request_proportion must be between 0.0 and 1.0, got -0.100000",
		},
		{
			name: "invalid request proportion - greater than 1",
			config: YAMLConfig{
				BackendsConfig: BackendsConfig{
					Endpoints: []BackendEndpoint{
						{
							Name:      "backend1",
							URL:       "http://localhost:9090",
							Preferred: true,
						},
						{
							Name:              "backend2",
							URL:               "http://localhost:9091",
							Preferred:         false,
							RequestProportion: 1.5,
						},
					},
				},
			},
			expectError: "backend[1] (backend2): request_proportion must be between 0.0 and 1.0, got 1.500000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectError)
			}
		})
	}
}

func TestBackendsConfigValidation_DefaultValues(t *testing.T) {
	config := &BackendsConfig{
		Endpoints: []BackendEndpoint{
			{
				Name:      "preferred",
				URL:       "http://localhost:9090",
				Preferred: true,
			},
			{
				Name:      "secondary",
				URL:       "http://localhost:9091",
				Preferred: false,
			},
		},
	}

	err := config.Validate()
	require.NoError(t, err)

	// Check that defaults are applied
	assert.Equal(t, 150*time.Second, config.Endpoints[0].Timeout)
	assert.Equal(t, 150*time.Second, config.Endpoints[1].Timeout)
	assert.Equal(t, 1.0, config.Endpoints[1].RequestProportion) // Secondary backend should default to 1.0
}

func TestYAMLParsing(t *testing.T) {
	yamlContent := `
server:
  http_service:
    address: "127.0.0.1"
    port: 8080
  grpc_service:
    address: "127.0.0.1"
    port: 9095
  graceful_shutdown_timeout: 30s
  path_prefix: "/mimir"

backends:
  endpoints:
    - name: "preferred-backend"
      url: "http://mimir-1.example.com:8080"
      preferred: true
      timeout: 2m
      skip_tls_verify: false
      request_headers:
        X-Scope-OrgID: ["tenant-1"]
        Authorization: ["Bearer token123"]
    - name: "secondary-backend"
      url: "http://mimir-2.example.com:8080"
      preferred: false
      timeout: 90s
      skip_tls_verify: true
      request_proportion: 0.8

proxy:
  compare_responses: true
  passthrough_non_registered_routes: false
  add_missing_time_parameter_to_instant_queries: true
  log_slow_query_response_threshold: 5s
  skip_preferred_backend_failures: true

comparison:
  value_tolerance: 0.001
  use_relative_error: true
  skip_recent_samples: 2m
  require_exact_error_match: false
`

	var config YAMLConfig
	err := yaml.Unmarshal([]byte(yamlContent), &config)
	require.NoError(t, err)

	// Validate the parsed config
	err = config.Validate()
	require.NoError(t, err)

	// Verify server configuration
	assert.Equal(t, "127.0.0.1", config.ServerConfig.HTTPService.Address)
	assert.Equal(t, 8080, config.ServerConfig.HTTPService.Port)
	assert.Equal(t, "127.0.0.1", config.ServerConfig.GRPCService.Address)
	assert.Equal(t, 9095, config.ServerConfig.GRPCService.Port)
	assert.Equal(t, 30*time.Second, config.ServerConfig.GracefulShutdownTimeout)
	assert.Equal(t, "/mimir", config.ServerConfig.PathPrefix)

	// Verify backends configuration
	require.Len(t, config.BackendsConfig.Endpoints, 2)

	// Preferred backend
	preferred := config.BackendsConfig.Endpoints[0]
	assert.Equal(t, "preferred-backend", preferred.Name)
	assert.Equal(t, "http://mimir-1.example.com:8080", preferred.URL)
	assert.True(t, preferred.Preferred)
	assert.Equal(t, 2*time.Minute, preferred.Timeout)
	assert.False(t, preferred.SkipTLSVerify)
	assert.Equal(t, []string{"tenant-1"}, preferred.RequestHeaders["X-Scope-Orgid"])
	assert.Equal(t, []string{"Bearer token123"}, preferred.RequestHeaders["Authorization"])

	// Secondary backend
	secondary := config.BackendsConfig.Endpoints[1]
	assert.Equal(t, "secondary-backend", secondary.Name)
	assert.Equal(t, "http://mimir-2.example.com:8080", secondary.URL)
	assert.False(t, secondary.Preferred)
	assert.Equal(t, 90*time.Second, secondary.Timeout)
	assert.True(t, secondary.SkipTLSVerify)
	assert.Equal(t, 0.8, secondary.RequestProportion)

	// Verify proxy configuration
	assert.True(t, config.ProxyConfig.CompareResponses)
	assert.False(t, config.ProxyConfig.PassthroughNonRegisteredRoutes)
	assert.True(t, config.ProxyConfig.AddMissingTimeParameterToInstantQueries)
	assert.Equal(t, 5*time.Second, config.ProxyConfig.LogSlowQueryResponseThreshold)
	assert.True(t, config.ProxyConfig.SkipPreferredBackendFailures)

	// Verify comparison configuration
	assert.Equal(t, 0.001, config.ComparisonConfig.ValueTolerance)
	assert.True(t, config.ComparisonConfig.UseRelativeError)
	assert.Equal(t, 2*time.Minute, config.ComparisonConfig.SkipRecentSamples)
	assert.False(t, config.ComparisonConfig.RequireExactErrorMatch)
}

func TestApplyYAMLConfig(t *testing.T) {
	yamlConfig := &YAMLConfig{
		ServerConfig: ServerConfig{
			HTTPService: HTTPServiceConfig{
				Address: "0.0.0.0",
				Port:    8080,
			},
			GRPCService: GRPCServiceConfig{
				Address: "0.0.0.0",
				Port:    9095,
			},
			GracefulShutdownTimeout: 45 * time.Second,
		},
		BackendsConfig: BackendsConfig{
			Endpoints: []BackendEndpoint{
				{
					Name:      "preferred",
					URL:       "http://localhost:9090",
					Preferred: true,
					Timeout:   120 * time.Second,
					RequestHeaders: http.Header{
						"X-Scope-OrgID": []string{"tenant-1"},
						"Authorization": []string{"Bearer token123"},
					},
				},
				{
					Name:              "secondary",
					URL:               "http://localhost:9091",
					Preferred:         false,
					RequestProportion: 0.7,
				},
			},
		},
		ProxyConfig: YAMLProxyConfig{
			CompareResponses:                        true,
			PassthroughNonRegisteredRoutes:          true,
			AddMissingTimeParameterToInstantQueries: false,
			LogSlowQueryResponseThreshold:           15 * time.Second,
			SkipPreferredBackendFailures:            true,
		},
		ComparisonConfig: ComparisonConfig{
			ValueTolerance:         0.002,
			UseRelativeError:       true,
			SkipRecentSamples:      5 * time.Minute,
			RequireExactErrorMatch: true,
		},
	}

	proxyConfig := &ProxyConfig{}
	err := proxyConfig.ApplyYAMLConfig(yamlConfig)
	require.NoError(t, err)

	// Verify server configuration is applied
	assert.Equal(t, "0.0.0.0", proxyConfig.ServerHTTPServiceAddress)
	assert.Equal(t, 8080, proxyConfig.ServerHTTPServicePort)
	assert.Equal(t, "0.0.0.0", proxyConfig.ServerGRPCServiceAddress)
	assert.Equal(t, 9095, proxyConfig.ServerGRPCServicePort)
	assert.Equal(t, 45*time.Second, proxyConfig.ServerGracefulShutdownTimeout)

	// Verify preferred backend is set correctly
	assert.Equal(t, "preferred", proxyConfig.PreferredBackend)

	// Verify proxy configuration is applied
	assert.True(t, proxyConfig.CompareResponses)
	assert.True(t, proxyConfig.PassThroughNonRegisteredRoutes)
	assert.False(t, proxyConfig.AddMissingTimeParamToInstantQueries)
	assert.Equal(t, 15*time.Second, proxyConfig.LogSlowQueryResponseThreshold)
	assert.True(t, proxyConfig.SkipPreferredBackendFailures)

	// Verify comparison configuration is applied
	assert.Equal(t, 0.002, proxyConfig.ValueComparisonTolerance)
	assert.True(t, proxyConfig.UseRelativeError)
	assert.Equal(t, 5*time.Minute, proxyConfig.SkipRecentSamples)
	assert.True(t, proxyConfig.RequireExactErrorMatch)

	// Verify the YAML config is stored for backend creation
	assert.NotNil(t, proxyConfig.parsedYAMLConfig)
	assert.Equal(t, yamlConfig, proxyConfig.parsedYAMLConfig)

	require.NotNil(t, proxyConfig.BackendConfigs)
	assert.Len(t, proxyConfig.BackendConfigs, 2)

	preferredCfg, exists := proxyConfig.BackendConfigs["preferred"]
	require.True(t, exists)
	require.NotNil(t, preferredCfg)
	assert.Equal(t, http.Header{"X-Scope-OrgID": []string{"tenant-1"}, "Authorization": []string{"Bearer token123"}}, preferredCfg.RequestHeaders)

	secondaryCfg, exists := proxyConfig.BackendConfigs["secondary"]
	require.True(t, exists)
	require.NotNil(t, secondaryCfg)
	assert.Empty(t, secondaryCfg.RequestHeaders)
}
