// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/runtime_config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"errors"
	"io"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/runtimeconfig"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errMultipleDocuments = errors.New("the provided runtime configuration contains multiple documents")
)

// runtimeConfigValues are values that can be reloaded from configuration file while Mimir is running.
// Reloading is done by runtime_config.Manager, which also keeps the currently loaded config.
// These values are then pushed to the components that are interested in them.
type runtimeConfigValues struct {
	TenantLimits map[string]*validation.Limits `yaml:"overrides"`

	Multi kv.MultiRuntimeConfig `yaml:"multi_kv_config"`

	IngesterChunkStreaming *bool `yaml:"ingester_stream_chunks_when_using_blocks"`

	IngesterLimits    *ingester.InstanceLimits    `yaml:"ingester_limits"`
	DistributorLimits *distributor.InstanceLimits `yaml:"distributor_limits"`
}

// runtimeConfigTenantLimits provides per-tenant limit overrides based on a runtimeconfig.Manager
// that reads limits from a configuration file on disk and periodically reloads them.
type runtimeConfigTenantLimits struct {
	manager *runtimeconfig.Manager
}

// newTenantLimits creates a new validation.TenantLimits that loads per-tenant limit overrides from
// a runtimeconfig.Manager
func newTenantLimits(manager *runtimeconfig.Manager) validation.TenantLimits {
	return &runtimeConfigTenantLimits{
		manager: manager,
	}
}

func (l *runtimeConfigTenantLimits) ByUserID(userID string) *validation.Limits {
	return l.AllByUserID()[userID]
}

func (l *runtimeConfigTenantLimits) AllByUserID() map[string]*validation.Limits {
	cfg, ok := l.manager.GetConfig().(*runtimeConfigValues)
	if cfg != nil && ok {
		return cfg.TenantLimits
	}

	return nil
}

// runtimeConfigLoader loads and validates the per-tenant limits
type runtimeConfigLoader struct {
	validate func(limits validation.Limits) error
}

func (l *runtimeConfigLoader) load(r io.Reader) (interface{}, error) {
	var overrides = &runtimeConfigValues{}

	decoder := yaml.NewDecoder(r)
	decoder.KnownFields(true)

	// Decode the first document. An empty document (EOF) is OK.
	if err := decoder.Decode(&overrides); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	// Ensure the provided YAML config is not composed of multiple documents,
	if err := decoder.Decode(&runtimeConfigValues{}); !errors.Is(err, io.EOF) {
		return nil, errMultipleDocuments
	}

	if l.validate != nil {
		for _, limits := range overrides.TenantLimits {
			if limits == nil {
				continue
			}
			if err := l.validate(*limits); err != nil {
				return nil, err
			}
		}
	}

	return overrides, nil
}

func multiClientRuntimeConfigChannel(manager *runtimeconfig.Manager) func() <-chan kv.MultiRuntimeConfig {
	if manager == nil {
		return nil
	}
	// returns function that can be used in MultiConfig.ShipperConfigProvider
	return func() <-chan kv.MultiRuntimeConfig {
		outCh := make(chan kv.MultiRuntimeConfig, 1)

		// push initial config to the channel
		val := manager.GetConfig()
		if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
			outCh <- cfg.Multi
		}

		ch := manager.CreateListenerChannel(1)
		go func() {
			for val := range ch {
				if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
					outCh <- cfg.Multi
				}
			}
		}()

		return outCh
	}
}

func ingesterChunkStreaming(manager *runtimeconfig.Manager) func() ingester.QueryStreamType {
	if manager == nil {
		return nil
	}

	return func() ingester.QueryStreamType {
		val := manager.GetConfig()
		if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
			if cfg.IngesterChunkStreaming == nil {
				return ingester.QueryStreamDefault
			}

			if *cfg.IngesterChunkStreaming {
				return ingester.QueryStreamChunks
			}
			return ingester.QueryStreamSamples
		}

		return ingester.QueryStreamDefault
	}
}

func ingesterInstanceLimits(manager *runtimeconfig.Manager) func() *ingester.InstanceLimits {
	if manager == nil {
		return nil
	}

	return func() *ingester.InstanceLimits {
		val := manager.GetConfig()
		if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
			return cfg.IngesterLimits
		}
		return nil
	}
}

func distributorInstanceLimits(manager *runtimeconfig.Manager) func() *distributor.InstanceLimits {
	if manager == nil {
		return nil
	}

	return func() *distributor.InstanceLimits {
		val := manager.GetConfig()
		if cfg, ok := val.(*runtimeConfigValues); ok && cfg != nil {
			return cfg.DistributorLimits
		}
		return nil
	}
}

func runtimeConfigHandler(runtimeCfgManager *runtimeconfig.Manager, defaultLimits validation.Limits) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, ok := runtimeCfgManager.GetConfig().(*runtimeConfigValues)
		if !ok || cfg == nil {
			util.WriteTextResponse(w, "runtime config file doesn't exist")
			return
		}

		var output interface{}
		switch r.URL.Query().Get("mode") {
		case "diff":
			// Default runtime config is just empty struct, but to make diff work,
			// we set defaultLimits for every tenant that exists in runtime config.
			defaultCfg := runtimeConfigValues{}
			defaultCfg.TenantLimits = map[string]*validation.Limits{}
			for k, v := range cfg.TenantLimits {
				if v != nil {
					defaultCfg.TenantLimits[k] = &defaultLimits
				}
			}

			cfgYaml, err := util.YAMLMarshalUnmarshal(cfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			defaultCfgYaml, err := util.YAMLMarshalUnmarshal(defaultCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			output, err = util.DiffConfig(defaultCfgYaml, cfgYaml)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		default:
			output = cfg
		}
		util.WriteYAMLResponse(w, output)
	}
}

// NewRuntimeManager returns a runtimeconfig.Manager, a services.Service that must be explicitly started to perform any work.
// cfg is initialized as necessary, before being passed to runtimeconfig.New.
func NewRuntimeManager(cfg *Config, name string, reg prometheus.Registerer, logger log.Logger) (*runtimeconfig.Manager, error) {
	loader := runtimeConfigLoader{validate: cfg.ValidateLimits}
	cfg.RuntimeConfig.Loader = loader.load

	// DeprecatedAlignQueriesWithStep is moving from a global config that can in the frontend yaml to a limit config
	// We need to preserve the option in the frontend yaml for two releases
	// If the frontend config is configured by the user, the default limit is overwritten
	// TODO: Remove in Mimir 2.14
	if cfg.Frontend.QueryMiddleware.DeprecatedAlignQueriesWithStep != querymiddleware.DefaultDeprecatedAlignQueriesWithStep {
		cfg.LimitsConfig.AlignQueriesWithStep = cfg.Frontend.QueryMiddleware.DeprecatedAlignQueriesWithStep
	}

	// Make sure to set default limits before we start loading configuration into memory.
	validation.SetDefaultLimitsForYAMLUnmarshalling(cfg.LimitsConfig)
	ingester.SetDefaultInstanceLimitsForYAMLUnmarshalling(cfg.Ingester.DefaultLimits)
	distributor.SetDefaultInstanceLimitsForYAMLUnmarshalling(cfg.Distributor.DefaultLimits)
	return runtimeconfig.New(cfg.RuntimeConfig, name, reg, logger)
}
