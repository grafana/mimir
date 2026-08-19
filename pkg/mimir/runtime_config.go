// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/runtime_config.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/runtimeconfig"
	"github.com/prometheus/client_golang/prometheus"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errMultipleDocuments = errors.New("the provided runtime configuration contains multiple documents")
)

const (
	// runtimeConfigLoaderMap decodes runtime configuration files directly from
	// their merged map representation using mapstructure.
	runtimeConfigLoaderMap = "map"
	// runtimeConfigLoaderYAML decodes runtime configuration files by round-tripping
	// them through YAML.
	runtimeConfigLoaderYAML = "yaml"
)

// RuntimeConfigConfig wraps dskit's runtimeconfig.Config with a Mimir-specific,
// experimental Loader selector that controls how runtime configuration files
// are decoded. NewRuntimeManager translates the selector into the appropriate
// dskit loader (Config.MapLoader or Config.Loader).
type RuntimeConfigConfig struct {
	runtimeconfig.Config `yaml:",inline"`

	// Loader selects how runtime configuration files are decoded. Valid values
	// are runtimeConfigLoaderMap and runtimeConfigLoaderYAML.
	Loader string `yaml:"loader" category:"experimental"`
}

// RegisterFlags registers the underlying runtimeconfig.Config flags plus the
// Mimir-specific loader selector.
func (c *RuntimeConfigConfig) RegisterFlags(f *flag.FlagSet) {
	c.Config.RegisterFlags(f)
	f.StringVar(&c.Loader, "runtime-config.loader", runtimeConfigLoaderMap, fmt.Sprintf("Method used to decode the runtime configuration files. Supported values are: %q (decode directly using mapstructure) and %q (decode by round-tripping through YAML).", runtimeConfigLoaderMap, runtimeConfigLoaderYAML))
}

// runtimeConfigValues are values that can be reloaded from configuration file while Mimir is running.
// Reloading is done by runtime_config.Manager, which also keeps the currently loaded config.
// These values are then pushed to the components that are interested in them.
type runtimeConfigValues struct {
	TenantLimits map[string]*validation.Limits `yaml:"overrides"`

	Multi kv.MultiRuntimeConfig `yaml:"multi_kv_config"`

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
	validate func(limits *validation.Limits) error
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

	if err := l.validateAll(overrides); err != nil {
		return nil, err
	}

	return overrides, nil
}

func (l *runtimeConfigLoader) loadFromMap(m map[string]interface{}) (interface{}, error) {
	overrides := &runtimeConfigValues{}

	if err := validation.DecodeLimitsMap(m, overrides); err != nil {
		return nil, err
	}

	if err := l.validateAll(overrides); err != nil {
		return nil, err
	}

	return overrides, nil
}

func (l *runtimeConfigLoader) validateAll(overrides *runtimeConfigValues) error {
	if l.validate == nil {
		return nil
	}
	for tenantID, limits := range overrides.TenantLimits {
		if limits == nil {
			continue
		}
		if err := l.validate(limits); err != nil {
			return fmt.Errorf("tenant %q: %w", tenantID, err)
		}
	}
	return nil
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
	switch cfg.RuntimeConfig.Loader {
	case runtimeConfigLoaderMap, "":
		cfg.RuntimeConfig.Config.MapLoader = loader.loadFromMap
	case runtimeConfigLoaderYAML:
		cfg.RuntimeConfig.Config.Loader = loader.load
	default:
		return nil, fmt.Errorf("invalid runtime config loader %q: supported values are %q and %q", cfg.RuntimeConfig.Loader, runtimeConfigLoaderMap, runtimeConfigLoaderYAML)
	}

	// Make sure to set default limits before we start loading configuration into memory.
	validation.SetDefaultLimitsForYAMLUnmarshalling(cfg.LimitsConfig)
	ingester.SetDefaultInstanceLimitsForYAMLUnmarshalling(cfg.Ingester.DefaultLimits)
	distributor.SetDefaultInstanceLimitsForYAMLUnmarshalling(cfg.Distributor.DefaultLimits)
	return runtimeconfig.New(cfg.RuntimeConfig.Config, name, reg, logger)
}
