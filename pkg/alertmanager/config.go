package alertmanager

import (
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"gopkg.in/yaml.v3"
)

type AlertmanagerConfig struct {
	config.Config
	// GrafanaManagedReceivers alertingNotify.GrafanaIntegrations
}

// Copied...
func LoadConfig(s string) (*AlertmanagerConfig, error) {
	var cfg AlertmanagerConfig
	err := yaml.Unmarshal([]byte(s), &cfg)
	if err != nil {
		return nil, err
	}
	// Check if we have a root route. We cannot check for it in the
	// UnmarshalYAML method because it won't be called if the input is empty
	// (e.g. the config file is empty or only contains whitespace).
	if cfg.Route == nil {
		return nil, errors.New("no route provided in config")
	}

	// Check if continue in root route.
	if cfg.Route.Continue {
		return nil, errors.New("cannot have continue in root route")
	}

	return &cfg, nil
}
