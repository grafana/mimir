package alertmanager

import (
	"encoding/json"
	"fmt"

	alertingNotify "github.com/grafana/alerting/notify"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"gopkg.in/yaml.v3"
)

type AlertmanagerConfig struct {
	config.Config
	GrafanaReceivers []*alertingNotify.APIReceiver
}

// Copied...
func LoadConfig(s string) (*AlertmanagerConfig, error) {
	var cfg config.Config
	err := yaml.Unmarshal([]byte(s), &cfg)
	if err != nil {
		return nil, err
	}

	// TODO(santiago): this is horrible
	type grafanaReceiverConfigs struct {
		UID                   string            `json:"uid" yaml:"uid"`
		Name                  string            `json:"name" yaml:"name"`
		Type                  string            `json:"type" yaml:"type"`
		DisableResolveMessage bool              `json:"disableResolveMessage" yaml:"disableResolveMessage"`
		SecureSettings        map[string]string `json:"secureSettings" yaml:"secureSettings"`

		// NOTE(santiago): the original json.RawMessage errors when trying to unmarshal a YAML map into it
		Settings map[string]interface{} `json:"settings" yaml:"settings"`
	}
	type receiver struct {
		Name    string                    `yaml:"name,omitempty" json:"name,omitempty"`
		Configs []*grafanaReceiverConfigs `yaml:"grafana_managed_receiver_configs,omitempty" json:"grafana_managed_receiver_configs,omitempty"`
	}
	type grafanaReceivers struct {
		Receivers []receiver `yaml:"receivers,omitempty" json:"receivers,omitempty"`
	}

	var gr grafanaReceivers
	if err := yaml.Unmarshal([]byte(s), &gr); err != nil {
		fmt.Println("Error unmarshaling into grafanaStuff")
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

	b, err := json.Marshal(gr.Receivers)
	if err != nil {
		return nil, err
	}

	var apiReceivers []*alertingNotify.APIReceiver
	if err := json.Unmarshal(b, &apiReceivers); err != nil {
		return nil, err
	}

	return &AlertmanagerConfig{
		Config:           cfg,
		GrafanaReceivers: apiReceivers,
	}, nil
}
