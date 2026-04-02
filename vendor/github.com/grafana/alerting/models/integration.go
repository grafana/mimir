package models

import (
	"encoding/json"

	"github.com/grafana/alerting/receivers/schema"
)

type IntegrationConfig struct {
	UID                   string                 `json:"uid" yaml:"uid"`
	Name                  string                 `json:"name" yaml:"name"`
	Type                  schema.IntegrationType `json:"type" yaml:"type"`
	Version               schema.Version         `json:"version" yaml:"version"`
	DisableResolveMessage bool                   `json:"disableResolveMessage" yaml:"disableResolveMessage"`
	Settings              json.RawMessage        `json:"settings" yaml:"settings"`
	SecureSettings        map[string]string      `json:"secureSettings" yaml:"secureSettings"`
}

type ReceiverConfig struct {
	Integrations []*IntegrationConfig `yaml:"grafana_managed_receiver_configs,omitempty" json:"grafana_managed_receiver_configs,omitempty"`
}
