// Copyright 2019 Prometheus Team
// Modifications Copyright Grafana Labs, licensed under AGPL-3.0
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v0mimir1

import (
	"errors"

	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for Webhook configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
}

// Config configures notifications via a generic webhook.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	// URL to send POST request to.
	URL     *receivers.SecretURL `yaml:"url" json:"url"`
	URLFile string               `yaml:"url_file" json:"url_file"`

	// MaxAlerts is the maximum number of alerts to be sent per webhook message.
	// Alerts exceeding this threshold will be truncated. Setting this to 0
	// allows an unlimited number of alerts.
	MaxAlerts uint64 `yaml:"max_alerts" json:"max_alerts"`

	// Timeout is the maximum time allowed to invoke the webhook. Setting this to 0
	// does not impose a timeout.
	Timeout model.Duration `yaml:"timeout" json:"timeout"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.URL == nil && c.URLFile == "" {
		return errors.New("one of url or url_file must be configured")
	}
	if c.URL != nil && c.URLFile != "" {
		return errors.New("at most one of url & url_file must be configured")
	}
	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "URL",
			Description:  "The endpoint to send HTTP POST requests to.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "url",
			Secure:       true,
			Required:     true,
		},
		{
			Label:          "Max alerts",
			Description:    "The maximum number of alerts to include in a single webhook message. Alerts above this threshold are truncated. When leaving this at its default value of 0, all alerts are included.",
			Element:        schema.ElementTypeInput,
			InputType:      "number",
			PropertyName:   "max_alerts",
			ValidationRule: "(^\\d+$|^$)",
		},
		{
			Label:        "Timeout",
			Description:  "The maximum time to wait for a webhook request to complete, before failing the request and allowing it to be retried. The default value of 0s indicates that no timeout should be applied. NOTE: This will have no effect if set higher than the group_interval.",
			Placeholder:  "Use duration format, for example: 1.2s, 100ms",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "timeout",
		},
		schema.V0HttpConfigOption(),
	},
}
