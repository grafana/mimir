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
	"fmt"

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for VictorOps configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	MessageType:       `CRITICAL`,
	StateMessage:      `{{ template "victorops.default.state_message" . }}`,
	EntityDisplayName: `{{ template "victorops.default.entity_display_name" . }}`,
	MonitoringTool:    `{{ template "victorops.default.monitoring_tool" . }}`,
}

// Config configures notifications via VictorOps.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIKey            receivers.Secret  `yaml:"api_key,omitempty" json:"api_key,omitempty"`
	APIKeyFile        string            `yaml:"api_key_file,omitempty" json:"api_key_file,omitempty"`
	APIURL            *receivers.URL    `yaml:"api_url" json:"api_url"`
	RoutingKey        string            `yaml:"routing_key" json:"routing_key"`
	MessageType       string            `yaml:"message_type" json:"message_type"`
	StateMessage      string            `yaml:"state_message" json:"state_message"`
	EntityDisplayName string            `yaml:"entity_display_name" json:"entity_display_name"`
	MonitoringTool    string            `yaml:"monitoring_tool" json:"monitoring_tool"`
	CustomFields      map[string]string `yaml:"custom_fields,omitempty" json:"custom_fields,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.RoutingKey == "" {
		return errors.New("missing Routing key in VictorOps config")
	}

	if c.APIKey != "" && c.APIKeyFile != "" {
		return errors.New("at most one of api_key & api_key_file must be configured")
	}

	reservedFields := []string{"routing_key", "message_type", "state_message", "entity_display_name", "monitoring_tool", "entity_id", "entity_state"}

	for _, v := range reservedFields {
		if _, ok := c.CustomFields[v]; ok {
			return fmt.Errorf("victorOps config contains custom field %s which cannot be used as it conflicts with the fixed/static fields", v)
		}
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API key",
			Description:  "The API key to use when talking to the VictorOps API.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_key",
			Secure:       true,
		},
		{
			Label:        "API URL",
			Description:  "The VictorOps API URL.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
		},
		{
			Label:        "Routing key",
			Description:  "A key used to map the alert to a team.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "routing_key",
			Required:     true,
		},
		{
			Label:        "Message type",
			Description:  "Describes the behavior of the alert (CRITICAL, WARNING, INFO).",
			Placeholder:  DefaultConfig.MessageType,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message_type",
		},
		{
			Label:        "Entity display name",
			Description:  "Contains summary of the alerted problem.",
			Placeholder:  DefaultConfig.EntityDisplayName,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "entity_display_name",
		},
		{
			Label:        "State message",
			Description:  "Contains long explanation of the alerted problem.",
			Placeholder:  DefaultConfig.StateMessage,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "state_message",
		},
		{
			Label:        "Monitoring tool",
			Description:  "The monitoring tool the state message is from.",
			Placeholder:  DefaultConfig.MonitoringTool,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "monitoring_tool",
		},
		{
			Label:        "Custom Fields",
			Description:  "A set of arbitrary key/value pairs that provide further detail about the alert.",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "custom_fields",
		},
		schema.V0HttpConfigOption(),
	},
}
