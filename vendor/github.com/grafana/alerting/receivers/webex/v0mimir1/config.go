// Copyright 2022 Prometheus Team
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
	"encoding/json"
	"errors"
	"fmt"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for Webex configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Message: `{{ template "webex.default.message" . }}`,
}

// Config configures notifications via Webex.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`
	HTTPConfig               *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`
	APIURL                   *receivers.URL            `yaml:"api_url,omitempty" json:"api_url,omitempty"`
	Message                  string                    `yaml:"message,omitempty" json:"message,omitempty"`
	RoomID                   string                    `yaml:"room_id" json:"room_id"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return c.validate()
}

// NewConfig creates a Config from raw JSON and a decrypt function for secure fields.
func NewConfig(jsonData json.RawMessage, decrypt receivers.DecryptFunc) (Config, error) {
	settings := DefaultConfig
	var err error
	if err := json.Unmarshal(jsonData, &settings); err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal settings: %w", err)
	}
	settings.HTTPConfig, err = httpcfg.DecryptHTTPConfig("http_config", settings.HTTPConfig, decrypt)
	if err != nil {
		return Config{}, fmt.Errorf("failed to decrypt http_config: %w", err)
	}
	if err := settings.Validate(); err != nil {
		return Config{}, err
	}
	return settings, nil
}

func (c *Config) Validate() error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.APIURL == nil {
		return errors.New("missing api_url in webex config")
	}
	if c.HTTPConfig != nil {
		if err := c.HTTPConfig.Validate(); err != nil {
			return fmt.Errorf("invalid http_config: %w", err)
		}
	}
	return nil
}

func (c *Config) validate() error {
	if c.RoomID == "" {
		return errors.New("missing room_id on webex_config")
	}

	if c.HTTPConfig == nil || c.HTTPConfig.Authorization == nil {
		return errors.New("missing webex_configs.http_config.authorization")
	}

	return nil
}

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API URL",
			Description:  "The Webex Teams API URL",
			Placeholder:  "https://webexapis.com/v1/messages",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
		},
		{
			Label:        "Room ID",
			Description:  "ID of the Webex Teams room where to send the messages",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "room_id",
			Required:     true,
		},
		{
			Label:        "Message",
			Description:  "Message template",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		httpcfg.V0HttpConfigOption(),
	},
})
