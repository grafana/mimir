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
	"errors"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
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

	if c.RoomID == "" {
		return errors.New("missing room_id on webex_config")
	}

	if c.HTTPConfig == nil || c.HTTPConfig.Authorization == nil {
		return errors.New("missing webex_configs.http_config.authorization")
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
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
		schema.V0HttpConfigOption(),
	},
}
