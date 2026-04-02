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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

const wechatValidTypesRe = `^(text|markdown)$`

var wechatTypeMatcher = regexp.MustCompile(wechatValidTypesRe)

// DefaultConfig defines default values for wechat configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: false,
	},
	Message: `{{ template "wechat.default.message" . }}`,
	ToUser:  `{{ template "wechat.default.to_user" . }}`,
	ToParty: `{{ template "wechat.default.to_party" . }}`,
	ToTag:   `{{ template "wechat.default.to_tag" . }}`,
	AgentID: `{{ template "wechat.default.agent_id" . }}`,
}

// Config configures notifications via Wechat.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APISecret   receivers.Secret `yaml:"api_secret,omitempty" json:"api_secret,omitempty"`
	CorpID      string           `yaml:"corp_id,omitempty" json:"corp_id,omitempty"`
	Message     string           `yaml:"message,omitempty" json:"message,omitempty"`
	APIURL      *receivers.URL   `yaml:"api_url,omitempty" json:"api_url,omitempty"`
	ToUser      string           `yaml:"to_user,omitempty" json:"to_user,omitempty"`
	ToParty     string           `yaml:"to_party,omitempty" json:"to_party,omitempty"`
	ToTag       string           `yaml:"to_tag,omitempty" json:"to_tag,omitempty"`
	AgentID     string           `yaml:"agent_id,omitempty" json:"agent_id,omitempty"`
	MessageType string           `yaml:"message_type,omitempty" json:"message_type,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.MessageType == "" {
		c.MessageType = "text"
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
	if settings.MessageType == "" {
		settings.MessageType = "text"
	}
	if decrypted, ok := decrypt.DecryptSecret("api_secret"); ok {
		settings.APISecret = decrypted
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
		return errors.New("missing api_url in wechat config")
	}
	if c.APISecret == "" {
		return errors.New("missing api_secret in wechat config")
	}
	if c.CorpID == "" {
		return errors.New("missing corp_id in wechat config")
	}
	if c.HTTPConfig != nil {
		if err := c.HTTPConfig.Validate(); err != nil {
			return fmt.Errorf("invalid http_config: %w", err)
		}
	}
	return nil
}

func (c *Config) validate() error {
	if !wechatTypeMatcher.MatchString(c.MessageType) {
		return fmt.Errorf("weChat message type %q does not match valid options %s", c.MessageType, wechatValidTypesRe)
	}

	return nil
}

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
		},
		{
			Label:        "API Secret",
			Description:  "The API key to use when talking to the WeChat API",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_secret",
			Secure:       true,
		},
		{
			Label:        "Corp ID",
			Description:  "The corp id for authentication",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "corp_id",
		},
		{
			Label:        "Message",
			Description:  "API request data as defined by the WeChat API",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		{
			Label:        "Message type",
			Description:  "Type of the message type",
			Element:      schema.ElementTypeSelect,
			PropertyName: "message_type",
			Placeholder:  "text",
			SelectOptions: []schema.SelectOption{
				{Value: "text", Label: "Text"},
				{Value: "markdown", Label: "Markdown"},
			},
		},
		{
			Label:        "Agent ID",
			Placeholder:  DefaultConfig.AgentID,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "agent_id",
		},
		{
			Label:        "To User",
			Placeholder:  DefaultConfig.ToUser,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "to_user",
		},
		{
			Label:        "To Party",
			Placeholder:  DefaultConfig.ToParty,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "to_party",
		},
		{
			Label:        "To Tag",
			Placeholder:  DefaultConfig.ToTag,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "to_tag",
		},
		httpcfg.V0HttpConfigOption(),
	},
})
