// Copyright 2021 Prometheus Team
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

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for Discord configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Title:   `{{ template "discord.default.title" . }}`,
	Message: `{{ template "discord.default.message" . }}`,
}

// Config configures notifications via Discord.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig     *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`
	WebhookURL     *receivers.SecretURL      `yaml:"webhook_url,omitempty" json:"webhook_url,omitempty"`
	WebhookURLFile string                    `yaml:"webhook_url_file,omitempty" json:"webhook_url_file,omitempty"`

	Title   string `yaml:"title,omitempty" json:"title,omitempty"`
	Message string `yaml:"message,omitempty" json:"message,omitempty"`
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
	if webhookURL, ok, err := decrypt.DecryptSecretURL("webhook_url"); ok {
		if err != nil {
			return Config{}, fmt.Errorf("failed to decrypt webhook_url: %w", err)
		}
		settings.WebhookURL = &webhookURL
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
	if c.HTTPConfig != nil {
		if err := c.HTTPConfig.Validate(); err != nil {
			return fmt.Errorf("invalid http_config: %w", err)
		}
	}
	return nil
}

func (c *Config) validate() error {
	if c.WebhookURL == nil && c.WebhookURLFile == "" {
		return errors.New("one of webhook_url or webhook_url_file must be configured")
	}

	if c.WebhookURL != nil && len(c.WebhookURLFile) > 0 {
		return errors.New("at most one of webhook_url & webhook_url_file must be configured")
	}

	return nil
}

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "Webhook URL",
			Placeholder:  "Discord webhook URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "webhook_url",
			Required:     true,
			Secure:       true,
		},
		{
			Label:        "Title",
			Description:  "Templated title of the message",
			Placeholder:  DefaultConfig.Title,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "title",
		},
		{
			Label:        "Message Content",
			Description:  "Mention a group using @ or a user using <@ID> when notifying in a channel",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		httpcfg.V0HttpConfigOption(),
	},
})
