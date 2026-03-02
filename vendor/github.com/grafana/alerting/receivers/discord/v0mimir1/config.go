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
	"errors"

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
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

	if c.WebhookURL == nil && c.WebhookURLFile == "" {
		return errors.New("one of webhook_url or webhook_url_file must be configured")
	}

	if c.WebhookURL != nil && len(c.WebhookURLFile) > 0 {
		return errors.New("at most one of webhook_url & webhook_url_file must be configured")
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
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
		schema.V0HttpConfigOption(),
	},
}
