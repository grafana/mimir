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
	"time"

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers/schema"
)

type FractionalDuration time.Duration

func (d *FractionalDuration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err == nil {
		*d = FractionalDuration(parsed)
	}
	return err
}

func (d FractionalDuration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

const Version = schema.V0mimir1

// DefaultConfig defines default values for Pushover configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Title:    `{{ template "pushover.default.title" . }}`,
	Message:  `{{ template "pushover.default.message" . }}`,
	URL:      `{{ template "pushover.default.url" . }}`,
	Priority: `{{ if eq .Status "firing" }}2{{ else }}0{{ end }}`,
	Retry:    FractionalDuration(1 * time.Minute),
	Expire:   FractionalDuration(1 * time.Hour),
	HTML:     false,
}

// Config configures notifications via Pushover.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig  *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`
	UserKey     receivers.Secret          `yaml:"user_key,omitempty" json:"user_key,omitempty"`
	UserKeyFile string                    `yaml:"user_key_file,omitempty" json:"user_key_file,omitempty"`
	Token       receivers.Secret          `yaml:"token,omitempty" json:"token,omitempty"`
	TokenFile   string                    `yaml:"token_file,omitempty" json:"token_file,omitempty"`
	Title       string                    `yaml:"title,omitempty" json:"title,omitempty"`
	Message     string                    `yaml:"message,omitempty" json:"message,omitempty"`
	URL         string                    `yaml:"url,omitempty" json:"url,omitempty"`
	URLTitle    string                    `yaml:"url_title,omitempty" json:"url_title,omitempty"`
	Device      string                    `yaml:"device,omitempty" json:"device,omitempty"`
	Sound       string                    `yaml:"sound,omitempty" json:"sound,omitempty"`
	Priority    string                    `yaml:"priority,omitempty" json:"priority,omitempty"`
	Retry       FractionalDuration        `yaml:"retry,omitempty" json:"retry,omitempty"`
	Expire      FractionalDuration        `yaml:"expire,omitempty" json:"expire,omitempty"`
	TTL         FractionalDuration        `yaml:"ttl,omitempty" json:"ttl,omitempty"`
	HTML        bool                      `yaml:"html" json:"html,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.UserKey == "" && c.UserKeyFile == "" {
		return errors.New("one of user_key or user_key_file must be configured")
	}
	if c.UserKey != "" && c.UserKeyFile != "" {
		return errors.New("at most one of user_key & user_key_file must be configured")
	}
	if c.Token == "" && c.TokenFile == "" {
		return errors.New("one of token or token_file must be configured")
	}
	if c.Token != "" && c.TokenFile != "" {
		return errors.New("at most one of token & token_file must be configured")
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "User key",
			Description:  "The recipient user's user key.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "user_key",
			Required:     true,
			Secure:       true,
		},
		{
			Label:        "Token",
			Description:  "Your registered application's API token, see https://pushover.net/app",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "token",
			Required:     true,
			Secure:       true,
		},
		{
			Label:        "Title",
			Description:  "Notification title.",
			Placeholder:  DefaultConfig.Title,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "title",
		},
		{
			Label:        "Message",
			Description:  "Notification message.",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		{
			Label:        "URL",
			Description:  "A supplementary URL shown alongside the message.",
			Placeholder:  DefaultConfig.URL,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "url",
		},
		{
			Label:        "URL Title",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "url_title",
		},
		{
			Label:        "Device",
			Description:  "Optional device to send notification to, see https://pushover.net/api#device",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "device",
		},
		{
			Label:        "Sound",
			Description:  "Optional sound to use for notification, see https://pushover.net/api#sound",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "sound",
		},
		{
			Label:        "Priority",
			Description:  "Priority, see https://pushover.net/api#priority",
			Placeholder:  DefaultConfig.Priority,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "priority",
		},
		{
			Label:        "Retry",
			Description:  "How often the Pushover servers will send the same notification to the user. Must be at least 30 seconds.",
			Placeholder:  "1m",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "retry",
		},
		{
			Label:        "Expire",
			Description:  "How long your notification will continue to be retried for, unless the user acknowledges the notification.",
			Placeholder:  "1h",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "expire",
		},
		{
			Label:        "TTL",
			Description:  "The number of seconds before a message expires and is deleted automatically. Examples: 10s, 5m30s, 8h.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "ttl",
		},
		{
			Label:        "HTML",
			Description:  "Enables HTML formatting of the message.",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "html",
		},
		schema.V0HttpConfigOption(),
	},
}
