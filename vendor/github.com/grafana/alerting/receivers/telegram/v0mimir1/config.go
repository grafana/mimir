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

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for Telegram configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	DisableNotifications: false,
	Message:              `{{ template "telegram.default.message" . }}`,
	ParseMode:            "HTML",
}

// Config configures notifications via Telegram.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIUrl               *receivers.URL   `yaml:"api_url" json:"api_url,omitempty"`
	BotToken             receivers.Secret `yaml:"bot_token,omitempty" json:"token,omitempty"`
	BotTokenFile         string           `yaml:"bot_token_file,omitempty" json:"token_file,omitempty"`
	ChatID               int64            `yaml:"chat_id,omitempty" json:"chat,omitempty"`
	Message              string           `yaml:"message,omitempty" json:"message,omitempty"`
	DisableNotifications bool             `yaml:"disable_notifications,omitempty" json:"disable_notifications,omitempty"`
	ParseMode            string           `yaml:"parse_mode,omitempty" json:"parse_mode,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.BotToken == "" && c.BotTokenFile == "" {
		return errors.New("missing bot_token or bot_token_file on telegram_config")
	}
	if c.BotToken != "" && c.BotTokenFile != "" {
		return errors.New("at most one of bot_token & bot_token_file must be configured")
	}
	if c.ChatID == 0 {
		return errors.New("missing chat_id on telegram_config")
	}
	if c.ParseMode != "" &&
		c.ParseMode != "Markdown" &&
		c.ParseMode != "MarkdownV2" &&
		c.ParseMode != "HTML" {
		return errors.New("unknown parse_mode on telegram_config, must be Markdown, MarkdownV2, HTML or empty string")
	}
	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API URL",
			Description:  "The Telegram API URL",
			Placeholder:  "https://api.telegram.org",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
		},
		{
			Label:        "Bot token",
			Description:  "Telegram bot token",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "token",
			Required:     true,
			Secure:       true,
		},
		{
			Label:        "Chat ID",
			Description:  "ID of the chat where to send the messages",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "chat_id",
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
		{
			Label:        "Disable notifications",
			Description:  "Disable telegram notifications",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "disable_notifications",
		},
		{
			Label:        "Parse mode",
			Description:  "Parse mode for telegram message",
			Element:      schema.ElementTypeSelect,
			PropertyName: "parse_mode",
			SelectOptions: []schema.SelectOption{
				{Value: "", Label: "None"},
				{Value: "MarkdownV2", Label: "MarkdownV2"},
				{Value: "Markdown", Label: "Markdown"},
				{Value: "HTML", Label: "HTML"},
			},
		},
		schema.V0HttpConfigOption(),
	},
}
