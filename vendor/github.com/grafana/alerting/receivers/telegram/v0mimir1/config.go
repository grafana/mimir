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

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
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
	type plain Config
	type withFallback struct {
		plain        `yaml:",inline" json:",inline"`
		BotTokenJson receivers.Secret `yaml:"token"`
		ChatIDJson   int64            `yaml:"chat"`
	}
	pl := withFallback{plain: plain(DefaultConfig)}
	if err := unmarshal(&pl); err != nil {
		return err
	}
	*c = Config(pl.plain)
	if c.BotToken == "" && pl.BotTokenJson != "" {
		c.BotToken = pl.BotTokenJson
	}
	if c.ChatID == 0 && pl.ChatIDJson != 0 {
		c.ChatID = pl.ChatIDJson
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
	if decrypted, ok := decrypt.DecryptSecret("token"); ok {
		settings.BotToken = decrypted
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

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
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
		httpcfg.V0HttpConfigOption(),
	},
})
