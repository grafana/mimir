// Copyright 2023 Prometheus Team
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

// DefaultConfig defines default values for MS Teams configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Title:   `{{ template "msteams.default.title" . }}`,
	Summary: `{{ template "msteams.default.summary" . }}`,
	Text:    `{{ template "msteams.default.text" . }}`,
}

// Config configures notifications via MS Teams.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig     *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`
	WebhookURL     *receivers.SecretURL      `yaml:"webhook_url,omitempty" json:"webhook_url,omitempty"`
	WebhookURLFile string                    `yaml:"webhook_url_file,omitempty" json:"webhook_url_file,omitempty"`

	Title   string `yaml:"title,omitempty" json:"title,omitempty"`
	Summary string `yaml:"summary,omitempty" json:"summary,omitempty"`
	Text    string `yaml:"text,omitempty" json:"text,omitempty"`
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
	TypeAlias: "msteams",
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "Webhook URL",
			Description:  "The incoming webhook URL.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "webhook_url",
			Secure:       true,
			Required:     true,
		},
		{
			Label:        "Title",
			Description:  "Message title template.",
			Placeholder:  DefaultConfig.Title,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "title",
		},
		{
			Label:        "Summary",
			Description:  "Message summary template.",
			Placeholder:  DefaultConfig.Summary,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "summary",
		},
		{
			Label:        "Text",
			Description:  "Message body template.",
			Placeholder:  DefaultConfig.Text,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "text",
		},
		schema.V0HttpConfigOption(),
	},
}
