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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/prometheus/common/model"

	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for Jira configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Summary:     `{{ template "jira.default.summary" . }}`,
	Description: `{{ template "jira.default.description" . }}`,
	Priority:    `{{ template "jira.default.priority" . }}`,
}

// Config configures notifications via JIRA.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`
	HTTPConfig               *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIURL *receivers.URL `yaml:"api_url,omitempty" json:"api_url,omitempty"`

	Project     string   `yaml:"project,omitempty" json:"project,omitempty"`
	Summary     string   `yaml:"summary,omitempty" json:"summary,omitempty"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	Labels      []string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Priority    string   `yaml:"priority,omitempty" json:"priority,omitempty"`
	IssueType   string   `yaml:"issue_type,omitempty" json:"issue_type,omitempty"`

	ReopenTransition  string         `yaml:"reopen_transition,omitempty" json:"reopen_transition,omitempty"`
	ResolveTransition string         `yaml:"resolve_transition,omitempty" json:"resolve_transition,omitempty"`
	WontFixResolution string         `yaml:"wont_fix_resolution,omitempty" json:"wont_fix_resolution,omitempty"`
	ReopenDuration    model.Duration `yaml:"reopen_duration,omitempty" json:"reopen_duration,omitempty"`

	Fields map[string]any `yaml:"fields,omitempty" json:"custom_fields,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type Plain Config
	type withFallback struct {
		Plain            `yaml:",inline" json:",inline"`
		CustomFieldsJson map[string]any `yaml:"custom_fields"`
	}
	pl := withFallback{Plain: Plain(DefaultConfig)}
	if err := unmarshal(&pl); err != nil {
		return err
	}
	*c = Config(pl.Plain)
	if c.Fields == nil && pl.CustomFieldsJson != nil {
		c.Fields = pl.CustomFieldsJson
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
		return errors.New("missing api_url in jira_config")
	}
	if c.HTTPConfig != nil {
		if err := c.HTTPConfig.Validate(); err != nil {
			return fmt.Errorf("invalid http_config: %w", err)
		}
	}
	return nil
}

func (c *Config) validate() error {
	if c.Project == "" {
		return errors.New("missing project in jira_config")
	}
	if c.IssueType == "" {
		return errors.New("missing issue_type in jira_config")
	}

	return nil
}

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API URL",
			Description:  "The host to send Jira API requests to",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
			Required:     true,
		},
		{
			Label:        "Project Key",
			Description:  "The project key where issues are created",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "project",
			Required:     true,
		},
		{
			Label:        "Issue Type",
			Description:  "Type of the issue (e.g. Bug)",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "issue_type",
			Required:     true,
		},
		{
			Label:        "Summary",
			Description:  "Issue summary template",
			Placeholder:  DefaultConfig.Summary,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "summary",
		},
		{
			Label:        "Description",
			Description:  "Issue description template",
			Placeholder:  DefaultConfig.Description,
			Element:      schema.ElementTypeTextArea,
			PropertyName: "description",
		},
		{
			Label:        "Labels",
			Description:  " Labels to be added to the issue",
			Element:      schema.ElementStringArray,
			PropertyName: "labels",
		},
		{
			Label:        "Priority",
			Description:  "Priority of the issue",
			Placeholder:  DefaultConfig.Priority,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "priority",
		},
		{
			Label:        "Reopen transition",
			Description:  "Name of the workflow transition to reopen an issue. The target status should not have the category \"done\"",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "reopen_transition",
		},
		{
			Label:        "Resolve transition",
			Description:  "Name of the workflow transition to resolve an issue. The target status must have the category \"done\"",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "resolve_transition",
		},
		{
			Label:        "Won't fix resolution",
			Description:  "If \"Reopen transition\" is defined, ignore issues with that resolution",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "wont_fix_resolution",
		},
		{
			Label:        "Reopen duration",
			Description:  "If \"Reopen transition\" is defined, reopen the issue when it is not older than this value (rounded down to the nearest minute)",
			Placeholder:  "Use duration format, for example: 1.2s, 100ms",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "reopen_duration",
		},
		{
			Label:        "Fields",
			Description:  "Other issue and custom fields",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "fields",
		},
		httpcfg.V0HttpConfigOption(),
	},
})
