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
	"fmt"
	"regexp"
	"strings"
	"text/template"

	"github.com/grafana/alerting/receivers"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultConfig defines default values for OpsGenie configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Message:     `{{ template "opsgenie.default.message" . }}`,
	Description: `{{ template "opsgenie.default.description" . }}`,
	Source:      `{{ template "opsgenie.default.source" . }}`,
}

// Config configures notifications via OpsGenie.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIKey       receivers.Secret  `yaml:"api_key,omitempty" json:"api_key,omitempty"`
	APIKeyFile   string            `yaml:"api_key_file,omitempty" json:"api_key_file,omitempty"`
	APIURL       *receivers.URL    `yaml:"api_url,omitempty" json:"api_url,omitempty"`
	Message      string            `yaml:"message,omitempty" json:"message,omitempty"`
	Description  string            `yaml:"description,omitempty" json:"description,omitempty"`
	Source       string            `yaml:"source,omitempty" json:"source,omitempty"`
	Details      map[string]string `yaml:"details,omitempty" json:"details,omitempty"`
	Entity       string            `yaml:"entity,omitempty" json:"entity,omitempty"`
	Responders   []Responder       `yaml:"responders,omitempty" json:"responders,omitempty"`
	Actions      string            `yaml:"actions,omitempty" json:"actions,omitempty"`
	Tags         string            `yaml:"tags,omitempty" json:"tags,omitempty"`
	Note         string            `yaml:"note,omitempty" json:"note,omitempty"`
	Priority     string            `yaml:"priority,omitempty" json:"priority,omitempty"`
	UpdateAlerts bool              `yaml:"update_alerts,omitempty" json:"update_alerts,omitempty"`
}

type Responder struct {
	// One of those 3 should be filled.
	ID       string `yaml:"id,omitempty" json:"id,omitempty"`
	Name     string `yaml:"name,omitempty" json:"name,omitempty"`
	Username string `yaml:"username,omitempty" json:"username,omitempty"`

	// team, user, escalation, schedule etc.
	Type string `yaml:"type,omitempty" json:"type,omitempty"`
}

const opsgenieValidTypesRe = `^(team|teams|user|escalation|schedule)$`

var opsgenieTypeMatcher = regexp.MustCompile(opsgenieValidTypesRe)

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.APIKey != "" && len(c.APIKeyFile) > 0 {
		return errors.New("at most one of api_key & api_key_file must be configured")
	}

	for _, r := range c.Responders {
		if r.ID == "" && r.Username == "" && r.Name == "" {
			return fmt.Errorf("opsGenieConfig responder %v has to have at least one of id, username or name specified", r)
		}

		if strings.Contains(r.Type, "{{") {
			_, err := template.New("").Parse(r.Type)
			if err != nil {
				return fmt.Errorf("opsGenieConfig responder %v type is not a valid template: %w", r, err)
			}
		} else {
			r.Type = strings.ToLower(r.Type)
			if !opsgenieTypeMatcher.MatchString(r.Type) {
				return fmt.Errorf("opsGenieConfig responder %v type does not match valid options %s", r, opsgenieValidTypesRe)
			}
		}
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:    Version,
	CanCreate:  false,
	Deprecated: true,
	Options: []schema.Field{
		{
			Label:        "API key",
			Description:  "The API key to use when talking to the OpsGenie API.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_key",
			Secure:       true,
			Required:     true,
		},
		{
			Label:        "API URL",
			Description:  "The host to send OpsGenie API requests to.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
			Required:     true,
		},
		{
			Label:        "Message",
			Description:  "Alert text limited to 130 characters.",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		{
			Label:        "Description",
			Description:  "A description of the incident.",
			Placeholder:  DefaultConfig.Description,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "description",
		},
		{
			Label:        "Source",
			Description:  "A backlink to the sender of the notification.",
			Placeholder:  DefaultConfig.Source,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "source",
		},
		{
			Label:        "Details",
			Description:  "A set of arbitrary key/value pairs that provide further detail about the incident.",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "details",
		},
		{
			Label:        "Entity",
			Description:  "Optional field that can be used to specify which domain alert is related to.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "entity",
		},
		{
			Label:        "Actions",
			Description:  "Comma separated list of actions that will be available for the alert.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "actions",
		},
		{
			Label:        "Tags",
			Description:  "Comma separated list of tags attached to the notifications.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "tags",
		},
		{
			Label:        "Note",
			Description:  "Additional alert note.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "note",
		},
		{
			Label:        "Priority",
			Description:  "Priority level of alert. Possible values are P1, P2, P3, P4, and P5.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "priority",
		},
		{
			Label:        "Update Alerts",
			Description:  "Whether to update message and description of the alert in OpsGenie if it already exists. By default, the alert is never updated in OpsGenie, the new message only appears in activity log.",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "update_alerts",
		},
		{
			Label:        "Responders",
			Description:  "List of responders responsible for notifications.",
			Element:      schema.ElementSubformArray,
			PropertyName: "responders",
			SubformOptions: []schema.Field{
				{
					Label:        "Type",
					Description:  "\"team\", \"user\", \"escalation\" or schedule\".",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "type",
				},
				{
					Label:        "ID",
					Description:  "Exactly one of these fields should be defined.",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "id",
				},
				{
					Label:        "Name",
					Description:  "Exactly one of these fields should be defined.",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "name",
				},
				{
					Label:        "Username",
					Description:  "Exactly one of these fields should be defined.",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "username",
				},
			},
		},
		schema.V0HttpConfigOption(),
	},
}
