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

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultPagerdutyDetails defines the default values for PagerDuty details.
var DefaultPagerdutyDetails = map[string]string{
	"firing":       `{{ template "pagerduty.default.instances" .Alerts.Firing }}`,
	"resolved":     `{{ template "pagerduty.default.instances" .Alerts.Resolved }}`,
	"num_firing":   `{{ .Alerts.Firing | len }}`,
	"num_resolved": `{{ .Alerts.Resolved | len }}`,
}

// DefaultConfig defines default values for PagerDuty configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Description: `{{ template "pagerduty.default.description" .}}`,
	Client:      `{{ template "pagerduty.default.client" . }}`,
	ClientURL:   `{{ template "pagerduty.default.clientURL" . }}`,
}

// Config configures notifications via PagerDuty.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	ServiceKey     receivers.Secret  `yaml:"service_key,omitempty" json:"service_key,omitempty"`
	ServiceKeyFile string            `yaml:"service_key_file,omitempty" json:"service_key_file,omitempty"`
	RoutingKey     receivers.Secret  `yaml:"routing_key,omitempty" json:"routing_key,omitempty"`
	RoutingKeyFile string            `yaml:"routing_key_file,omitempty" json:"routing_key_file,omitempty"`
	URL            *receivers.URL    `yaml:"url,omitempty" json:"url,omitempty"`
	Client         string            `yaml:"client,omitempty" json:"client,omitempty"`
	ClientURL      string            `yaml:"client_url,omitempty" json:"client_url,omitempty"`
	Description    string            `yaml:"description,omitempty" json:"description,omitempty"`
	Details        map[string]string `yaml:"details,omitempty" json:"details,omitempty"`
	Images         []PagerdutyImage  `yaml:"images,omitempty" json:"images,omitempty"`
	Links          []PagerdutyLink   `yaml:"links,omitempty" json:"links,omitempty"`
	Source         string            `yaml:"source,omitempty" json:"source,omitempty"`
	Severity       string            `yaml:"severity,omitempty" json:"severity,omitempty"`
	Class          string            `yaml:"class,omitempty" json:"class,omitempty"`
	Component      string            `yaml:"component,omitempty" json:"component,omitempty"`
	Group          string            `yaml:"group,omitempty" json:"group,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.RoutingKey == "" && c.ServiceKey == "" && c.RoutingKeyFile == "" && c.ServiceKeyFile == "" {
		return errors.New("missing service or routing key in PagerDuty config")
	}
	if len(c.RoutingKey) > 0 && len(c.RoutingKeyFile) > 0 {
		return errors.New("at most one of routing_key & routing_key_file must be configured")
	}
	if len(c.ServiceKey) > 0 && len(c.ServiceKeyFile) > 0 {
		return errors.New("at most one of service_key & service_key_file must be configured")
	}
	if c.Details == nil {
		c.Details = make(map[string]string)
	}
	if c.Source == "" {
		c.Source = c.Client
	}
	for k, v := range DefaultPagerdutyDetails {
		if _, ok := c.Details[k]; !ok {
			c.Details[k] = v
		}
	}
	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "URL",
			Description:  "The URL to send API requests to",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "url",
			Required:     true,
		},
		{
			Label:        "Routing key",
			Description:  "The PagerDuty integration key (when using PagerDuty integration type `Events API v2`)",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "routing_key",
			Secure:       true,
			Required:     true,
		},
		{
			Label:        "Service key",
			Description:  "The PagerDuty integration key (when using PagerDuty integration type `Prometheus`).",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "service_key",
			Secure:       true,
			Required:     true,
		},
		{
			Label:        "Client",
			Description:  "The client identification of the Alertmanager.",
			Placeholder:  DefaultConfig.Client,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "client",
		},
		{
			Label:        "Client URL",
			Description:  "A backlink to the sender of the notification.",
			Placeholder:  DefaultConfig.ClientURL,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "client_url",
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
			Label:        "Details",
			Description:  "A set of arbitrary key/value pairs that provide further detail about the incident.",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "details",
		},
		{
			Label:        "Images",
			Description:  "Images to attach to the incident.",
			Element:      schema.ElementSubformArray,
			PropertyName: "images",
			SubformOptions: []schema.Field{
				{
					Label:        "URL",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "href",
				},
				{
					Label:        "Source",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "source",
				},
				{
					Label:        "Alt",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "alt",
				},
			},
		},
		{
			Label:        "Links",
			Description:  "Links to attach to the incident.",
			Element:      schema.ElementSubformArray,
			PropertyName: "links",
			SubformOptions: []schema.Field{
				{
					Label:        "URL",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "href",
				},
				{
					Label:        "Text",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "text",
				},
			},
		},
		{
			Label:        "Source",
			Description:  "Unique location of the affected system.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "source",
		},
		{
			Label:        "Severity",
			Description:  "Severity of the incident.",
			Placeholder:  "error",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "severity",
		},
		{
			Label:        "Class",
			Description:  "The class/type of the event.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "class",
		},
		{
			Label:        "Component",
			Description:  "The part or component of the affected system that is broken.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "component",
		},
		{
			Label:        "Group",
			Description:  "A cluster or grouping of sources.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "group",
		},
		schema.V0HttpConfigOption(),
	},
}

// PagerdutyLink is used to add link to an incident.
type PagerdutyLink struct {
	Href string `yaml:"href,omitempty" json:"href,omitempty"`
	Text string `yaml:"text,omitempty" json:"text,omitempty"`
}

// PagerdutyImage is an image attached to an incident.
type PagerdutyImage struct {
	Src  string `yaml:"src,omitempty" json:"src,omitempty"`
	Alt  string `yaml:"alt,omitempty" json:"alt,omitempty"`
	Href string `yaml:"href,omitempty" json:"href,omitempty"`
}
