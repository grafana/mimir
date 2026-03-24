// Copyright 2015 Prometheus Team
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
	"net/textproto"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

const Version = schema.V0mimir1

// DefaultEmailSubject defines the default Subject header of an Email.
var DefaultEmailSubject = `{{ template "email.default.subject" . }}`

// DefaultConfig defines default values for Email configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: false,
	},
	HTML: `{{ template "email.default.html" . }}`,
	Text: ``,
}

// Config configures notifications via mail.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	// Email address to notify.
	To               string             `yaml:"to,omitempty" json:"to,omitempty"`
	From             string             `yaml:"from,omitempty" json:"from,omitempty"`
	Hello            string             `yaml:"hello,omitempty" json:"hello,omitempty"`
	Smarthost        receivers.HostPort `yaml:"smarthost,omitempty" json:"smarthost,omitempty"`
	AuthUsername     string             `yaml:"auth_username,omitempty" json:"auth_username,omitempty"`
	AuthPassword     receivers.Secret   `yaml:"auth_password,omitempty" json:"auth_password,omitempty"`
	AuthPasswordFile string             `yaml:"auth_password_file,omitempty" json:"auth_password_file,omitempty"`
	AuthSecret       receivers.Secret   `yaml:"auth_secret,omitempty" json:"auth_secret,omitempty"`
	AuthIdentity     string             `yaml:"auth_identity,omitempty" json:"auth_identity,omitempty"`
	Headers          map[string]string  `yaml:"headers,omitempty" json:"headers,omitempty"`
	HTML             string             `yaml:"html,omitempty" json:"html,omitempty"`
	Text             string             `yaml:"text,omitempty" json:"text,omitempty"`
	RequireTLS       *bool              `yaml:"require_tls,omitempty" json:"require_tls,omitempty"`
	TLSConfig        httpcfg.TLSConfig  `yaml:"tls_config,omitempty" json:"tls_config,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.To == "" {
		return errors.New("missing to address in email config")
	}
	// Header names are case-insensitive, check for collisions.
	normalizedHeaders := map[string]string{}
	for h, v := range c.Headers {
		normalized := textproto.CanonicalMIMEHeaderKey(h)
		if _, ok := normalizedHeaders[normalized]; ok {
			return fmt.Errorf("duplicate header %q in email config", normalized)
		}
		normalizedHeaders[normalized] = v
	}
	c.Headers = normalizedHeaders

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "To",
			Description:  "The email address to send notifications to. You can enter multiple addresses using a \",\" separator. You can use templates to customize this field.",
			Element:      schema.ElementTypeTextArea,
			PropertyName: "to",
			Required:     true,
		},
		{
			Label:        "From",
			Description:  "The sender address.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "from",
		},
		{
			Label:        "SMTP host",
			Description:  "The SMTP host and port through which emails are sent.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "smarthost",
		},
		{
			Label:        "Hello",
			Description:  "The hostname to identify to the SMTP server.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "hello",
		},
		{
			Label:        "Username",
			Description:  "SMTP authentication information",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "auth_username",
		},
		{
			Label:        "Password",
			Description:  "SMTP authentication information",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypePassword,
			PropertyName: "auth_password",
			Secure:       true,
		},
		{
			Label:        "Secret",
			Description:  "SMTP authentication information",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypePassword,
			PropertyName: "auth_secret",
			Secure:       true,
		},
		{
			Label:        "Identity",
			Description:  "SMTP authentication information",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "auth_identity",
		},
		{
			Label:        "Require TLS",
			Description:  "The SMTP TLS requirement",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "require_tls",
		},
		{
			Label:        "Email HTML body",
			Description:  "The HTML body of the email notification.",
			Placeholder:  DefaultConfig.HTML,
			Element:      schema.ElementTypeTextArea,
			PropertyName: "html",
		},
		{
			Label:        "Email text body",
			Description:  "The text body of the email notification.",
			Placeholder:  DefaultConfig.Text,
			Element:      schema.ElementTypeTextArea,
			PropertyName: "text",
		},
		{
			Label:        "Headers",
			Description:  "Further headers email header key/value pairs. Overrides any headers previously set by the notification implementation.",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "headers",
		},
		schema.V0TLSConfigOption("tls_config"),
	},
}
