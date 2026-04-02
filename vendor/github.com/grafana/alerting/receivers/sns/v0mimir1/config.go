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

// DefaultConfig defines default values for SNS configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: true,
	},
	Subject: `{{ template "sns.default.subject" . }}`,
	Message: `{{ template "sns.default.message" . }}`,
}

// Config configures notifications via SNS.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIUrl      string            `yaml:"api_url,omitempty" json:"api_url,omitempty"`
	Sigv4       SigV4Config       `yaml:"sigv4" json:"sigv4"`
	TopicARN    string            `yaml:"topic_arn,omitempty" json:"topic_arn,omitempty"`
	PhoneNumber string            `yaml:"phone_number,omitempty" json:"phone_number,omitempty"`
	TargetARN   string            `yaml:"target_arn,omitempty" json:"target_arn,omitempty"`
	Subject     string            `yaml:"subject,omitempty" json:"subject,omitempty"`
	Message     string            `yaml:"message,omitempty" json:"message,omitempty"`
	Attributes  map[string]string `yaml:"attributes,omitempty" json:"attributes,omitempty"`
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
	if decrypted, ok := decrypt.DecryptSecret(schema.NewIntegrationFieldPath("sigv4", "secret_key").String()); ok {
		settings.Sigv4.SecretKey = decrypted
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
	if err := c.Sigv4.Validate(); err != nil {
		return fmt.Errorf("invalid sigv4: %w", err)
	}
	return nil
}

func (c *Config) validate() error {
	if (c.TargetARN == "") != (c.TopicARN == "") != (c.PhoneNumber == "") {
		return errors.New("must provide either a Target ARN, Topic ARN, or Phone Number for SNS config")
	}
	return nil
}

type SigV4Config struct {
	Region    string           `yaml:"region,omitempty" json:"region,omitempty"`
	AccessKey string           `yaml:"access_key,omitempty" json:"access_key,omitempty"`
	SecretKey receivers.Secret `yaml:"secret_key,omitempty" json:"secret_key,omitempty"`
	Profile   string           `yaml:"profile,omitempty" json:"profile,omitempty"`
	RoleARN   string           `yaml:"role_arn,omitempty" json:"role_arn,omitempty"`
}

func (c *SigV4Config) Validate() error {
	return c.validate()
}

func (c *SigV4Config) validate() error {
	if (c.AccessKey == "") != (c.SecretKey == "") {
		return fmt.Errorf("must provide a AWS SigV4 Access key and Secret Key if credentials are specified in the SigV4 config")
	}
	return nil
}

func (c *SigV4Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain SigV4Config
	*c = SigV4Config{}
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return c.validate()
}

var Schema = schema.NewIntegrationSchemaVersion(schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "API URL",
			Description:  "The Amazon SNS API URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
		},
		{
			Label:        "SigV4 authentication",
			Description:  "Configures AWS's Signature Verification 4 signing process to sign requests",
			Element:      schema.ElementTypeSubform,
			PropertyName: "sigv4",
			SubformOptions: []schema.Field{
				{
					Label:        "Region",
					Description:  "The AWS region. If blank, the region from the default credentials chain is used",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "region",
				},
				{
					Label:        "Access key",
					Description:  "The AWS API access_key. If blank the environment variable \"AWS_ACCESS_KEY_ID\" is used",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "access_key",
					Secure:       false,
				},
				{
					Label:        "Secret key",
					Description:  "The AWS API secret_key. If blank the environment variable \"AWS_ACCESS_SECRET_ID\" is used",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypePassword,
					PropertyName: "secret_key",
					Secure:       true,
				},
				{
					Label:        "Profile",
					Description:  "Named AWS profile used to authenticate",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "profile",
				},
				{
					Label:        "Role ARN",
					Description:  "AWS Role ARN, an alternative to using AWS API keys",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "role_arn",
				},
			},
		},
		{
			Label:        "SNS topic ARN",
			Description:  "If you don't specify this value, you must specify a value for the phone_number or target_arn. If you are using a FIFO SNS topic you should set a message group interval longer than 5 minutes to prevent messages with the same group key being deduplicated by the SNS default deduplication window",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "topic_arn",
		},
		{
			Label:        "Phone number",
			Description:  "Phone number if message is delivered via SMS in E.164 format. If you don't specify this value, you must specify a value for the topic_arn or target_arn",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "phone_number",
		},
		{
			Label:        "Target ARN",
			Description:  "The mobile platform endpoint ARN if message is delivered via mobile notifications. If you don't specify this value, you must specify a value for the topic_arn or phone_number",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "target_arn",
		},
		{
			Label:        "Subject",
			Description:  "Subject line when the message is delivered",
			Placeholder:  DefaultConfig.Subject,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "subject",
		},
		{
			Label:        "Message",
			Description:  "The message content of the SNS notification",
			Placeholder:  DefaultConfig.Message,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "message",
		},
		{
			Label:        "Attributes",
			Description:  "SNS message attributes",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "attributes",
		},
		httpcfg.V0HttpConfigOption(),
	},
})
