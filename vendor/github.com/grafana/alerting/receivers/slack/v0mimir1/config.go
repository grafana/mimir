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

// DefaultConfig defines default values for Slack configurations.
var DefaultConfig = Config{
	NotifierConfig: receivers.NotifierConfig{
		VSendResolved: false,
	},
	Color:      `{{ if eq .Status "firing" }}danger{{ else }}good{{ end }}`,
	Username:   `{{ template "slack.default.username" . }}`,
	Title:      `{{ template "slack.default.title" . }}`,
	TitleLink:  `{{ template "slack.default.titlelink" . }}`,
	IconEmoji:  `{{ template "slack.default.iconemoji" . }}`,
	IconURL:    `{{ template "slack.default.iconurl" . }}`,
	Pretext:    `{{ template "slack.default.pretext" . }}`,
	Text:       `{{ template "slack.default.text" . }}`,
	Fallback:   `{{ template "slack.default.fallback" . }}`,
	CallbackID: `{{ template "slack.default.callbackid" . }}`,
	Footer:     `{{ template "slack.default.footer" . }}`,
}

// Config configures notifications via Slack.
type Config struct {
	receivers.NotifierConfig `yaml:",inline" json:",inline"`

	HTTPConfig *httpcfg.HTTPClientConfig `yaml:"http_config,omitempty" json:"http_config,omitempty"`

	APIURL     *receivers.SecretURL `yaml:"api_url,omitempty" json:"api_url,omitempty"`
	APIURLFile string               `yaml:"api_url_file,omitempty" json:"api_url_file,omitempty"`

	// Slack channel override, (like #other-channel or @username).
	Channel  string `yaml:"channel,omitempty" json:"channel,omitempty"`
	Username string `yaml:"username,omitempty" json:"username,omitempty"`
	Color    string `yaml:"color,omitempty" json:"color,omitempty"`

	Title       string         `yaml:"title,omitempty" json:"title,omitempty"`
	TitleLink   string         `yaml:"title_link,omitempty" json:"title_link,omitempty"`
	Pretext     string         `yaml:"pretext,omitempty" json:"pretext,omitempty"`
	Text        string         `yaml:"text,omitempty" json:"text,omitempty"`
	Fields      []*SlackField  `yaml:"fields,omitempty" json:"fields,omitempty"`
	ShortFields bool           `yaml:"short_fields" json:"short_fields,omitempty"`
	Footer      string         `yaml:"footer,omitempty" json:"footer,omitempty"`
	Fallback    string         `yaml:"fallback,omitempty" json:"fallback,omitempty"`
	CallbackID  string         `yaml:"callback_id,omitempty" json:"callback_id,omitempty"`
	IconEmoji   string         `yaml:"icon_emoji,omitempty" json:"icon_emoji,omitempty"`
	IconURL     string         `yaml:"icon_url,omitempty" json:"icon_url,omitempty"`
	ImageURL    string         `yaml:"image_url,omitempty" json:"image_url,omitempty"`
	ThumbURL    string         `yaml:"thumb_url,omitempty" json:"thumb_url,omitempty"`
	LinkNames   bool           `yaml:"link_names" json:"link_names,omitempty"`
	MrkdwnIn    []string       `yaml:"mrkdwn_in,omitempty" json:"mrkdwn_in,omitempty"`
	Actions     []*SlackAction `yaml:"actions,omitempty" json:"actions,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *Config) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultConfig
	type plain Config
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}

	if c.APIURL != nil && len(c.APIURLFile) > 0 {
		return errors.New("at most one of api_url & api_url_file must be configured")
	}

	return nil
}

var Schema = schema.IntegrationSchemaVersion{
	Version:   Version,
	CanCreate: false,
	Options: []schema.Field{
		{
			Label:        "Webhook URL",
			Description:  "The Slack webhook URL.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "api_url",
			Secure:       true,
			Required:     true,
		},
		{
			Label:        "Channel",
			Description:  "The #channel or @user to send notifications to.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "channel",
		},
		{
			Label:        "Username",
			Placeholder:  DefaultConfig.Username,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "username",
		},
		{
			Label:        "Emoji icon",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "icon_emoji",
		},
		{
			Label:        "Icon URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "icon_url",
		},
		{
			Label:        "Names link",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "link_names",
		},
		{
			Label:        "Callback ID",
			Placeholder:  DefaultConfig.CallbackID,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "callback_id",
		},
		{
			Label:        "Color",
			Placeholder:  DefaultConfig.Color,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "color",
		},
		{
			Label:        "Fallback",
			Placeholder:  DefaultConfig.Fallback,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "fallback",
		},
		{
			Label:        "Footer",
			Placeholder:  DefaultConfig.Footer,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "footer",
		},
		{
			Label:        "Markdown Fields",
			Description:  "An array of field names that should be formatted by markdown syntax.",
			Element:      schema.ElementStringArray,
			PropertyName: "mrkdwn_in",
		},
		{
			Label:        "Pre-text",
			Placeholder:  DefaultConfig.Pretext,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "pretext",
		},
		{
			Label:        "Short Fields",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "short_fields",
		},
		{
			Label:        "Message body",
			Placeholder:  DefaultConfig.Text,
			Element:      schema.ElementTypeTextArea,
			PropertyName: "text",
		},
		{
			Label:        "Title",
			Placeholder:  DefaultConfig.Title,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "title",
		},
		{
			Label:        "Title Link",
			Placeholder:  DefaultConfig.TitleLink,
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "title_link",
		},
		{
			Label:        "Image URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "image_url",
		},
		{
			Label:        "Thumbnail URL",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "thumb_url",
		},
		{
			Label:        "Actions",
			Element:      schema.ElementSubformArray,
			PropertyName: "actions",
			SubformOptions: []schema.Field{
				{
					Label:        "Text",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "text",
					Required:     true,
				},
				{
					Label:        "Type",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "type",
					Required:     true,
				},
				{
					Label:        "URL",
					Description:  "Either url or name and value are mandatory.",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "url",
				},
				{
					Label:        "Name",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "name",
				},
				{
					Label:        "Value",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "value",
				},
				{
					Label:        "Confirm",
					Element:      schema.ElementTypeSubform,
					PropertyName: "confirm",
					SubformOptions: []schema.Field{
						{
							Label:        "Text",
							Element:      schema.ElementTypeInput,
							InputType:    schema.InputTypeText,
							PropertyName: "text",
							Required:     true,
						},
						{
							Label:        "Dismiss text",
							Element:      schema.ElementTypeInput,
							InputType:    schema.InputTypeText,
							PropertyName: "dismiss_text",
						},
						{
							Label:        "OK text",
							Element:      schema.ElementTypeInput,
							InputType:    schema.InputTypeText,
							PropertyName: "ok_text",
						},
						{
							Label:        "Title",
							Element:      schema.ElementTypeInput,
							InputType:    schema.InputTypeText,
							PropertyName: "title",
						},
					},
				},
				{
					Label:        "Style",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "style",
				},
			},
		},
		{
			Label:        "Fields",
			Element:      schema.ElementSubformArray,
			PropertyName: "fields",
			SubformOptions: []schema.Field{
				{
					Label:        "Title",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "title",
					Required:     true,
				},
				{
					Label:        "Value",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "value",
					Required:     true,
				},
				{
					Label:        "Short",
					Element:      schema.ElementTypeCheckbox,
					PropertyName: "short",
				},
			},
		},
		schema.V0HttpConfigOption(),
	},
}

// SlackAction configures a single Slack action that is sent with each notification.
// See https://api.slack.com/docs/message-attachments#action_fields and https://api.slack.com/docs/message-buttons
// for more information.
type SlackAction struct {
	Type         string                  `yaml:"type,omitempty"  json:"type,omitempty"`
	Text         string                  `yaml:"text,omitempty"  json:"text,omitempty"`
	URL          string                  `yaml:"url,omitempty"   json:"url,omitempty"`
	Style        string                  `yaml:"style,omitempty" json:"style,omitempty"`
	Name         string                  `yaml:"name,omitempty"  json:"name,omitempty"`
	Value        string                  `yaml:"value,omitempty"  json:"value,omitempty"`
	ConfirmField *SlackConfirmationField `yaml:"confirm,omitempty"  json:"confirm,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for SlackAction.
func (c *SlackAction) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain SlackAction
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.Type == "" {
		return errors.New("missing type in Slack action configuration")
	}
	if c.Text == "" {
		return errors.New("missing text in Slack action configuration")
	}
	if c.URL != "" {
		// Clear all message action fields.
		c.Name = ""
		c.Value = ""
		c.ConfirmField = nil
	} else if c.Name != "" {
		c.URL = ""
	} else {
		return errors.New("missing name or url in Slack action configuration")
	}
	return nil
}

// SlackConfirmationField protect users from destructive actions or particularly distinguished decisions
// by asking them to confirm their button click one more time.
// See https://api.slack.com/docs/interactive-message-field-guide#confirmation_fields for more information.
type SlackConfirmationField struct {
	Text        string `yaml:"text,omitempty"  json:"text,omitempty"`
	Title       string `yaml:"title,omitempty"  json:"title,omitempty"`
	OkText      string `yaml:"ok_text,omitempty"  json:"ok_text,omitempty"`
	DismissText string `yaml:"dismiss_text,omitempty"  json:"dismiss_text,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for SlackConfirmationField.
func (c *SlackConfirmationField) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain SlackConfirmationField
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.Text == "" {
		return errors.New("missing text in Slack confirmation configuration")
	}
	return nil
}

// SlackField configures a single Slack field that is sent with each notification.
// Each field must contain a title, value, and optionally, a boolean value to indicate if the field
// is short enough to be displayed next to other fields designated as short.
// See https://api.slack.com/docs/message-attachments#fields for more information.
type SlackField struct {
	Title string `yaml:"title,omitempty" json:"title,omitempty"`
	Value string `yaml:"value,omitempty" json:"value,omitempty"`
	Short *bool  `yaml:"short,omitempty" json:"short,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface for SlackField.
func (c *SlackField) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain SlackField
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	if c.Title == "" {
		return errors.New("missing title in Slack field configuration")
	}
	if c.Value == "" {
		return errors.New("missing value in Slack field configuration")
	}
	return nil
}
