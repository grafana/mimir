package opsgenie

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

const (
	SendTags    = "tags"
	SendDetails = "details"
	SendBoth    = "both"

	DefaultAlertsURL = "https://api.opsgenie.com/v2/alerts"
)

type Config struct {
	APIKey           string
	APIUrl           string
	Message          string
	Description      string
	AutoClose        bool
	OverridePriority bool
	SendTagsAs       string
}

func NewConfig(jsonData json.RawMessage, decryptFn receivers.DecryptFunc) (Config, error) {
	type rawSettings struct {
		APIKey           string `json:"apiKey,omitempty" yaml:"apiKey,omitempty"`
		APIUrl           string `json:"apiUrl,omitempty" yaml:"apiUrl,omitempty"`
		Message          string `json:"message,omitempty" yaml:"message,omitempty"`
		Description      string `json:"description,omitempty" yaml:"description,omitempty"`
		AutoClose        *bool  `json:"autoClose,omitempty" yaml:"autoClose,omitempty"`
		OverridePriority *bool  `json:"overridePriority,omitempty" yaml:"overridePriority,omitempty"`
		SendTagsAs       string `json:"sendTagsAs,omitempty" yaml:"sendTagsAs,omitempty"`
	}

	raw := rawSettings{}
	err := json.Unmarshal(jsonData, &raw)
	if err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	raw.APIKey = decryptFn("apiKey", raw.APIKey)
	if raw.APIKey == "" {
		return Config{}, errors.New("could not find api key property in settings")
	}
	if raw.APIUrl == "" {
		raw.APIUrl = DefaultAlertsURL
	}

	if strings.TrimSpace(raw.Message) == "" {
		raw.Message = templates.DefaultMessageTitleEmbed
	}

	switch raw.SendTagsAs {
	case SendTags, SendDetails, SendBoth:
	case "":
		raw.SendTagsAs = SendTags
	default:
		return Config{}, fmt.Errorf("invalid value for sendTagsAs: %q", raw.SendTagsAs)
	}

	if raw.AutoClose == nil {
		autoClose := true
		raw.AutoClose = &autoClose
	}
	if raw.OverridePriority == nil {
		overridePriority := true
		raw.OverridePriority = &overridePriority
	}

	return Config{
		APIKey:           raw.APIKey,
		APIUrl:           raw.APIUrl,
		Message:          raw.Message,
		Description:      raw.Description,
		AutoClose:        *raw.AutoClose,
		OverridePriority: *raw.OverridePriority,
		SendTagsAs:       raw.SendTagsAs,
	}, nil
}
