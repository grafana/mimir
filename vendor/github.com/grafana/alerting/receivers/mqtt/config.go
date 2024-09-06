package mqtt

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

const (
	MessageFormatJSON string = "json"
	MessageFormatText string = "text"
)

type Config struct {
	BrokerURL          string `json:"brokerUrl,omitempty" yaml:"brokerUrl,omitempty"`
	ClientID           string `json:"clientId,omitempty" yaml:"clientId,omitempty"`
	Topic              string `json:"topic,omitempty" yaml:"topic,omitempty"`
	Message            string `json:"message,omitempty" yaml:"message,omitempty"`
	MessageFormat      string `json:"messageFormat,omitempty" yaml:"messageFormat,omitempty"`
	Username           string `json:"username,omitempty" yaml:"username,omitempty"`
	Password           string `json:"password,omitempty" yaml:"password,omitempty"`
	InsecureSkipVerify bool   `json:"insecureSkipVerify,omitempty" yaml:"insecureSkipVerify,omitempty"`
}

func NewConfig(jsonData json.RawMessage, decryptFn receivers.DecryptFunc) (Config, error) {
	var settings Config
	err := json.Unmarshal(jsonData, &settings)
	if err != nil {
		return Config{}, fmt.Errorf("failed to unmarshal settings: %w", err)
	}

	if settings.BrokerURL == "" {
		return Config{}, errors.New("MQTT broker URL must be specified")
	}

	if settings.Topic == "" {
		return Config{}, errors.New("MQTT topic must be specified")
	}

	if settings.ClientID == "" {
		settings.ClientID = fmt.Sprintf("grafana_%d", rand.Int31())
	}

	if settings.Message == "" {
		settings.Message = templates.DefaultMessageEmbed
	}

	if settings.MessageFormat == "" {
		settings.MessageFormat = MessageFormatJSON
	}
	if settings.MessageFormat != MessageFormatJSON && settings.MessageFormat != MessageFormatText {
		return Config{}, errors.New("Invalid message format, must be 'json' or 'text'")
	}

	password := decryptFn("password", settings.Password)
	settings.Password = password

	return settings, nil
}
