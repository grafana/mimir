package webhook

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

type Config struct {
	URL        string
	HTTPMethod string
	MaxAlerts  int
	// Authorization Header.
	AuthorizationScheme      string
	AuthorizationCredentials string
	// HTTP Basic Authentication.
	User     string
	Password string

	Title     string
	Message   string
	TLSConfig *receivers.TLSConfig
}

func NewConfig(jsonData json.RawMessage, decryptFn receivers.DecryptFunc) (Config, error) {
	settings := Config{}
	rawSettings := struct {
		URL                      string                   `json:"url,omitempty" yaml:"url,omitempty"`
		HTTPMethod               string                   `json:"httpMethod,omitempty" yaml:"httpMethod,omitempty"`
		MaxAlerts                receivers.OptionalNumber `json:"maxAlerts,omitempty" yaml:"maxAlerts,omitempty"`
		AuthorizationScheme      string                   `json:"authorization_scheme,omitempty" yaml:"authorization_scheme,omitempty"`
		AuthorizationCredentials string                   `json:"authorization_credentials,omitempty" yaml:"authorization_credentials,omitempty"`
		User                     string                   `json:"username,omitempty" yaml:"username,omitempty"`
		Password                 string                   `json:"password,omitempty" yaml:"password,omitempty"`
		Title                    string                   `json:"title,omitempty" yaml:"title,omitempty"`
		Message                  string                   `json:"message,omitempty" yaml:"message,omitempty"`
		TLSConfig                *receivers.TLSConfig     `json:"tlsConfig,omitempty" yaml:"tlsConfig,omitempty"`
	}{}

	err := json.Unmarshal(jsonData, &rawSettings)
	if err != nil {
		return settings, fmt.Errorf("failed to unmarshal settings: %w", err)
	}
	if rawSettings.URL == "" {
		return settings, errors.New("required field 'url' is not specified")
	}
	settings.URL = rawSettings.URL
	settings.AuthorizationScheme = rawSettings.AuthorizationScheme

	if rawSettings.HTTPMethod == "" {
		rawSettings.HTTPMethod = http.MethodPost
	}
	settings.HTTPMethod = rawSettings.HTTPMethod

	if rawSettings.MaxAlerts != "" {
		settings.MaxAlerts, _ = strconv.Atoi(rawSettings.MaxAlerts.String())
	}

	settings.User = decryptFn("username", rawSettings.User)
	settings.Password = decryptFn("password", rawSettings.Password)
	settings.AuthorizationCredentials = decryptFn("authorization_credentials", rawSettings.AuthorizationCredentials)

	if settings.AuthorizationCredentials != "" && settings.AuthorizationScheme == "" {
		settings.AuthorizationScheme = "Bearer"
	}
	if settings.User != "" && settings.Password != "" && settings.AuthorizationScheme != "" && settings.AuthorizationCredentials != "" {
		return settings, errors.New("both HTTP Basic Authentication and Authorization Header are set, only 1 is permitted")
	}
	settings.Title = rawSettings.Title
	if settings.Title == "" {
		settings.Title = templates.DefaultMessageTitleEmbed
	}
	settings.Message = rawSettings.Message
	if settings.Message == "" {
		settings.Message = templates.DefaultMessageEmbed
	}

	if tlsConfig := rawSettings.TLSConfig; tlsConfig != nil {
		settings.TLSConfig = &receivers.TLSConfig{
			InsecureSkipVerify: tlsConfig.InsecureSkipVerify,
			CACertificate:      decryptFn("tlsConfig.caCertificate", tlsConfig.CACertificate),
			ClientCertificate:  decryptFn("tlsConfig.clientCertificate", tlsConfig.ClientCertificate),
			ClientKey:          decryptFn("tlsConfig.clientKey", tlsConfig.ClientKey),
		}
	}

	return settings, err
}
