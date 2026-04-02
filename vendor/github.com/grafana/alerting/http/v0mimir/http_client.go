// Copyright 2016 The Prometheus Authors
// Modifications Copyright Grafana Labs
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

package v0mimir

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	commoncfg "github.com/prometheus/common/config"

	"github.com/grafana/alerting/receivers/schema"
)

// DefaultHTTPClientConfig is the default HTTP client configuration.
var DefaultHTTPClientConfig = HTTPClientConfig{
	FollowRedirects: true,
	EnableHTTP2:     true,
}

// BasicAuth contains basic HTTP authentication credentials.
type BasicAuth struct {
	Username     string `yaml:"username" json:"username"`
	UsernameFile string `yaml:"username_file,omitempty" json:"username_file,omitempty"`
	// UsernameRef is the name of the secret within the secret manager to use as the username.
	UsernameRef  string           `yaml:"username_ref,omitempty" json:"username_ref,omitempty"`
	Password     commoncfg.Secret `yaml:"password,omitempty" json:"password,omitempty"`
	PasswordFile string           `yaml:"password_file,omitempty" json:"password_file,omitempty"`
	// PasswordRef is the name of the secret within the secret manager to use as the password.
	PasswordRef string `yaml:"password_ref,omitempty" json:"password_ref,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (a *BasicAuth) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain BasicAuth
	return unmarshal((*plain)(a))
}

// Authorization contains HTTP authorization credentials.
type Authorization struct {
	Type            string           `yaml:"type,omitempty" json:"type,omitempty"`
	Credentials     commoncfg.Secret `yaml:"credentials,omitempty" json:"credentials,omitempty"`
	CredentialsFile string           `yaml:"credentials_file,omitempty" json:"credentials_file,omitempty"`
	// CredentialsRef is the name of the secret within the secret manager to use as credentials.
	CredentialsRef string `yaml:"credentials_ref,omitempty" json:"credentials_ref,omitempty"`
}

// OAuth2 is the oauth2 client configuration.
type OAuth2 struct {
	ClientID         string           `yaml:"client_id" json:"client_id"`
	ClientSecret     commoncfg.Secret `yaml:"client_secret" json:"client_secret"`
	ClientSecretFile string           `yaml:"client_secret_file" json:"client_secret_file"`
	// ClientSecretRef is the name of the secret within the secret manager to use as the client
	// secret.
	ClientSecretRef string            `yaml:"client_secret_ref" json:"client_secret_ref"`
	Scopes          []string          `yaml:"scopes,omitempty" json:"scopes,omitempty"`
	TokenURL        string            `yaml:"token_url" json:"token_url"`
	EndpointParams  map[string]string `yaml:"endpoint_params,omitempty" json:"endpoint_params,omitempty"`
	TLSConfig       TLSConfig         `yaml:"tls_config,omitempty" json:"tls_config,omitempty"`
	ProxyConfig     `yaml:",inline"`
}

func (o *OAuth2) Validate() error {
	if err := o.validate(); err != nil {
		return err
	}
	if err := o.ProxyConfig.Validate(); err != nil {
		return fmt.Errorf("invalid proxy config: %w", err)
	}
	if err := o.TLSConfig.Validate(); err != nil {
		return fmt.Errorf("invalid tls_config: %w", err)
	}
	return nil
}

// validate validates the OAuth2 Config.
func (o *OAuth2) validate() error {
	if len(o.ClientID) == 0 {
		return errors.New("oauth2 client_id must be configured")
	}
	if len(o.TokenURL) == 0 {
		return errors.New("oauth2 token_url must be configured")
	}
	if nonZeroCount(len(o.ClientSecret) > 0, len(o.ClientSecretFile) > 0, len(o.ClientSecretRef) > 0) > 1 {
		return errors.New("at most one of oauth2 client_secret, client_secret_file & client_secret_ref must be configured")
	}
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
func (o *OAuth2) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain OAuth2
	if err := unmarshal((*plain)(o)); err != nil {
		return err
	}
	return o.validate()
}

// UnmarshalJSON implements the json.Unmarshaler interface for OAuth2.
func (o *OAuth2) UnmarshalJSON(data []byte) error {
	type plain OAuth2
	if err := json.Unmarshal(data, (*plain)(o)); err != nil {
		return err
	}
	return o.validate()
}

// ReservedHeaders that change the connection, are set by Prometheus, or can
// be changed otherwise.
var ReservedHeaders = map[string]struct{}{
	"Authorization":                       {},
	"Host":                                {},
	"Content-Encoding":                    {},
	"Content-Length":                      {},
	"Content-Type":                        {},
	"User-Agent":                          {},
	"Connection":                          {},
	"Keep-Alive":                          {},
	"Proxy-Authenticate":                  {},
	"Proxy-Authorization":                 {},
	"Www-Authenticate":                    {},
	"Accept-Encoding":                     {},
	"X-Prometheus-Remote-Write-Version":   {},
	"X-Prometheus-Remote-Read-Version":    {},
	"X-Prometheus-Scrape-Timeout-Seconds": {},

	// Added by SigV4.
	"X-Amz-Date":           {},
	"X-Amz-Security-Token": {},
	"X-Amz-Content-Sha256": {},
}

// Headers represents the configuration for HTTP headers.
type Headers map[string]Header

func (h Headers) Validate() error { return h.validate() }

// validate validates the Headers config.
func (h Headers) validate() error {
	for n := range h {
		if _, ok := ReservedHeaders[http.CanonicalHeaderKey(n)]; ok {
			return fmt.Errorf("setting header %q is not allowed", http.CanonicalHeaderKey(n))
		}
	}
	return nil
}

// Header represents the configuration for a single HTTP header.
type Header struct {
	Values  []string           `yaml:"values,omitempty" json:"values,omitempty"`
	Secrets []commoncfg.Secret `yaml:"secrets,omitempty" json:"secrets,omitempty"`
	Files   []string           `yaml:"files,omitempty" json:"files,omitempty"`
}

// HTTPClientConfig configures an HTTP client.
type HTTPClientConfig struct {
	// The HTTP basic authentication credentials for the targets.
	BasicAuth *BasicAuth `yaml:"basic_auth,omitempty" json:"basic_auth,omitempty"`
	// The HTTP authorization credentials for the targets.
	Authorization *Authorization `yaml:"authorization,omitempty" json:"authorization,omitempty"`
	// The OAuth2 client credentials used to fetch a token for the targets.
	OAuth2 *OAuth2 `yaml:"oauth2,omitempty" json:"oauth2,omitempty"`
	// The bearer token for the targets. Deprecated in favour of
	// Authorization.Credentials.
	BearerToken commoncfg.Secret `yaml:"bearer_token,omitempty" json:"bearer_token,omitempty"`
	// The bearer token file for the targets. Deprecated in favour of
	// Authorization.CredentialsFile.now
	BearerTokenFile string `yaml:"bearer_token_file,omitempty" json:"bearer_token_file,omitempty"`
	// TLSConfig to use to connect to the targets.
	TLSConfig TLSConfig `yaml:"tls_config,omitempty" json:"tls_config,omitempty"`
	// FollowRedirects specifies whether the client should follow HTTP 3xx redirects.
	// The omitempty flag is not set, because it would be hidden from the
	// marshalled configuration when set to false.
	FollowRedirects bool `yaml:"follow_redirects" json:"follow_redirects"`
	// EnableHTTP2 specifies whether the client should configure HTTP2.
	// The omitempty flag is not set, because it would be hidden from the
	// marshalled configuration when set to false.
	EnableHTTP2 bool `yaml:"enable_http2" json:"enable_http2"`
	// Proxy configuration.
	ProxyConfig `yaml:",inline"`
	// HTTPHeaders specify headers to inject in the requests. Those headers
	// could be marshalled back to the users.
	HTTPHeaders Headers `yaml:"http_headers,omitempty" json:"http_headers,omitempty"`
}

func (c *HTTPClientConfig) Validate() error {
	if err := c.validate(); err != nil {
		return err
	}
	if c.OAuth2 != nil {
		if err := c.OAuth2.Validate(); err != nil {
			return fmt.Errorf("invalid oauth2: %w", err)
		}
	}
	if err := c.ProxyConfig.Validate(); err != nil {
		return fmt.Errorf("invalid proxy config: %w", err)
	}
	if c.HTTPHeaders != nil {
		if err := c.HTTPHeaders.Validate(); err != nil {
			return fmt.Errorf("invalid http_headers: %w", err)
		}
	}
	if err := c.TLSConfig.Validate(); err != nil {
		return fmt.Errorf("invalid tls_config: %w", err)
	}
	return nil
}

// validate validates the HTTPClientConfig to check only one of BearerToken,
// BasicAuth and BearerTokenFile is configured. It also validates that ProxyURL
// is set if ProxyConnectHeader is set.
func (c *HTTPClientConfig) validate() error {
	// Backwards compatibility with the bearer_token field.
	if len(c.BearerToken) > 0 && len(c.BearerTokenFile) > 0 {
		return errors.New("at most one of bearer_token & bearer_token_file must be configured")
	}
	if (c.BasicAuth != nil || c.OAuth2 != nil) && (len(c.BearerToken) > 0 || len(c.BearerTokenFile) > 0) {
		return errors.New("at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured")
	}
	if c.BasicAuth != nil && nonZeroCount(c.BasicAuth.Username != "", c.BasicAuth.UsernameFile != "", c.BasicAuth.UsernameRef != "") > 1 {
		return errors.New("at most one of basic_auth username, username_file & username_ref must be configured")
	}
	if c.BasicAuth != nil && nonZeroCount(string(c.BasicAuth.Password) != "", c.BasicAuth.PasswordFile != "", c.BasicAuth.PasswordRef != "") > 1 {
		return errors.New("at most one of basic_auth password, password_file & password_ref must be configured")
	}
	if c.Authorization != nil {
		if len(c.BearerToken) > 0 || len(c.BearerTokenFile) > 0 {
			return errors.New("authorization is not compatible with bearer_token & bearer_token_file")
		}
		if nonZeroCount(string(c.Authorization.Credentials) != "", c.Authorization.CredentialsFile != "", c.Authorization.CredentialsRef != "") > 1 {
			return errors.New("at most one of authorization credentials & credentials_file must be configured")
		}
		c.Authorization.Type = strings.TrimSpace(c.Authorization.Type)
		if len(c.Authorization.Type) == 0 {
			c.Authorization.Type = "Bearer"
		}
		if strings.ToLower(c.Authorization.Type) == "basic" {
			return errors.New(`authorization type cannot be set to "basic", use "basic_auth" instead`)
		}
		if c.BasicAuth != nil || c.OAuth2 != nil {
			return errors.New("at most one of basic_auth, oauth2 & authorization must be configured")
		}
	} else {
		if len(c.BearerToken) > 0 {
			c.Authorization = &Authorization{Credentials: c.BearerToken}
			c.Authorization.Type = "Bearer"
			c.BearerToken = ""
		}
		if len(c.BearerTokenFile) > 0 {
			c.Authorization = &Authorization{CredentialsFile: c.BearerTokenFile}
			c.Authorization.Type = "Bearer"
			c.BearerTokenFile = ""
		}
	}
	if c.OAuth2 != nil {
		if c.BasicAuth != nil {
			return errors.New("at most one of basic_auth, oauth2 & authorization must be configured")
		}
		if err := c.OAuth2.validate(); err != nil {
			return fmt.Errorf("invalid oauth2 config: %w", err)
		}
	}
	if err := c.ProxyConfig.validate(); err != nil {
		return fmt.Errorf("invalid proxy config: %w", err)
	}
	if c.HTTPHeaders != nil {
		if err := c.HTTPHeaders.validate(); err != nil {
			return fmt.Errorf("invalid http_headers: %w", err)
		}
	}
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
func (c *HTTPClientConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain HTTPClientConfig
	*c = DefaultHTTPClientConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return c.validate()
}

// UnmarshalJSON implements the json.Unmarshaler interface for HTTPClientConfig.
func (c *HTTPClientConfig) UnmarshalJSON(data []byte) error {
	type plain HTTPClientConfig
	*c = DefaultHTTPClientConfig
	if err := json.Unmarshal(data, (*plain)(c)); err != nil {
		return err
	}
	return c.validate()
}

// nonZeroCount returns the amount of values that are non-zero.
func nonZeroCount[T comparable](values ...T) int {
	count := 0
	var zero T
	for _, value := range values {
		if value != zero {
			count += 1
		}
	}
	return count
}

func V0HttpConfigOption() schema.Field {
	oauth2ConfigOption := func() schema.Field {
		return schema.Field{
			Label:        "OAuth2",
			Description:  "Configures the OAuth2 settings.",
			PropertyName: "oauth2",
			Element:      schema.ElementTypeSubform,
			SubformOptions: append([]schema.Field{
				{
					Label:        "Client ID",
					Description:  "The OAuth2 client ID",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "client_id",
					Required:     true,
				},
				{
					Label:        "Client secret",
					Description:  "The OAuth2 client secret",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypePassword,
					PropertyName: "client_secret",
					Required:     true,
					Secure:       true,
				},
				{
					Label:        "Token URL",
					Description:  "The OAuth2 token exchange URL",
					Element:      schema.ElementTypeInput,
					InputType:    schema.InputTypeText,
					PropertyName: "token_url",
					Required:     true,
				},
				{
					Label:        "Scopes",
					Description:  "Comma-separated list of scopes",
					Element:      schema.ElementStringArray,
					PropertyName: "scopes",
				},
				{
					Label:        "Additional parameters",
					Element:      schema.ElementTypeKeyValueMap,
					PropertyName: "endpoint_params",
				},
				V0TLSConfigOption("tls_config"),
			}, V0ProxyConfigOptions()...),
		}
	}
	return schema.Field{
		Label:        "HTTP Config",
		Description:  "Note that `basic_auth` and `bearer_token` options are mutually exclusive.",
		PropertyName: "http_config",
		Element:      schema.ElementTypeSubform,
		SubformOptions: append([]schema.Field{
			{
				Label:        "Basic auth",
				Description:  "Sets the `Authorization` header with the configured username and password.",
				PropertyName: "basic_auth",
				Element:      schema.ElementTypeSubform,
				SubformOptions: []schema.Field{
					{
						Label:        "Username",
						Element:      schema.ElementTypeInput,
						InputType:    schema.InputTypeText,
						PropertyName: "username",
					},
					{
						Label:        "Password",
						Element:      schema.ElementTypeInput,
						InputType:    schema.InputTypePassword,
						PropertyName: "password",
						Secure:       true,
					},
				},
			},
			{
				Label:        "Authorization",
				Description:  "The HTTP authorization credentials for the targets.",
				Element:      schema.ElementTypeSubform,
				PropertyName: "authorization",
				SubformOptions: []schema.Field{
					{
						Label:        "Type",
						Element:      schema.ElementTypeInput,
						InputType:    schema.InputTypeText,
						PropertyName: "type",
					},
					{
						Label:        "Credentials",
						Element:      schema.ElementTypeInput,
						InputType:    schema.InputTypePassword,
						PropertyName: "credentials",
						Secure:       true,
					},
				},
			},
			{
				Label:        "Follow redirects",
				Description:  "Whether the client should follow HTTP 3xx redirects.",
				Element:      schema.ElementTypeCheckbox,
				PropertyName: "follow_redirects",
			},
			{
				Label:        "Enable HTTP2",
				Description:  "Whether the client should configure HTTP2.",
				Element:      schema.ElementTypeCheckbox,
				PropertyName: "enable_http2",
			},
			{
				Label:        "HTTP Headers",
				Description:  "Headers to inject in the requests.",
				Element:      schema.ElementTypeKeyValueMap,
				PropertyName: "http_headers",
			},
		}, append(
			V0ProxyConfigOptions(),
			V0TLSConfigOption("tls_config"),
			oauth2ConfigOption())...,
		),
	}
}
