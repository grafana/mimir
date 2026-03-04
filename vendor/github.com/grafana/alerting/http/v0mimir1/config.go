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

package v0mimir1

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	commoncfg "github.com/prometheus/common/config"
	"golang.org/x/net/http/httpproxy"
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
	TLSConfig       TLSConfig         `yaml:"tls_config,omitempty"`
	ProxyConfig     `yaml:",inline"`
}

// Validate validates the OAuth2 Config.
func (o *OAuth2) Validate() error {
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
	return o.Validate()
}

// UnmarshalJSON implements the json.Unmarshaler interface for OAuth2.
func (o *OAuth2) UnmarshalJSON(data []byte) error {
	type plain OAuth2
	if err := json.Unmarshal(data, (*plain)(o)); err != nil {
		return err
	}
	return o.Validate()
}

// TLSConfig configures the options for TLS connections.
type TLSConfig struct {
	// Text of the CA cert to use for the targets.
	CA string `yaml:"ca,omitempty" json:"ca,omitempty"`
	// Text of the client cert file for the targets.
	Cert string `yaml:"cert,omitempty" json:"cert,omitempty"`
	// Text of the client key file for the targets.
	Key commoncfg.Secret `yaml:"key,omitempty" json:"key,omitempty"`
	// The CA cert to use for the targets.
	CAFile string `yaml:"ca_file,omitempty" json:"ca_file,omitempty"`
	// The client cert file for the targets.
	CertFile string `yaml:"cert_file,omitempty" json:"cert_file,omitempty"`
	// The client key file for the targets.
	KeyFile string `yaml:"key_file,omitempty" json:"key_file,omitempty"`
	// CARef is the name of the secret within the secret manager to use as the CA cert for the
	// targets.
	CARef string `yaml:"ca_ref,omitempty" json:"ca_ref,omitempty"`
	// CertRef is the name of the secret within the secret manager to use as the client cert for
	// the targets.
	CertRef string `yaml:"cert_ref,omitempty" json:"cert_ref,omitempty"`
	// KeyRef is the name of the secret within the secret manager to use as the client key for
	// the targets.
	KeyRef string `yaml:"key_ref,omitempty" json:"key_ref,omitempty"`
	// Used to verify the hostname for the targets.
	ServerName string `yaml:"server_name,omitempty" json:"server_name,omitempty"`
	// Disable target certificate validation.
	InsecureSkipVerify bool `yaml:"insecure_skip_verify" json:"insecure_skip_verify"`
	// Minimum TLS version.
	MinVersion commoncfg.TLSVersion `yaml:"min_version,omitempty" json:"min_version,omitempty"`
	// Maximum TLS version.
	MaxVersion commoncfg.TLSVersion `yaml:"max_version,omitempty" json:"max_version,omitempty"`
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *TLSConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type plain TLSConfig
	if err := unmarshal((*plain)(c)); err != nil {
		return err
	}
	return c.Validate()
}

// Validate validates the TLSConfig to check that only one of the inlined or
// file-based fields for the TLS CA, client certificate, and client key are
// used.
func (c *TLSConfig) Validate() error {
	if nonZeroCount(len(c.CA) > 0, len(c.CAFile) > 0, len(c.CARef) > 0) > 1 {
		return errors.New("at most one of ca, ca_file & ca_ref must be configured")
	}
	if nonZeroCount(len(c.Cert) > 0, len(c.CertFile) > 0, len(c.CertRef) > 0) > 1 {
		return errors.New("at most one of cert, cert_file & cert_ref must be configured")
	}
	if nonZeroCount(len(c.Key) > 0, len(c.KeyFile) > 0, len(c.KeyRef) > 0) > 1 {
		return errors.New("at most one of key and key_file must be configured")
	}

	if c.usingClientCert() && !c.usingClientKey() {
		return errors.New("exactly one of key or key_file must be configured when a client certificate is configured")
	} else if c.usingClientKey() && !c.usingClientCert() {
		return errors.New("exactly one of cert or cert_file must be configured when a client key is configured")
	}

	return nil
}

func (c *TLSConfig) usingClientCert() bool {
	return len(c.Cert) > 0 || len(c.CertFile) > 0 || len(c.CertRef) > 0
}

func (c *TLSConfig) usingClientKey() bool {
	return len(c.Key) > 0 || len(c.KeyFile) > 0 || len(c.KeyRef) > 0
}

// ProxyHeader represents HTTP headers to send to proxies.
type ProxyHeader map[string][]commoncfg.Secret

func (h *ProxyHeader) HTTPHeader() http.Header {
	if h == nil || *h == nil {
		return nil
	}

	header := make(http.Header)

	for name, values := range *h {
		var s []string
		if values != nil {
			s = make([]string, 0, len(values))
			for _, value := range values {
				s = append(s, string(value))
			}
		}
		header[name] = s
	}

	return header
}

// ProxyConfig configures proxy settings.
type ProxyConfig struct {
	// HTTP proxy server to use to connect to the targets.
	ProxyURL commoncfg.URL `yaml:"proxy_url,omitempty" json:"proxy_url,omitempty"`
	// NoProxy contains addresses that should not use a proxy.
	NoProxy string `yaml:"no_proxy,omitempty" json:"no_proxy,omitempty"`
	// ProxyFromEnvironment makes use of net/http ProxyFromEnvironment function
	// to determine proxies.
	ProxyFromEnvironment bool `yaml:"proxy_from_environment,omitempty" json:"proxy_from_environment,omitempty"`
	// ProxyConnectHeader optionally specifies headers to send to
	// proxies during CONNECT requests. Assume that at least _some_ of
	// these headers are going to contain secrets and use Secret as the
	// value type instead of string.
	ProxyConnectHeader ProxyHeader `yaml:"proxy_connect_header,omitempty" json:"proxy_connect_header,omitempty"`

	proxyFunc func(*http.Request) (*url.URL, error)
}

// Validate validates the ProxyConfig.
func (c *ProxyConfig) Validate() error {
	if len(c.ProxyConnectHeader) > 0 && (!c.ProxyFromEnvironment && (c.ProxyURL.URL == nil || c.ProxyURL.String() == "")) {
		return errors.New("if proxy_connect_header is configured, proxy_url or proxy_from_environment must also be configured")
	}
	if c.ProxyFromEnvironment && c.ProxyURL.URL != nil && c.ProxyURL.String() != "" {
		return errors.New("if proxy_from_environment is configured, proxy_url must not be configured")
	}
	if c.ProxyFromEnvironment && c.NoProxy != "" {
		return errors.New("if proxy_from_environment is configured, no_proxy must not be configured")
	}
	if c.ProxyURL.URL == nil && c.NoProxy != "" {
		return errors.New("if no_proxy is configured, proxy_url must also be configured")
	}
	return nil
}

// Proxy returns the Proxy URL for a request.
func (c *ProxyConfig) Proxy() func(*http.Request) (*url.URL, error) {
	if c == nil {
		return nil
	}
	if c.proxyFunc != nil {
		return c.proxyFunc
	}
	if c.ProxyFromEnvironment {
		proxyFn := httpproxy.FromEnvironment().ProxyFunc()
		c.proxyFunc = func(req *http.Request) (*url.URL, error) {
			return proxyFn(req.URL)
		}
		return c.proxyFunc
	}
	if c.ProxyURL.URL != nil && c.ProxyURL.String() != "" {
		if c.NoProxy == "" {
			c.proxyFunc = http.ProxyURL(c.ProxyURL.URL)
			return c.proxyFunc
		}
		proxy := &httpproxy.Config{
			HTTPProxy:  c.ProxyURL.String(),
			HTTPSProxy: c.ProxyURL.String(),
			NoProxy:    c.NoProxy,
		}
		proxyFn := proxy.ProxyFunc()
		c.proxyFunc = func(req *http.Request) (*url.URL, error) {
			return proxyFn(req.URL)
		}
	}
	return c.proxyFunc
}

// GetProxyConnectHeader returns the Proxy Connect Headers.
func (c *ProxyConfig) GetProxyConnectHeader() http.Header {
	return c.ProxyConnectHeader.HTTPHeader()
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
type Headers struct {
	Headers map[string]Header `yaml:",inline"`
}

// MarshalJSON implements the json.Marshaler interface for Headers.
func (h Headers) MarshalJSON() ([]byte, error) {
	// Inline the Headers map when serializing JSON because json encoder doesn't support "inline" directive.
	return json.Marshal(h.Headers)
}

// Validate validates the Headers config.
func (h *Headers) Validate() error {
	for n := range h.Headers {
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
	HTTPHeaders *Headers `yaml:"http_headers,omitempty" json:"http_headers,omitempty"`
}

// Validate validates the HTTPClientConfig to check only one of BearerToken,
// BasicAuth and BearerTokenFile is configured. It also validates that ProxyURL
// is set if ProxyConnectHeader is set.
func (c *HTTPClientConfig) Validate() error {
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
		if err := c.OAuth2.Validate(); err != nil {
			return err
		}
	}
	if err := c.ProxyConfig.Validate(); err != nil {
		return err
	}
	if c.HTTPHeaders != nil {
		if err := c.HTTPHeaders.Validate(); err != nil {
			return err
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
	return c.Validate()
}

// UnmarshalJSON implements the json.Unmarshaler interface for HTTPClientConfig.
func (c *HTTPClientConfig) UnmarshalJSON(data []byte) error {
	type plain HTTPClientConfig
	*c = DefaultHTTPClientConfig
	if err := json.Unmarshal(data, (*plain)(c)); err != nil {
		return err
	}
	return c.Validate()
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
