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
	"errors"
	"net/http"
	"net/url"

	commoncfg "github.com/prometheus/common/config"
	"golang.org/x/net/http/httpproxy"

	"github.com/grafana/alerting/receivers/schema"
)

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

func (c *ProxyConfig) Validate() error { return c.validate() }

// validate validates the ProxyConfig.
func (c *ProxyConfig) validate() error {
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

func V0ProxyConfigOptions() []schema.Field {
	return []schema.Field{
		{
			Label:        "Proxy URL",
			Description:  "HTTP proxy server to use to connect to the targets.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "proxy_url",
		},
		{
			Label:        "No Proxy",
			Description:  "Comma-separated list of domains for which the proxy should not be used.",
			Element:      schema.ElementTypeInput,
			InputType:    schema.InputTypeText,
			PropertyName: "no_proxy",
		},
		{
			Label:        "Proxy From Environment",
			Description:  "Makes use of net/http ProxyFromEnvironment function to determine proxies.",
			Element:      schema.ElementTypeCheckbox,
			PropertyName: "proxy_from_environment",
		},
		{
			Label:        "Proxy Header Environment",
			Description:  "Headers to send to proxies during CONNECT requests.",
			Element:      schema.ElementTypeKeyValueMap,
			PropertyName: "proxy_connect_header",
		},
	}
}
