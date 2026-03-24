// SPDX-License-Identifier: AGPL-3.0-only

package notifier

import (
	"flag"
	"fmt"
	"hash/fnv"

	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util"
)

var (
	DefaultNotifierConfig = Config{
		OAuth2: OAuth2Config{
			EndpointParams: flagext.NewLimitsMap[string](nil),
		},
	}

	DefaultAlertmanagerClientConfig = AlertmanagerClientConfig{
		NotifierConfig: DefaultNotifierConfig,
	}
)

// Represents the client configuration for sending to an alertmanager.
// It is mountable as a single config option/yaml sub-block, or as individual CLI options.
type AlertmanagerClientConfig struct {
	AlertmanagerURL string `yaml:"alertmanager_url"`
	NotifierConfig  Config `yaml:",inline" json:",inline"`
}

func (acc *AlertmanagerClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&acc.AlertmanagerURL, "ruler.alertmanager-url", "", "Comma-separated list of URL(s) of the Alertmanager(s) to send notifications to. Each URL is treated as a separate group. Multiple Alertmanagers in HA per group can be supported by using DNS service discovery format, comprehensive of the scheme. Basic auth is supported as part of the URL.")
	acc.NotifierConfig.RegisterFlags(f)
}

func (acc *AlertmanagerClientConfig) String() string {
	out, err := yaml.Marshal(acc)
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

func (acc *AlertmanagerClientConfig) Set(s string) error {
	cfg := AlertmanagerClientConfig{}
	if err := yaml.Unmarshal([]byte(s), &cfg); err != nil {
		return err
	}
	*acc = cfg
	return nil
}

// Hash calculates a cryptographically weak, insecure hash of the configuration.
func (acc *AlertmanagerClientConfig) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(acc.String()))
	return h.Sum64()
}

func (acc *AlertmanagerClientConfig) Equal(other AlertmanagerClientConfig) bool {
	return acc.Hash() == other.Hash()
}

func (acc *AlertmanagerClientConfig) IsDefault() bool {
	return acc.Equal(DefaultAlertmanagerClientConfig)
}

type Config struct {
	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`
	BasicAuth  util.BasicAuth   `yaml:",inline"`
	OAuth2     OAuth2Config     `yaml:"oauth2"`
	ProxyURL   string           `yaml:"proxy_url" category:"advanced"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, "ruler.alertmanager-client.tls-enabled", true, "Enable TLS for gRPC client connecting to alertmanager.")
	cfg.TLS.RegisterFlagsWithPrefix("ruler.alertmanager-client", f)
	cfg.BasicAuth.RegisterFlagsWithPrefix("ruler.alertmanager-client.", f)
	cfg.OAuth2.RegisterFlagsWithPrefix("ruler.alertmanager-client.oauth.", f)
	f.StringVar(&cfg.ProxyURL, "ruler.alertmanager-client.proxy-url", "", "Optional HTTP, HTTPS via CONNECT, or SOCKS5 proxy URL to route requests through. Applies to all requests, including auxiliary traffic, such as OAuth token requests.")
}

func (cfg *Config) String() string {
	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

// Hash calculates a cryptographically weak, insecure hash of the configuration.
func (cfg *Config) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte(cfg.String()))
	return h.Sum64()
}

func (cfg *Config) Equal(other Config) bool {
	return cfg.Hash() == other.Hash()
}

func (cfg *Config) IsDefault() bool {
	return cfg.Equal(DefaultNotifierConfig)
}

type OAuth2Config struct {
	ClientID       string                    `yaml:"client_id"`
	ClientSecret   flagext.Secret            `yaml:"client_secret"`
	TokenURL       string                    `yaml:"token_url"`
	Scopes         flagext.StringSliceCSV    `yaml:"scopes,omitempty"`
	EndpointParams flagext.LimitsMap[string] `yaml:"endpoint_params" category:"advanced"`
}

func (cfg *OAuth2Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.ClientID, prefix+"client_id", "", "OAuth2 client ID. Enables the use of OAuth2 for authenticating with Alertmanager.")
	f.Var(&cfg.ClientSecret, prefix+"client_secret", "OAuth2 client secret.")
	f.StringVar(&cfg.TokenURL, prefix+"token_url", "", "Endpoint used to fetch access token.")
	f.Var(&cfg.Scopes, prefix+"scopes", "Optional scopes to include with the token request.")
	if !cfg.EndpointParams.IsInitialized() {
		cfg.EndpointParams = flagext.NewLimitsMap[string](nil)
	}
	f.Var(&cfg.EndpointParams, prefix+"endpoint-params", "Optional additional URL parameters to send to the token URL.")
}

func (cfg *OAuth2Config) IsEnabled() bool {
	return cfg.ClientID != "" || cfg.TokenURL != ""
}
