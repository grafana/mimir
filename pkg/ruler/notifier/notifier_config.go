package notifier

import (
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"

	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/util"
)

var DefaultAlertmanagerClientConfig = AlertmanagerClientConfig{
	NotifierConfig: Config{
		OAuth2: OAuth2Config{
			EndpointParams: flagext.NewLimitsMap[string](nil),
		},
	},
}

type AlertmanagerClientConfig struct {
	AlertmanagerURL string `yaml:"alertmanager_url" doc:"nocli"`
	NotifierConfig  Config `yaml:",inline" json:",inline"`
}

func (acc *AlertmanagerClientConfig) String() string {
	out, err := json.Marshal(acc)
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

func (acc *AlertmanagerClientConfig) Set(s string) error {
	cfg := AlertmanagerClientConfig{}
	if err := json.Unmarshal([]byte(s), &cfg); err != nil {
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
