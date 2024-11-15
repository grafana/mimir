// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"errors"
	"flag"
	"net/url"
	"strings"
	"sync"

	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"

	"github.com/grafana/mimir/pkg/util"
)

var errRulerNotifierStopped = cancellation.NewErrorf("rulerNotifier stopped")

type NotifierConfig struct {
	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`
	BasicAuth  util.BasicAuth   `yaml:",inline"`
	OAuth2     OAuth2Config     `yaml:"oauth2"`
	ProxyURL   string           `yaml:"proxy_url" category:"advanced"`
}

func (cfg *NotifierConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, "ruler.alertmanager-client.tls-enabled", true, "Enable TLS for gRPC client connecting to alertmanager.")
	cfg.TLS.RegisterFlagsWithPrefix("ruler.alertmanager-client", f)
	cfg.BasicAuth.RegisterFlagsWithPrefix("ruler.alertmanager-client.", f)
	cfg.OAuth2.RegisterFlagsWithPrefix("ruler.alertmanager-client.oauth.", f)
	f.StringVar(&cfg.ProxyURL, "ruler.alertmanager-client.proxy-url", "", "Optional HTTP, HTTPS via CONNECT, or SOCKS5 proxy URL to route requests through. Applies to all requests, including infra like oauth token requests.")
}

type OAuth2Config struct {
	ClientID     string              `yaml:"client_id"`
	ClientSecret flagext.Secret      `yaml:"client_secret"`
	TokenURL     string              `yaml:"token_url"`
	Scopes       flagext.StringSlice `yaml:"scopes,omitempty"`
}

func (cfg *OAuth2Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.ClientID, prefix+"client_id", "", "OAuth2 client ID. Enables the use of OAuth2 for authenticating with Alertmanager.")
	f.Var(&cfg.ClientSecret, prefix+"client_secret", "OAuth2 client secret.")
	f.StringVar(&cfg.TokenURL, prefix+"token_url", "", "Endpoint used to fetch access token from.")
	f.Var(&cfg.Scopes, prefix+"scopes", "Optional scopes to include with the token request.")
}

func (cfg *OAuth2Config) IsEnabled() bool {
	return cfg.ClientID != "" || cfg.TokenURL != ""
}

// rulerNotifier bundles a notifier.Manager together with an associated
// Alertmanager service discovery manager and handles the lifecycle
// of both actors.
type rulerNotifier struct {
	notifier  *notifier.Manager
	sdCancel  context.CancelCauseFunc
	sdManager *discovery.Manager
	wg        sync.WaitGroup
	logger    gklog.Logger
}

func newRulerNotifier(o *notifier.Options, l gklog.Logger) (*rulerNotifier, error) {
	sdCtx, sdCancel := context.WithCancelCause(context.Background())
	sdMetrics, err := discovery.CreateAndRegisterSDMetrics(o.Registerer)
	if err != nil {
		return nil, err
	}
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, l),
		sdCancel:  sdCancel,
		sdManager: discovery.NewManager(sdCtx, l, o.Registerer, sdMetrics),
		logger:    l,
	}, nil
}

// run starts the notifier. This function doesn't block and returns immediately.
func (rn *rulerNotifier) run() {
	rn.wg.Add(2)
	go func() {
		defer rn.wg.Done()

		// Ignore context cancelled errors: cancelling the context is how we stop the manager when shutting down normally.
		if err := rn.sdManager.Run(); err != nil && !errors.Is(err, context.Canceled) {
			level.Error(rn.logger).Log("msg", "error running notifier discovery manager", "err", err)
			return
		}

		level.Info(rn.logger).Log("msg", "notifier discovery manager stopped")
	}()
	go func() {
		rn.notifier.Run(rn.sdManager.SyncCh())
		rn.wg.Done()
	}()
}

func (rn *rulerNotifier) applyConfig(cfg *config.Config) error {
	if err := rn.notifier.ApplyConfig(cfg); err != nil {
		return err
	}

	sdCfgs := make(map[string]discovery.Configs)
	for k, v := range cfg.AlertingConfig.AlertmanagerConfigs.ToMap() {
		sdCfgs[k] = v.ServiceDiscoveryConfigs
	}
	return rn.sdManager.ApplyConfig(sdCfgs)
}

// stop stops the notifier and waits for it to terminate.
//
// Note that this can take quite some time if draining the notification queue is enabled.
func (rn *rulerNotifier) stop() {
	rn.sdCancel(errRulerNotifierStopped)
	rn.notifier.Stop()
	rn.wg.Wait()
}

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config, resolver cache.AddressProvider, rmi discovery.RefreshMetricsManager) (*config.Config, error) {
	if rulerConfig.AlertmanagerURL == "" {
		// no AM URLs were provided, so we can just return a default config without errors
		return &config.Config{}, nil
	}

	amURLs := strings.Split(rulerConfig.AlertmanagerURL, ",")
	amConfigs := make([]*config.AlertmanagerConfig, 0, len(amURLs))

	for _, rawURL := range amURLs {
		isSD, qType, url, err := sanitizedAlertmanagerURL(rawURL)
		if err != nil {
			return nil, err
		}

		var sdConfig discovery.Config
		if isSD {
			sdConfig = dnsSD(rulerConfig, resolver, qType, url, rmi)
		} else {
			sdConfig = staticTarget(url)
		}

		amCfgWithSD, err := amConfigWithSD(rulerConfig, url, sdConfig)
		if err != nil {
			return nil, err
		}
		amConfigs = append(amConfigs, amCfgWithSD)
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: amConfigs,
		},
	}

	return promConfig, nil
}

func amConfigWithSD(rulerConfig *Config, url *url.URL, sdConfig discovery.Config) (*config.AlertmanagerConfig, error) {
	amConfig := &config.AlertmanagerConfig{
		APIVersion:              config.AlertmanagerAPIVersionV2,
		Scheme:                  url.Scheme,
		PathPrefix:              url.Path,
		Timeout:                 model.Duration(rulerConfig.NotificationTimeout),
		ServiceDiscoveryConfigs: discovery.Configs{sdConfig},
		HTTPClientConfig:        config_util.HTTPClientConfig{},
	}

	// Check the URL for basic authentication information first
	if url.User != nil {
		amConfig.HTTPClientConfig.BasicAuth = &config_util.BasicAuth{
			Username: url.User.Username(),
		}

		if password, isSet := url.User.Password(); isSet {
			amConfig.HTTPClientConfig.BasicAuth.Password = config_util.Secret(password)
		}
	}

	// Override URL basic authentication configs with hard coded config values if present
	if rulerConfig.Notifier.BasicAuth.IsEnabled() {
		amConfig.HTTPClientConfig.BasicAuth = &config_util.BasicAuth{
			Username: rulerConfig.Notifier.BasicAuth.Username,
			Password: config_util.Secret(rulerConfig.Notifier.BasicAuth.Password.String()),
		}
	}

	// Whether to use an optional HTTP, HTTP+CONNECT, or SOCKS5 proxy.
	if rulerConfig.Notifier.ProxyURL != "" {
		url, err := url.Parse(rulerConfig.Notifier.ProxyURL)
		if err != nil {
			return nil, err
		}
		amConfig.HTTPClientConfig.ProxyURL = config_util.URL{URL: url}
	}

	// Whether to use OAuth2 or not.
	if rulerConfig.Notifier.OAuth2.IsEnabled() {
		amConfig.HTTPClientConfig.OAuth2 = &config_util.OAuth2{
			ClientID:     rulerConfig.Notifier.OAuth2.ClientID,
			ClientSecret: config_util.Secret(rulerConfig.Notifier.OAuth2.ClientSecret.String()),
			TokenURL:     rulerConfig.Notifier.OAuth2.TokenURL,
			Scopes:       rulerConfig.Notifier.OAuth2.Scopes,
		}

		if rulerConfig.Notifier.ProxyURL != "" {
			url, err := url.Parse(rulerConfig.Notifier.ProxyURL)
			if err != nil {
				return nil, err
			}
			amConfig.HTTPClientConfig.OAuth2.ProxyURL = config_util.URL{URL: url}
		}
	}

	// Whether to use TLS or not.
	if rulerConfig.Notifier.TLSEnabled {
		if rulerConfig.Notifier.TLS.Reader == nil {
			amConfig.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
				CAFile:             rulerConfig.Notifier.TLS.CAPath,
				CertFile:           rulerConfig.Notifier.TLS.CertPath,
				KeyFile:            rulerConfig.Notifier.TLS.KeyPath,
				InsecureSkipVerify: rulerConfig.Notifier.TLS.InsecureSkipVerify,
				ServerName:         rulerConfig.Notifier.TLS.ServerName,
			}
		} else {
			cert, err := rulerConfig.Notifier.TLS.Reader.ReadSecret(rulerConfig.Notifier.TLS.CertPath)
			if err != nil {
				return nil, err
			}

			key, err := rulerConfig.Notifier.TLS.Reader.ReadSecret(rulerConfig.Notifier.TLS.KeyPath)
			if err != nil {
				return nil, err
			}

			var ca []byte
			if rulerConfig.Notifier.TLS.CAPath != "" {
				ca, err = rulerConfig.Notifier.TLS.Reader.ReadSecret(rulerConfig.Notifier.TLS.CAPath)
				if err != nil {
					return nil, err
				}
			}

			amConfig.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
				CA:                 string(ca),
				Cert:               string(cert),
				Key:                config_util.Secret(key),
				InsecureSkipVerify: rulerConfig.Notifier.TLS.InsecureSkipVerify,
				ServerName:         rulerConfig.Notifier.TLS.ServerName,
			}
		}
	}

	return amConfig, nil
}
