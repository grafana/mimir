// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cancellation"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"

	mimirnotifier "github.com/grafana/mimir/pkg/ruler/notifier"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	errRulerNotifierStopped               = cancellation.NewErrorf("rulerNotifier stopped")
	errRulerSimultaneousBasicAuthAndOAuth = errors.New("cannot use both Basic Auth and OAuth2 simultaneously")
)

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

func newRulerNotifier(o *notifier.Options, nameValidationScheme model.ValidationScheme, l gklog.Logger) (*rulerNotifier, error) {
	sdMetrics, err := discovery.CreateAndRegisterSDMetrics(o.Registerer)
	if err != nil {
		return nil, err
	}

	sdCtx, sdCancel := context.WithCancelCause(context.Background())
	sl := util_log.SlogFromGoKit(l)
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, nameValidationScheme, sl),
		sdCancel:  sdCancel,
		sdManager: discovery.NewManager(sdCtx, sl, o.Registerer, sdMetrics),
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
func buildNotifierConfig(amURL string, notifierCfg mimirnotifier.Config, resolver AddressProvider, notificationTimeout, refreshInterval time.Duration, rmi discovery.RefreshMetricsManager) (*config.Config, error) {
	if amURL == "" {
		// no AM URLs were provided, so we can just return a default config without errors
		return &config.Config{}, nil
	}

	amURLs := strings.Split(amURL, ",")
	amConfigs := make([]*config.AlertmanagerConfig, 0, len(amURLs))

	for _, rawURL := range amURLs {
		isSD, qType, url, err := sanitizedAlertmanagerURL(rawURL)
		if err != nil {
			return nil, err
		}

		var sdConfig discovery.Config
		if isSD {
			sdConfig = dnsSD(refreshInterval, resolver, qType, url, rmi)
		} else {
			sdConfig = staticTarget(url)
		}

		amCfgWithSD, err := amConfigWithSD(notifierCfg, notificationTimeout, url, sdConfig)
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

func amConfigWithSD(notifierCfg mimirnotifier.Config, notificationTimeout time.Duration, url *url.URL, sdConfig discovery.Config) (*config.AlertmanagerConfig, error) {
	amConfig := &config.AlertmanagerConfig{
		APIVersion:              config.AlertmanagerAPIVersionV2,
		Scheme:                  url.Scheme,
		PathPrefix:              url.Path,
		Timeout:                 model.Duration(notificationTimeout),
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
	if notifierCfg.BasicAuth.IsEnabled() {
		amConfig.HTTPClientConfig.BasicAuth = &config_util.BasicAuth{
			Username: notifierCfg.BasicAuth.Username,
			Password: config_util.Secret(notifierCfg.BasicAuth.Password.String()),
		}
	}

	// Whether to use an optional HTTP, HTTP+CONNECT, or SOCKS5 proxy.
	if notifierCfg.ProxyURL != "" {
		url, err := url.Parse(notifierCfg.ProxyURL)
		if err != nil {
			return nil, err
		}
		amConfig.HTTPClientConfig.ProxyURL = config_util.URL{URL: url}
	}

	// Whether to use OAuth2 or not.
	if notifierCfg.OAuth2.IsEnabled() {
		if amConfig.HTTPClientConfig.BasicAuth != nil {
			return nil, errRulerSimultaneousBasicAuthAndOAuth
		}

		amConfig.HTTPClientConfig.OAuth2 = &config_util.OAuth2{
			ClientID:     notifierCfg.OAuth2.ClientID,
			ClientSecret: config_util.Secret(notifierCfg.OAuth2.ClientSecret.String()),
			TokenURL:     notifierCfg.OAuth2.TokenURL,
			Scopes:       notifierCfg.OAuth2.Scopes,
		}

		if notifierCfg.OAuth2.EndpointParams.IsInitialized() {
			amConfig.HTTPClientConfig.OAuth2.EndpointParams = notifierCfg.OAuth2.EndpointParams.Read()
		}

		if notifierCfg.ProxyURL != "" {
			url, err := url.Parse(notifierCfg.ProxyURL)
			if err != nil {
				return nil, err
			}
			amConfig.HTTPClientConfig.OAuth2.ProxyURL = config_util.URL{URL: url}
		}
	}

	// Whether to use TLS or not.
	if notifierCfg.TLSEnabled {
		if notifierCfg.TLS.Reader == nil {
			amConfig.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
				CAFile:             notifierCfg.TLS.CAPath,
				CertFile:           notifierCfg.TLS.CertPath,
				KeyFile:            notifierCfg.TLS.KeyPath,
				InsecureSkipVerify: notifierCfg.TLS.InsecureSkipVerify,
				ServerName:         notifierCfg.TLS.ServerName,
			}
		} else {
			cert, err := notifierCfg.TLS.Reader.ReadSecret(notifierCfg.TLS.CertPath)
			if err != nil {
				return nil, err
			}

			key, err := notifierCfg.TLS.Reader.ReadSecret(notifierCfg.TLS.KeyPath)
			if err != nil {
				return nil, err
			}

			var ca []byte
			if notifierCfg.TLS.CAPath != "" {
				ca, err = notifierCfg.TLS.Reader.ReadSecret(notifierCfg.TLS.CAPath)
				if err != nil {
					return nil, err
				}
			}

			amConfig.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
				CA:                 string(ca),
				Cert:               string(cert),
				Key:                config_util.Secret(key),
				InsecureSkipVerify: notifierCfg.TLS.InsecureSkipVerify,
				ServerName:         notifierCfg.TLS.ServerName,
			}
		}
	}

	return amConfig, nil
}
