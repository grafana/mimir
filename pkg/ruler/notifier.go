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
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"

	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var errRulerNotifierStopped = cancellation.NewErrorf("rulerNotifier stopped")

type NotifierConfig struct {
	TLSEnabled bool             `yaml:"tls_enabled" category:"advanced"`
	TLS        tls.ClientConfig `yaml:",inline"`
	BasicAuth  util.BasicAuth   `yaml:",inline"`
}

func (cfg *NotifierConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.TLSEnabled, "ruler.alertmanager-client.tls-enabled", true, "Enable TLS for gRPC client connecting to alertmanager.")
	cfg.TLS.RegisterFlagsWithPrefix("ruler.alertmanager-client", f)
	cfg.BasicAuth.RegisterFlagsWithPrefix("ruler.alertmanager-client.", f)
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
	sl := util_log.SlogFromGoKit(l)
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, sl),
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
