// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"flag"
	"net/url"
	"sync"

	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/crypto/tls"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"

	"github.com/grafana/mimir/pkg/util"
)

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
	sdCancel  context.CancelFunc
	sdManager *discovery.Manager
	wg        sync.WaitGroup
	logger    gklog.Logger
}

func newRulerNotifier(o *notifier.Options, l gklog.Logger) *rulerNotifier {
	sdCtx, sdCancel := context.WithCancel(context.Background())
	return &rulerNotifier{
		notifier:  notifier.NewManager(o, l),
		sdCancel:  sdCancel,
		sdManager: discovery.NewManager(sdCtx, l),
		logger:    l,
	}
}

// run starts the discovery.Manager and notifier.Manager. This function doesn't block and returns immediately.
func (rn *rulerNotifier) run() {
	rn.wg.Add(2)
	go func() {
		if err := rn.sdManager.Run(); err != nil {
			level.Error(rn.logger).Log("msg", "error starting notifier discovery manager", "err", err)
		}
		rn.wg.Done()
	}()
	go func() {
		rn.notifier.Run(rn.sdManager.SyncCh())
		rn.wg.Done()
	}()
}

// applyConfig applies the cfg to the notifier.Manager and discovery.Manager.
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

func (rn *rulerNotifier) stop() {
	rn.sdCancel()
	rn.notifier.Stop()
	rn.wg.Wait()
}

// Builds a Prometheus config.Config for the discovery.Config elements.
func buildNotifierConfig(rulerConfig *Config, discoveryConfigs map[string]discovery.Config) (*config.Config, error) {
	if len(discoveryConfigs) == 0 {
		// no AM URLs were provided, so we can just return a default config without errors
		return &config.Config{}, nil
	}

	var amConfigs []*config.AlertmanagerConfig
	for amURL, dcfg := range discoveryConfigs {
		amConfig, err := amConfigWithSD(rulerConfig, amURL, dcfg)
		if err != nil {
			return nil, err
		}
		amConfigs = append(amConfigs, amConfig)
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: amConfigs,
		},
	}

	return promConfig, nil
}

// amConfigWithSD builds a config.AlertmanagerConfig by combining the rulerCOnfig, url, and sdConfig.
func amConfigWithSD(rulerConfig *Config, rawURL string, sdConfig discovery.Config) (*config.AlertmanagerConfig, error) {
	amURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	amConfig := &config.AlertmanagerConfig{
		APIVersion:              config.AlertmanagerAPIVersionV2,
		Scheme:                  amURL.Scheme,
		PathPrefix:              amURL.Path,
		Timeout:                 model.Duration(rulerConfig.NotificationTimeout),
		ServiceDiscoveryConfigs: discovery.Configs{sdConfig},
		HTTPClientConfig:        config_util.HTTPClientConfig{},
	}

	// Check the URL for basic authentication information first
	if amURL.User != nil {
		amConfig.HTTPClientConfig.BasicAuth = &config_util.BasicAuth{
			Username: amURL.User.Username(),
		}

		if password, isSet := amURL.User.Password(); isSet {
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
		amConfig.HTTPClientConfig.TLSConfig = config_util.TLSConfig{
			CAFile:             rulerConfig.Notifier.TLS.CAPath,
			CertFile:           rulerConfig.Notifier.TLS.CertPath,
			KeyFile:            rulerConfig.Notifier.TLS.KeyPath,
			InsecureSkipVerify: rulerConfig.Notifier.TLS.InsecureSkipVerify,
			ServerName:         rulerConfig.Notifier.TLS.ServerName,
		}
	}

	return amConfig, nil
}
