// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"

	gklog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/crypto/tls"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/notifier"
	"github.com/thanos-io/thanos/pkg/cacheutil"
	thanosdns "github.com/thanos-io/thanos/pkg/discovery/dns"

	"github.com/grafana/mimir/pkg/util"
)

type NotifierConfig struct {
	TLS       tls.ClientConfig `yaml:",inline"`
	BasicAuth util.BasicAuth   `yaml:",inline"`
}

func (cfg *NotifierConfig) RegisterFlags(f *flag.FlagSet) {
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

// run starts the notifier. This function doesn't block and returns immediately.
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

// Builds a Prometheus config.Config from a ruler.Config with just the required
// options to configure notifications to Alertmanager.
func buildNotifierConfig(rulerConfig *Config, resolver cacheutil.AddressProvider) (*config.Config, error) {
	amURLs := strings.Split(rulerConfig.AlertmanagerURL, ",")
	amConfigs := make([]*config.AlertmanagerConfig, 0, len(amURLs))

	apiVersion := config.AlertmanagerAPIVersionV1
	if rulerConfig.AlertmanagerEnableV2API {
		apiVersion = config.AlertmanagerAPIVersionV2
	}

	prometheusSRVSDRegexp := regexp.MustCompile(`^_.+._.+`)

	for _, rawURL := range amURLs {
		var thanosQType string
		thanosQType, rawURL = thanosdns.GetQTypeName(rawURL)

		url, err := url.Parse(rawURL)
		if err != nil {
			return nil, err
		}

		if url.String() == "" {
			continue
		}

		if url.Host == "" {
			return nil, fmt.Errorf("improperly formatted alertmanager URL (maybe the scheme is missing?) %q", rawURL)
		}

		isThanosSD := thanosQType != ""
		isPromSD := prometheusSRVSDRegexp.MatchString(url.Host)

		var sdConfig discovery.Config
		switch {
		case isThanosSD:
			sdConfig = thanosSD(rulerConfig, resolver, thanosdns.QType(thanosQType), url)
		case isPromSD:
			sdConfig = promSD(rulerConfig, url)
		default:
			sdConfig = staticTarget(url)
		}

		amConfigs = append(amConfigs, amConfigWithSD(rulerConfig, url, apiVersion, sdConfig))
	}

	if len(amConfigs) == 0 {
		return &config.Config{}, nil
	}

	promConfig := &config.Config{
		AlertingConfig: config.AlertingConfig{
			AlertmanagerConfigs: amConfigs,
		},
	}

	return promConfig, nil
}

func amConfigWithSD(rulerConfig *Config, url *url.URL, apiVersion config.AlertmanagerAPIVersion, sdConfig discovery.Config) *config.AlertmanagerConfig {
	amConfig := &config.AlertmanagerConfig{
		APIVersion:              apiVersion,
		Scheme:                  url.Scheme,
		PathPrefix:              url.Path,
		Timeout:                 model.Duration(rulerConfig.NotificationTimeout),
		ServiceDiscoveryConfigs: discovery.Configs{sdConfig},
		HTTPClientConfig: config_util.HTTPClientConfig{
			TLSConfig: config_util.TLSConfig{
				CAFile:             rulerConfig.Notifier.TLS.CAPath,
				CertFile:           rulerConfig.Notifier.TLS.CertPath,
				KeyFile:            rulerConfig.Notifier.TLS.KeyPath,
				InsecureSkipVerify: rulerConfig.Notifier.TLS.InsecureSkipVerify,
				ServerName:         rulerConfig.Notifier.TLS.ServerName,
			},
		},
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
			Password: config_util.Secret(rulerConfig.Notifier.BasicAuth.Password),
		}
	}

	return amConfig
}
