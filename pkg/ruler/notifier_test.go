// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/stretchr/testify/require"
	thanosdns "github.com/thanos-io/thanos/pkg/discovery/dns"

	"github.com/grafana/mimir/pkg/ruler/thanossd"
	"github.com/grafana/mimir/pkg/util"
)

func TestBuildNotifierConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		ncfg *config.Config
		err  error
	}{
		{
			name: "with no valid hosts, returns an empty config",
			cfg:  &Config{},
			ncfg: &config.Config{},
		},
		{
			name: "with a single URL and no service discovery",
			cfg: &Config{
				AlertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{
									{
										Targets: []model.LabelSet{{"__address__": "alertmanager.default.svc.cluster.local"}},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with a single URL and service discovery",
			cfg: &Config{
				AlertmanagerURL:             "http://_http._tcp.alertmanager.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery:       true,
				AlertmanagerRefreshInterval: time.Duration(60),
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								&dns.SDConfig{
									Names:           []string{"_http._tcp.alertmanager.default.svc.cluster.local"},
									RefreshInterval: 60,
									Type:            "SRV",
									Port:            0,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with service discovery and an invalid URL",
			cfg: &Config{
				AlertmanagerURL:       "http://_http.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery: true,
			},
			err: fmt.Errorf("when alertmanager-discovery is on, host name must be of the form _portname._tcp.service.fqdn (is \"_http.default.svc.cluster.local\")"),
		},
		{
			name: "with multiple URLs and no service discovery",
			cfg: &Config{
				AlertmanagerURL: "http://alertmanager-0.default.svc.cluster.local/alertmanager,http://alertmanager-1.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{{
									Targets: []model.LabelSet{{"__address__": "alertmanager-0.default.svc.cluster.local"}},
								},
								},
							},
						},
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{{
									Targets: []model.LabelSet{{"__address__": "alertmanager-1.default.svc.cluster.local"}},
								},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with multiple URLs and service discovery",
			cfg: &Config{
				AlertmanagerURL:             "http://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager,http://_http._tcp.alertmanager-1.default.svc.cluster.local/alertmanager",
				AlertmanagerDiscovery:       true,
				AlertmanagerRefreshInterval: time.Duration(60),
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								&dns.SDConfig{
									Names:           []string{"_http._tcp.alertmanager-0.default.svc.cluster.local"},
									RefreshInterval: 60,
									Type:            "SRV",
									Port:            0,
								},
							},
						},
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								&dns.SDConfig{
									Names:           []string{"_http._tcp.alertmanager-1.default.svc.cluster.local"},
									RefreshInterval: 60,
									Type:            "SRV",
									Port:            0,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with Basic Authentication URL",
			cfg: &Config{
				AlertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								BasicAuth: &config_util.BasicAuth{Username: "marco", Password: "hunter2"},
							},
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{
									{
										Targets: []model.LabelSet{{"__address__": "alertmanager-0.default.svc.cluster.local"}},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with Basic Authentication URL and Explicit",
			cfg: &Config{
				AlertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
				Notifier: NotifierConfig{
					BasicAuth: util.BasicAuth{
						Username: "jacob",
						Password: "test",
					},
				},
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								BasicAuth: &config_util.BasicAuth{Username: "jacob", Password: "test"},
							},
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{
									{
										Targets: []model.LabelSet{{"__address__": "alertmanager-0.default.svc.cluster.local"}},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with multiple thanos service discovery",
			cfg: &Config{
				AlertmanagerURL: "dns+http://alertmanager.mimir.svc.cluster.local:8080/alertmanager,dnssrv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v1",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								thanossd.Config{
									Host:  "alertmanager.mimir.svc.cluster.local:8080",
									QType: thanosdns.A,
								},
							},
						},
						{
							APIVersion: "v1",
							Scheme:     "https",
							PathPrefix: "/am",
							ServiceDiscoveryConfigs: discovery.Configs{
								thanossd.Config{
									Host:  "_http._tcp.alertmanager2.mimir.svc.cluster.local",
									QType: thanosdns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with thanos service discovery and missing scheme",
			cfg: &Config{
				AlertmanagerURL: "dns+alertmanager.mimir.svc.cluster.local:8080/alertmanager",
			},
			err: errors.New("improperly formatted alertmanager URL (maybe the scheme is missing?) \"alertmanager.mimir.svc.cluster.local:8080/alertmanager\""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ncfg, err := buildNotifierConfig(tt.cfg, nil)
			if tt.err == nil {
				require.NoError(t, err)
				require.Equal(t, tt.ncfg, ncfg)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}
		})
	}
}
