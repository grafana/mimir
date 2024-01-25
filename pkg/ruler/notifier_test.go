// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/stretchr/testify/require"

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
							APIVersion: "v2",
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
			name: "with a single URL, v2 API, and no service discovery",
			cfg: &Config{
				AlertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v2",
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
			name: "with a SRV URL but no service discovery (missing dns+ prefix)",
			cfg: &Config{
				AlertmanagerURL:             "http://_http._tcp.alertmanager.default.svc.cluster.local/alertmanager",
				AlertmanagerRefreshInterval: time.Duration(60),
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v2",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								discovery.StaticConfig{
									{
										Targets: []model.LabelSet{{"__address__": "_http._tcp.alertmanager.default.svc.cluster.local"}},
									},
								},
							},
						},
					},
				},
			},
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
							APIVersion: "v2",
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
							APIVersion: "v2",
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
			name: "with basic authentication URL and no service discovery",
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
							APIVersion: "v2",
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
			name: "with basic authentication URL and service discovery",
			cfg: &Config{
				AlertmanagerURL: "dnssrv+https://marco:hunter2@_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								BasicAuth: &config_util.BasicAuth{Username: "marco", Password: "hunter2"},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									Host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									QType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with basic authentication URL, no service discovery, and explicit config",
			cfg: &Config{
				AlertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
				Notifier: NotifierConfig{
					BasicAuth: util.BasicAuth{
						Username: "jacob",
						Password: flagext.SecretWithValue("test"),
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
							APIVersion: "v2",
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
			name: "with multiple URLs and service discovery",
			cfg: &Config{
				AlertmanagerURL:             "dns+http://alertmanager.mimir.svc.cluster.local:8080/alertmanager,dnssrv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
				AlertmanagerRefreshInterval: time.Second,
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v2",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									Host:            "alertmanager.mimir.svc.cluster.local:8080",
									RefreshInterval: time.Second,
									QType:           dns.A,
								},
							},
						},
						{
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/am",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									Host:            "_http._tcp.alertmanager2.mimir.svc.cluster.local",
									RefreshInterval: time.Second,
									QType:           dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "with DNS service discovery and missing scheme",
			cfg: &Config{
				AlertmanagerURL: "dns+alertmanager.mimir.svc.cluster.local:8080/alertmanager",
			},
			err: errors.New("improperly formatted alertmanager URL \"alertmanager.mimir.svc.cluster.local:8080/alertmanager\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
		},
		{
			name: "with only dns+ prefix",
			cfg: &Config{
				AlertmanagerURL: "dns+",
			},
			err: errors.New("improperly formatted alertmanager URL \"\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
		},
		{
			name: "misspelled DNS SD format prefix (dnsserv+ vs dnssrv+)",
			cfg: &Config{
				AlertmanagerURL: "dnsserv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
			},
			err: errors.New("invalid DNS service discovery prefix \"dnsserv\""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ncfg, err := buildNotifierConfig(tt.cfg, nil, nil)
			if tt.err == nil {
				require.NoError(t, err)
				require.Equal(t, tt.ncfg, ncfg)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}
		})
	}
}
