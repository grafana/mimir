// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/notifier_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
	"errors"
	"flag"
	"maps"
	"net/url"
	"testing"
	"time"

	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/flagext"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/notifier"
	"github.com/grafana/mimir/pkg/util"
)

func TestBuildNotifierConfig(t *testing.T) {
	tests := []struct {
		name string

		alertmanagerURL             string
		notifier                    notifier.Config
		alertmanagerRefreshInterval time.Duration
		notificationTimeout         time.Duration

		ncfg *config.Config
		err  error
	}{
		{
			name:            "with no valid hosts, returns an empty config",
			alertmanagerURL: "",
			ncfg:            &config.Config{},
		},
		{
			name:            "with a single URL and no service discovery",
			alertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
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
			name:            "with a single URL, v2 API, and no service discovery",
			alertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
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
			name:                        "with a SRV URL but no service discovery (missing dns+ prefix)",
			alertmanagerURL:             "http://_http._tcp.alertmanager.default.svc.cluster.local/alertmanager",
			alertmanagerRefreshInterval: time.Duration(60),
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
			name:            "with multiple URLs and no service discovery",
			alertmanagerURL: "http://alertmanager-0.default.svc.cluster.local/alertmanager,http://alertmanager-1.default.svc.cluster.local/alertmanager",
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
			name:            "with basic authentication URL and no service discovery",
			alertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
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
			name:            "with basic authentication URL and service discovery",
			alertmanagerURL: "dnssrv+https://marco:hunter2@_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
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
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with basic authentication URL, no service discovery, and explicit config",
			alertmanagerURL: "http://marco:hunter2@alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				BasicAuth: util.BasicAuth{
					Username: "jacob",
					Password: flagext.SecretWithValue("test"),
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
			name:                        "with multiple URLs and service discovery",
			alertmanagerURL:             "dns+http://alertmanager.mimir.svc.cluster.local:8080/alertmanager,dnssrv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
			alertmanagerRefreshInterval: time.Second,
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							APIVersion: "v2",
							Scheme:     "http",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:            "alertmanager.mimir.svc.cluster.local:8080",
									refreshInterval: time.Second,
									qType:           dns.A,
								},
							},
						},
						{
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/am",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:            "_http._tcp.alertmanager2.mimir.svc.cluster.local",
									refreshInterval: time.Second,
									qType:           dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with service discovery URL, basic auth, and proxy URL",
			alertmanagerURL: "dnssrv+https://marco:hunter2@_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				ProxyURL: "http://my-proxy.proxy-namespace.svc.cluster.local.:1234",
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								BasicAuth: &config_util.BasicAuth{Username: "marco", Password: "hunter2"},
								ProxyConfig: config_util.ProxyConfig{
									ProxyURL: config_util.URL{URL: urlMustParse(t, "http://my-proxy.proxy-namespace.svc.cluster.local.:1234")},
								},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with OAuth2",
			alertmanagerURL: "dnssrv+https://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
				},
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								OAuth2: &config_util.OAuth2{
									ClientID:     "oauth2-client-id",
									ClientSecret: "test",
									TokenURL:     "https://oauth2-token-endpoint.local/token",
								},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with OAuth2 and optional scopes",
			alertmanagerURL: "dnssrv+https://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
					Scopes:       flagext.StringSliceCSV([]string{"action-1", "action-2"}),
				},
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								OAuth2: &config_util.OAuth2{
									ClientID:     "oauth2-client-id",
									ClientSecret: "test",
									TokenURL:     "https://oauth2-token-endpoint.local/token",
									Scopes:       []string{"action-1", "action-2"},
								},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with OAuth2 and optional endpoint params",
			alertmanagerURL: "dnssrv+https://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
					EndpointParams: flagext.NewLimitsMapWithData[string](
						map[string]string{
							"param1": "value1",
							"param2": "value2",
						},
						nil,
					),
				},
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								OAuth2: &config_util.OAuth2{
									ClientID:       "oauth2-client-id",
									ClientSecret:   "test",
									TokenURL:       "https://oauth2-token-endpoint.local/token",
									EndpointParams: map[string]string{"param1": "value1", "param2": "value2"},
								},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with OAuth2 and proxy_url simultaneously, inheriting proxy",
			alertmanagerURL: "dnssrv+https://_http._tcp.alertmanager-0.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				ProxyURL: "http://my-proxy.proxy-namespace.svc.cluster.local.:1234",
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
					Scopes:       flagext.StringSliceCSV([]string{"action-1", "action-2"}),
				},
			},
			ncfg: &config.Config{
				AlertingConfig: config.AlertingConfig{
					AlertmanagerConfigs: []*config.AlertmanagerConfig{
						{
							HTTPClientConfig: config_util.HTTPClientConfig{
								OAuth2: &config_util.OAuth2{
									ClientID:     "oauth2-client-id",
									ClientSecret: "test",
									TokenURL:     "https://oauth2-token-endpoint.local/token",
									Scopes:       []string{"action-1", "action-2"},
									ProxyConfig: config_util.ProxyConfig{
										ProxyURL: config_util.URL{URL: urlMustParse(t, "http://my-proxy.proxy-namespace.svc.cluster.local.:1234")},
									},
								},
								ProxyConfig: config_util.ProxyConfig{
									ProxyURL: config_util.URL{URL: urlMustParse(t, "http://my-proxy.proxy-namespace.svc.cluster.local.:1234")},
								},
							},
							APIVersion: "v2",
							Scheme:     "https",
							PathPrefix: "/alertmanager",
							ServiceDiscoveryConfigs: discovery.Configs{
								dnsServiceDiscovery{
									host:  "_http._tcp.alertmanager-0.default.svc.cluster.local",
									qType: dns.SRV,
								},
							},
						},
					},
				},
			},
		},
		{
			name:            "with DNS service discovery and missing scheme",
			alertmanagerURL: "dns+alertmanager.mimir.svc.cluster.local:8080/alertmanager",
			err:             errors.New("improperly formatted alertmanager URL \"alertmanager.mimir.svc.cluster.local:8080/alertmanager\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
		},
		{
			name:            "with only dns+ prefix",
			alertmanagerURL: "dns+",
			err:             errors.New("improperly formatted alertmanager URL \"\" (maybe the scheme is missing?); see DNS Service Discovery docs"),
		},
		{
			name:            "misspelled DNS SD format prefix (dnsserv+ vs dnssrv+)",
			alertmanagerURL: "dnsserv+https://_http._tcp.alertmanager2.mimir.svc.cluster.local/am",
			err:             errors.New("invalid DNS service discovery prefix \"dnsserv\""),
		},
		{
			name:            "misspelled proxy URL",
			alertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				ProxyURL: "http://example.local" + string(rune(0x7f)),
			},
			err: errors.New("parse \"http://example.local\\x7f\": net/url: invalid control character in URL"),
		},
		{
			name:            "basic auth and oauth provided at the same time",
			alertmanagerURL: "http://alertmanager.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				BasicAuth: util.BasicAuth{
					Username: "test-user",
				},
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
					Scopes:       flagext.StringSliceCSV([]string{"action-1", "action-2"}),
				},
			},
			err: errRulerSimultaneousBasicAuthAndOAuth,
		},
		{
			name:            "basic auth via URL and oauth provided at the same time",
			alertmanagerURL: "http://marco:hunter2@alertmanager.default.svc.cluster.local/alertmanager",
			notifier: notifier.Config{
				OAuth2: notifier.OAuth2Config{
					ClientID:     "oauth2-client-id",
					ClientSecret: flagext.SecretWithValue("test"),
					TokenURL:     "https://oauth2-token-endpoint.local/token",
					Scopes:       flagext.StringSliceCSV([]string{"action-1", "action-2"}),
				},
			},
			err: errRulerSimultaneousBasicAuthAndOAuth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ncfg, err := buildNotifierConfig(tt.alertmanagerURL, tt.notifier, nil, tt.notificationTimeout, tt.alertmanagerRefreshInterval, nil)
			if tt.err == nil {
				require.NoError(t, err)
				require.Equal(t, tt.ncfg, ncfg)
			} else {
				require.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

func TestOAuth2Config_ValidateEndpointParams(t *testing.T) {
	for name, tc := range map[string]struct {
		args     []string
		expected flagext.LimitsMap[string]
		error    string
	}{
		"basic test": {
			args: []string{"-map-flag", "{\"param1\": \"value1\" }"},
			expected: flagext.NewLimitsMapWithData(map[string]string{
				"param1": "value1",
			}, nil),
		},
		"parsing error": {
			args:  []string{"-map-flag", "{\"hello\": ..."},
			error: "invalid value \"{\\\"hello\\\": ...\" for flag -map-flag: invalid character '.' looking for beginning of value",
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := flagext.NewLimitsMap[string](nil)

			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			fs.SetOutput(&bytes.Buffer{}) // otherwise errors would go to stderr.
			fs.Var(v, "map-flag", "Map flag, you can pass JSON into this")
			err := fs.Parse(tc.args)

			if tc.error != "" {
				require.NotNil(t, err)
				assert.Equal(t, tc.error, err.Error())
			} else {
				assert.NoError(t, err)
				assert.True(t, maps.Equal(tc.expected.Read(), v.Read()))
			}
		})
	}
}

func urlMustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
