// SPDX-License-Identifier: AGPL-3.0-only

package notifier

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/require"
)

func TestAlertmanagerClientConfig(t *testing.T) {
	t.Run("IsDefault", func(t *testing.T) {
		tc := []struct {
			name string
			cfg  *AlertmanagerClientConfig
			exp  bool
		}{
			{
				name: "default",
				cfg:  &DefaultAlertmanagerClientConfig,
				exp:  true,
			},
			{
				name: "initialized limits map",
				cfg: &AlertmanagerClientConfig{
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							EndpointParams: flagext.NewLimitsMap[string](nil),
						},
					},
				},
				exp: true,
			},
			{
				name: "empty scopes",
				cfg: &AlertmanagerClientConfig{
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							Scopes: []string{},
						},
					},
				},
				exp: true,
			},
			{
				name: "golang default",
				cfg:  &AlertmanagerClientConfig{},
				exp:  true,
			},
			{
				name: "custom TLS reader ignored",
				cfg: &AlertmanagerClientConfig{
					NotifierConfig: Config{
						TLS: tls.ClientConfig{
							Reader: &fakeSecretReader{},
						},
					},
				},
				exp: true,
			},
			{
				name: "modified field",
				cfg: &AlertmanagerClientConfig{
					AlertmanagerURL: "test",
				},
				exp: false,
			},
			{
				name: "modified endpoint param",
				cfg: &AlertmanagerClientConfig{
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{"k1": "v1"}, nil),
						},
					},
				},
				exp: false,
			},
			{
				name: "modified scope",
				cfg: &AlertmanagerClientConfig{
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							Scopes: []string{"asdf"},
						},
					},
				},
				exp: false,
			},
		}

		for _, tt := range tc {
			t.Run(tt.name, func(t *testing.T) {
				require.Equal(t, tt.exp, tt.cfg.IsDefault(), tt.cfg)
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		tc := []struct {
			name string
			cfg1 AlertmanagerClientConfig
			cfg2 AlertmanagerClientConfig
			exp  bool
		}{
			{
				name: "same values",
				cfg1: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						ProxyURL: "http://some-proxy:1234",
						TLS: tls.ClientConfig{
							CertPath:           "cert-path",
							KeyPath:            "key-path",
							CAPath:             "ca-path",
							ServerName:         "server",
							InsecureSkipVerify: true,
							CipherSuites:       "TLS_AES_256_GCM_SHA384",
							MinVersion:         "1.3",
						},
						OAuth2: OAuth2Config{
							ClientID:     "myclient",
							ClientSecret: flagext.SecretWithValue("mysecret"),
							TokenURL:     "http://token-url",
							Scopes:       []string{"abc", "def"},
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key1": "value1",
							}, nil),
						},
					},
				},
				cfg2: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						ProxyURL: "http://some-proxy:1234",
						TLS: tls.ClientConfig{
							CertPath:           "cert-path",
							KeyPath:            "key-path",
							CAPath:             "ca-path",
							ServerName:         "server",
							InsecureSkipVerify: true,
							CipherSuites:       "TLS_AES_256_GCM_SHA384",
							MinVersion:         "1.3",
						},
						OAuth2: OAuth2Config{
							ClientID:     "myclient",
							ClientSecret: flagext.SecretWithValue("mysecret"),
							TokenURL:     "http://token-url",
							Scopes:       []string{"abc", "def"},
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key1": "value1",
							}, nil),
						},
					},
				},
				exp: true,
			},
			{
				name: "differing value",
				cfg1: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						ProxyURL: "http://some-proxy:1234",
						TLS: tls.ClientConfig{
							CertPath:           "cert-path",
							KeyPath:            "key-path",
							CAPath:             "ca-path",
							ServerName:         "server",
							InsecureSkipVerify: true,
							CipherSuites:       "TLS_AES_256_GCM_SHA384",
							MinVersion:         "1.3",
						},
						OAuth2: OAuth2Config{
							ClientID:     "myclient",
							ClientSecret: flagext.SecretWithValue("mysecret"),
							TokenURL:     "http://token-url",
							Scopes:       []string{"abc", "def"},
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key1": "value1",
							}, nil),
						},
					},
				},
				cfg2: AlertmanagerClientConfig{
					AlertmanagerURL: "http://another-url",
					NotifierConfig: Config{
						ProxyURL: "http://some-proxy:1234",
						TLS: tls.ClientConfig{
							CertPath:           "cert-path",
							KeyPath:            "key-path",
							CAPath:             "ca-path",
							ServerName:         "server",
							InsecureSkipVerify: true,
							CipherSuites:       "TLS_AES_256_GCM_SHA384",
							MinVersion:         "1.3",
						},
						OAuth2: OAuth2Config{
							ClientID:     "myclient",
							ClientSecret: flagext.SecretWithValue("mysecret"),
							TokenURL:     "http://token-url",
							Scopes:       []string{"abc", "def"},
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key1": "value1",
							}, nil),
						},
					},
				},
				exp: false,
			},
			{
				name: "different endpoint params order",
				cfg1: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key1": "value1",
								"key2": "value2",
							}, nil),
						},
					},
				},
				cfg2: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
								"key2": "value2",
								"key1": "value1",
							}, nil),
						},
					},
				},
				exp: true,
			},
			{
				name: "different scopes order",
				cfg1: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							Scopes: []string{"s1", "s2"},
						},
					},
				},
				cfg2: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						OAuth2: OAuth2Config{
							Scopes: []string{"s2", "s1"},
						},
					},
				},
				exp: false,
			},
			{
				name: "ignores different secrets reader",
				cfg1: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						TLS: tls.ClientConfig{
							Reader: &fakeSecretReader{},
						},
						OAuth2: OAuth2Config{
							ClientID: "myclient",
						},
					},
				},
				cfg2: AlertmanagerClientConfig{
					AlertmanagerURL: "http://some-url",
					NotifierConfig: Config{
						TLS: tls.ClientConfig{
							Reader: nil,
						},
						OAuth2: OAuth2Config{
							ClientID: "myclient",
						},
					},
				},
				exp: true,
			},
		}

		for _, tt := range tc {
			t.Run(tt.name, func(t *testing.T) {
				require.Equal(t, tt.exp, tt.cfg1.Equal(tt.cfg2), cmp.Diff(tt.cfg1, tt.cfg2))
			})
		}
	})
}

type fakeSecretReader struct{}

func (fsr *fakeSecretReader) ReadSecret(_ string) ([]byte, error) {
	return []byte{}, nil
}
