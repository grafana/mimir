// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/alerting/definition"
	"github.com/grafana/alerting/receivers"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

const grafanaConfigWithDuplicateReceiverName = `{
  "template_files": {},
  "alertmanager_config": {
    "route": {
      "receiver": "test-receiver8",
      "group_by": [
        "grafana_folder",
        "alertname"
      ]
    },
    "templates": null,
    "receivers": [
      {"name": "test-receiver1","grafana_managed_receiver_configs": []},
      {
        "name": "test-receiver8",
        "grafana_managed_receiver_configs": [
          {
            "uid": "dde6ntuob69dtf",
            "name": "test-receiver",
            "type": "webhook",
            "disableResolveMessage": false,
            "settings": {
              "url": "http://localhost:8080",
              "username": "test"
            },
            "secureSettings": {
              "password": "test"
            }
          }
        ]
      },
      {"name": "test-receiver2","grafana_managed_receiver_configs": []},
      {"name": "test-receiver3","grafana_managed_receiver_configs": []},
      {"name": "test-receiver4","grafana_managed_receiver_configs": []},
      {"name": "test-receiver5","grafana_managed_receiver_configs": []},
      {"name": "test-receiver6","grafana_managed_receiver_configs": []},
      {"name": "test-receiver7","grafana_managed_receiver_configs": []},
      {
        "name": "test-receiver8",
        "grafana_managed_receiver_configs": [
          {
            "uid": "dde7ntuob69dtf",
            "name": "test-receiver",
            "type": "webhook",
            "disableResolveMessage": false,
            "settings": {
              "url": "http://localhost:8080",
              "username": "test"
            },
            "secureSettings": {
              "password": "test"
            }
          }
        ]
      },
	  {"name": "test-receiver9","grafana_managed_receiver_configs": []}
    ]
  }
}`

func TestCreateUsableGrafanaConfig(t *testing.T) {
	defaultFromAddress := "grafana@example.com"
	mimirConfig := fmt.Sprintf(`
global:
  smtp_from: %s
route:
  receiver: dummy
receivers:
  - name: dummy
`, defaultFromAddress)

	staticHeaders := map[string]string{"test": "test"}
	smtpConfig := &alertspb.SmtpConfig{
		StaticHeaders: staticHeaders,
		FromAddress:   "test-instance@grafana.com",
	}
	externalURL := "http://test:3000"
	baseEmailSenderConfig := receivers.EmailSenderConfig{
		ContentTypes:  []string{"text/html"},
		EhloIdentity:  "localhost",
		ExternalURL:   externalURL,
		FromAddress:   smtpConfig.FromAddress,
		FromName:      "Grafana",
		SentBy:        "Mimir vunknown", // no 'version' flag passed in tests.
		StaticHeaders: smtpConfig.StaticHeaders,
	}

	tests := []struct {
		name                 string
		grafanaConfig        alertspb.GrafanaAlertConfigDesc
		mimirConfig          string
		expEmailSenderConfig receivers.EmailSenderConfig
		expErr               string
	}{
		{
			name: "empty grafana config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   "",
			},
			mimirConfig: mimirConfig,
			expErr:      "failed to unmarshal Grafana Alertmanager configuration: unexpected end of JSON input",
		},
		{
			name: "invalid grafana config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   "invalid",
				SmtpConfig:  smtpConfig,
			},
			mimirConfig: mimirConfig,
			expErr:      "failed to unmarshal Grafana Alertmanager configuration: invalid character 'i' looking for beginning of value",
		},
		{
			name: "no mimir config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig:  smtpConfig,
			},
			expEmailSenderConfig: baseEmailSenderConfig,
		},
		{
			name: "no mimir config, custom SMTP config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig: &alertspb.SmtpConfig{
					StaticHeaders:  map[string]string{"test": "test"},
					EhloIdentity:   "custom-identity",
					FromAddress:    "custom@address.com",
					FromName:       "Custom From Name",
					Host:           "custom-host",
					Password:       "custom-password",
					SkipVerify:     true,
					StartTlsPolicy: "custom-policy",
					User:           "custom-user",
				},
			},
			expEmailSenderConfig: receivers.EmailSenderConfig{
				AuthPassword:   "custom-password",
				AuthUser:       "custom-user",
				ContentTypes:   baseEmailSenderConfig.ContentTypes,
				EhloIdentity:   "custom-identity",
				ExternalURL:    baseEmailSenderConfig.ExternalURL,
				FromAddress:    "custom@address.com",
				FromName:       "Custom From Name",
				Host:           "custom-host",
				SentBy:         baseEmailSenderConfig.SentBy,
				SkipVerify:     true,
				StartTLSPolicy: "custom-policy",
				StaticHeaders:  map[string]string{"test": "test"},
			},
		},
		{
			name: "duplicate grafana receiver name config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfigWithDuplicateReceiverName,
				SmtpConfig:  smtpConfig,
			},
			expEmailSenderConfig: baseEmailSenderConfig,
		},
		{
			name: "non-empty mimir config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig:  smtpConfig,
			},
			mimirConfig:          mimirConfig,
			expEmailSenderConfig: baseEmailSenderConfig,
		},
		{
			name: "non-empty mimir config, empty SMTP from address",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig: &alertspb.SmtpConfig{
					StaticHeaders: staticHeaders,
				},
			},
			mimirConfig: mimirConfig,
			expEmailSenderConfig: receivers.EmailSenderConfig{
				ContentTypes:  baseEmailSenderConfig.ContentTypes,
				EhloIdentity:  baseEmailSenderConfig.EhloIdentity,
				ExternalURL:   baseEmailSenderConfig.ExternalURL,
				FromAddress:   defaultFromAddress,
				FromName:      baseEmailSenderConfig.FromName,
				SentBy:        baseEmailSenderConfig.SentBy,
				StaticHeaders: baseEmailSenderConfig.StaticHeaders,
			},
		},
		{
			name: "non-empty mimir config, SmtpFrom and StaticHeaders fields",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl:   externalURL,
				RawConfig:     grafanaConfig,
				SmtpFrom:      "custom@example.com",
				StaticHeaders: map[string]string{"test": "test"},
			},
			mimirConfig: mimirConfig,
			expEmailSenderConfig: receivers.EmailSenderConfig{
				ContentTypes:  baseEmailSenderConfig.ContentTypes,
				EhloIdentity:  baseEmailSenderConfig.EhloIdentity,
				ExternalURL:   baseEmailSenderConfig.ExternalURL,
				FromAddress:   "custom@example.com",
				FromName:      baseEmailSenderConfig.FromName,
				SentBy:        baseEmailSenderConfig.SentBy,
				StaticHeaders: map[string]string{"test": "test"},
			},
		},
		{
			name: "non-empty mimir config, custom SMTP config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig: &alertspb.SmtpConfig{
					StaticHeaders:  map[string]string{"test": "test"},
					EhloIdentity:   "custom-identity",
					FromAddress:    "custom@address.com",
					FromName:       "Custom From Name",
					Host:           "custom-host",
					Password:       "custom-password",
					SkipVerify:     true,
					StartTlsPolicy: "custom-policy",
					User:           "custom-user",
				},
			},
			mimirConfig: mimirConfig,
			expEmailSenderConfig: receivers.EmailSenderConfig{
				AuthPassword:   "custom-password",
				AuthUser:       "custom-user",
				ContentTypes:   baseEmailSenderConfig.ContentTypes,
				EhloIdentity:   "custom-identity",
				ExternalURL:    baseEmailSenderConfig.ExternalURL,
				FromAddress:    "custom@address.com",
				FromName:       "Custom From Name",
				Host:           "custom-host",
				SentBy:         baseEmailSenderConfig.SentBy,
				SkipVerify:     true,
				StartTLSPolicy: "custom-policy",
				StaticHeaders:  map[string]string{"test": "test"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			am := MultitenantAlertmanager{logger: log.NewNopLogger()}
			cfg, err := createUsableGrafanaConfig(am.logger, test.grafanaConfig, test.mimirConfig)
			if test.expErr != "" {
				require.Error(t, err)
				require.Equal(t, test.expErr, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.grafanaConfig.User, cfg.User)
			require.Equal(t, test.grafanaConfig.ExternalUrl, cfg.tmplExternalURL.String())
			require.True(t, cfg.usingGrafanaConfig)

			if test.grafanaConfig.SmtpConfig != nil {
				require.Equal(t, test.grafanaConfig.SmtpConfig.StaticHeaders, cfg.emailConfig.StaticHeaders)
			} else {
				require.Equal(t, test.grafanaConfig.StaticHeaders, cfg.emailConfig.StaticHeaders)
			}

			// Custom SMTP settings should be part of the config.
			require.Equal(t, test.expEmailSenderConfig, cfg.emailConfig)

			// Receiver names should be unique.
			var finalCfg definition.PostableApiAlertingConfig
			require.NoError(t, json.Unmarshal([]byte(cfg.RawConfig), &finalCfg))
			receiverNames := map[string]struct{}{}
			for _, rcv := range finalCfg.Receivers {
				_, ok := receiverNames[rcv.Name]
				require.False(t, ok, fmt.Sprintf("duplicate receiver name %q found in final configuration", rcv.Name))
				receiverNames[rcv.Name] = struct{}{}
			}

			// Ensure that configuration is deterministic. For example, ordering of receivers.
			// This is important for change detection.
			cfg2, err := createUsableGrafanaConfig(am.logger, test.grafanaConfig, test.mimirConfig)
			require.NoError(t, err)

			require.Equal(t, cfg.RawConfig, cfg2.RawConfig)
		})
	}
}
