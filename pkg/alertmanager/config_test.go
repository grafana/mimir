// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/alerting/definition"
	"github.com/grafana/alerting/receivers"
	"github.com/prometheus/alertmanager/config"
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

var grafanaConfigWithTemplates = `{"template_files":{"first.tpl":"{{ define \"t1\" }}Gra-gra{{end}}"},"templates":[{"name":"def.tpl","kind":"mimir","content":"{{ define \"t1\" }}Mimi-mi{{end}}"}],"alertmanager_config":{"route":{"receiver":"default","group_by":["grafana_folder","alertname"]},"receivers":[{"name":"default","grafana_managed_receiver_configs":[{"name":"WH","type":"webhook","settings":{"url":"http://localhost:8080"}}],"webhook_configs":[{"url":"http://localhost:8081"}]}]}}`

func TestAmConfigFromGrafanaConfig(t *testing.T) {
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
		fallbackConfig       string
		expEmailSenderConfig receivers.EmailSenderConfig
		expErr               string
		expTemplates         []definition.PostableApiTemplate
	}{
		{
			name: "empty grafana config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   "",
			},
			fallbackConfig: mimirConfig,
			expErr:         "failed to unmarshal Grafana Alertmanager configuration: unexpected end of JSON input",
		},
		{
			name: "invalid grafana config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   "invalid",
				SmtpConfig:  smtpConfig,
			},
			fallbackConfig: mimirConfig,
			expErr:         "failed to unmarshal Grafana Alertmanager configuration: invalid character 'i' looking for beginning of value",
		},
		{
			name: "no fallback config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig:  smtpConfig,
			},
			expEmailSenderConfig: baseEmailSenderConfig,
		},
		{
			name: "no fallback config, custom SMTP config",
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
			name: "non-empty fallback config",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig:  smtpConfig,
			},
			fallbackConfig:       mimirConfig,
			expEmailSenderConfig: baseEmailSenderConfig,
		},
		{
			name: "non-empty fallback config, empty SMTP from address",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfig,
				SmtpConfig: &alertspb.SmtpConfig{
					StaticHeaders: staticHeaders,
				},
			},
			fallbackConfig: mimirConfig,
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
			name: "non-empty fallback config, SmtpFrom and StaticHeaders fields",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl:   externalURL,
				RawConfig:     grafanaConfig,
				SmtpFrom:      "custom@example.com",
				StaticHeaders: map[string]string{"test": "test"},
			},
			fallbackConfig: mimirConfig,
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
			name: "non-empty fallback config, custom SMTP config",
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
			fallbackConfig: mimirConfig,
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
			name: "Grafana config with multiple templates",
			grafanaConfig: alertspb.GrafanaAlertConfigDesc{
				ExternalUrl: externalURL,
				RawConfig:   grafanaConfigWithTemplates,
				SmtpConfig:  smtpConfig,
			},
			expEmailSenderConfig: baseEmailSenderConfig,
			expTemplates: []definition.PostableApiTemplate{
				{
					Name:    "def.tpl",
					Kind:    definition.MimirTemplateKind,
					Content: `{{ define "t1" }}Mimi-mi{{end}}`,
				},
				{
					Name:    "first.tpl",
					Kind:    definition.GrafanaTemplateKind,
					Content: `{{ define "t1" }}Gra-gra{{end}}`,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			am := MultitenantAlertmanager{
				logger:         log.NewNopLogger(),
				fallbackConfig: test.fallbackConfig,
			}
			cfg, err := am.amConfigFromGrafanaConfig(test.grafanaConfig)
			if test.expErr != "" {
				require.Error(t, err)
				require.Equal(t, test.expErr, err.Error())
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.grafanaConfig.User, cfg.User)
			require.Equal(t, test.grafanaConfig.ExternalUrl, cfg.TmplExternalURL.String())
			require.True(t, cfg.UsingGrafanaConfig)

			secret, err := json.Marshal(config.Secret("very-secret"))
			require.NoError(t, err)
			require.NotContainsf(t, cfg.RawConfig, string(secret), "masked secrets should not be present in the config")

			if test.grafanaConfig.SmtpConfig != nil {
				require.Equal(t, test.grafanaConfig.SmtpConfig.StaticHeaders, cfg.EmailConfig.StaticHeaders)
			} else {
				require.Equal(t, test.grafanaConfig.StaticHeaders, cfg.EmailConfig.StaticHeaders)
			}

			// Custom SMTP settings should be part of the config.
			require.Equal(t, test.expEmailSenderConfig, cfg.EmailConfig)

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
			cfg2, err := am.amConfigFromGrafanaConfig(test.grafanaConfig)
			require.NoError(t, err)

			require.Equal(t, cfg.RawConfig, cfg2.RawConfig)
		})
	}
}
