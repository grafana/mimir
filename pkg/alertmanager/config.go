// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	"github.com/grafana/alerting/receivers"
	"github.com/prometheus/alertmanager/config"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/util/version"
)

// createUsableGrafanaConfig creates an amConfig from a GrafanaAlertConfigDesc.
// If provided, it assigns the global section from the Mimir config to the Grafana config.
// The amConfig.EmailConfig field can be used to create Grafana email integrations.
func createUsableGrafanaConfig(logger log.Logger, gCfg alertspb.GrafanaAlertConfigDesc, rawMimirConfig string) (amConfig, error) {
	externalURL, err := url.Parse(gCfg.ExternalUrl)
	if err != nil {
		return amConfig{}, err
	}

	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(gCfg.RawConfig), &amCfg); err != nil {
		return amConfig{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration: %w", err)
	}

	if rawMimirConfig != "" {
		cfg, err := definition.LoadCompat([]byte(rawMimirConfig))
		if err != nil {
			return amConfig{}, fmt.Errorf("failed to unmarshal Mimir Alertmanager configuration: %w", err)
		}

		amCfg.AlertmanagerConfig.Global = cfg.Global
	}

	// If configured, use the SMTP From address sent by Grafana.
	// TODO: Remove once it's sent in the SmtpConfig field.
	if gCfg.SmtpFrom != "" {
		if amCfg.AlertmanagerConfig.Global == nil {
			defaultGlobals := config.DefaultGlobalConfig()
			amCfg.AlertmanagerConfig.Global = &defaultGlobals
		}
		amCfg.AlertmanagerConfig.Global.SMTPFrom = gCfg.SmtpFrom
	}

	// We want to:
	// 1. Remove duplicate receivers and keep the last receiver occurrence if there are conflicts. This is based on the upstream implementation.
	// 2. Maintain a consistent ordering and preferably original ordering. Otherwise, change detection will be impacted.
	lastIndex := make(map[string]int, len(amCfg.AlertmanagerConfig.Receivers))
	for i, receiver := range amCfg.AlertmanagerConfig.Receivers {
		lastIndex[receiver.Name] = i
	}
	rcvs := make([]*definition.PostableApiReceiver, 0, len(lastIndex))
	for i, rcv := range amCfg.AlertmanagerConfig.Receivers {
		if i != lastIndex[rcv.Name] {
			itypes := make([]string, 0, len(rcv.GrafanaManagedReceivers))
			for _, integration := range rcv.GrafanaManagedReceivers {
				itypes = append(itypes, integration.Type)
			}
			level.Debug(logger).Log("msg", "receiver with same name is defined multiple times. Only the last one will be used", "receiver_name", rcv.Name, "overwritten_integrations", strings.Join(itypes, ","))
			continue
		}
		rcvs = append(rcvs, rcv)
	}
	amCfg.AlertmanagerConfig.Receivers = rcvs

	rawCfg, err := definition.MarshalJSONWithSecrets(amCfg.AlertmanagerConfig)
	if err != nil {
		return amConfig{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	emailCfg := genEmailConfig(gCfg, amCfg.AlertmanagerConfig.Global)

	// The map can only contain templates of the Grafana kind.
	// Do not care about possible duplicates because Grafana will provide Grafana kind templates in either Templates or TemplateFiles.
	tmpl := append(amCfg.Templates, definition.TemplatesMapToPostableAPITemplates(amCfg.TemplateFiles, definition.GrafanaTemplateKind)...)

	return amConfig{
		User:               gCfg.User,
		RawConfig:          string(rawCfg),
		Templates:          tmpl,
		TmplExternalURL:    externalURL,
		UsingGrafanaConfig: true,
		EmailConfig:        emailCfg,
	}, nil
}

func templateDescToPostableApiTemplate(t []*alertspb.TemplateDesc, kind definition.TemplateKind) []definition.PostableApiTemplate {
	result := make([]definition.PostableApiTemplate, 0, len(t))
	for _, desc := range t {
		result = append(result, definition.PostableApiTemplate{
			Name:    desc.Filename,
			Content: desc.Body,
			Kind:    kind,
		})
	}
	return result
}

// genEmailConfig returns an EmailSenderConfig built with globals + custom Grafana SMTP configs.
// If no globals are provided, the default globals are used.
func genEmailConfig(cfg alertspb.GrafanaAlertConfigDesc, g *config.GlobalConfig) receivers.EmailSenderConfig {
	if g == nil {
		defaultGlobals := config.DefaultGlobalConfig()
		g = &defaultGlobals
	}

	emailCfg := receivers.EmailSenderConfig{
		AuthPassword: string(g.SMTPAuthPassword),
		AuthUser:     g.SMTPAuthUsername,
		CertFile:     g.HTTPConfig.TLSConfig.CertFile,
		ContentTypes: []string{"text/html"},
		EhloIdentity: g.SMTPHello,
		ExternalURL:  cfg.ExternalUrl,
		FromAddress:  g.SMTPFrom,
		FromName:     "Grafana",
		Host:         g.SMTPSmarthost.String(),
		KeyFile:      g.HTTPConfig.TLSConfig.KeyFile,
		SkipVerify:   !g.SMTPRequireTLS,
		SentBy:       fmt.Sprintf("Mimir v%s", version.Version),

		// TODO: Remove once it's sent in SmtpConfig.
		StaticHeaders: cfg.StaticHeaders,
	}

	// Patch the base config with the custom SMTP config sent by Grafana.
	if cfg.SmtpConfig != nil {
		if cfg.SmtpConfig.EhloIdentity != "" {
			emailCfg.EhloIdentity = cfg.SmtpConfig.EhloIdentity
		}
		if cfg.SmtpConfig.FromAddress != "" {
			emailCfg.FromAddress = cfg.SmtpConfig.FromAddress
		}
		if cfg.SmtpConfig.FromName != "" {
			emailCfg.FromName = cfg.SmtpConfig.FromName
		}
		if cfg.SmtpConfig.Host != "" {
			emailCfg.Host = cfg.SmtpConfig.Host
		}
		if cfg.SmtpConfig.Password != "" {
			emailCfg.AuthPassword = cfg.SmtpConfig.Password
		}
		emailCfg.SkipVerify = cfg.SmtpConfig.SkipVerify
		if cfg.SmtpConfig.StartTlsPolicy != "" {
			emailCfg.StartTLSPolicy = cfg.SmtpConfig.StartTlsPolicy
		}
		if cfg.SmtpConfig.StaticHeaders != nil {
			emailCfg.StaticHeaders = cfg.SmtpConfig.StaticHeaders
		}
		if cfg.SmtpConfig.User != "" {
			emailCfg.AuthUser = cfg.SmtpConfig.User
		}
	}
	return emailCfg
}
