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

	"github.com/go-kit/log/level"
	"github.com/grafana/alerting/definition"
	alertingReceivers "github.com/grafana/alerting/receivers"
	"github.com/prometheus/alertmanager/config"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/util/version"
)

// amConfigFromGrafanaConfig creates an amConfig from a GrafanaAlertConfigDesc.
// It uses the 'global' section from the Alertmanager's fallback config (if any) in the final config.
// The amConfig.EmailConfig field can be used to create Grafana email integrations.
func (am *MultitenantAlertmanager) amConfigFromGrafanaConfig(gCfg alertspb.GrafanaAlertConfigDesc) (amConfig, error) {
	externalURL, err := url.Parse(gCfg.ExternalUrl)
	if err != nil {
		return amConfig{}, err
	}

	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(gCfg.RawConfig), &amCfg); err != nil {
		return amConfig{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration: %w", err)
	}

	if am.fallbackConfig != "" {
		cfg, err := definition.LoadCompat([]byte(am.fallbackConfig))
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
			level.Debug(am.logger).Log("msg", "receiver with same name is defined multiple times. Only the last one will be used", "receiver_name", rcv.Name, "overwritten_integrations", strings.Join(itypes, ","))
			continue
		}
		rcvs = append(rcvs, rcv)
	}
	amCfg.AlertmanagerConfig.Receivers = rcvs

	rawCfg, err := definition.MarshalJSONWithSecrets(amCfg.AlertmanagerConfig)
	if err != nil {
		return amConfig{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	// Create base config using globals.
	g := amCfg.AlertmanagerConfig.Global
	if g == nil {
		defaultGlobals := config.DefaultGlobalConfig()
		g = &defaultGlobals
	}

	emailCfg := alertingReceivers.EmailSenderConfig{
		AuthPassword: string(g.SMTPAuthPassword),
		AuthUser:     g.SMTPAuthUsername,
		CertFile:     g.HTTPConfig.TLSConfig.CertFile,
		ContentTypes: []string{"text/html"},
		EhloIdentity: g.SMTPHello,
		ExternalURL:  externalURL.String(),
		FromAddress:  g.SMTPFrom,
		FromName:     "Grafana",
		Host:         g.SMTPSmarthost.String(),
		KeyFile:      g.HTTPConfig.TLSConfig.KeyFile,
		SkipVerify:   !g.SMTPRequireTLS,
		SentBy:       fmt.Sprintf("Mimir v%s", version.Version),

		// TODO: Remove once it's sent in SmtpConfig.
		StaticHeaders: gCfg.StaticHeaders,
	}

	// Patch the base config with the custom SMTP config sent by Grafana.
	if gCfg.SmtpConfig != nil {
		if gCfg.SmtpConfig.EhloIdentity != "" {
			emailCfg.EhloIdentity = gCfg.SmtpConfig.EhloIdentity
		}
		if gCfg.SmtpConfig.FromAddress != "" {
			emailCfg.FromAddress = gCfg.SmtpConfig.FromAddress
		}
		if gCfg.SmtpConfig.FromName != "" {
			emailCfg.FromName = gCfg.SmtpConfig.FromName
		}
		if gCfg.SmtpConfig.Host != "" {
			emailCfg.Host = gCfg.SmtpConfig.Host
		}
		if gCfg.SmtpConfig.Password != "" {
			emailCfg.AuthPassword = gCfg.SmtpConfig.Password
		}
		emailCfg.SkipVerify = gCfg.SmtpConfig.SkipVerify
		if gCfg.SmtpConfig.StartTlsPolicy != "" {
			emailCfg.StartTLSPolicy = gCfg.SmtpConfig.StartTlsPolicy
		}
		if gCfg.SmtpConfig.StaticHeaders != nil {
			emailCfg.StaticHeaders = gCfg.SmtpConfig.StaticHeaders
		}
		if gCfg.SmtpConfig.User != "" {
			emailCfg.AuthUser = gCfg.SmtpConfig.User
		}
	}

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

func amConfigFromMimirConfig(dec alertspb.AlertConfigDesc, url *url.URL) amConfig {
	return amConfig{
		User:               dec.User,
		RawConfig:          dec.RawConfig,
		Templates:          templateDescToPostableApiTemplate(dec.Templates, definition.MimirTemplateKind),
		TmplExternalURL:    url,
		UsingGrafanaConfig: false,
	}
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
