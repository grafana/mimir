package alertmanager

import (
	"encoding/json"
	"fmt"

	"github.com/grafana/alerting/definition"
	alertingNotify "github.com/grafana/alerting/notify"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/dispatch"
	"gopkg.in/yaml.v3"
)

func parseGrafanaConfig(cfg alertspb.GrafanaAlertConfigDesc) (alertspb.AlertConfigDesc, error) {
	var amCfg GrafanaAlertmanagerConfig
	if err := json.Unmarshal([]byte(cfg.RawConfig), &amCfg); err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to unmarshal Grafana Alertmanager configuration %w", err)
	}

	rawCfg, err := json.Marshal(amCfg.AlertmanagerConfig)
	if err != nil {
		return alertspb.AlertConfigDesc{}, fmt.Errorf("failed to marshal Grafana Alertmanager configuration %w", err)
	}

	return alertspb.ToProto(string(rawCfg), amCfg.Templates, cfg.User), nil
}

func loadConfig(rawCfg []byte) (*definition.PostableApiAlertingConfig, error) {
	var cfg definition.PostableApiAlertingConfig
	if err := yaml.Unmarshal(rawCfg, &cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Note: This could be a method in the alerting package.
func grafanaToUpstreamConfig(cfg *definition.PostableApiAlertingConfig) amconfig.Config {
	// Note: We're ignoring the Grafana part of the receivers.
	// TODO: Use it.
	rcvs := make([]amconfig.Receiver, 0, len(cfg.Receivers))
	for _, r := range cfg.Receivers {
		rcvs = append(rcvs, r.Receiver)
	}

	return amconfig.Config{
		Global:            cfg.Config.Global,
		Route:             cfg.Config.Route.AsAMRoute(),
		InhibitRules:      cfg.Config.InhibitRules,
		Receivers:         rcvs,
		Templates:         cfg.Config.Templates,
		MuteTimeIntervals: cfg.Config.MuteTimeIntervals,
		TimeIntervals:     cfg.Config.TimeIntervals,
	}
}

// TODO: move to alerting package.
func postableApiReceiverToApiReceiver(r *definition.PostableApiReceiver) *alertingNotify.APIReceiver {
	integrations := alertingNotify.GrafanaIntegrations{
		Integrations: make([]*alertingNotify.GrafanaIntegrationConfig, 0, len(r.GrafanaManagedReceivers)),
	}
	for _, cfg := range r.GrafanaManagedReceivers {
		integrations.Integrations = append(integrations.Integrations, postableGrafanaReceiverToGrafanaIntegrationConfig(cfg))
	}

	return &alertingNotify.APIReceiver{
		ConfigReceiver:      r.Receiver,
		GrafanaIntegrations: integrations,
	}
}

// TODO: move to alerting package.
func postableGrafanaReceiverToGrafanaIntegrationConfig(p *definition.PostableGrafanaReceiver) *alertingNotify.GrafanaIntegrationConfig {
	return &alertingNotify.GrafanaIntegrationConfig{
		UID:                   p.UID,
		Name:                  p.Name,
		Type:                  p.Type,
		DisableResolveMessage: p.DisableResolveMessage,
		Settings:              json.RawMessage(p.Settings),
		SecureSettings:        p.SecureSettings,
	}
}

// TODO: move to alerting package.
func getActiveReceiversMap(r *dispatch.Route) map[string]struct{} {
	activeReceivers := make(map[string]struct{})
	r.Walk(func(r *dispatch.Route) {
		activeReceivers[r.RouteOpts.Receiver] = struct{}{}
	})
	return activeReceivers
}
