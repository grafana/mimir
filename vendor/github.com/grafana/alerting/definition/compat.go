package definition

import (
	"encoding/json"

	"github.com/grafana/alerting/notify"
	"github.com/prometheus/alertmanager/config"
)

// GrafanaToUpstreamConfig converts a Grafana alerting configuration into an upstream Alertmanager configuration.
// It ignores the configuration for Grafana receivers, adding only their names.
func GrafanaToUpstreamConfig(cfg *PostableApiAlertingConfig) config.Config {
	rcvs := make([]config.Receiver, 0, len(cfg.Receivers))
	for _, r := range cfg.Receivers {
		rcvs = append(rcvs, r.Receiver)
	}

	return config.Config{
		Global:            cfg.Config.Global,
		Route:             cfg.Config.Route.AsAMRoute(),
		InhibitRules:      cfg.Config.InhibitRules,
		Receivers:         rcvs,
		Templates:         cfg.Config.Templates,
		MuteTimeIntervals: cfg.Config.MuteTimeIntervals,
		TimeIntervals:     cfg.Config.TimeIntervals,
	}
}

func PostableAPIReceiverToAPIReceiver(r *PostableApiReceiver) *notify.APIReceiver {
	integrations := notify.GrafanaIntegrations{
		Integrations: make([]*notify.GrafanaIntegrationConfig, 0, len(r.GrafanaManagedReceivers)),
	}
	for _, p := range r.GrafanaManagedReceivers {
		integrations.Integrations = append(integrations.Integrations, &notify.GrafanaIntegrationConfig{
			UID:                   p.UID,
			Name:                  p.Name,
			Type:                  p.Type,
			DisableResolveMessage: p.DisableResolveMessage,
			Settings:              json.RawMessage(p.Settings),
			SecureSettings:        p.SecureSettings,
		})
	}

	return &notify.APIReceiver{
		ConfigReceiver:      r.Receiver,
		GrafanaIntegrations: integrations,
	}
}
