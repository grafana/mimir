package notify

import (
	"encoding/json"

	"github.com/grafana/alerting/definition"
)

func PostableAPIReceiverToAPIReceiver(r *definition.PostableApiReceiver) *APIReceiver {
	integrations := GrafanaIntegrations{
		Integrations: make([]*GrafanaIntegrationConfig, 0, len(r.GrafanaManagedReceivers)),
	}
	for _, p := range r.GrafanaManagedReceivers {
		integrations.Integrations = append(integrations.Integrations, &GrafanaIntegrationConfig{
			UID:                   p.UID,
			Name:                  p.Name,
			Type:                  p.Type,
			DisableResolveMessage: p.DisableResolveMessage,
			Settings:              json.RawMessage(p.Settings),
			SecureSettings:        p.SecureSettings,
		})
	}

	return &APIReceiver{
		ConfigReceiver:      r.Receiver,
		GrafanaIntegrations: integrations,
	}
}
