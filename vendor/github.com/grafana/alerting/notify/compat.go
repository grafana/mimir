package notify

import (
	"encoding/json"

	"github.com/grafana/alerting/definition"
	"github.com/grafana/alerting/models"
	"github.com/grafana/alerting/templates"
)

func PostableAPIReceiversToAPIReceivers(r []*definition.PostableApiReceiver) []*APIReceiver {
	result := make([]*APIReceiver, 0, len(r))
	for _, receiver := range r {
		result = append(result, PostableAPIReceiverToAPIReceiver(receiver))
	}
	return result
}

func PostableAPIReceiverToAPIReceiver(r *definition.PostableApiReceiver) *APIReceiver {
	integrations := models.ReceiverConfig{
		Integrations: make([]*models.IntegrationConfig, 0, len(r.GrafanaManagedReceivers)),
	}
	for _, p := range r.GrafanaManagedReceivers {
		integrations.Integrations = append(integrations.Integrations, PostableGrafanaReceiverToIntegrationConfig(p))
	}

	return &APIReceiver{
		ConfigReceiver: r.Receiver,
		ReceiverConfig: integrations,
	}
}

func PostableGrafanaReceiverToIntegrationConfig(r *definition.PostableGrafanaReceiver) *models.IntegrationConfig {
	return &models.IntegrationConfig{
		UID:                   r.UID,
		Name:                  r.Name,
		Type:                  r.Type,
		DisableResolveMessage: r.DisableResolveMessage,
		Settings:              json.RawMessage(r.Settings),
		SecureSettings:        r.SecureSettings,
	}
}

// PostableAPITemplateToTemplateDefinition converts a definition.PostableApiTemplate to a templates.TemplateDefinition
func PostableAPITemplateToTemplateDefinition(t definition.PostableApiTemplate) templates.TemplateDefinition {
	var kind templates.Kind
	switch t.Kind {
	case definition.GrafanaTemplateKind:
		kind = templates.GrafanaKind
	case definition.MimirTemplateKind:
		kind = templates.MimirKind
	}
	d := templates.TemplateDefinition{
		Name:     t.Name,
		Template: t.Content,
		Kind:     kind,
	}
	return d
}

func PostableAPITemplatesToTemplateDefinitions(ts []definition.PostableApiTemplate) []templates.TemplateDefinition {
	defs := make([]templates.TemplateDefinition, 0, len(ts))
	for _, t := range ts {
		defs = append(defs, PostableAPITemplateToTemplateDefinition(t))
	}
	return defs
}
