package line

import (
	v1 "github.com/grafana/alerting/receivers/line/v1"
	"github.com/grafana/alerting/receivers/schema"
)

const Type schema.IntegrationType = "LINE"

func Schema() schema.IntegrationTypeSchema {
	return schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "LINE",
		Description:    "Send notifications to LINE notify",
		Heading:        "LINE notify settings",
		CurrentVersion: v1.Version,
		Versions: []schema.IntegrationSchemaVersion{
			v1.Schema(),
		},
	}
}
