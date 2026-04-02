package oncall

import (
	v1 "github.com/grafana/alerting/receivers/oncall/v1"
	"github.com/grafana/alerting/receivers/schema"
)

const Type = schema.OnCallType

var Schema = schema.InitSchema(
	schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "Grafana IRM",
		Description:    "Sends alerts to Grafana IRM",
		Heading:        "Grafana IRM settings",
		CurrentVersion: v1.Version,
	},
	v1.Schema,
)
