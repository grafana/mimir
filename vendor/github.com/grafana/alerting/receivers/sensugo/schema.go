package sensugo

import (
	"github.com/grafana/alerting/receivers/schema"
	v1 "github.com/grafana/alerting/receivers/sensugo/v1"
)

const Type = schema.SensuGoType

var Schema = schema.InitSchema(
	schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "Sensu Go",
		Description:    "Sends HTTP POST request to a Sensu Go API",
		Heading:        "Sensu Go Settings",
		CurrentVersion: v1.Version,
	},
	v1.Schema,
)
