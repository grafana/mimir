package dingding

import (
	v1 "github.com/grafana/alerting/receivers/dingding/v1"
	"github.com/grafana/alerting/receivers/schema"
)

const Type = schema.DingDingType

var Schema = schema.InitSchema(
	schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "DingDing",
		Description:    "Sends HTTP POST request to DingDing",
		Heading:        "DingDing settings",
		CurrentVersion: v1.Version,
	},
	v1.Schema,
)
