package pagerduty

import (
	"github.com/grafana/alerting/receivers/pagerduty/v0mimir1"
	v1 "github.com/grafana/alerting/receivers/pagerduty/v1"
	"github.com/grafana/alerting/receivers/schema"
)

const Type = schema.PagerDutyType

var Schema = schema.InitSchema(
	schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "PagerDuty",
		Description:    "Sends notifications to PagerDuty",
		Heading:        "PagerDuty settings",
		CurrentVersion: v1.Version,
	},
	v1.Schema,
	v0mimir1.Schema,
)
