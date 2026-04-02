package telegram

import (
	"github.com/grafana/alerting/receivers/schema"
	"github.com/grafana/alerting/receivers/telegram/v0mimir1"
	v1 "github.com/grafana/alerting/receivers/telegram/v1"
)

const Type = schema.TelegramType

var Schema = schema.InitSchema(
	schema.IntegrationTypeSchema{
		Type:           Type,
		Name:           "Telegram",
		Description:    "Sends notifications to Telegram",
		Heading:        "Telegram API settings",
		CurrentVersion: v1.Version,
	},
	v1.Schema,
	v0mimir1.Schema,
)
