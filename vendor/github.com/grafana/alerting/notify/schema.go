package notify

import (
	"slices"

	"github.com/grafana/alerting/receivers/alertmanager"
	"github.com/grafana/alerting/receivers/dingding"
	"github.com/grafana/alerting/receivers/discord"
	"github.com/grafana/alerting/receivers/email"
	"github.com/grafana/alerting/receivers/googlechat"
	"github.com/grafana/alerting/receivers/jira"
	"github.com/grafana/alerting/receivers/kafka"
	"github.com/grafana/alerting/receivers/line"
	"github.com/grafana/alerting/receivers/mqtt"
	"github.com/grafana/alerting/receivers/oncall"
	"github.com/grafana/alerting/receivers/opsgenie"
	"github.com/grafana/alerting/receivers/pagerduty"
	"github.com/grafana/alerting/receivers/pushover"
	"github.com/grafana/alerting/receivers/schema"
	"github.com/grafana/alerting/receivers/sensugo"
	"github.com/grafana/alerting/receivers/slack"
	"github.com/grafana/alerting/receivers/sns"
	"github.com/grafana/alerting/receivers/teams"
	"github.com/grafana/alerting/receivers/telegram"
	"github.com/grafana/alerting/receivers/threema"
	"github.com/grafana/alerting/receivers/victorops"
	"github.com/grafana/alerting/receivers/webex"
	"github.com/grafana/alerting/receivers/webhook"
	"github.com/grafana/alerting/receivers/wechat"
	"github.com/grafana/alerting/receivers/wecom"
)

// GetSchemaForAllIntegrations returns the current schema for all allSchemas.
func GetSchemaForAllIntegrations() []schema.IntegrationTypeSchema {
	return []schema.IntegrationTypeSchema{
		alertmanager.Schema(),
		dingding.Schema(),
		discord.Schema(),
		email.Schema(),
		googlechat.Schema(),
		jira.Schema(),
		kafka.Schema(),
		line.Schema(),
		mqtt.Schema(),
		oncall.Schema(),
		opsgenie.Schema(),
		pagerduty.Schema(),
		pushover.Schema(),
		sensugo.Schema(),
		slack.Schema(),
		sns.Schema(),
		teams.Schema(),
		telegram.Schema(),
		threema.Schema(),
		victorops.Schema(),
		webex.Schema(),
		webhook.Schema(),
		wechat.Schema(),
		wecom.Schema(),
	}
}

// GetSchemaForIntegration returns the schema for a specific integration type of its alias.
func GetSchemaForIntegration(integrationType schema.IntegrationType) (schema.IntegrationTypeSchema, bool) {
	for _, s := range GetSchemaForAllIntegrations() {
		if slices.Contains(s.GetAllTypes(), integrationType) {
			return s, true
		}
	}
	return schema.IntegrationTypeSchema{}, false
}

// GetSchemaVersionForIntegration returns the schema version for a specific integration type and version.
// Type aliases are supported but the returned version type alias may not match the type.
func GetSchemaVersionForIntegration(integrationType schema.IntegrationType, version schema.Version) (schema.IntegrationSchemaVersion, bool) {
	s, ok := GetSchemaForIntegration(integrationType)
	if !ok {
		return schema.IntegrationSchemaVersion{}, false
	}
	return s.GetVersion(version)
}
