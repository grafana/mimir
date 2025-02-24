package notify

import (
	"fmt"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"

	"github.com/grafana/alerting/images"
	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/alertmanager"
	"github.com/grafana/alerting/receivers/dinding"
	"github.com/grafana/alerting/receivers/discord"
	"github.com/grafana/alerting/receivers/email"
	"github.com/grafana/alerting/receivers/googlechat"
	"github.com/grafana/alerting/receivers/kafka"
	"github.com/grafana/alerting/receivers/line"
	"github.com/grafana/alerting/receivers/mqtt"
	"github.com/grafana/alerting/receivers/oncall"
	"github.com/grafana/alerting/receivers/opsgenie"
	"github.com/grafana/alerting/receivers/pagerduty"
	"github.com/grafana/alerting/receivers/pushover"
	"github.com/grafana/alerting/receivers/sensugo"
	"github.com/grafana/alerting/receivers/slack"
	"github.com/grafana/alerting/receivers/sns"
	"github.com/grafana/alerting/receivers/teams"
	"github.com/grafana/alerting/receivers/telegram"
	"github.com/grafana/alerting/receivers/threema"
	"github.com/grafana/alerting/receivers/victorops"
	"github.com/grafana/alerting/receivers/webex"
	"github.com/grafana/alerting/receivers/webhook"
	"github.com/grafana/alerting/receivers/wecom"
	"github.com/grafana/alerting/templates"
)

// BuildReceiverIntegrations creates integrations for each configured notification channel in GrafanaReceiverConfig.
// It returns a slice of Integration objects, one for each notification channel, along with any errors that occurred.
func BuildReceiverIntegrations(
	receiver GrafanaReceiverConfig,
	tmpl *templates.Template,
	img images.Provider,
	logger logging.LoggerFactory,
	newWebhookSender func(n receivers.Metadata) (receivers.WebhookSender, error),
	newEmailSender func(n receivers.Metadata) (receivers.EmailSender, error),
	orgID int64,
	version string,
) ([]*Integration, error) {
	type notificationChannel interface {
		notify.Notifier
		notify.ResolvedSender
	}
	var (
		integrations []*Integration
		errors       types.MultiError
		nl           = func(meta receivers.Metadata) logging.Logger {
			return logger("ngalert.notifier."+meta.Type, "notifierUID", meta.UID)
		}
		ci = func(idx int, cfg receivers.Metadata, n notificationChannel) {
			i := NewIntegration(n, n, cfg.Type, idx, cfg.Name)
			integrations = append(integrations, i)
		}
		nw = func(cfg receivers.Metadata) receivers.WebhookSender {
			w, e := newWebhookSender(cfg)
			if e != nil {
				errors.Add(fmt.Errorf("unable to build webhook client for %s notifier %s (UID: %s): %w ", cfg.Type, cfg.Name, cfg.UID, e))
				return nil // return nil to simplify the construction code. This works because constructor in notifiers do not check the argument for nil.
				// This does not cause misconfigured notifiers because it populates `errors`, which causes the function to return nil integrations and non-nil error.
			}
			return w
		}
	)
	// Range through each notification channel in the receiver and create an integration for it.
	for i, cfg := range receiver.AlertmanagerConfigs {
		ci(i, cfg.Metadata, alertmanager.New(cfg.Settings, cfg.Metadata, img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.DingdingConfigs {
		ci(i, cfg.Metadata, dinding.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.DiscordConfigs {
		ci(i, cfg.Metadata, discord.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), version))
	}
	for i, cfg := range receiver.EmailConfigs {
		mailCli, e := newEmailSender(cfg.Metadata)
		if e != nil {
			errors.Add(fmt.Errorf("unable to build email client for %s notifier %s (UID: %s): %w ", cfg.Type, cfg.Name, cfg.UID, e))
			continue
		}
		ci(i, cfg.Metadata, email.New(cfg.Settings, cfg.Metadata, tmpl, mailCli, img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.GooglechatConfigs {
		ci(i, cfg.Metadata, googlechat.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), version))
	}
	for i, cfg := range receiver.KafkaConfigs {
		ci(i, cfg.Metadata, kafka.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.LineConfigs {
		ci(i, cfg.Metadata, line.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.MqttConfigs {
		ci(i, cfg.Metadata, mqtt.New(cfg.Settings, cfg.Metadata, tmpl, nl(cfg.Metadata), nil))
	}
	for i, cfg := range receiver.OnCallConfigs {
		ci(i, cfg.Metadata, oncall.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), orgID))
	}
	for i, cfg := range receiver.OpsgenieConfigs {
		ci(i, cfg.Metadata, opsgenie.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.PagerdutyConfigs {
		ci(i, cfg.Metadata, pagerduty.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.PushoverConfigs {
		ci(i, cfg.Metadata, pushover.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.SensugoConfigs {
		ci(i, cfg.Metadata, sensugo.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.SNSConfigs {
		ci(i, cfg.Metadata, sns.New(cfg.Settings, cfg.Metadata, tmpl, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.SlackConfigs {
		ci(i, cfg.Metadata, slack.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), version))
	}
	for i, cfg := range receiver.TeamsConfigs {
		ci(i, cfg.Metadata, teams.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.TelegramConfigs {
		ci(i, cfg.Metadata, telegram.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.ThreemaConfigs {
		ci(i, cfg.Metadata, threema.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.VictoropsConfigs {
		ci(i, cfg.Metadata, victorops.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), version))
	}
	for i, cfg := range receiver.WebhookConfigs {
		ci(i, cfg.Metadata, webhook.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), orgID))
	}
	for i, cfg := range receiver.WecomConfigs {
		ci(i, cfg.Metadata, wecom.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), nl(cfg.Metadata)))
	}
	for i, cfg := range receiver.WebexConfigs {
		ci(i, cfg.Metadata, webex.New(cfg.Settings, cfg.Metadata, tmpl, nw(cfg.Metadata), img, nl(cfg.Metadata), orgID))
	}
	if errors.Len() > 0 {
		return nil, &errors
	}
	return integrations, nil
}
