package line

import (
	"context"
	"fmt"
	"net/url"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"

	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

var (
	// APIURL of where the notification payload is sent. It is public to be overridable in integration tests.
	// API document link: https://notify-bot.line.me/doc/en/
	APIURL = "https://notify-api.line.me/api/notify"
)

// LINE Notify supports 1000 chars max - from https://notify-bot.line.me/doc/en/
const lineMaxMessageLenRunes = 1000

// Notifier is responsible for sending
// alert notifications to LINE.
type Notifier struct {
	*receivers.Base
	log      logging.Logger
	ns       receivers.WebhookSender
	tmpl     *templates.Template
	settings Config
}

func New(cfg Config, meta receivers.Metadata, template *templates.Template, sender receivers.WebhookSender, logger logging.Logger) *Notifier {
	return &Notifier{
		Base:     receivers.NewBase(meta),
		log:      logger,
		ns:       sender,
		tmpl:     template,
		settings: cfg,
	}
}

// Notify send an alert notification to LINE
func (ln *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	ln.log.Debug("executing line notification", "notification", ln.Name)

	body, err := ln.buildLineMessage(ctx, as...)
	if err != nil {
		return false, fmt.Errorf("failed to build message: %w", err)
	}

	form := url.Values{}
	form.Add("message", body)

	cmd := &receivers.SendWebhookSettings{
		URL:        APIURL,
		HTTPMethod: "POST",
		HTTPHeader: map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", ln.settings.Token),
			"Content-Type":  "application/x-www-form-urlencoded;charset=UTF-8",
		},
		Body: form.Encode(),
	}

	if err := ln.ns.SendWebhook(ctx, cmd); err != nil {
		ln.log.Error("failed to send notification to LINE", "error", err, "body", body)
		return false, err
	}

	return true, nil
}

func (ln *Notifier) SendResolved() bool {
	return !ln.GetDisableResolveMessage()
}

func (ln *Notifier) buildLineMessage(ctx context.Context, as ...*types.Alert) (string, error) {
	var tmplErr error
	tmpl, _ := templates.TmplText(ctx, ln.tmpl, as, ln.log, &tmplErr)

	body := fmt.Sprintf(
		"%s\n%s",
		tmpl(ln.settings.Title),
		tmpl(ln.settings.Description),
	)
	if tmplErr != nil {
		ln.log.Warn("failed to template Line message", "error", tmplErr.Error())
	}

	message, truncated := receivers.TruncateInRunes(body, lineMaxMessageLenRunes)
	if truncated {
		key, err := notify.ExtractGroupKey(ctx)
		if err != nil {
			return "", err
		}
		ln.log.Warn("Truncated message", "alert", key, "max_runes", lineMaxMessageLenRunes)
	}
	return message, nil
}
