package oncall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"

	"github.com/grafana/alerting/images"
	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/templates"
)

// Notifier is responsible for sending
// alert notifications as webhooks.
type Notifier struct {
	*receivers.Base
	log      logging.Logger
	ns       receivers.WebhookSender
	images   images.Provider
	tmpl     *templates.Template
	orgID    int64
	settings Config
}

// New is the constructor for
// the WebHook notifier.
func New(cfg Config, meta receivers.Metadata, template *templates.Template, sender receivers.WebhookSender, images images.Provider, logger logging.Logger, orgID int64) *Notifier {
	return &Notifier{
		Base:     receivers.NewBase(meta),
		orgID:    orgID,
		log:      logger,
		ns:       sender,
		images:   images,
		tmpl:     template,
		settings: cfg,
	}
}

// oncallMessage defines the JSON object send to Grafana on-call.
type oncallMessage struct {
	*templates.ExtendedData

	// The protocol version.
	Version         string `json:"version"`
	GroupKey        string `json:"groupKey"`
	OrgID           int64  `json:"orgId"`
	Title           string `json:"title"`
	State           string `json:"state"`
	Message         string `json:"message"`
	TruncatedAlerts uint64 `json:"truncatedAlerts"`
}

// Notify implements the Notifier interface.
func (n *Notifier) Notify(ctx context.Context, as ...*types.Alert) (bool, error) {
	groupKey, err := notify.ExtractGroupKey(ctx)
	if err != nil {
		return false, err
	}

	var numFiring, numResolved uint64
	for _, a := range as {
		if a.Resolved() {
			numResolved++
		} else {
			numFiring++
		}
	}

	as, numTruncated := truncateAlerts(n.settings.MaxAlerts, as)
	var tmplErr error
	tmpl, data := templates.TmplText(ctx, n.tmpl, as, n.log, &tmplErr)

	// Augment our Alert data with ImageURLs if available.
	_ = images.WithStoredImages(ctx, n.log, n.images,
		func(index int, image images.Image) error {
			if len(image.URL) != 0 {
				data.Alerts[index].ImageURL = image.URL
			}
			return nil
		},
		as...)

	msg := &oncallMessage{
		Version:         "1",
		ExtendedData:    data,
		GroupKey:        groupKey.String(),
		OrgID:           n.orgID,
		Title:           tmpl(n.settings.Title),
		Message:         tmpl(n.settings.Message),
		TruncatedAlerts: uint64(numTruncated),
	}
	if types.Alerts(as...).Status() == model.AlertFiring {
		msg.State = string(receivers.AlertStateAlerting)
	} else {
		msg.State = string(receivers.AlertStateOK)
	}

	if tmplErr != nil {
		n.log.Warn("failed to template oncall message", "error", tmplErr.Error())
		tmplErr = nil
	}

	body, err := json.Marshal(msg)
	if err != nil {
		return false, err
	}

	headers := make(map[string]string)
	if n.settings.AuthorizationScheme != "" && n.settings.AuthorizationCredentials != "" {
		headers["Authorization"] = fmt.Sprintf("%s %s", n.settings.AuthorizationScheme, n.settings.AuthorizationCredentials)
	}

	parsedURL := tmpl(n.settings.URL)
	if tmplErr != nil {
		return false, tmplErr
	}

	cmd := &receivers.SendWebhookSettings{
		URL:        parsedURL,
		User:       n.settings.User,
		Password:   n.settings.Password,
		Body:       string(body),
		HTTPMethod: n.settings.HTTPMethod,
		HTTPHeader: headers,
	}

	if err := n.ns.SendWebhook(ctx, cmd); err != nil {
		return false, err
	}

	return true, nil
}

func truncateAlerts(maxAlerts int, alerts []*types.Alert) ([]*types.Alert, int) {
	if maxAlerts > 0 && len(alerts) > maxAlerts {
		return alerts[:maxAlerts], len(alerts) - maxAlerts
	}

	return alerts, 0
}

func (n *Notifier) SendResolved() bool {
	return !n.GetDisableResolveMessage()
}
