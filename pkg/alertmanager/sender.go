package alertmanager

import (
	"context"

	alertingReceivers "github.com/grafana/alerting/receivers"
)

// sender is a no-op webhook and email sender.
// TODO: make it work.
type sender struct{}

// SendWebhook implements alertingReceivers.WebhookSender.
func (s *sender) SendWebhook(ctx context.Context, cmd *alertingReceivers.SendWebhookSettings) error {
	return nil
}

// SendEmail implements alertingReceivers.EmailSender.
func (s *sender) SendEmail(ctx context.Context, cmd *alertingReceivers.SendEmailSettings) error {
	return nil
}
