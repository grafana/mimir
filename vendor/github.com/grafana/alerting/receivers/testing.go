package receivers

import (
	"context"
)

type NotificationServiceMock struct {
	Webhook     SendWebhookSettings
	EmailSync   SendEmailSettings
	ShouldError error
}

func (ns *NotificationServiceMock) SendWebhook(ctx context.Context, cmd *SendWebhookSettings) error {
	ns.Webhook = *cmd
	return ns.ShouldError
}
func (ns *NotificationServiceMock) SendEmail(ctx context.Context, cmd *SendEmailSettings) error {
	ns.EmailSync = *cmd
	return ns.ShouldError
}

func MockNotificationService() *NotificationServiceMock { return &NotificationServiceMock{} }
