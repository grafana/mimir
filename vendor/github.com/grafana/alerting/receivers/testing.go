package receivers

import (
	"context"
)

type NotificationServiceMock struct {
	WebhookCalls []SendWebhookSettings
	Webhook      SendWebhookSettings
	EmailSync    SendEmailSettings
	ShouldError  error
}

func (ns *NotificationServiceMock) SendWebhook(_ context.Context, cmd *SendWebhookSettings) error {
	ns.WebhookCalls = append(ns.WebhookCalls, *cmd)
	ns.Webhook = *cmd
	return ns.ShouldError
}

func (ns *NotificationServiceMock) SendEmail(_ context.Context, cmd *SendEmailSettings) error {
	ns.EmailSync = *cmd
	return ns.ShouldError
}

func MockNotificationService() *NotificationServiceMock { return &NotificationServiceMock{} }
