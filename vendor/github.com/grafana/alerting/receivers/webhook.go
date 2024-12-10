package receivers

import (
	"context"
	"crypto/tls"
)

type SendWebhookSettings struct {
	URL         string
	User        string
	Password    string
	Body        string
	HTTPMethod  string
	HTTPHeader  map[string]string
	ContentType string
	Validation  func(body []byte, statusCode int) error
	TLSConfig   *tls.Config
}

type WebhookSender interface {
	SendWebhook(ctx context.Context, cmd *SendWebhookSettings) error
}
