package receivers

import "context"

type SendWebhookSettings struct {
	URL         string
	User        string
	Password    string
	Body        string
	HTTPMethod  string
	HTTPHeader  map[string]string
	ContentType string
	Validation  func(body []byte, statusCode int) error
}

type WebhookSender interface {
	SendWebhook(ctx context.Context, cmd *SendWebhookSettings) error
}
