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

	// Validation is a function that will validate the response body and statusCode of the webhook. Any returned error will cause the webhook request to be considered failed.
	// This can be useful when a webhook service communicates failures in creative ways, such as using the response body instead of the status code.
	Validation func(body []byte, statusCode int) error
	TLSConfig  *tls.Config
}

type WebhookSender interface {
	SendWebhook(ctx context.Context, cmd *SendWebhookSettings) error
}
