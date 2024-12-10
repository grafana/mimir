package receivers

import "context"

// SendEmailSettings is the command for sending emails
type SendEmailSettings struct {
	To            []string
	SingleEmail   bool
	Template      string
	Subject       string
	Data          map[string]interface{}
	ReplyTo       []string
	EmbeddedFiles []string
}

type EmailSender interface {
	SendEmail(ctx context.Context, cmd *SendEmailSettings) error
}
