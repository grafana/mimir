package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/grafana/alerting/receivers"
)

type sender struct{}

// Copied from Grafana
func (s sender) SendWebhook(ctx context.Context, cmd *receivers.SendWebhookSettings) error {
	if cmd.HTTPMethod == "" {
		cmd.HTTPMethod = http.MethodPost
	}

	// TODO(santiago): proper logs...
	fmt.Println("Sending webhook", "url", cmd.URL, "http method", cmd.HTTPMethod)

	if cmd.HTTPMethod != http.MethodPost && cmd.HTTPMethod != http.MethodPut {
		return fmt.Errorf("webhook only supports HTTP methods PUT or POST")
	}

	req, err := http.NewRequestWithContext(ctx, cmd.HTTPMethod, cmd.URL, bytes.NewReader([]byte(cmd.Body)))
	if err != nil {
		return err
	}

	if cmd.ContentType == "" {
		cmd.ContentType = "application/json"
	}

	req.Header.Set("Content-Type", cmd.ContentType)
	req.Header.Set("User-Agent", "Grafana")

	if cmd.User != "" && cmd.Password != "" {
		req.SetBasicAuth(cmd.User, cmd.Password)
	}

	for k, v := range cmd.HTTPHeader {
		req.Header.Set(k, v)
	}

	// TODO(santiago): don't use the default client
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			// s.log.Warn("Failed to close response body", "err", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if cmd.Validation != nil {
		err := cmd.Validation(body, resp.StatusCode)
		if err != nil {
			// ns.log.Debug("Webhook failed validation", "url", cmd.URL, "statuscode", resp.Status, "body", string(body))
			return fmt.Errorf("webhook failed validation: %w", err)
		}
	}

	if resp.StatusCode/100 == 2 {
		// ns.log.Debug("Webhook succeeded", "url", cmd.URL, "statuscode", resp.Status)
		return nil
	}

	// ns.log.Debug("Webhook failed", "url", cmd.URL, "statuscode", resp.Status, "body", string(body))
	return fmt.Errorf("webhook response status %v", resp.Status)
}

// TODO(santiago): implement!
func (s sender) SendEmail(ctx context.Context, cmd *receivers.SendEmailSettings) error {
	fmt.Println("SendEmail() called with", cmd)
	return nil
}
