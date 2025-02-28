package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/benbjohnson/clock"

	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
)

var ErrInvalidMethod = errors.New("webhook only supports HTTP methods PUT or POST")

type ClientConfiguration struct {
	UserAgent string
}

var DefaultClientConfiguration = ClientConfiguration{
	UserAgent: "Grafana",
}

type Client struct {
	log   logging.Logger
	agent string
}

func NewClient(log logging.Logger, cfg ClientConfiguration) *Client {
	return &Client{
		log:   log,
		agent: cfg.UserAgent,
	}
}

func (ns *Client) SendWebhook(ctx context.Context, webhook *receivers.SendWebhookSettings) error {
	// This method was moved from https://github.com/grafana/grafana/blob/71d04a326be9578e2d678f23c1efa61768e0541f/pkg/services/notifications/webhook.go#L38
	if webhook.HTTPMethod == "" {
		webhook.HTTPMethod = http.MethodPost
	}
	ns.log.Debug("Sending webhook", "url", webhook.URL, "http method", webhook.HTTPMethod)

	if webhook.HTTPMethod != http.MethodPost && webhook.HTTPMethod != http.MethodPut {
		return ErrInvalidMethod
	}

	request, err := http.NewRequestWithContext(ctx, webhook.HTTPMethod, webhook.URL, bytes.NewReader([]byte(webhook.Body)))
	if err != nil {
		return err
	}
	url, err := url.Parse(webhook.URL)
	if err != nil {
		// Should not be possible - NewRequestWithContext should also err if the URL is bad.
		return err
	}

	if webhook.ContentType == "" {
		webhook.ContentType = "application/json"
	}

	request.Header.Set("Content-Type", webhook.ContentType)
	request.Header.Set("User-Agent", ns.agent)

	if webhook.User != "" && webhook.Password != "" {
		request.Header.Set("Authorization", GetBasicAuthHeader(webhook.User, webhook.Password))
	}

	for k, v := range webhook.HTTPHeader {
		request.Header.Set(k, v)
	}

	client := NewTLSClient(webhook.TLSConfig)

	if webhook.HMACConfig != nil {
		ns.log.Debug("Adding HMAC roundtripper to client")
		client.Transport, err = NewHMACRoundTripper(
			client.Transport,
			clock.New(),
			webhook.HMACConfig.Secret,
			webhook.HMACConfig.Header,
			webhook.HMACConfig.TimestampHeader,
		)
		if err != nil {
			ns.log.Error("Failed to add HMAC roundtripper to client", "error", err)
			return err
		}
	}

	resp, err := client.Do(request)
	if err != nil {
		return redactURL(err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			ns.log.Warn("Failed to close response body", "err", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if webhook.Validation != nil {
		err := webhook.Validation(body, resp.StatusCode)
		if err != nil {
			ns.log.Debug("Webhook failed validation", "url", url.Redacted(), "statuscode", resp.Status, "body", string(body), "error", err)
			return fmt.Errorf("webhook failed validation: %w", err)
		}
	}

	if resp.StatusCode/100 == 2 {
		ns.log.Debug("Webhook succeeded", "url", url.Redacted(), "statuscode", resp.Status)
		return nil
	}

	ns.log.Debug("Webhook failed", "url", url.Redacted(), "statuscode", resp.Status, "body", string(body))
	return fmt.Errorf("webhook response status %v", resp.Status)
}

func redactURL(err error) error {
	var e *url.Error
	if !errors.As(err, &e) {
		return err
	}
	e.URL = "<redacted>"
	return e
}

func GetBasicAuthHeader(user string, password string) string {
	var userAndPass = user + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(userAndPass))
}
