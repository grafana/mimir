package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/grafana/alerting/logging"
	"github.com/grafana/alerting/receivers"
)

var ErrInvalidMethod = errors.New("webhook only supports HTTP methods PUT or POST")

type clientConfiguration struct {
	userAgent string
	dialer    net.Dialer // We use Dialer here instead of DialContext as our mqtt client doesn't support DialContext.
}

// defaultDialTimeout is the default timeout for the dialer, 30 seconds to match http.DefaultTransport.
const defaultDialTimeout = 30 * time.Second

type Client struct {
	log logging.Logger
	cfg clientConfiguration
}

func NewClient(log logging.Logger, opts ...ClientOption) *Client {
	cfg := clientConfiguration{
		userAgent: "Grafana",
		dialer:    net.Dialer{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.dialer.Timeout == 0 {
		// Mostly defensive to ensure that timeout semantics don't change when given a custom dialer without a timeout.
		cfg.dialer.Timeout = defaultDialTimeout
	}
	return &Client{
		log: log,
		cfg: cfg,
	}
}

type ClientOption func(*clientConfiguration)

func WithUserAgent(userAgent string) ClientOption {
	return func(c *clientConfiguration) {
		c.userAgent = userAgent
	}
}

func WithDialer(dialer net.Dialer) ClientOption {
	return func(c *clientConfiguration) {
		c.dialer = dialer
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
	request.Header.Set("User-Agent", ns.cfg.userAgent)

	if webhook.User != "" && webhook.Password != "" {
		request.Header.Set("Authorization", GetBasicAuthHeader(webhook.User, webhook.Password))
	}

	for k, v := range webhook.HTTPHeader {
		request.Header.Set(k, v)
	}

	client := NewTLSClient(webhook.TLSConfig, ns.cfg.dialer.DialContext)

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
