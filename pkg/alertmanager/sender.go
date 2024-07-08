// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

// Mostly taken from http://github.com/grafana/grafana/main/pkg/services/notifications/webhook.go
package alertmanager

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	alertingReceivers "github.com/grafana/alerting/receivers"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/util/version"
)

var (
	ErrInvalidMethod = errors.New("webhook only supports HTTP methods PUT or POST")
)

type Sender struct {
	c   *http.Client
	log log.Logger
}

func NewSender(log log.Logger) *Sender {
	netTransport := &http.Transport{
		TLSClientConfig: &tls.Config{
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
		Proxy: http.ProxyFromEnvironment,
		Dial: (&net.Dialer{
			Timeout: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	c := &http.Client{
		Timeout:   time.Second * 30,
		Transport: netTransport,
	}
	return &Sender{
		c:   c,
		log: log,
	}
}

// SendWebhook implements alertingReceivers.WebhookSender.
func (s *Sender) SendWebhook(ctx context.Context, cmd *alertingReceivers.SendWebhookSettings) error {
	if cmd.HTTPMethod == "" {
		cmd.HTTPMethod = http.MethodPost
	}

	level.Debug(s.log).Log("msg", "Sending webhook", "url", cmd.URL, "http method", cmd.HTTPMethod)

	if cmd.HTTPMethod != http.MethodPost && cmd.HTTPMethod != http.MethodPut {
		return ErrInvalidMethod
	}

	request, err := http.NewRequestWithContext(ctx, cmd.HTTPMethod, cmd.URL, bytes.NewReader([]byte(cmd.Body)))
	if err != nil {
		return err
	}

	if cmd.ContentType == "" {
		cmd.ContentType = "application/json"
	}

	request.Header.Set("Content-Type", cmd.ContentType)
	request.Header.Set("User-Agent", version.UserAgent())

	if cmd.User != "" && cmd.Password != "" {
		request.SetBasicAuth(cmd.User, cmd.Password)
	}

	for k, v := range cmd.HTTPHeader {
		request.Header.Set(k, v)
	}

	resp, err := s.c.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			level.Warn(s.log).Log("msg", "Failed to close response body", "err", err)
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if cmd.Validation != nil {
		err := cmd.Validation(body, resp.StatusCode)
		if err != nil {
			level.Debug(s.log).Log("msg", "Webhook failed validation", "url", cmd.URL, "statuscode", resp.Status, "body", string(body))
			return fmt.Errorf("webhook failed validation: %w", err)
		}
	}

	if resp.StatusCode/100 == 2 {
		level.Debug(s.log).Log("msg", "Webhook succeeded", "url", cmd.URL, "statuscode", resp.Status)
		return nil
	}

	level.Debug(s.log).Log("msg", "Webhook failed", "url", cmd.URL, "statuscode", resp.Status, "body", string(body))
	return fmt.Errorf("webhook response status %v", resp.Status)
}
