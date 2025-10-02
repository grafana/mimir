// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/configs.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/grafana/e2e"

	"github.com/prometheus/common/model"
)

const (
	defaultWebhookImage = "webhook-receiver"
	webhookBinary       = "/bin/main"
	webhookHTTPPort     = 8080
)

type WebhookService struct {
	*e2e.HTTPService
}

func NewWebhookService(name string, flags, envVars map[string]string) *WebhookService {
	defaultFlags := map[string]string{
		"container": name,
	}

	svc := &WebhookService{
		HTTPService: e2e.NewHTTPService(
			name,
			"webhook_listener",
			e2e.NewCommandWithoutEntrypoint(webhookBinary, e2e.BuildArgs(mergeFlags(defaultFlags, flags))...),
			e2e.NewHTTPReadinessProbe(webhookHTTPPort, "/ready", 200, 299),
			webhookHTTPPort),
	}

	svc.SetEnvVars(envVars)

	return svc
}

type WebhookClient struct {
	c            http.Client
	u            *url.URL
	retryBackoff *backoff.Backoff
}

func NewWebhookClient(u string) (*WebhookClient, error) {
	pu, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	return &WebhookClient{
		c: http.Client{},
		u: pu,
		retryBackoff: backoff.New(context.Background(), backoff.Config{
			MinBackoff: 300 * time.Millisecond,
			// MaxBackoff: 600 * time.Millisecond,
			MaxBackoff: 1 * time.Second,
			MaxRetries: 100, // Sometimes the CI is slow ¯\_(ツ)_/¯
		}),
	}, nil
}

type Notification struct {
	Alerts            int               `json:"alerts"`
	Fingerprints      map[string]string `json:"fingerprints"`
	Status            string            `json:"status"`
	GroupKey          string            `json:"groupKey"`
	GroupLabels       model.LabelSet    `json:"groupLabels"`
	GroupFingerprint  string            `json:"groupFingerprint"`
	TimeNow           time.Time         `json:"timeNow"`
	Node              string            `json:"node"`
	DeltaLastSeconds  float64           `json:"deltaLastSeconds"`
	DeltaStartSeconds float64           `json:"deltaStartSeconds"`
}

type GetNotificationsResponse struct {
	Stats   map[string]int `json:"stats"`
	History []Notification `json:"history"`
}

// GetNotifications fetches notifications from the webhook server
func (c *WebhookClient) GetNotifications() (*GetNotificationsResponse, error) {
	u := c.u.ResolveReference(&url.URL{Path: "/notifications"})

	resp, err := c.c.Get(u.String())
	if err != nil {
		return nil, err
	}
	//nolint:errcheck
	defer resp.Body.Close()

	res := GetNotificationsResponse{}

	err = json.NewDecoder(resp.Body).Decode(&res)
	if err != nil {
		return nil, err
	}

	return &res, nil
}

type isExpected func(*GetNotificationsResponse) bool

func (c *WebhookClient) WaitForNotifications(isExpected isExpected) (err error) {
	for c.retryBackoff.Reset(); c.retryBackoff.Ongoing(); {
		var res *GetNotificationsResponse

		if res, err = c.GetNotifications(); err != nil || isExpected(res) {
			return
		}

		c.retryBackoff.Wait()
	}

	err = fmt.Errorf("failed to get expected notifications")
	return
}
