// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/util/test"
)

const (
	successJSON       = `{ "status": "success" }`
	testGrafanaConfig = `{
		"template_files": {},
		"alertmanager_config": {
			"route": {
				"receiver": "test_receiver",
				"group_by": ["alertname"]
			},
			"global": {
				"http_config": {
					"enable_http2": true,
					"follow_redirects": true,
					"proxy_url": null,
					"tls_config": {
						"insecure_skip_verify": true
					}
				},
				"opsgenie_api_url": "https://api.opsgenie.com/",
				"pagerduty_url": "https://events.pagerduty.com/v2/enqueue",
				"resolve_timeout": "5m",
				"smtp_hello": "localhost",
				"smtp_require_tls": true,
				"smtp_smarthost": "",
				"telegram_api_url": "https://api.telegram.org",
				"victorops_api_url": "https://alert.victorops.com/integrations/generic/20131114/alert/",
				"webex_api_url": "https://webexapis.com/v1/messages",
				"wechat_api_url": "https://qyapi.weixin.qq.com/cgi-bin/"
			},
			"receivers": [{
				"name": "test_receiver",
				"grafana_managed_receiver_configs": [{
					"uid": "",
					"name": "email test",
					"type": "email",
					"disableResolveMessage": true,
					"settings": {
						"addresses": "test@test.com"
					}
				}]
			}]
		}
	}`
	testGrafanaConfigWithMixedReceivers = `{
		"template_files": {},
		"alertmanager_config": {
			"route": {
				"receiver": "test_receiver",
				"group_by": ["alertname"],
				"routes": [{
					"receiver": "standard_email_receiver",
					"matchers": ["imported=\"true\""]
				}]
			},
			"global": {
				"resolve_timeout": "5m",
				"smtp_smarthost": "localhost:587"
			},
			"receivers": [{
				"name": "test_receiver",
				"grafana_managed_receiver_configs": [{
					"uid": "",
					"name": "email test",
					"type": "email",
					"disableResolveMessage": true,
					"settings": {
						"addresses": "test@test.com"
					}
				}]
			}, {
				"name": "standard_email_receiver",
				"email_configs": [{
					"to": "alerts@example.com",
					"from": "alertmanager@example.com",
					"smarthost": "localhost:587",
					"auth_username": "alertmanager@localhost",
					"auth_password": "my_secret_password",
					"subject": "Alert: {{ .GroupLabels.alertname }}"
				}]
			}]
		}
	}`
)
