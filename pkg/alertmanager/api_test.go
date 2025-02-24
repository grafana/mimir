// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/api_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/client_golang/prometheus"
	commoncfg "github.com/prometheus/common/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestAMConfigValidationAPI(t *testing.T) {
	testCases := []struct {
		name            string
		cfg             string
		maxConfigSize   int
		maxTemplates    int
		maxTemplateSize int

		response string
		err      error
	}{
		{
			name: "Should return error if the alertmanager config contains no receivers",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
`,
			err: fmt.Errorf("error validating Alertmanager config: undefined receiver \"default-receiver\" used in route"),
		},
		{
			name: "Should pass if the alertmanager config is valid",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
`,
		},
		{
			name: "Should return error if the config is empty due to wrong indentation",
			cfg: `
alertmanager_config: |
route:
  receiver: 'default-receiver'
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h
  group_by: [cluster, alertname]
receivers:
  - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  "not/very/good.tpl": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
		{
			name: "Should return error if the alertmanager config is empty due to wrong key",
			cfg: `
XWRONGalertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
`,
			err: fmt.Errorf("error validating Alertmanager config: configuration provided is empty, if you'd like to remove your configuration please use the delete configuration endpoint"),
		},
		{
			name: "Should return error if the external template file name contains an absolute path",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "/absolute/filepath": "a simple template"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/absolute/filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the external template file name contains a relative path",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "../filepath": "a simple template"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "../filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the external template file name is not a valid filename",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "good.tpl": "good-templ"
  ".": "bad-template"
`,
			err: fmt.Errorf("error validating Alertmanager config: invalid template name \".\""),
		},
		{
			name: "Should return error if the referenced template contains the root /",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "/"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains the root with repeated separators ///",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "///"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "///": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains an absolute path",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "/absolute/filepath"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "/absolute/filepath": the template name cannot contain any path`),
		},
		{
			name: "Should return error if the referenced template contains a relative path",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "../filepath"
`,
			err: fmt.Errorf(`error validating Alertmanager config: invalid template name "../filepath": the template name cannot contain any path`),
		},
		{
			name: "Should pass if the referenced template is valid filename",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "something.tmpl"
`,
		},
		{
			name: "Should return error if global HTTP password_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      basic_auth:
        password_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global HTTP bearer_token_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      bearer_token_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global HTTP credentials_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      authorization:
        credentials_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should NOT return error if global HTTP proxy_url is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      proxy_url: http://example.com
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: nil,
		},
		{
			name: "Should NOT return error if global HTTP proxy_from_environment is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      proxy_from_environment: true
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: nil,
		},
		{
			name: "Should return error if global OAuth2 client_secret_file is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        token_url: http://example.com
        client_secret_file: /secrets

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errOAuth2SecretFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global OAuth2 proxy_url is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        client_secret: xxx
        token_url: http://example.com
        proxy_url: http://example.com
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errProxyURLNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global OAuth2 proxy_from_environment is set",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        client_secret: xxx
        token_url: http://example.com
        proxy_from_environment: true
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errProxyFromEnvironmentURLNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global OAuth2 TLS is configured through files",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        client_secret: secret
        token_url: http://example.com
        tls_config:
          key_file: /secrets/key
          cert_file: /secrets/cert

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errTLSConfigNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global OAuth2 TLS is configured through byte slices",
			cfg: `
alertmanager_config: |
  global:
    http_config:
      oauth2:
        client_id: test
        client_secret: secret
        token_url: http://example.com
        tls_config:
          key: key
          cert: cert

  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
`,
			err: errors.Wrap(errTLSConfigNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP password_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            basic_auth:
              password_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP bearer_token_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            bearer_token_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's HTTP credentials_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            authorization:
              credentials_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPasswordFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should NOT return error if receiver's HTTP proxy_url is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            proxy_url: http://example.com
  route:
    receiver: 'default-receiver'
`,
			err: nil,
		},
		{
			name: "Should NOT return error if receiver's HTTP proxy_from_environment is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            proxy_from_environment: true
  route:
    receiver: 'default-receiver'
`,
			err: nil,
		},
		{
			name: "Should return error if receiver's OAuth2 client_secret_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            oauth2:
              client_id: test
              token_url: http://example.com
              client_secret_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errOAuth2SecretFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's OAuth2 proxy_url is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            oauth2:
              client_id: test
              token_url: http://example.com
              client_secret: xxx
              proxy_url: http://localhost
  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errProxyURLNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if receiver's OAuth2 proxy_from_environment is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost
          http_config:
            oauth2:
              client_id: test
              token_url: http://example.com
              client_secret: xxx
              proxy_from_environment: true
  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errProxyFromEnvironmentURLNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global slack_api_url_file is set",
			cfg: `
alertmanager_config: |
  global:
    slack_api_url_file: /secrets

  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errSlackAPIURLFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if Slack api_url_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      slack_configs:
        - api_url_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errSlackAPIURLFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global opsgenie_api_key_file is set",
			cfg: `
alertmanager_config: |
  global:
    opsgenie_api_key_file: /secrets

  receivers:
    - name: default-receiver
      webhook_configs:
        - url: http://localhost

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errOpsGenieAPIKeyFileFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if OpsGenie api_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      opsgenie_configs:
        - api_key_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errOpsGenieAPIKeyFileFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if global victorops_api_key_file is set",
			cfg: `
alertmanager_config: |
  global:
    victorops_api_key_file: /secrets
  receivers:
    - name: default-receiver
      victorops_configs:
        - routing_key: test

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errVictorOpsAPIKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if VictorOps api_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      victorops_configs:
        - api_key_file: /secrets
          routing_key: test

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errVictorOpsAPIKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if PagerDuty service_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      pagerduty_configs:
        - service_key_file: /secrets
          routing_key: test

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPagerDutyServiceKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if PagerDuty routing_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      pagerduty_configs:
        - routing_key_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPagerDutyRoutingKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if Pushover user_key_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      pushover_configs:
        - user_key_file: /secrets
          token: xxx

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPushoverUserKeyFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "Should return error if Pushover token_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      pushover_configs:
        - token_file: /secrets
          user_key: xxx

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errPushoverTokenFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "should return error if Telegram bot_token_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      telegram_configs:
        - bot_token_file: /secrets
          chat_id: 123

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errTelegramBotTokenFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "should return error if Webhook url_file is set",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      webhook_configs:
        - url_file: /secrets

  route:
    receiver: 'default-receiver'
`,
			err: errors.Wrap(errWebhookURLFileNotAllowed, "error validating Alertmanager config"),
		},
		{
			name: "should return error if template is wrong",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
  templates:
    - "*.tmpl"
template_files:
  "test.tmpl": "{{ invalid Go template }}"
`,
			err: fmt.Errorf(`error validating Alertmanager config: template: test.tmpl:1: function "invalid" not defined`),
		},
		{
			name: "should return error if template is wrong even when not referenced by config",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
template_files:
  "test.tmpl": "{{ invalid Go template }}"
`,
			err: fmt.Errorf(`error validating Alertmanager config: template: test.tmpl:1: function "invalid" not defined`),
		},
		{
			name: "config too big",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
`,
			maxConfigSize: 10,
			err:           fmt.Errorf(errConfigurationTooBig, 10),
		},
		{
			name: "config size OK",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
    group_wait: 30s
    group_interval: 5m
    repeat_interval: 4h
    group_by: [cluster, alertname]
  receivers:
    - name: default-receiver
`,
			maxConfigSize: 1000,
			err:           nil,
		},
		{
			name: "templates limit reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Some template"
  "t2.tmpl": "Some template"
  "t3.tmpl": "Some template"
  "t4.tmpl": "Some template"
  "t5.tmpl": "Some template"
`,
			maxTemplates: 3,
			err:          errors.Wrap(fmt.Errorf(errTooManyTemplates, 5, 3), "error validating Alertmanager config"),
		},
		{
			name: "templates limit not reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Some template"
  "t2.tmpl": "Some template"
  "t3.tmpl": "Some template"
  "t4.tmpl": "Some template"
  "t5.tmpl": "Some template"
`,
			maxTemplates: 10,
			err:          nil,
		},
		{
			name: "template size limit reached",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Very big template"
`,
			maxTemplateSize: 5,
			err:             errors.Wrap(fmt.Errorf(errTemplateTooBig, "t1.tmpl", 17, 5), "error validating Alertmanager config"),
		},
		{
			name: "template size limit ok",
			cfg: `
alertmanager_config: |
  route:
    receiver: 'default-receiver'
  receivers:
    - name: default-receiver
template_files:
  "t1.tmpl": "Very big template"
`,
			maxTemplateSize: 20,
			err:             nil,
		},
		{
			name: "Should pass if template uses the tenantID custom function",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      slack_configs:
        - api_url: http://localhost
          channel: "test"
          title: "Hello {{tenantID}}"
          text: "Hello {{tenantID}}"

  route:
    receiver: 'default-receiver'
`,
		},
		{
			name: "Should pass if template uses the grafanaExploreURL custom function",
			cfg: `
alertmanager_config: |
  receivers:
    - name: default-receiver
      slack_configs:
        - api_url: http://localhost
          channel: "test"
          title: "Hello {{ grafanaExploreURL \"https://foo.bar\" \"xyz\" \"now-12h\" \"now\" \"up{foo='bar'}\" }}"
          text: "Hello {{ grafanaExploreURL \"https://foo.bar\" \"xyz\" \"now-12h\" \"now\" \"up{foo='bar'}\" }}"

  route:
    receiver: 'default-receiver'
`,
		},
	}

	limits := &mockAlertManagerLimits{}
	am := &MultitenantAlertmanager{
		cfg:    mockAlertmanagerConfig(t),
		store:  prepareInMemoryAlertStore(),
		logger: test.NewTestingLogger(t),
		limits: limits,
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limits.maxConfigSize = tc.maxConfigSize
			limits.maxTemplatesCount = tc.maxTemplates
			limits.maxSizeOfTemplate = tc.maxTemplateSize

			req := httptest.NewRequest(http.MethodPost, "http://alertmanager/api/v2/alerts", bytes.NewReader([]byte(tc.cfg)))
			ctx := user.InjectOrgID(req.Context(), "testing")
			w := httptest.NewRecorder()
			am.SetUserConfig(w, req.WithContext(ctx))
			resp := w.Result()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			if tc.err == nil {
				require.Equal(t, http.StatusCreated, resp.StatusCode)
				require.Equal(t, "", string(body))
			} else {
				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				require.Equal(t, tc.err.Error()+"\n", string(body))
			}
		})
	}
}

func TestMultitenantAlertmanager_DeleteUserConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertStore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertStore,
		logger: test.NewTestingLogger(t),
	}

	require.NoError(t, alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
		User:      "test_user",
		RawConfig: "config",
	}))

	require.Equal(t, 1, len(storage.Objects()))

	req := httptest.NewRequest("POST", "/multitenant_alertmanager/delete_tenant_config", nil)
	// Missing user returns error 401. (DeleteUserConfig does this, but in practice, authentication middleware will do it first)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Equal(t, 1, len(storage.Objects()))
	}

	// With user in the context.
	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}

	// Repeating the request still reports 200
	{
		rec := httptest.NewRecorder()
		am.DeleteUserConfig(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 0, len(storage.Objects()))
	}
}

func TestAMConfigListUserConfig(t *testing.T) {
	testCases := map[string]*UserConfig{
		"user1": {
			AlertmanagerConfig: `
global:
  resolve_timeout: 5m
route:
  receiver: route1
  group_by:
  - '...'
  continue: false
receivers:
- name: route1
  webhook_configs:
  - send_resolved: true
    http_config: {}
    url: http://alertmanager/api/notifications?orgId=1&rrid=7
    max_alerts: 0
`,
		},
		"user2": {
			AlertmanagerConfig: `
global:
  resolve_timeout: 5m
route:
  receiver: route1
  group_by:
  - '...'
  continue: false
receivers:
- name: route1
  webhook_configs:
  - send_resolved: true
    http_config: {}
    url: http://alertmanager/api/notifications?orgId=2&rrid=7
    max_alerts: 0
`,
		},
	}

	storage := objstore.NewInMemBucket()
	alertStore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	for u, cfg := range testCases {
		err := alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
			User:      u,
			RawConfig: cfg.AlertmanagerConfig,
		})
		require.NoError(t, err)
	}

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, alertStore, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 2)

	router := mux.NewRouter()
	router.Path("/multitenant_alertmanager/configs").Methods(http.MethodGet).HandlerFunc(am.ListAllConfigs)
	req := httptest.NewRequest("GET", "https://localhost:8080/multitenant_alertmanager/configs", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	resp := w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/yaml", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	expectedYaml := `user1:
    template_files: {}
    alertmanager_config: |4
        global:
          resolve_timeout: 5m
        route:
          receiver: route1
          group_by:
          - '...'
          continue: false
        receivers:
        - name: route1
          webhook_configs:
          - send_resolved: true
            http_config: {}
            url: http://alertmanager/api/notifications?orgId=1&rrid=7
            max_alerts: 0
user2:
    template_files: {}
    alertmanager_config: |4
        global:
          resolve_timeout: 5m
        route:
          receiver: route1
          group_by:
          - '...'
          continue: false
        receivers:
        - name: route1
          webhook_configs:
          - send_resolved: true
            http_config: {}
            url: http://alertmanager/api/notifications?orgId=2&rrid=7
            max_alerts: 0
`

	require.YAMLEq(t, expectedYaml, string(body))
}

func TestValidateAlertmanagerConfig(t *testing.T) {
	tests := map[string]struct {
		input    interface{}
		expected error
	}{
		"*HTTPClientConfig": {
			input: &commoncfg.HTTPClientConfig{
				BasicAuth: &commoncfg.BasicAuth{
					PasswordFile: "/secrets",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"HTTPClientConfig": {
			input: commoncfg.HTTPClientConfig{
				BasicAuth: &commoncfg.BasicAuth{
					PasswordFile: "/secrets",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"*TLSConfig": {
			input: &commoncfg.TLSConfig{
				CertFile: "/cert",
			},
			expected: errTLSConfigNotAllowed,
		},
		"TLSConfig": {
			input: commoncfg.TLSConfig{
				CertFile: "/cert",
			},
			expected: errTLSConfigNotAllowed,
		},
		"*GlobalConfig.SMTPAuthPasswordFile": {
			input: &config.GlobalConfig{
				SMTPAuthPasswordFile: "/file",
			},
			expected: errPasswordFileNotAllowed,
		},
		"GlobalConfig.SMTPAuthPasswordFile": {
			input: config.GlobalConfig{
				SMTPAuthPasswordFile: "/file",
			},
			expected: errPasswordFileNotAllowed,
		},
		"*DiscordConfig.HTTPConfig": {
			input: &config.DiscordConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BearerTokenFile: "/file",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"DiscordConfig.HTTPConfig": {
			input: &config.DiscordConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BearerTokenFile: "/file",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"*DiscordConfig.WebhookURLFile": {
			input: &config.DiscordConfig{
				WebhookURLFile: "/file",
			},
			expected: errWebhookURLFileNotAllowed,
		},
		"DiscordConfig.WebhookURLFile": {
			input: config.DiscordConfig{
				WebhookURLFile: "/file",
			},
			expected: errWebhookURLFileNotAllowed,
		},
		"*EmailConfig.AuthPasswordFile": {
			input: &config.EmailConfig{
				AuthPasswordFile: "/file",
			},
			expected: errPasswordFileNotAllowed,
		},
		"EmailConfig.AuthPasswordFile": {
			input: config.EmailConfig{
				AuthPasswordFile: "/file",
			},
			expected: errPasswordFileNotAllowed,
		},
		"*MSTeams.HTTPConfig": {
			input: &config.MSTeamsConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BearerTokenFile: "/file",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"MSTeams.HTTPConfig": {
			input: &config.MSTeamsConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BearerTokenFile: "/file",
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"*MSTeams.WebhookURLFile": {
			input: &config.MSTeamsConfig{
				WebhookURLFile: "/file",
			},
			expected: errWebhookURLFileNotAllowed,
		},
		"MSTeams.WebhookURLFile": {
			input: config.MSTeamsConfig{
				WebhookURLFile: "/file",
			},
			expected: errWebhookURLFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as direct child": {
			input: config.GlobalConfig{
				HTTPConfig: &commoncfg.HTTPClientConfig{
					BasicAuth: &commoncfg.BasicAuth{
						PasswordFile: "/secrets",
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as nested child": {
			input: config.Config{
				Global: &config.GlobalConfig{
					HTTPConfig: &commoncfg.HTTPClientConfig{
						BasicAuth: &commoncfg.BasicAuth{
							PasswordFile: "/secrets",
						},
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"struct containing *HTTPClientConfig as nested child within a slice": {
			input: config.Config{
				Receivers: []config.Receiver{{
					Name: "test",
					WebhookConfigs: []*config.WebhookConfig{{
						HTTPConfig: &commoncfg.HTTPClientConfig{
							BasicAuth: &commoncfg.BasicAuth{
								PasswordFile: "/secrets",
							},
						},
					}}},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"map containing *HTTPClientConfig": {
			input: map[string]*commoncfg.HTTPClientConfig{
				"test": {
					BasicAuth: &commoncfg.BasicAuth{
						PasswordFile: "/secrets",
					},
				},
			},
			expected: errPasswordFileNotAllowed,
		},
		"map containing TLSConfig as nested child": {
			input: map[string][]config.EmailConfig{
				"test": {{
					TLSConfig: commoncfg.TLSConfig{
						CAFile: "/file",
					},
				}},
			},
			expected: errTLSConfigNotAllowed,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			err := validateAlertmanagerConfig(testData.input)
			assert.ErrorIs(t, err, testData.expected)
		})
	}
}
