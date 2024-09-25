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
	"github.com/prometheus/alertmanager/cluster/clusterpb"
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
)

func TestMultitenantAlertmanager_DeleteUserGrafanaConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())
	now := time.Now().UnixMilli()

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: test.NewTestingLogger(t),
	}

	require.NoError(t, alertstore.SetGrafanaAlertConfig(context.Background(), alertspb.GrafanaAlertConfigDesc{
		User:               "test_user",
		RawConfig:          "a grafana config",
		Hash:               "bb788eaa294c05ec556c1ed87546b7a9",
		CreatedAtTimestamp: now,
		Default:            false,
	}))

	require.Len(t, storage.Objects(), 1)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/grafana/config", nil)

	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 1)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaConfig(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Len(t, storage.Objects(), 0)
	}

	// Repeating the request still reports 200
	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaConfig(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Equal(t, 0, len(storage.Objects()))
	}
}

func TestMultitenantAlertmanager_DeleteUserGrafanaState(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: test.NewTestingLogger(t),
	}

	require.NoError(t, alertstore.SetFullGrafanaState(context.Background(), "test_user", alertspb.FullStateDesc{
		State: &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "nflog",
					Data: []byte("somedata"),
				},
			},
		},
	}))

	require.Len(t, storage.Objects(), 1)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/grafana/state", nil)

	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaState(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 1)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaState(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Len(t, storage.Objects(), 0)
	}

	// Repeating the request still reports 200.
	{
		rec := httptest.NewRecorder()
		am.DeleteUserGrafanaState(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Equal(t, 0, len(storage.Objects()))
	}
}

func TestMultitenantAlertmanager_GetUserGrafanaConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())
	now := time.Now().UnixMilli()

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: test.NewTestingLogger(t),
	}

	externalURL := "http://test.grafana.com"
	require.NoError(t, alertstore.SetGrafanaAlertConfig(context.Background(), alertspb.GrafanaAlertConfigDesc{
		User:               "test_user",
		RawConfig:          testGrafanaConfig,
		Hash:               "bb788eaa294c05ec556c1ed87546b7a9",
		CreatedAtTimestamp: now,
		Default:            false,
		Promoted:           true,
		ExternalUrl:        externalURL,
		StaticHeaders:      map[string]string{"Header-1": "Value-1", "Header-2": "Value-2"},
	}))

	require.Len(t, storage.Objects(), 1)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/grafana/config", nil)

	{
		rec := httptest.NewRecorder()
		am.GetUserGrafanaConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 1)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.GetUserGrafanaConfig(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		json := fmt.Sprintf(`
		{
			"data": {
				 "configuration": %s,
				 "configuration_hash": "bb788eaa294c05ec556c1ed87546b7a9",
				 "created": %d,
				 "default": false,
				 "promoted": true,
				 "external_url": %q,
				 "static_headers": {
					"Header-1": "Value-1",
					"Header-2": "Value-2"
				 }
			},
			"status": "success"
		}
		`, testGrafanaConfig, now, externalURL)

		require.JSONEq(t, json, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Len(t, storage.Objects(), 1)
	}
}

func TestMultitenantAlertmanager_GetUserGrafanaState(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: test.NewTestingLogger(t),
	}

	require.NoError(t, alertstore.SetFullGrafanaState(context.Background(), "test_user", alertspb.FullStateDesc{
		State: &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "nflog",
					Data: []byte("somedata"),
				},
			},
		},
	}))

	require.Len(t, storage.Objects(), 1)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/grafana/state", nil)

	{
		rec := httptest.NewRecorder()
		am.GetUserGrafanaState(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 1)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		rec := httptest.NewRecorder()
		am.GetUserGrafanaState(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		json := `
		{
			"data": {
				"state": "ChEKBW5mbG9nEghzb21lZGF0YQ=="
			},
			"status": "success"
		}
		`
		require.JSONEq(t, json, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Len(t, storage.Objects(), 1)
	}
}

func TestMultitenantAlertmanager_SetUserGrafanaConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	cases := []struct {
		name            string
		maxConfigSize   int
		orgID           string
		body            string
		expStatusCode   int
		expResponseBody string
		expStorageKey   string
	}{
		{
			name:          "missing org id",
			expStatusCode: http.StatusUnauthorized,
		},
		{
			name: "invalid config",
			body: `
			{
				"configuration_hash": "some_hash",
				"created": 12312414343,
				"default": false
			}
			`,
			orgID:         "test_user",
			expStatusCode: http.StatusBadRequest,
			expResponseBody: `
			{
				"error": "error marshalling JSON Grafana Alertmanager config: no route provided in config",
				"status": "error"
			}
			`,
		},
		{
			name: "with valid config",
			body: fmt.Sprintf(`
			{
				"configuration": %s,
				"configuration_hash": "ChEKBW5mbG9nEghzb21lZGF0YQ==",
				"created": 12312414343,
				"default": false,
				"promoted": true,
				"external_url": "http://test.grafana.com",
				"static_headers": {
					"Header-1": "Value-1",
					"Header-2": "Value-2"
				}
			}
			`, testGrafanaConfig),
			orgID:           "test_user",
			expStatusCode:   http.StatusCreated,
			expResponseBody: successJSON,
			expStorageKey:   "grafana_alertmanager/test_user/grafana_config",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			am := &MultitenantAlertmanager{
				store:  alertstore,
				logger: test.NewTestingLogger(t),
				limits: &mockAlertManagerLimits{
					maxGrafanaConfigSize: tc.maxConfigSize,
				},
			}
			rec := httptest.NewRecorder()
			ctx := context.Background()
			if tc.orgID != "" {
				ctx = user.InjectOrgID(ctx, "test_user")
			}

			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/grafana/config",
				io.NopCloser(strings.NewReader(tc.body)),
			).WithContext(ctx)

			am.SetUserGrafanaConfig(rec, req)
			require.Equal(t, tc.expStatusCode, rec.Code)

			if tc.expResponseBody != "" {
				body, err := io.ReadAll(rec.Body)
				require.NoError(t, err)

				require.JSONEq(t, tc.expResponseBody, string(body))
			}

			if tc.expStorageKey == "" {
				require.Len(t, storage.Objects(), 0)
			} else {
				require.Len(t, storage.Objects(), 1)
				_, ok := storage.Objects()[tc.expStorageKey]
				require.True(t, ok)
			}
		})
	}
}

func TestMultitenantAlertmanager_SetUserGrafanaState(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: test.NewTestingLogger(t),
	}

	require.Len(t, storage.Objects(), 0)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/grafana/state", nil)

	{
		rec := httptest.NewRecorder()
		am.SetUserGrafanaState(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 0)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		// First, try with invalid state payload.
		rec := httptest.NewRecorder()
		json := `
		{
		}
		`
		req.Body = io.NopCloser(strings.NewReader(json))
		am.SetUserGrafanaState(rec, req)

		require.Equal(t, http.StatusBadRequest, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		failureJSON := `
		{
			"error": "error marshalling JSON Grafana Alertmanager state: no state specified",
			"status": "error"
		}
		`
		require.JSONEq(t, failureJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		// Now, with a valid one.
		rec = httptest.NewRecorder()
		json = `
		{
			"state": "ChEKBW5mbG9nEghzb21lZGF0YQ=="
		}
		`
		req.Body = io.NopCloser(strings.NewReader(json))
		am.SetUserGrafanaState(rec, req)

		require.Equal(t, http.StatusCreated, rec.Code)
		body, err = io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Len(t, storage.Objects(), 1)
		_, ok := storage.Objects()["grafana_alertmanager/test_user/grafana_fullstate"]
		require.True(t, ok)
	}
}
