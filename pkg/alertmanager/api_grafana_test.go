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
	util_log "github.com/grafana/mimir/pkg/util/log"
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
			"receivers": [{
				"name": "test_receiver",
				"grafana_managed_receiver_configs": [{
					"uid": "",
					"name": "email test",
					"type": "email",
					"disableResolveMessage": true,
					"settings": {
						"addresses": "test@test.com"
					},
					"secureSettings": null
				}]
			}],
			"templates": null
		}
	}`
)

func TestMultitenantAlertmanager_DeleteUserGrafanaConfig(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())
	now := time.Now().UnixMilli()

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
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
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
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
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())
	now := time.Now().UnixMilli()

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
	}

	require.NoError(t, alertstore.SetGrafanaAlertConfig(context.Background(), alertspb.GrafanaAlertConfigDesc{
		User:               "test_user",
		RawConfig:          testGrafanaConfig,
		Hash:               "bb788eaa294c05ec556c1ed87546b7a9",
		CreatedAtTimestamp: now,
		Default:            false,
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
				 "default": false
			},
			"status": "success"
		}
		`, testGrafanaConfig, now)
		require.JSONEq(t, json, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		require.Len(t, storage.Objects(), 1)
	}
}

func TestMultitenantAlertmanager_GetUserGrafanaState(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
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
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
	}

	require.Len(t, storage.Objects(), 0)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/grafana/config", nil)
	{
		rec := httptest.NewRecorder()
		am.SetUserGrafanaConfig(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
		require.Len(t, storage.Objects(), 0)
	}

	ctx := user.InjectOrgID(context.Background(), "test_user")
	req = req.WithContext(ctx)
	{
		// First, try with invalid configuration.
		rec := httptest.NewRecorder()
		json := `
		{
			"configuration_hash": "some_hash",
			"created": 12312414343,
			"default": false
		}
		`
		req.Body = io.NopCloser(strings.NewReader(json))
		am.SetUserGrafanaConfig(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		body, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		failedJSON := `
		{
			"error": "error marshalling JSON Grafana Alertmanager config: no route provided in config",
			"status": "error"
		}
		`
		require.JSONEq(t, failedJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		// Now, with a valid configuration.
		rec = httptest.NewRecorder()
		json = fmt.Sprintf(`
		{
			"configuration": %s,
			"configuration_hash": "ChEKBW5mbG9nEghzb21lZGF0YQ==",
			"created": 12312414343,
			"default": false
		}
		`, testGrafanaConfig)
		req.Body = io.NopCloser(strings.NewReader(json))
		am.SetUserGrafanaConfig(rec, req)

		require.Equal(t, http.StatusCreated, rec.Code)
		body, err = io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.JSONEq(t, successJSON, string(body))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		require.Len(t, storage.Objects(), 1)
		_, ok := storage.Objects()["grafana_alertmanager/test_user/grafana_config"]
		require.True(t, ok)
	}
}

func TestMultitenantAlertmanager_SetUserGrafanaState(t *testing.T) {
	storage := objstore.NewInMemBucket()
	alertstore := bucketclient.NewBucketAlertStore(storage, nil, log.NewNopLogger())

	am := &MultitenantAlertmanager{
		store:  alertstore,
		logger: util_log.Logger,
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
