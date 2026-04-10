// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/client/client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestBuildURL(t *testing.T) {
	tc := []struct {
		name      string
		path      string
		method    string
		url       string
		resultURL string
	}{
		{
			name:      "builds the correct URL with a trailing slash",
			path:      "/prometheus/config/v1/rules",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/",
			resultURL: "http://mimirurl.com/prometheus/config/v1/rules",
		},
		{
			name:      "builds the correct URL without a trailing slash",
			path:      "/prometheus/config/v1/rules",
			method:    http.MethodPost,
			url:       "http://mimirurl.com",
			resultURL: "http://mimirurl.com/prometheus/config/v1/rules",
		},
		{
			name:      "builds the correct URL when the base url has a path",
			path:      "/prometheus/config/v1/rules",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/apathto",
			resultURL: "http://mimirurl.com/apathto/prometheus/config/v1/rules",
		},
		{
			name:      "builds the correct URL when the base url has a path with trailing slash",
			path:      "/prometheus/config/v1/rules",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/apathto/",
			resultURL: "http://mimirurl.com/apathto/prometheus/config/v1/rules",
		},
		{
			name:      "builds the correct URL with a trailing slash and the target path contains special characters",
			path:      "/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/",
			resultURL: "http://mimirurl.com/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
		},
		{
			name:      "builds the correct URL without a trailing slash and the target path contains special characters",
			path:      "/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
			method:    http.MethodPost,
			url:       "http://mimirurl.com",
			resultURL: "http://mimirurl.com/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
		},
		{
			name:      "builds the correct URL when the base url has a path and the target path contains special characters",
			path:      "/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/apathto",
			resultURL: "http://mimirurl.com/apathto/prometheus/config/v1/rules/%20%2Fspace%F0%9F%8D%BB",
		},
		{
			name:      "builds the correct URL when the base url has a path and the target path starts with a escaped slash",
			path:      "/prometheus/config/v1/rules/%2F-first-char-slash",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/apathto",
			resultURL: "http://mimirurl.com/apathto/prometheus/config/v1/rules/%2F-first-char-slash",
		},
		{
			name:      "builds the correct URL when the base url has a path and the target path ends with a escaped slash",
			path:      "/prometheus/config/v1/rules/last-char-slash%2F",
			method:    http.MethodPost,
			url:       "http://mimirurl.com/apathto",
			resultURL: "http://mimirurl.com/apathto/prometheus/config/v1/rules/last-char-slash%2F",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			url, err := url.Parse(tt.url)
			require.NoError(t, err)

			req, err := buildRequest(context.Background(), tt.path, tt.method, *url, bytes.NewBuffer(nil), 0)
			require.NoError(t, err)
			require.Equal(t, tt.resultURL, req.URL.String())
		})
	}

}

func TestDoRequest(t *testing.T) {
	requestCh := make(chan *http.Request, 1)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCh <- r
		fmt.Fprintln(w, "hello")
	}))
	defer ts.Close()

	for _, tc := range []struct {
		name         string
		user         string
		key          string
		id           string
		authToken    string
		sigV4        SigV4Config
		extraHeaders map[string]string
		expectedErr  string
		validate     func(t *testing.T, req *http.Request)
	}{
		{
			name:        "errors because user, key and authToken are provided",
			user:        "my-ba-user",
			key:         "my-ba-password",
			authToken:   "RandomJwt",
			expectedErr: "at most one of basic_auth, auth_token or sigv4 must be configured",
		},
		{
			name: "errors because basic auth and sigv4 are provided",
			user: "my-ba-user",
			key:  "my-ba-password",
			sigV4: SigV4Config{
				Region:      "ap-northeast-1",
				AccessKey:   "test-access-key",
				SecretKey:   "test-secret-key",
				ServiceName: "execute-api",
			},
			expectedErr: "at most one of basic_auth, auth_token or sigv4 must be configured",
		},
		{
			name:      "errors because auth token and sigv4 are provided",
			authToken: "RandomJwt",
			sigV4: SigV4Config{
				Region:      "ap-northeast-1",
				AccessKey:   "test-access-key",
				SecretKey:   "test-secret-key",
				ServiceName: "execute-api",
			},
			expectedErr: "at most one of basic_auth, auth_token or sigv4 must be configured",
		},
		{
			name: "user provided so uses key as password",
			user: "my-ba-user",
			key:  "my-ba-password",
			id:   "my-tenant-id",
			validate: func(t *testing.T, req *http.Request) {
				user, pass, ok := req.BasicAuth()
				require.True(t, ok)
				require.Equal(t, "my-ba-user", user)
				require.Equal(t, "my-ba-password", pass)
				require.Equal(t, "my-tenant-id", req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name: "user not provided so uses id as username and key as password",
			key:  "my-ba-password",
			id:   "my-tenant-id",
			validate: func(t *testing.T, req *http.Request) {
				user, pass, ok := req.BasicAuth()
				require.True(t, ok)
				require.Equal(t, "my-tenant-id", user)
				require.Equal(t, "my-ba-password", pass)
				require.Equal(t, "my-tenant-id", req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name:      "authToken is provided",
			id:        "my-tenant-id",
			authToken: "RandomJwt",
			validate: func(t *testing.T, req *http.Request) {
				require.Equal(t, "Bearer RandomJwt", req.Header.Get("Authorization"))
				require.Equal(t, "my-tenant-id", req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name: "sigv4 signs the request",
			id:   "my-tenant-id",
			sigV4: SigV4Config{
				Region:      "ap-northeast-1",
				AccessKey:   "test-access-key",
				SecretKey:   "test-secret-key",
				ServiceName: "execute-api",
			},
			validate: func(t *testing.T, req *http.Request) {
				require.Contains(t, req.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=test-access-key/")
				require.NotEmpty(t, req.Header.Get("X-Amz-Date"))
				require.Equal(t, "my-tenant-id", req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name: "no auth options and tenant are provided",
			validate: func(t *testing.T, req *http.Request) {
				require.Empty(t, req.Header.Get("Authorization"))
				require.Empty(t, req.Header.Get("X-Scope-OrgID"))
			},
		},
		{
			name: "extraHeaders are added",
			id:   "my-tenant-id",
			extraHeaders: map[string]string{
				"key1":          "value1",
				"key2":          "value2",
				"X-Scope-OrgID": "first-tenant-id",
			},
			validate: func(t *testing.T, req *http.Request) {
				require.Equal(t, "value1", req.Header.Get("key1"))
				require.Equal(t, "value2", req.Header.Get("key2"))
				require.Equal(t, []string{"first-tenant-id", "my-tenant-id"}, req.Header.Values("X-Scope-OrgID"))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			client, err := New(Config{
				Address:      ts.URL,
				User:         tc.user,
				Key:          tc.key,
				AuthToken:    tc.authToken,
				SigV4:        tc.sigV4,
				ID:           tc.id,
				ExtraHeaders: tc.extraHeaders,
			}, log.NewNopLogger())

			// Validate errors
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
				return
			}

			require.NoError(t, err)

			res, err := client.doRequest(ctx, "/test", http.MethodGet, nil, -1)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode)
			req := <-requestCh
			tc.validate(t, req)
		})
	}
}

func TestNewErrorsOnInvalidSigV4Config(t *testing.T) {
	_, err := New(Config{
		Address: "http://example.com",
		SigV4: SigV4Config{
			ServiceName: "execute-api",
		},
	}, log.NewNopLogger())

	require.Error(t, err)
	require.Contains(t, err.Error(), "sigv4 region must be configured")
}

func TestSigV4ConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cfg         SigV4Config
		expectedErr string
	}{
		{
			name: "zero value is valid",
			cfg:  SigV4Config{},
		},
		{
			name: "configured settings are valid",
			cfg: SigV4Config{
				Region:      "ap-northeast-1",
				AccessKey:   "test-access-key",
				SecretKey:   "test-secret-key",
				ServiceName: "execute-api",
			},
		},
		{
			name: "configured settings without region are invalid",
			cfg: SigV4Config{
				ServiceName: "execute-api",
			},
			expectedErr: "sigv4 region must be configured",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()

			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
				return
			}

			require.NoError(t, err)
		})
	}
}
