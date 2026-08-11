// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewHTTPRoundTripper_BasicAuth(t *testing.T) {
	var gotUser, gotPass string
	var gotOK bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, gotOK = r.BasicAuth()
	}))
	t.Cleanup(srv.Close)

	t.Run("sets basic auth when enabled", func(t *testing.T) {
		cfg := &HTTPConfig{}
		cfg.BasicAuth.Username = "user"
		require.NoError(t, cfg.BasicAuth.Password.Set("pass"))

		rt := newHTTPRoundTripper(cfg)
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		require.True(t, gotOK)
		require.Equal(t, "user", gotUser)
		require.Equal(t, "pass", gotPass)
	})

	t.Run("no basic auth when disabled", func(t *testing.T) {
		gotOK = false
		rt := newHTTPRoundTripper(&HTTPConfig{})
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())

		require.False(t, gotOK)
	})
}
