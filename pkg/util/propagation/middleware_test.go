// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMiddleware_HappyPath(t *testing.T) {
	p := &testExtractor{key: "X-Header"}

	handler := Middleware(p).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "header-value", r.Context().Value(testContextKey("X-Header")))
		require.Equal(t, "existing-value", r.Context().Value(testContextKey("existing-key")))
		w.WriteHeader(http.StatusTeapot)
	}))

	ctx := context.WithValue(context.Background(), testContextKey("existing-key"), "existing-value")
	req, err := http.NewRequestWithContext(ctx, "GET", "/", nil)
	require.NoError(t, err)
	req.Header.Set("X-Header", "header-value")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusTeapot, w.Code)
}
