package compactor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"
)

// Test MultitenantCompactor.CreateBlockUpload.
func TestMultitenantCompactor_CreateBlockUpload(t *testing.T) {
	c := &MultitenantCompactor{
		logger: log.NewNopLogger(),
	}

	t.Run("without tenant ID", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		w := httptest.NewRecorder()
		c.CreateBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "invalid tenant ID\n", string(body))
	})

	t.Run("without block ID", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.CreateBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing block ID\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r.Header.Set(user.OrgIDHeaderName, "test")
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		w := httptest.NewRecorder()
		c.CreateBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Empty(t, string(body))
	})
}
