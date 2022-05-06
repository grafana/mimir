package compactor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

// Test MultitenantCompactor.UploadBlockFile.
func TestMultitenantCompactor_UploadBlockFile(t *testing.T) {
	t.Run("without tenant ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "invalid tenant ID\n", string(body))
	})

	t.Run("without block ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing block ID\n", string(body))
	})

	t.Run("without path", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing file path\n", string(body))
	})

	t.Run("bad path", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "..%2Fchunks%2F001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "invalid path: \"../chunks/001\"\n", string(body))
	})

	t.Run("empty file", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "file cannot be empty\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		bkt := bucket.ClientMock{}
		const expPath = "test/1234/chunks/001"
		bkt.MockUpload(expPath, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}
		const pth = "chunks%2F001"
		buf := bytes.NewBuffer([]byte("content"))
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/%s", pth), buf)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		bkt.AssertCalled(t, "Upload", mock.Anything, expPath, bodyReader{
			r: r,
		})
	})
}
