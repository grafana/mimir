// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=false (the default).
func TestMultitenantCompactor_HandleBlockUpload_Create(t *testing.T) {
	t.Run("without tenant ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}

		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

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

		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing block ID\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		var bkt bucket.ClientMock
		bkt.MockIter("test/1234", []string{}, nil)
		bkt.MockUpload(mock.Anything, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}

		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234", nil)
		r.Header.Set(user.OrgIDHeaderName, "test")
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

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
		const pth = "chunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), nil)
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
		const pth = "chunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), nil)
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
		const pth = "chunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), nil)
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
		const pth = "..%2Fchunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "invalid path: \"../chunks/000001\"\n", string(body))
	})

	t.Run("empty file", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), nil)
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

	t.Run("attempt meta.json", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "meta.json"
		buf := bytes.NewBuffer([]byte("content"))
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), buf)
		r = mux.SetURLVars(r, map[string]string{"block": "1234", "path": pth})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "meta.json is not allowed\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		var bkt bucket.ClientMock
		const expPath = "test/1234/chunks/000001"
		bkt.MockUpload(expPath, nil)
		bkt.MockIter("test/1234", []string{"random.lock"}, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}
		const pth = "chunks%2F000001"
		buf := bytes.NewBuffer([]byte("content"))
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/1234/files?path=%s", pth), buf)
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

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=true.
func TestMultitenantCompactor_HandleBlockUpload_Complete(t *testing.T) {
	t.Run("without tenant ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234?uploadComplete=true", nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

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
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234?uploadComplete=true", nil)
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing block ID\n", string(body))
	})

	t.Run("without body", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234?uploadComplete=true", nil)
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "malformed request body\n", string(body))
	})

	t.Run("malformed body", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/1234?uploadComplete=true",
			bytes.NewBuffer([]byte("{")))
		r = mux.SetURLVars(r, map[string]string{"block": "1234"})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "malformed request body\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		var bkt bucket.ClientMock
		const expPath = "test/01G3FZ0JWJYJC0ZM6Y9778P6KD/meta.json"
		bkt.MockUpload(expPath, nil)
		bkt.MockDelete(mock.Anything, nil)
		bkt.MockIter("test/01G3FZ0JWJYJC0ZM6Y9778P6KD", []string{"random.lock"}, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}
		meta := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: ulid.MustParse("01G3FZ0JWJYJC0ZM6Y9778P6KD"),
			},
			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.TenantIDExternalLabel: "test",
				},
				Files: []metadata.File{
					{
						RelPath:   "index",
						SizeBytes: 1,
					},
					{
						RelPath:   "chunks/000001",
						SizeBytes: 1024,
					},
				},
			},
		}
		metaJSON, err := json.Marshal(meta)
		require.NoError(t, err)
		r := httptest.NewRequest(http.MethodPost, "/api/v1/upload/block/01G3FZ0JWJYJC0ZM6Y9778P6KD?uploadComplete=true",
			bytes.NewBuffer(metaJSON))
		r = mux.SetURLVars(r, map[string]string{"block": "01G3FZ0JWJYJC0ZM6Y9778P6KD"})
		r.Header.Set(user.OrgIDHeaderName, "test")
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		expBuf := bytes.NewBuffer(metaJSON)
		require.NoError(t, expBuf.WriteByte('\n'))
		bkt.AssertCalled(t, "Upload", mock.Anything, expPath, expBuf)
	})
}
