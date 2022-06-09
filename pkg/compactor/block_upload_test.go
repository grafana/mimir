// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=false (the default).
func TestMultitenantCompactor_HandleBlockUpload_Create(t *testing.T) {
	const tenantID = "test"
	blockID := ulid.MustParse("01G3FZ0JWJYJC0ZM6Y9778P6KD")
	now := time.Now().UTC().UnixMilli()

	testCases := []struct {
		name          string
		tenantID      string
		unsetBlockID  bool
		body          string
		meta          *metadata.Meta
		expBadRequest string
	}{
		{
			name:          "without block ID",
			tenantID:      tenantID,
			unsetBlockID:  true,
			expBadRequest: "missing block ID",
		},
		{
			name:          "missing body",
			tenantID:      tenantID,
			expBadRequest: "malformed request body",
		},
		{
			name:          "malformed body",
			tenantID:      tenantID,
			body:          "{",
			expBadRequest: "malformed request body",
		},
		{
			name:     "valid request",
			tenantID: tenantID,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					MinTime: now - 1000,
					MaxTime: now,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "test",
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
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bkt bucket.ClientMock
			if !tc.unsetBlockID && tc.tenantID != "" {
				bkt.MockExists(path.Join(tenantID, blockID.String(), block.MetaFilename), false, nil)
				if tc.expBadRequest == "" {
					bkt.MockUpload(mock.Anything, nil)
				}
			}
			cfgProvider := newMockConfigProvider()
			cfgProvider.userRetentionPeriods[tenantID] = time.Second
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: &bkt,
				cfgProvider:  cfgProvider,
			}
			var rdr io.Reader
			if tc.body != "" {
				rdr = bytes.NewReader([]byte(tc.body))
			} else if tc.meta != nil {
				buf := bytes.NewBuffer(nil)
				require.NoError(t, json.NewEncoder(buf).Encode(tc.meta))
				rdr = buf
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/%s", blockID), rdr)
			if tc.tenantID != "" {
				r.Header.Set(user.OrgIDHeaderName, tenantID)
			}
			if !tc.unsetBlockID {
				r = mux.SetURLVars(r, map[string]string{"block": blockID.String()})
			}
			w := httptest.NewRecorder()
			c.HandleBlockUpload(w, r)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			switch {
			case tc.expBadRequest != "":
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expBadRequest), string(body))
			default:
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Empty(t, string(body))
			}

			bkt.AssertExpectations(t)
		})
	}
}

// Test MultitenantCompactor.UploadBlockFile.
func TestMultitenantCompactor_UploadBlockFile(t *testing.T) {
	const tenantID = "test"
	blockID := ulid.MustParse("01G3FZ0JWJYJC0ZM6Y9778P6KD")
	uploadingMetaPath := path.Join(tenantID, blockID.String(), "uploading-meta.json")

	t.Run("without block ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "chunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files?path=%s", blockID, pth), nil)
		r = mux.SetURLVars(r, map[string]string{"path": pth})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
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
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files", blockID), nil)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String()})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing or invalid file path\n", string(body))
	})

	t.Run("bad path", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		const pth = "..%2Fchunks%2F000001"
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files?path=%s", blockID, pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String(), "path": pth})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
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
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files?path=%s", blockID, pth), nil)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String(), "path": pth})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "file cannot be empty\n", string(body))
	})

	t.Run("attempt block metadata file", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		buf := bytes.NewBuffer([]byte("content"))
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files?path=%s", blockID, block.MetaFilename), buf)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String(), "path": block.MetaFilename})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, fmt.Sprintf("%s is not allowed\n", block.MetaFilename), string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		var bkt bucket.ClientMock
		expPath := path.Join(tenantID, blockID.String(), "chunks/000001")
		bkt.MockUpload(expPath, nil)
		bkt.MockExists(uploadingMetaPath, true, nil)
		bkt.MockExists(path.Join(tenantID, blockID.String(), block.MetaFilename), false, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}
		const pth = "chunks%2F000001"
		buf := bytes.NewBuffer([]byte("content"))
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s/files?path=%s", blockID, pth), buf)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String(), "path": pth})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.UploadBlockFile(w, r)

		resp := w.Result()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		bkt.AssertExpectations(t)
	})
}

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=true.
func TestMultitenantCompactor_HandleBlockUpload_Complete(t *testing.T) {
	const tenantID = "test"
	blockID := ulid.MustParse("01G3FZ0JWJYJC0ZM6Y9778P6KD")
	uploadingMetaPath := path.Join(tenantID, blockID.String(), "uploading-meta.json")

	t.Run("without tenant ID", func(t *testing.T) {
		c := &MultitenantCompactor{
			logger: log.NewNopLogger(),
		}
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s?uploadComplete=true", blockID), nil)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String()})
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
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s?uploadComplete=true", blockID), nil)
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, "missing block ID\n", string(body))
	})

	t.Run("valid request", func(t *testing.T) {
		meta := metadata.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID: blockID,
			},
			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.CompactorShardIDExternalLabel: "test",
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
		var bkt bucket.ClientMock
		bkt.MockExists(path.Join(tenantID, blockID.String(), block.MetaFilename), false, nil)
		bkt.On("Get", mock.Anything, uploadingMetaPath).Return(func(_ context.Context, _ string) (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(metaJSON)), nil
		})
		bkt.MockDelete(uploadingMetaPath, nil)
		expPath := path.Join("test", blockID.String(), block.MetaFilename)
		bkt.MockUpload(expPath, nil)
		c := &MultitenantCompactor{
			logger:       log.NewNopLogger(),
			bucketClient: &bkt,
		}
		r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
			"/api/v1/upload/block/%s?uploadComplete=true", blockID),
			nil)
		r = mux.SetURLVars(r, map[string]string{"block": blockID.String()})
		r.Header.Set(user.OrgIDHeaderName, tenantID)
		w := httptest.NewRecorder()
		c.HandleBlockUpload(w, r)

		resp := w.Result()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		expBuf := bytes.NewBuffer(metaJSON)
		require.NoError(t, expBuf.WriteByte('\n'))
		bkt.AssertExpectations(t)
	})
}
