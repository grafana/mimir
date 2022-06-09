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
	const blockID = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
	bULID := ulid.MustParse(blockID)
	now := time.Now().UTC().UnixMilli()
	validMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    bULID,
			MinTime: now - 1000,
			MaxTime: now,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				mimir_tsdb.CompactorShardIDExternalLabel: "test",
			},
			Files: []metadata.File{
				{
					RelPath: block.MetaFilename,
				},
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

	setUpPartialBlock := func(bkt *bucket.ClientMock) {
		bkt.MockExists(path.Join(tenantID, blockID, block.MetaFilename), false, nil)
	}

	testCases := []struct {
		name                   string
		tenantID               string
		blockID                string
		body                   string
		meta                   *metadata.Meta
		retention              time.Duration
		expBadRequest          string
		expConflict            string
		expInternalServerError bool
		setUpBucketMock        func(bkt *bucket.ClientMock)
	}{
		{
			name:          "missing tenant ID",
			tenantID:      "",
			blockID:       blockID,
			expBadRequest: "invalid tenant ID",
		},
		{
			name:          "missing block ID",
			tenantID:      tenantID,
			blockID:       "",
			expBadRequest: "invalid block ID",
		},
		{
			name:          "invalid block ID",
			tenantID:      tenantID,
			blockID:       "1234",
			expBadRequest: "invalid block ID",
		},
		{
			name:            "missing body",
			tenantID:        tenantID,
			blockID:         blockID,
			expBadRequest:   "malformed request body",
			setUpBucketMock: setUpPartialBlock,
		},
		{
			name:            "malformed body",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			body:            "{",
			expBadRequest:   "malformed request body",
		},
		{
			name:            "invalid file path",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{
							RelPath:   "chunks/invalid-file",
							SizeBytes: 1024,
						},
					},
				},
			},
			expBadRequest: fmt.Sprintf("file with invalid path in %s: chunks/invalid-file", block.MetaFilename),
		},
		{
			name:            "missing file size",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				Thanos: metadata.Thanos{
					Files: []metadata.File{
						{
							RelPath: block.MetaFilename,
						},
						{
							RelPath:   "index",
							SizeBytes: 1,
						},
						{
							RelPath: "chunks/000001",
						},
					},
				},
			},
			expBadRequest: fmt.Sprintf("file with invalid size in %s: chunks/000001", block.MetaFilename),
		},
		{
			name:            "invalid minTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					MinTime: -1,
					MaxTime: 0,
				},
			},
			expBadRequest: "invalid minTime/maxTime in meta.json: minTime=-1, maxTime=0",
		},
		{
			name:            "invalid maxTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					MinTime: 0,
					MaxTime: -1,
				},
			},
			expBadRequest: "invalid minTime/maxTime in meta.json: minTime=0, maxTime=-1",
		},
		{
			name:            "maxTime before minTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					MinTime: 1,
					MaxTime: 0,
				},
			},
			expBadRequest: "invalid minTime/maxTime in meta.json: minTime=1, maxTime=0",
		},
		{
			name:            "block before retention period",
			tenantID:        tenantID,
			blockID:         blockID,
			retention:       10 * time.Second,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					MinTime: 0,
					MaxTime: 1000,
				},
			},
			expBadRequest: "block max time (1970-01-01 00:00:01 +0000 UTC) older than retention period",
		},
		{
			name:      "ignore retention period if == 0",
			tenantID:  tenantID,
			blockID:   blockID,
			retention: 0,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				setUpPartialBlock(bkt)
				pth := path.Join(tenantID, blockID, tmpMetaFilename)
				bkt.MockUpload(pth, nil)
			},
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					MinTime: 0,
					MaxTime: 1000,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "test",
					},
					Files: []metadata.File{
						{
							RelPath: block.MetaFilename,
						},
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
		{
			name:     "failure checking for complete block",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(path.Join(tenantID, blockID, block.MetaFilename), false, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "complete block already exists",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(path.Join(tenantID, blockID, block.MetaFilename), true, nil)
			},
			expConflict: "block already exists in object storage",
		},
		{
			name:     "failure uploading meta file",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				setUpPartialBlock(bkt)
				pth := path.Join(tenantID, blockID, fmt.Sprintf("uploading-%s", block.MetaFilename))
				bkt.MockUpload(pth, fmt.Errorf("test"))
			},
			meta:                   &validMeta,
			expInternalServerError: true,
		},
		{
			name:     "valid request",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				setUpPartialBlock(bkt)
				pth := path.Join(tenantID, blockID, fmt.Sprintf("uploading-%s", block.MetaFilename))
				bkt.MockUpload(pth, nil)
			},
			meta: &validMeta,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bkt bucket.ClientMock
			if tc.setUpBucketMock != nil {
				tc.setUpBucketMock(&bkt)
			}

			cfgProvider := newMockConfigProvider()
			cfgProvider.userRetentionPeriods[tenantID] = tc.retention
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
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/%s", tc.blockID), rdr)
			if tc.tenantID != "" {
				r.Header.Set(user.OrgIDHeaderName, tenantID)
			}
			if tc.blockID != "" {
				r = mux.SetURLVars(r, map[string]string{"block": tc.blockID})
			}
			w := httptest.NewRecorder()
			c.HandleBlockUpload(w, r)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			switch {
			case tc.expInternalServerError:
				assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
				assert.Equal(t, "internal server error\n", string(body))
			case tc.expBadRequest != "":
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expBadRequest), string(body))
			case tc.expConflict != "":
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expConflict), string(body))
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
	uploadingMetaPath := path.Join(tenantID, blockID.String(), fmt.Sprintf("uploading-%s", block.MetaFilename))

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
		assert.Equal(t, "invalid block ID\n", string(body))
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
	uploadingMetaPath := path.Join(tenantID, blockID.String(), fmt.Sprintf("uploading-%s", block.MetaFilename))

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
		assert.Equal(t, "invalid block ID\n", string(body))
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
