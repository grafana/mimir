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
	"net/url"
	"path"
	"strings"
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
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func verifyUploadedMeta(t *testing.T, bkt *bucket.ClientMock, expMeta metadata.Meta) {
	var call mock.Call
	for _, c := range bkt.Calls {
		if c.Method == "Upload" {
			call = c
			break
		}
	}

	rdr := call.Arguments[2].(io.Reader)
	var gotMeta metadata.Meta
	require.NoError(t, json.NewDecoder(rdr).Decode(&gotMeta))
	assert.Equal(t, expMeta, gotMeta)
}

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=false (the default).
func TestMultitenantCompactor_HandleBlockUpload_Create(t *testing.T) {
	const tenantID = "test"
	const blockID = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
	bULID := ulid.MustParse(blockID)
	now := time.Now().UnixMilli()
	validMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    bULID,
			Version: metadata.TSDBVersion1,
			MinTime: now - 1000,
			MaxTime: now,
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
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

	metaPath := path.Join(tenantID, blockID, block.MetaFilename)
	uploadingMetaPath := path.Join(tenantID, blockID, fmt.Sprintf("uploading-%s", block.MetaFilename))

	setUpPartialBlock := func(bkt *bucket.ClientMock) {
		bkt.MockExists(path.Join(tenantID, blockID, block.MetaFilename), false, nil)
	}
	setUpUpload := func(bkt *bucket.ClientMock) {
		setUpPartialBlock(bkt)
		bkt.MockUpload(uploadingMetaPath, nil)
	}

	verifyUpload := func(t *testing.T, bkt *bucket.ClientMock, labels map[string]string) {
		t.Helper()

		expMeta := validMeta
		expMeta.Compaction.Parents = nil
		expMeta.Compaction.Sources = []ulid.ULID{expMeta.ULID}
		expMeta.Thanos.Source = "upload"
		expMeta.Thanos.Labels = labels
		verifyUploadedMeta(t, bkt, expMeta)
	}

	testCases := []struct {
		name                   string
		tenantID               string
		blockID                string
		body                   string
		meta                   *metadata.Meta
		retention              time.Duration
		disableBlockUpload     bool
		expBadRequest          string
		expConflict            string
		expUnprocessableEntity string
		expInternalServerError bool
		setUpBucketMock        func(bkt *bucket.ClientMock)
		verifyUpload           func(*testing.T, *bucket.ClientMock)
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
			expBadRequest: "file with invalid path: chunks/invalid-file",
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
			expBadRequest: "file with invalid size: chunks/000001",
		},
		{
			name:            "invalid minTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: -1,
					MaxTime: 0,
				},
			},
			expBadRequest: "invalid minTime/maxTime: minTime=-1, maxTime=0",
		},
		{
			name:            "invalid maxTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: 0,
					MaxTime: -1,
				},
			},
			expBadRequest: "invalid minTime/maxTime: minTime=0, maxTime=-1",
		},
		{
			name:            "maxTime before minTime",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: 1,
					MaxTime: 0,
				},
			},
			expBadRequest: "invalid minTime/maxTime: minTime=1, maxTime=0",
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
					Version: metadata.TSDBVersion1,
					MinTime: 0,
					MaxTime: 1000,
				},
			},
			expUnprocessableEntity: "block max time (1970-01-01 00:00:01 +0000 UTC) older than retention period",
		},
		{
			name:            "invalid version",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: 0,
				},
			},
			expBadRequest: fmt.Sprintf("version must be %d", metadata.TSDBVersion1),
		},
		{
			name:            "ignore retention period if == 0",
			tenantID:        tenantID,
			blockID:         blockID,
			retention:       0,
			setUpBucketMock: setUpUpload,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: 0,
					MaxTime: 1000,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
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
			name:            "ignore retention period if < 0",
			tenantID:        tenantID,
			blockID:         blockID,
			retention:       -1,
			setUpBucketMock: setUpUpload,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: 0,
					MaxTime: 1000,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
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
			name:            "invalid compactor shard ID label",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpPartialBlock,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "test",
					},
				},
			},
			expBadRequest: fmt.Sprintf(`invalid %s external label: "test"`, mimir_tsdb.CompactorShardIDExternalLabel),
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
				bkt.MockUpload(uploadingMetaPath, fmt.Errorf("test"))
			},
			meta:                   &validMeta,
			expInternalServerError: true,
		},
		{
			name:               "block upload disabled",
			tenantID:           tenantID,
			blockID:            blockID,
			disableBlockUpload: true,
			expBadRequest:      "block upload is disabled",
		},
		{
			name:            "valid request",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpUpload,
			meta:            &validMeta,
			verifyUpload: func(t *testing.T, bkt *bucket.ClientMock) {
				verifyUpload(t, bkt, map[string]string{
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
				})
			},
		},
		{
			name:            "valid request with empty compactor shard ID label",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpUpload,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: now - 1000,
					MaxTime: now,
				},
				Thanos: metadata.Thanos{
					Labels: map[string]string{
						mimir_tsdb.CompactorShardIDExternalLabel: "",
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
			verifyUpload: func(t *testing.T, bkt *bucket.ClientMock) {
				verifyUpload(t, bkt, map[string]string{})
			},
		},
		{
			name:            "valid request without compactor shard ID label",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpUpload,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    bULID,
					Version: metadata.TSDBVersion1,
					MinTime: now - 1000,
					MaxTime: now,
				},
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
							RelPath:   "chunks/000001",
							SizeBytes: 1024,
						},
					},
				},
			},
			verifyUpload: func(t *testing.T, bkt *bucket.ClientMock) {
				verifyUpload(t, bkt, nil)
			},
		},
		{
			name:            "valid request with different block ID in meta file",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpUpload,
			meta: &metadata.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid.MustParse("11A2FZ0JWJYJC0ZM6Y9778P6KD"),
					Version: metadata.TSDBVersion1,
					MinTime: now - 1000,
					MaxTime: now,
				},
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
							RelPath:   "chunks/000001",
							SizeBytes: 1024,
						},
					},
				},
			},
			verifyUpload: func(t *testing.T, bkt *bucket.ClientMock) {
				verifyUpload(t, bkt, nil)
			},
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
			cfgProvider.blockUploadEnabled[tenantID] = !tc.disableBlockUpload
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: &bkt,
				cfgProvider:  cfgProvider,
			}
			var rdr io.Reader
			if tc.body != "" {
				rdr = strings.NewReader(tc.body)
			} else if tc.meta != nil {
				buf := bytes.NewBuffer(nil)
				require.NoError(t, json.NewEncoder(buf).Encode(tc.meta))
				rdr = buf
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/%s", tc.blockID), rdr)
			if tc.tenantID != "" {
				r = r.WithContext(user.InjectOrgID(r.Context(), tc.tenantID))
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
			case tc.expUnprocessableEntity != "":
				assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expUnprocessableEntity), string(body))
			default:
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Empty(t, string(body))
			}

			bkt.AssertExpectations(t)

			if tc.verifyUpload != nil {
				tc.verifyUpload(t, &bkt)
			}
		})
	}

	downloadMeta := func(t *testing.T, bkt *objstore.InMemBucket, pth string) metadata.Meta {
		t.Helper()

		ctx := context.Background()
		rdr, err := bkt.Get(ctx, pth)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = rdr.Close()
		})
		var gotMeta metadata.Meta
		require.NoError(t, json.NewDecoder(rdr).Decode(&gotMeta))
		return gotMeta
	}

	// Additional test cases using an in-memory bucket for state testing
	extraCases := []struct {
		name          string
		setUp         func(*testing.T, *objstore.InMemBucket) metadata.Meta
		verifyBucket  func(*testing.T, *objstore.InMemBucket)
		expBadRequest string
		expConflict   string
	}{
		{
			name: "valid request when both in-flight meta file and complete meta file exist in object storage",
			setUp: func(t *testing.T, bkt *objstore.InMemBucket) metadata.Meta {
				uploadMeta(t, bkt, uploadingMetaPath, validMeta)
				uploadMeta(t, bkt, metaPath, validMeta)
				return validMeta
			},
			verifyBucket: func(t *testing.T, bkt *objstore.InMemBucket) {
				assert.Equal(t, validMeta, downloadMeta(t, bkt, uploadingMetaPath))
				assert.Equal(t, validMeta, downloadMeta(t, bkt, metaPath))
			},
			expConflict: "block already exists in object storage",
		},
		{
			name: "invalid request when in-flight meta file exists in object storage",
			setUp: func(t *testing.T, bkt *objstore.InMemBucket) metadata.Meta {
				uploadMeta(t, bkt, uploadingMetaPath, validMeta)

				meta := validMeta
				// Invalid version
				meta.Version = 0
				return meta
			},
			verifyBucket: func(t *testing.T, bkt *objstore.InMemBucket) {
				assert.Equal(t, validMeta, downloadMeta(t, bkt, uploadingMetaPath))
			},
			expBadRequest: fmt.Sprintf("version must be %d", metadata.TSDBVersion1),
		},
		{
			name: "valid request when same in-flight meta file exists in object storage",
			setUp: func(t *testing.T, bkt *objstore.InMemBucket) metadata.Meta {
				uploadMeta(t, bkt, uploadingMetaPath, validMeta)
				return validMeta
			},
			verifyBucket: func(t *testing.T, bkt *objstore.InMemBucket) {
				expMeta := validMeta
				expMeta.Compaction.Sources = []ulid.ULID{expMeta.ULID}
				expMeta.Thanos.Source = "upload"
				assert.Equal(t, expMeta, downloadMeta(t, bkt, uploadingMetaPath))
			},
		},
		{
			name: "valid request when different in-flight meta file exists in object storage",
			setUp: func(t *testing.T, bkt *objstore.InMemBucket) metadata.Meta {
				meta := validMeta
				meta.MinTime -= 1000
				meta.MaxTime -= 1000
				uploadMeta(t, bkt, uploadingMetaPath, meta)

				// Return meta file that differs from the one in bucket
				return validMeta
			},
			verifyBucket: func(t *testing.T, bkt *objstore.InMemBucket) {
				expMeta := validMeta
				expMeta.Compaction.Sources = []ulid.ULID{expMeta.ULID}
				expMeta.Thanos.Source = "upload"
				assert.Equal(t, expMeta, downloadMeta(t, bkt, uploadingMetaPath))
			},
		},
	}
	for _, tc := range extraCases {
		t.Run(tc.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			meta := tc.setUp(t, bkt)
			metaJSON, err := json.Marshal(meta)
			require.NoError(t, err)

			cfgProvider := newMockConfigProvider()
			cfgProvider.blockUploadEnabled[tenantID] = true
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: bkt,
				cfgProvider:  cfgProvider,
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/upload/block/%s", blockID), bytes.NewReader(metaJSON))
			r = r.WithContext(user.InjectOrgID(r.Context(), tenantID))
			r = mux.SetURLVars(r, map[string]string{"block": blockID})
			w := httptest.NewRecorder()
			c.HandleBlockUpload(w, r)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			switch {
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
		})
	}
}

// Test MultitenantCompactor.UploadBlockFile.
func TestMultitenantCompactor_UploadBlockFile(t *testing.T) {
	const tenantID = "test"
	const blockID = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
	uploadingMetaFilename := fmt.Sprintf("uploading-%s", block.MetaFilename)
	uploadingMetaPath := path.Join(tenantID, blockID, uploadingMetaFilename)
	metaPath := path.Join(tenantID, blockID, block.MetaFilename)
	validMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: ulid.MustParse(blockID),
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
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

	testCases := []struct {
		name                   string
		tenantID               string
		blockID                string
		path                   string
		body                   string
		disableBlockUpload     bool
		expBadRequest          string
		expConflict            string
		expNotFound            string
		expInternalServerError bool
		setUpBucketMock        func(bkt *bucket.ClientMock)
		verifyUpload           func(*testing.T, *bucket.ClientMock, string)
	}{
		{
			name:          "without tenant ID",
			blockID:       blockID,
			path:          "chunks/000001",
			expBadRequest: "invalid tenant ID",
		},
		{
			name:          "without block ID",
			tenantID:      tenantID,
			path:          "chunks/000001",
			expBadRequest: "invalid block ID",
		},
		{
			name:          "invalid block ID",
			tenantID:      tenantID,
			blockID:       "1234",
			path:          "chunks/000001",
			expBadRequest: "invalid block ID",
		},
		{
			name:          "without path",
			tenantID:      tenantID,
			blockID:       blockID,
			expBadRequest: "missing or invalid file path",
		},
		{
			name:          "invalid path",
			tenantID:      tenantID,
			blockID:       blockID,
			path:          "../chunks/000001",
			expBadRequest: `invalid path: "../chunks/000001"`,
		},
		{
			name:          "empty file",
			tenantID:      tenantID,
			blockID:       blockID,
			path:          "chunks/000001",
			expBadRequest: "file cannot be empty",
		},
		{
			name:          "attempt block metadata file",
			tenantID:      tenantID,
			blockID:       blockID,
			path:          block.MetaFilename,
			body:          "content",
			expBadRequest: fmt.Sprintf("%s is not allowed", block.MetaFilename),
		},
		{
			name:          "attempt in-flight block metadata file",
			tenantID:      tenantID,
			blockID:       blockID,
			path:          uploadingMetaFilename,
			body:          "content",
			expBadRequest: fmt.Sprintf("invalid path: %q", uploadingMetaFilename),
		},
		{
			name:               "block upload disabled",
			tenantID:           tenantID,
			blockID:            blockID,
			disableBlockUpload: true,
			path:               "chunks/000001",
			expBadRequest:      "block upload is disabled",
		},
		{
			name:     "complete block already exists",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, true, nil)
			},
			expConflict: "block already exists in object storage",
		},
		{
			name:     "failure checking for complete block",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "failure checking for in-flight meta file",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				bkt.MockExists(uploadingMetaPath, false, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "missing in-flight meta file",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				bkt.MockExists(uploadingMetaPath, false, nil)
			},
			expNotFound: fmt.Sprintf("upload of block %s not started yet", blockID),
		},
		{
			name:     "file upload fails",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(uploadingMetaPath, true, nil)
				bkt.MockExists(metaPath, false, nil)
				bkt.MockUpload(path.Join(tenantID, blockID, "chunks/000001"), fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "valid request",
			tenantID: tenantID,
			blockID:  blockID,
			path:     "chunks/000001",
			body:     "content",
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(uploadingMetaPath, true, nil)
				bkt.MockExists(metaPath, false, nil)
				bkt.MockUpload(path.Join(tenantID, blockID, "chunks/000001"), nil)
			},
			verifyUpload: func(t *testing.T, bkt *bucket.ClientMock, expContent string) {
				var call mock.Call
				for _, c := range bkt.Calls {
					if c.Method == "Upload" {
						call = c
						break
					}
				}

				rdr := call.Arguments[2].(io.Reader)
				got, err := io.ReadAll(rdr)
				require.NoError(t, err)
				assert.Equal(t, []byte(expContent), got)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bkt bucket.ClientMock
			if tc.setUpBucketMock != nil {
				tc.setUpBucketMock(&bkt)
			}

			cfgProvider := newMockConfigProvider()
			cfgProvider.blockUploadEnabled[tc.tenantID] = !tc.disableBlockUpload
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: &bkt,
				cfgProvider:  cfgProvider,
			}
			var rdr io.Reader
			if tc.body != "" {
				rdr = strings.NewReader(tc.body)
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
				"/api/v1/upload/block/%s/files?path=%s", blockID, url.QueryEscape(tc.path)), rdr)
			if tc.tenantID != "" {
				r = r.WithContext(user.InjectOrgID(r.Context(), tenantID))
			}
			if tc.blockID != "" {
				r = mux.SetURLVars(r, map[string]string{"block": tc.blockID})
			}
			w := httptest.NewRecorder()
			c.UploadBlockFile(w, r)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			switch {
			case tc.expBadRequest != "":
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expBadRequest), string(body))
			case tc.expConflict != "":
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expConflict), string(body))
			case tc.expNotFound != "":
				assert.Equal(t, http.StatusNotFound, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expNotFound), string(body))
			case tc.expInternalServerError:
				assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
				assert.Equal(t, "internal server error\n", string(body))
			default:
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Empty(t, string(body))
			}

			bkt.AssertExpectations(t)

			if tc.verifyUpload != nil {
				tc.verifyUpload(t, &bkt, tc.body)
			}
		})
	}

	type file struct {
		path    string
		content string
	}

	// Additional test cases using an in-memory bucket for state testing
	extraCases := []struct {
		name         string
		files        []file
		setUpBucket  func(*testing.T, *objstore.InMemBucket)
		verifyBucket func(*testing.T, *objstore.InMemBucket, []file)
	}{
		{
			name: "multiple sequential uploads of same file",
			files: []file{
				{
					path:    "chunks/000001",
					content: "first",
				},
				{
					path:    "chunks/000001",
					content: "second",
				},
			},
			setUpBucket: func(t *testing.T, bkt *objstore.InMemBucket) {
				uploadMeta(t, bkt, uploadingMetaPath, validMeta)
			},
			verifyBucket: func(t *testing.T, bkt *objstore.InMemBucket, files []file) {
				t.Helper()

				ctx := context.Background()
				rdr, err := bkt.Get(ctx, path.Join(tenantID, blockID, files[1].path))
				require.NoError(t, err)
				t.Cleanup(func() {
					_ = rdr.Close()
				})

				content, err := io.ReadAll(rdr)
				require.NoError(t, err)
				assert.Equal(t, files[1].content, string(content))
			},
		},
	}
	for _, tc := range extraCases {
		t.Run(tc.name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			tc.setUpBucket(t, bkt)
			cfgProvider := newMockConfigProvider()
			cfgProvider.blockUploadEnabled[tenantID] = true
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: bkt,
				cfgProvider:  cfgProvider,
			}

			for _, f := range tc.files {
				rdr := strings.NewReader(f.content)
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
					"/api/v1/upload/block/%s/files?path=%s", blockID, url.QueryEscape(f.path)), rdr)
				urlVars := map[string]string{
					"block": blockID,
				}
				r = mux.SetURLVars(r, urlVars)
				r = r.WithContext(user.InjectOrgID(r.Context(), tenantID))
				w := httptest.NewRecorder()
				c.UploadBlockFile(w, r)

				resp := w.Result()
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				require.Empty(t, body)
			}

			tc.verifyBucket(t, bkt, tc.files)
		})
	}
}

func setUpGet(bkt *bucket.ClientMock, pth string, content []byte, err error) {
	bkt.On("Get", mock.Anything, pth).Return(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(content)), err
	})
}

// Test MultitenantCompactor.HandleBlockUpload with uploadComplete=true.
func TestMultitenantCompactor_HandleBlockUpload_Complete(t *testing.T) {
	const tenantID = "test"
	const blockID = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
	uploadingMetaPath := path.Join(tenantID, blockID, fmt.Sprintf("uploading-%s", block.MetaFilename))
	metaPath := path.Join(tenantID, blockID, block.MetaFilename)
	validMeta := metadata.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID: ulid.MustParse(blockID),
		},
		Thanos: metadata.Thanos{
			Labels: map[string]string{
				mimir_tsdb.CompactorShardIDExternalLabel: "1_of_3",
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

	setUpSuccessfulComplete := func(bkt *bucket.ClientMock) {
		metaJSON, err := json.Marshal(validMeta)
		require.NoError(t, err)
		bkt.MockExists(metaPath, false, nil)
		setUpGet(bkt, uploadingMetaPath, metaJSON, nil)
		bkt.MockUpload(metaPath, nil)
		bkt.MockDelete(uploadingMetaPath, nil)
	}
	testCases := []struct {
		name                   string
		tenantID               string
		blockID                string
		disableBlockUpload     bool
		expMeta                metadata.Meta
		expBadRequest          string
		expConflict            string
		expNotFound            string
		expInternalServerError bool
		setUpBucketMock        func(bkt *bucket.ClientMock)
		verifyUpload           func(*testing.T, *bucket.ClientMock, metadata.Meta)
	}{
		{
			name:          "without tenant ID",
			blockID:       blockID,
			expBadRequest: "invalid tenant ID",
		},
		{
			name:          "without block ID",
			tenantID:      tenantID,
			expBadRequest: "invalid block ID",
		},
		{
			name:          "invalid block ID",
			tenantID:      tenantID,
			blockID:       "1234",
			expBadRequest: "invalid block ID",
		},
		{
			name:               "block upload disabled",
			tenantID:           tenantID,
			blockID:            blockID,
			disableBlockUpload: true,
			expBadRequest:      "block upload is disabled",
		},
		{
			name:     "complete block already exists",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, true, nil)
			},
			expConflict: "block already exists in object storage",
		},
		{
			name:     "checking for complete block fails",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "missing in-flight meta file",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				setUpGet(bkt, uploadingMetaPath, nil, bucket.ErrObjectDoesNotExist)
			},
			expNotFound: fmt.Sprintf("upload of block %s not started yet", blockID),
		},
		{
			name:     "downloading in-flight meta file fails",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				setUpGet(bkt, uploadingMetaPath, nil, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "corrupt in-flight meta file",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				setUpGet(bkt, uploadingMetaPath, []byte("{"), nil)
			},
			expInternalServerError: true,
		},
		{
			name:     "uploading meta file fails",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				metaJSON, err := json.Marshal(validMeta)
				require.NoError(t, err)
				setUpGet(bkt, uploadingMetaPath, metaJSON, nil)
				bkt.MockUpload(metaPath, fmt.Errorf("test"))
			},
			expInternalServerError: true,
		},
		{
			name:     "removing in-flight meta file fails",
			tenantID: tenantID,
			blockID:  blockID,
			setUpBucketMock: func(bkt *bucket.ClientMock) {
				bkt.MockExists(metaPath, false, nil)
				metaJSON, err := json.Marshal(validMeta)
				require.NoError(t, err)
				setUpGet(bkt, uploadingMetaPath, metaJSON, nil)
				bkt.MockUpload(metaPath, nil)
				bkt.MockDelete(uploadingMetaPath, fmt.Errorf("test"))
			},
			expMeta:      validMeta,
			verifyUpload: verifyUploadedMeta,
		},
		{
			name:            "valid request",
			tenantID:        tenantID,
			blockID:         blockID,
			setUpBucketMock: setUpSuccessfulComplete,
			expMeta:         validMeta,
			verifyUpload:    verifyUploadedMeta,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var bkt bucket.ClientMock
			if tc.setUpBucketMock != nil {
				tc.setUpBucketMock(&bkt)
			}
			cfgProvider := newMockConfigProvider()
			cfgProvider.blockUploadEnabled[tc.tenantID] = !tc.disableBlockUpload
			c := &MultitenantCompactor{
				logger:       log.NewNopLogger(),
				bucketClient: &bkt,
				cfgProvider:  cfgProvider,
			}
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf(
				"/api/v1/upload/block/%s?uploadComplete=true", tc.blockID), nil)
			if tc.tenantID != "" {
				r = r.WithContext(user.InjectOrgID(r.Context(), tenantID))
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
			case tc.expBadRequest != "":
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expBadRequest), string(body))
			case tc.expConflict != "":
				assert.Equal(t, http.StatusConflict, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expConflict), string(body))
			case tc.expNotFound != "":
				assert.Equal(t, http.StatusNotFound, resp.StatusCode)
				assert.Equal(t, fmt.Sprintf("%s\n", tc.expNotFound), string(body))
			case tc.expInternalServerError:
				assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
				assert.Equal(t, "internal server error\n", string(body))
			default:
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				assert.Empty(t, string(body))
			}

			bkt.AssertExpectations(t)

			if tc.verifyUpload != nil {
				tc.verifyUpload(t, &bkt, tc.expMeta)
			}
		})
	}
}

// uploadMeta is a test helper for uploading a meta file to a certain path in a bucket.
func uploadMeta(t *testing.T, bkt *objstore.InMemBucket, pth string, meta metadata.Meta) {
	t.Helper()

	buf := bytes.NewBuffer(nil)
	require.NoError(t, json.NewEncoder(buf).Encode(meta))
	ctx := context.Background()
	require.NoError(t, bkt.Upload(ctx, pth, buf))
}
