// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools" //lint:ignore faillint objtools is allowed in tools and their tests
)

// newTestBucket returns an objtools filesystem bucket backed by a temporary directory.
func newTestBucket(t *testing.T) objtools.Bucket {
	t.Helper()
	cfg := objtools.FilesystemClientConfig{Directory: t.TempDir()}
	bkt, err := cfg.ToBucket()
	require.NoError(t, err)
	return bkt
}

func uploadString(t *testing.T, bkt objtools.Bucket, name string, data []byte) {
	t.Helper()
	require.NoError(t, bkt.Upload(context.Background(), name, bytes.NewReader(data), int64(len(data))))
}

func TestGetBlockMeta(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)

	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "no prefix", prefix: ""},
		{name: "with tenant prefix", prefix: "tenant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bkt := newTestBucket(t)

			meta := block.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    blockID,
					Version: 1,
					MinTime: 100,
					MaxTime: 200,
				},
			}
			metaBytes, err := json.Marshal(meta)
			require.NoError(t, err)

			base := blockID.String()
			if tc.prefix != "" {
				base = tc.prefix + "/" + base
			}
			uploadString(t, bkt, base+"/meta.json", metaBytes)
			uploadString(t, bkt, base+"/index", make([]byte, 1))
			uploadString(t, bkt, base+"/chunks/000001", make([]byte, 2))
			uploadString(t, bkt, base+"/chunks/000002", make([]byte, 3))

			result, err := GetBlockMeta(context.Background(), bkt, tc.prefix, blockID)
			require.NoError(t, err)

			assert.Equal(t, blockID, result.ULID)
			assert.Equal(t, int64(100), result.MinTime)
			assert.Equal(t, int64(200), result.MaxTime)

			fileSizes := map[string]int64{}
			for _, f := range result.Thanos.Files {
				fileSizes[f.RelPath] = f.SizeBytes
			}
			assert.Len(t, fileSizes, 4)
			assert.Contains(t, fileSizes, "meta.json")
			assert.Equal(t, int64(1), fileSizes["index"])
			assert.Equal(t, int64(2), fileSizes["chunks/000001"])
			assert.Equal(t, int64(3), fileSizes["chunks/000002"])
		})
	}
}

func TestGetBlockMeta_RejectsUnsupportedVersion(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	bkt := newTestBucket(t)

	meta := block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID, Version: 99}}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	uploadString(t, bkt, blockID.String()+"/meta.json", metaBytes)

	_, err = GetBlockMeta(context.Background(), bkt, "", blockID)
	require.Error(t, err)
}

func TestBackfillBlock(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	prefix := blockID.String()
	indexData := []byte("index-content")
	chunkData := []byte("chunk-content")

	bkt := newTestBucket(t)
	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			Version: 1,
			MinTime: 100,
			MaxTime: 200,
		},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	ctx := context.Background()
	uploadString(t, bkt, prefix+"/meta.json", metaBytes)
	uploadString(t, bkt, prefix+"/index", indexData)
	uploadString(t, bkt, prefix+"/chunks/000001", chunkData)

	type recorded struct {
		method string
		path   string
		file   string // "path" query param for file uploads
		body   []byte
	}
	var reqs []recorded

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		reqs = append(reqs, recorded{
			method: r.Method,
			path:   r.URL.Path,
			file:   r.URL.Query().Get("path"),
			body:   body,
		})
		if strings.HasSuffix(r.URL.Path, "/check") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"result":"complete"}`)
		}
	}))
	defer srv.Close()

	c, err := New(Config{Address: srv.URL, ID: "test"}, log.NewNopLogger())
	require.NoError(t, err)

	require.NoError(t, c.BackfillBlock(ctx, bkt, "", blockID, 0))

	blockPath := "/api/v1/upload/block/" + prefix

	// start, upload index, upload chunk, finish, check
	require.Len(t, reqs, 5)

	assert.Equal(t, http.MethodPost, reqs[0].method)
	assert.Equal(t, blockPath+"/start", reqs[0].path)
	var sentMeta block.Meta
	require.NoError(t, json.Unmarshal(reqs[0].body, &sentMeta))
	assert.Equal(t, blockID, sentMeta.ULID)

	uploadedFiles := map[string][]byte{}
	for _, r := range reqs[1:3] {
		assert.Equal(t, http.MethodPost, r.method)
		assert.Equal(t, blockPath+"/files", r.path)
		uploadedFiles[r.file] = r.body
	}
	assert.Equal(t, indexData, uploadedFiles["index"])
	assert.Equal(t, chunkData, uploadedFiles["chunks/000001"])

	assert.Equal(t, http.MethodPost, reqs[3].method)
	assert.Equal(t, blockPath+"/finish", reqs[3].path)

	assert.Equal(t, http.MethodGet, reqs[4].method)
	assert.Equal(t, blockPath+"/check", reqs[4].path)
}
