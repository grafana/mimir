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
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestGetBlockMeta(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	bkt := objstore.NewInMemBucket()

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
	prefix := blockID.String()
	require.NoError(t, bkt.Upload(ctx, prefix+"/meta.json", bytes.NewReader(metaBytes)))
	require.NoError(t, bkt.Upload(ctx, prefix+"/index", bytes.NewReader(make([]byte, 1))))
	require.NoError(t, bkt.Upload(ctx, prefix+"/chunks/000001", bytes.NewReader(make([]byte, 2))))
	require.NoError(t, bkt.Upload(ctx, prefix+"/chunks/000002", bytes.NewReader(make([]byte, 3))))

	result, err := GetBlockMeta(ctx, bkt, blockID)
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
}

func TestGetBlockMeta_RejectsUnsupportedVersion(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	bkt := objstore.NewInMemBucket()

	meta := block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID, Version: 99}}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/meta.json", bytes.NewReader(metaBytes)))

	_, err = GetBlockMeta(context.Background(), bkt, blockID)
	require.Error(t, err)
}

func TestBackfillBlock(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	prefix := blockID.String()
	indexData := []byte("index-content")
	chunkData := []byte("chunk-content")

	bkt := objstore.NewInMemBucket()
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
	require.NoError(t, bkt.Upload(ctx, prefix+"/meta.json", bytes.NewReader(metaBytes)))
	require.NoError(t, bkt.Upload(ctx, prefix+"/index", bytes.NewReader(indexData)))
	require.NoError(t, bkt.Upload(ctx, prefix+"/chunks/000001", bytes.NewReader(chunkData)))

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

	require.NoError(t, c.BackfillBlock(ctx, bkt, blockID, 0))

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
