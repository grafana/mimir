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

func TestGetBlockMetaFromBucket(t *testing.T) {
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

	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/meta.json", bytes.NewReader(metaBytes)))
	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/index", bytes.NewReader([]byte("index-data"))))
	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/chunks/000001", bytes.NewReader([]byte("chunk-1"))))
	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/chunks/000002", bytes.NewReader([]byte("chunk-22"))))

	result, err := GetBlockMetaFromBucket(context.Background(), bkt, blockID)
	require.NoError(t, err)

	assert.Equal(t, blockID, result.ULID)
	assert.Equal(t, int64(100), result.MinTime)
	assert.Equal(t, int64(200), result.MaxTime)

	filesByPath := map[string]int64{}
	for _, f := range result.Thanos.Files {
		filesByPath[f.RelPath] = f.SizeBytes
	}

	assert.Contains(t, filesByPath, "meta.json")
	assert.Equal(t, int64(len("index-data")), filesByPath["index"])
	assert.Equal(t, int64(len("chunk-1")), filesByPath["chunks/000001"])
	assert.Equal(t, int64(len("chunk-22")), filesByPath["chunks/000002"])
}

func TestGetBlockMetaFromBucket_InvalidVersion(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	bkt := objstore.NewInMemBucket()

	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			Version: 99,
		},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/meta.json", bytes.NewReader(metaBytes)))

	_, err = GetBlockMetaFromBucket(context.Background(), bkt, blockID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only version 1")
}

func TestBackfillBlock(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	blockIDStr := blockID.String()

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

	chunkData := []byte("chunk-data-here")
	indexData := []byte("index-data-here")
	require.NoError(t, bkt.Upload(context.Background(), blockIDStr+"/meta.json", bytes.NewReader(metaBytes)))
	require.NoError(t, bkt.Upload(context.Background(), blockIDStr+"/index", bytes.NewReader(indexData)))
	require.NoError(t, bkt.Upload(context.Background(), blockIDStr+"/chunks/000001", bytes.NewReader(chunkData)))

	var calls []string
	uploadedFiles := map[string][]byte{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		calls = append(calls, r.Method+" "+p)

		switch {
		case strings.HasSuffix(p, "/start"):
			w.WriteHeader(http.StatusOK)
		case strings.Contains(p, "/files"):
			filePath := r.URL.Query().Get("path")
			body, _ := io.ReadAll(r.Body)
			uploadedFiles[filePath] = body
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(p, "/finish"):
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(p, "/check"):
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"result":"complete"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	mimirClient, err := New(Config{
		Address: srv.URL,
		ID:      "test-tenant",
	}, log.NewNopLogger())
	require.NoError(t, err)

	err = mimirClient.BackfillBlock(context.Background(), bkt, blockID, 0)
	srv.Close()
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(calls), 4)
	assert.Contains(t, calls[0], "/start")
	assert.Contains(t, calls[len(calls)-2], "/finish")
	assert.Contains(t, calls[len(calls)-1], "/check")

	assert.Equal(t, indexData, uploadedFiles["index"])
	assert.Equal(t, chunkData, uploadedFiles["chunks/000001"])
	assert.NotContains(t, uploadedFiles, "meta.json")
}

func TestBackfillBlock_AlreadyExists(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)

	bkt := objstore.NewInMemBucket()
	meta := block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    blockID,
			Version: 1,
		},
	}
	metaBytes, err := json.Marshal(meta)
	require.NoError(t, err)

	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/meta.json", bytes.NewReader(metaBytes)))
	require.NoError(t, bkt.Upload(context.Background(), blockID.String()+"/index", bytes.NewReader([]byte("data"))))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/start") {
			w.WriteHeader(http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))

	mimirClient, err := New(Config{
		Address: srv.URL,
		ID:      "test-tenant",
	}, log.NewNopLogger())
	require.NoError(t, err)

	err = mimirClient.BackfillBlock(context.Background(), bkt, blockID, 0)
	srv.Close()
	require.ErrorIs(t, err, ErrConflict)
}
