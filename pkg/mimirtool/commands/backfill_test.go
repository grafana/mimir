// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestBackfill_BlockDirWithTrailingSlash(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	blockDir := filepath.Join(t.TempDir(), blockID.String())
	require.NoError(t, os.MkdirAll(filepath.Join(blockDir, block.ChunksDirname), 0o755))
	require.NoError(t, block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID, Version: 1}}.WriteToDir(log.NewNopLogger(), blockDir))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.IndexFilename), []byte("index"), 0o644))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/check") {
			_, _ = w.Write([]byte(`{"result":"complete"}`))
		}
	}))
	t.Cleanup(srv.Close)

	var blocks blockList
	require.NoError(t, blocks.Set(blockDir+"/"))

	cli, err := client.New(client.Config{Address: srv.URL, ID: "test"}, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, cli.BackfillWithOptions(t.Context(), blocks, 0, nil, false))
}
