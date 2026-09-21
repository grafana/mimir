// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	testTenantID = "test"
	testBlockID  = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
)

func TestAPI_Start(t *testing.T) {
	a, bkt := newTestAPI()

	jobIDs := map[string]struct{}{}
	for range 3 {
		jobIDs[startJob(t, a)] = struct{}{}
	}

	assert.Len(t, jobIDs, 3)
	assert.Empty(t, bkt.Objects())
}

func TestAPI_BlockUpload(t *testing.T) {
	a, bkt := newTestAPI()

	meta := testMeta()
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	jobID := startJob(t, a)

	require.Equal(t, http.StatusOK, do(a.StartBlockUpload, jobID, "", bytes.NewReader(metaJSON)).StatusCode)

	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}

		content := bytes.NewReader(bytes.Repeat([]byte("x"), int(f.SizeBytes)))
		resp := do(a.UploadBlockFile, jobID, "?path="+f.RelPath, content)
		require.Equal(t, http.StatusOK, resp.StatusCode, "uploading %s", f.RelPath)
	}

	require.Equal(t, http.StatusOK, do(a.FinishBlockUpload, jobID, "", nil).StatusCode)

	blockPath := path.Join(blocksPathPrefix, jobID, testBlockID)
	assert.Equal(t, []string{
		path.Join(blockPath, "chunks/000001"),
		path.Join(blockPath, "index"),
		path.Join(blockPath, block.MetaFilename),
	}, slices.Sorted(maps.Keys(bkt.Objects())))
}

func TestAPI_BlockUpload_RejectsInvalidMeta(t *testing.T) {
	a, bkt := newTestAPI()

	invalidMeta := testMeta()
	invalidMeta.Version = 0
	invalidJSON, err := json.Marshal(invalidMeta)
	require.NoError(t, err)

	resp := do(a.StartBlockUpload, uuid.New().String(), "", bytes.NewReader(invalidJSON))

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, bkt.Objects())
}

func newTestAPI() (*API, *objstore.InMemBucket) {
	bkt := objstore.NewInMemBucket()
	return NewAPI(validation.MockDefaultOverrides(), bkt, log.NewNopLogger(), nil), bkt
}

func testMeta() block.Meta {
	now := time.Now()
	return block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    ulid.MustParse(testBlockID),
			Version: block.TSDBVersion1,
			MinTime: now.UnixMilli() - 1000,
			MaxTime: now.UnixMilli(),
		},
		Thanos: block.ThanosMeta{
			Files: []block.File{
				{RelPath: block.MetaFilename},
				{RelPath: "index", SizeBytes: 5},
				{RelPath: "chunks/000001", SizeBytes: 7},
			},
		},
	}
}

func do(h http.HandlerFunc, jobID, query string, body io.Reader) *http.Response {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/backfill/"+jobID+query, body)
	r = r.WithContext(user.InjectOrgID(r.Context(), testTenantID))
	r = mux.SetURLVars(r, map[string]string{"block": testBlockID, jobPathVar: jobID})

	w := httptest.NewRecorder()
	h(w, r)
	return w.Result()
}

func startJob(t *testing.T, a *API) string {
	t.Helper()

	resp := do(a.Start, "", "", nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res startResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
	_, err := uuid.Parse(res.JobID)
	require.NoError(t, err, "job ID must be a UUID, got %q", res.JobID)
	return res.JobID
}
