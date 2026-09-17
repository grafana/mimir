// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
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

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	testTenantID = "test"
	testBlockID  = "01G3FZ0JWJYJC0ZM6Y9778P6KD"
)

type handler func(*API, http.ResponseWriter, *http.Request)

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

// newTestAPI builds a backfill API on the default limits, where validation is enabled, to show
// that the backfill path never validates regardless of the tenant's limits.
func newTestAPI() (*API, *objstore.InMemBucket) {
	bkt := objstore.NewInMemBucket()
	return NewAPI(validation.MockDefaultOverrides(), bkt, log.NewNopLogger(), nil), bkt
}

func do(a *API, h handler, jobID string, body io.Reader) *http.Response {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/backfill/"+jobID, body)
	r = r.WithContext(user.InjectOrgID(r.Context(), testTenantID))

	vars := map[string]string{"block": testBlockID}
	if jobID != "" {
		vars[jobPathVar] = jobID
	}

	w := httptest.NewRecorder()
	h(a, w, mux.SetURLVars(r, vars))
	return w.Result()
}

func uploadFile(a *API, jobID string, f block.File) *http.Response {
	r := httptest.NewRequest(http.MethodPost, "/api/v1/backfill/"+jobID+"?path="+f.RelPath, bytes.NewReader(bytes.Repeat([]byte("x"), int(f.SizeBytes))))
	r = r.WithContext(user.InjectOrgID(r.Context(), testTenantID))
	r = mux.SetURLVars(r, map[string]string{"block": testBlockID, jobPathVar: jobID})

	w := httptest.NewRecorder()
	a.UploadBlockFile(w, r)
	return w.Result()
}

func requireJobID(t *testing.T, resp *http.Response) string {
	t.Helper()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var res startResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
	_, err := uuid.Parse(res.JobID)
	require.NoError(t, err, "job ID must be a UUID, got %q", res.JobID)
	return res.JobID
}

func TestAPI_Start(t *testing.T) {
	t.Run("returns a fresh job ID and touches no storage", func(t *testing.T) {
		a, bkt := newTestAPI()

		jobIDs := map[string]struct{}{}
		for range 3 {
			jobIDs[requireJobID(t, do(a, (*API).Start, "", nil))] = struct{}{}
		}

		assert.Len(t, jobIDs, 3)
		assert.Empty(t, bkt.Objects())
	})

}

func TestAPI_JobIDValidation(t *testing.T) {
	handlers := map[string]handler{
		"finish":       (*API).Finish,
		"cancel":       (*API).Cancel,
		"status":       (*API).Status,
		"block/start":  (*API).StartBlockUpload,
		"block/files":  (*API).UploadBlockFile,
		"block/finish": (*API).FinishBlockUpload,
	}

	for name, h := range handlers {
		for _, jobID := range []string{"not-a-uuid", ""} {
			t.Run(fmt.Sprintf("%s rejects job ID %q", name, jobID), func(t *testing.T) {
				a, bkt := newTestAPI()

				resp := do(a, h, jobID, nil)

				require.Equal(t, http.StatusBadRequest, resp.StatusCode)
				body, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				assert.Contains(t, string(body), errInvalidJobID.Error())
				assert.Empty(t, bkt.Objects())
			})
		}
	}
}

func TestAPI_OperationStubs(t *testing.T) {
	testCases := map[string]struct {
		handler        handler
		expectedStatus int
	}{
		"finish": {(*API).Finish, http.StatusOK},
		"cancel": {(*API).Cancel, http.StatusOK},
		"status": {(*API).Status, http.StatusNotImplemented},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			a, bkt := newTestAPI()

			resp := do(a, tc.handler, uuid.New().String(), nil)

			require.Equal(t, tc.expectedStatus, resp.StatusCode)
			assert.Empty(t, bkt.Objects())
		})
	}
}

func TestAPI_StartBlockUpload_RejectsInvalidMeta(t *testing.T) {
	a, bkt := newTestAPI()

	invalidMeta := testMeta()
	invalidMeta.Version = 0
	invalidJSON, err := json.Marshal(invalidMeta)
	require.NoError(t, err)

	resp := do(a, (*API).StartBlockUpload, uuid.New().String(), bytes.NewReader(invalidJSON))

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, bkt.Objects())
}

func TestAPI_RoundTrip(t *testing.T) {
	a, bkt := newTestAPI()

	meta := testMeta()
	metaJSON, err := json.Marshal(meta)
	require.NoError(t, err)

	jobID := requireJobID(t, do(a, (*API).Start, "", nil))

	require.Equal(t, http.StatusOK, do(a, (*API).StartBlockUpload, jobID, bytes.NewReader(metaJSON)).StatusCode)

	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}

		require.Equal(t, http.StatusOK, uploadFile(a, jobID, f).StatusCode, "uploading %s", f.RelPath)
	}

	require.Equal(t, http.StatusOK, do(a, (*API).FinishBlockUpload, jobID, nil).StatusCode)
	require.Equal(t, http.StatusOK, do(a, (*API).Finish, jobID, nil).StatusCode)

	blockPath := path.Join(blocksPathPrefix, jobID, testBlockID)
	assert.Equal(t, []string{
		path.Join(blockPath, "chunks/000001"),
		path.Join(blockPath, "index"),
		path.Join(blockPath, block.MetaFilename),
	}, slices.Sorted(maps.Keys(bkt.Objects())))
}

func TestConfig_RegisterFlags(t *testing.T) {
	var cfg Config
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	cfg.RegisterFlags(fs)
	require.NoError(t, fs.Parse([]string{
		"-backfill.storage.backend=s3",
		"-backfill.storage.s3.bucket-name=backfill-blocks",
		"-backfill.storage.storage-prefix=backfill",
	}))

	assert.Equal(t, bucket.S3, cfg.Storage.Backend)
	assert.Equal(t, "backfill-blocks", cfg.Storage.BucketName())
	assert.Equal(t, "backfill", cfg.Storage.StoragePrefix)
}
