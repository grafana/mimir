// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const testJobID = "11111111-1111-1111-1111-111111111111"

type backfillTestServer struct {
	t             *testing.T
	requests      []backfillTestRequest
	errorStatuses map[string][]int
}

type backfillTestRequest struct {
	path string
	file string
	body string
}

func newBackfillTestServer(t *testing.T, errorStatuses map[string][]int) (*backfillTestServer, *MimirClient) {
	s := &backfillTestServer{t: t, errorStatuses: errorStatuses}
	c, err := New(Config{Address: "http://mimir", ID: "test"}, log.NewNopLogger())
	require.NoError(t, err)
	c.Client.Transport = s
	return s, c
}

func (s *backfillTestServer) RoundTrip(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		require.NoError(s.t, err)
		require.NoError(s.t, r.Body.Close())
	}
	s.requests = append(s.requests, backfillTestRequest{path: r.URL.Path, file: r.URL.Query().Get("path"), body: string(body)})

	w := httptest.NewRecorder()
	if queued := s.errorStatuses[r.URL.Path]; len(queued) > 0 {
		s.errorStatuses[r.URL.Path] = queued[1:]
		http.Error(w, http.StatusText(queued[0]), queued[0])
	} else if r.URL.Path == "/api/v1/backfill/start" {
		_, _ = fmt.Fprintf(w, `{"job":%q}`, testJobID)
	}
	return w.Result(), nil
}

func (s *backfillTestServer) paths() []string {
	paths := make([]string, 0, len(s.requests))
	for _, r := range s.requests {
		paths = append(paths, r.path)
	}
	return paths
}

const (
	testIndexContent = "index-content"
	testChunkContent = "chunk-content"
)

func writeTestBlock(t *testing.T, dir string, blockID ulid.ULID) string {
	blockDir := filepath.Join(dir, blockID.String())
	require.NoError(t, os.MkdirAll(filepath.Join(blockDir, block.ChunksDirname), 0o755))

	meta := block.Meta{BlockMeta: tsdb.BlockMeta{ULID: blockID, Version: 1, MinTime: 100, MaxTime: 200}}
	require.NoError(t, meta.WriteToDir(log.NewNopLogger(), blockDir))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.IndexFilename), []byte(testIndexContent), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(blockDir, block.ChunksDirname, "000001"), []byte(testChunkContent), 0o644))
	return blockDir
}

func blockRequestPath(blockID ulid.ULID, step string) string {
	return "/api/v1/backfill/" + testJobID + "/block/" + blockID.String() + "/" + step
}

func blockRequestPaths(blockID ulid.ULID, steps ...string) []string {
	paths := make([]string, 0, len(steps))
	for _, step := range steps {
		paths = append(paths, blockRequestPath(blockID, step))
	}
	return paths
}

func TestStartAndFinishBackfillJob(t *testing.T) {
	s, c := newBackfillTestServer(t, nil)

	jobID, err := c.StartBackfillJob(t.Context())
	require.NoError(t, err)
	require.Equal(t, testJobID, jobID)
	require.NoError(t, c.FinishBackfillJob(t.Context(), jobID))
	assert.Equal(t, []string{"/api/v1/backfill/start", "/api/v1/backfill/" + testJobID + "/finish"}, s.paths())
}

func TestUploadBackfillBlocks(t *testing.T) {
	dir := t.TempDir()
	uploaded := ulid.MustNew(1000, nil)
	rejected := ulid.MustNew(2000, nil)
	existing := ulid.MustNew(3000, nil)
	uploadedDir := writeTestBlock(t, dir, uploaded)
	rejectedDir := writeTestBlock(t, dir, rejected)
	existingDir := writeTestBlock(t, dir, existing)

	s, c := newBackfillTestServer(t, map[string][]int{
		blockRequestPath(rejected, "start"): {http.StatusUnprocessableEntity},
		blockRequestPath(existing, "start"): {http.StatusConflict},
	})

	err := c.UploadBackfillBlocks(t.Context(), testJobID, []string{uploadedDir, rejectedDir, existingDir})
	require.EqualError(t, err, fmt.Sprintf("failed to upload 1 block(s) to backfill job %s: %s", testJobID, rejectedDir))

	var expected []string
	expected = append(expected, blockRequestPaths(uploaded, "start", "files", "files", "finish")...)
	expected = append(expected, blockRequestPaths(rejected, "start")...)
	expected = append(expected, blockRequestPaths(existing, "start")...)
	assert.Equal(t, expected, s.paths())
}

func TestUploadBackfillBlocks_Retries(t *testing.T) {
	blockID := ulid.MustNew(1000, nil)
	filesPath := blockRequestPath(blockID, "files")
	finishPath := blockRequestPath(blockID, "finish")

	for name, tc := range map[string]struct {
		errorStatuses map[string][]int
		expectedPaths []string
		expectFailure bool
	}{
		"retries a file upload and sends the whole file again": {
			errorStatuses: map[string][]int{filesPath: {http.StatusServiceUnavailable}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "files", "finish"),
		},
		"retries 429 and 5xx, and treats a conflict on a retried finish as success": {
			errorStatuses: map[string][]int{finishPath: {http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusConflict}},
			expectedPaths: blockRequestPaths(blockID, "start", "files", "files", "finish", "finish", "finish"),
		},
		"does not retry other 4xx": {
			errorStatuses: map[string][]int{filesPath: {http.StatusBadRequest}},
			expectedPaths: blockRequestPaths(blockID, "start", "files"),
			expectFailure: true,
		},
		"gives up after the maximum number of attempts": {
			errorStatuses: map[string][]int{finishPath: slices.Repeat([]int{http.StatusBadGateway}, 10)},
			expectedPaths: blockRequestPaths(blockID, append([]string{"start", "files", "files"}, slices.Repeat([]string{"finish"}, 10)...)...),
			expectFailure: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				blockDir := writeTestBlock(t, t.TempDir(), blockID)
				s, c := newBackfillTestServer(t, tc.errorStatuses)

				err := c.UploadBackfillBlocks(t.Context(), testJobID, []string{blockDir})
				if tc.expectFailure {
					require.EqualError(t, err, fmt.Sprintf("failed to upload 1 block(s) to backfill job %s: %s", testJobID, blockDir))
				} else {
					require.NoError(t, err)
				}
				assert.Equal(t, tc.expectedPaths, s.paths())

				expectedBodies := map[string]string{block.IndexFilename: testIndexContent, "chunks/000001": testChunkContent}
				for _, r := range s.requests {
					if r.path == filesPath {
						assert.Equal(t, expectedBodies[r.file], r.body, "body of %s", r.file)
					}
				}
			})
		})
	}
}
