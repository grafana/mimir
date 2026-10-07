// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
)

func TestPlannedJobsHandler(t *testing.T) {
	const user = "testuser"

	bucketClient, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bucketClient = block.BucketWithGlobalMarkers(bucketClient)

	cfg := prepareConfig(t)
	c, _, _, _, _ := prepare(t, cfg, bucketClient)
	c.bucketClient = bucketClient // this is normally done in starting, but we these tests don't require running compactor. (TODO: check if we can get rid of bucketClientFactory)

	twoHoursMS := 2 * time.Hour.Milliseconds()
	dayMS := 24 * time.Hour.Milliseconds()

	blockMarkedForNoCompact := ulid.MustNew(ulid.Now(), rand.Reader)

	index := bucketindex.Index{}
	index.Blocks = bucketindex.Blocks{
		// Some 2h blocks that should be compacted together and split.
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: 0, MaxTime: twoHoursMS},

		// Some merge jobs.
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "1_of_3"},
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "1_of_3"},

		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "2_of_3"},
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "2_of_3"},

		// This merge job is skipped, as block is marked for no-compaction.
		&bucketindex.Block{ID: ulid.MustNew(ulid.Now(), rand.Reader), MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "3_of_3"},
		&bucketindex.Block{ID: blockMarkedForNoCompact, MinTime: dayMS, MaxTime: 2 * dayMS, CompactorShardID: "3_of_3"},
	}

	require.NoError(t, bucketindex.WriteIndex(context.Background(), bucketClient, user, nil, &index))

	userBucket := bucket.NewUserBucketClient(user, bucketClient, nil)
	// Mark block for no-compaction.
	require.NoError(t, block.MarkForNoCompact(context.Background(), log.NewNopLogger(), userBucket, blockMarkedForNoCompact, block.CriticalNoCompactReason, "testing", promauto.With(nil).NewCounter(prometheus.CounterOpts{})))

	headersWithJSONAccept := http.Header{}
	headersWithJSONAccept.Set("Accept", "application/json")

	t.Run("tenants handler html", func(t *testing.T) {
		resp := httptest.NewRecorder()
		c.TenantsHandler(resp, &http.Request{})

		require.Equal(t, http.StatusOK, resp.Code)
		require.Contains(t, resp.Body.String(), "/"+user+"/planned_jobs")
	})

	t.Run("tenants handler json", func(t *testing.T) {
		resp := httptest.NewRecorder()
		c.TenantsHandler(resp, &http.Request{Header: headersWithJSONAccept})

		require.Equal(t, http.StatusOK, resp.Code)
		require.Contains(t, resp.Body.String(), `"tenants":["testuser"]`)
	})

	t.Run("compaction jobs html", func(t *testing.T) {
		resp := httptest.NewRecorder()

		req := mux.SetURLVars(&http.Request{}, map[string]string{
			"tenant":       user,
			"split_count":  "0",
			"merge_shards": "3",
		})
		c.PlannedJobsHandler(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)

		require.Contains(t, resp.Body.String(), "<td>0@17241709254077376921-merge--0-7200000</td>")
		require.Contains(t, resp.Body.String(), "<td>0@17241709254077376921-merge-1_of_3-86400000-172800000</td>")
		require.Contains(t, resp.Body.String(), "<td>0@17241709254077376921-merge-2_of_3-86400000-172800000</td>")
	})

	t.Run("show compactors option is offered in standalone mode", func(t *testing.T) {
		resp := httptest.NewRecorder()

		req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/", nil), map[string]string{"tenant": user})
		c.PlannedJobsHandler(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)
		require.Contains(t, resp.Body.String(), `id="show-compactors"`)
	})

	t.Run("show compactors option is not offered in scheduler mode", func(t *testing.T) {
		schedulerCfg := prepareConfig(t)
		schedulerCfg.SchedulerClientConfig.Enabled = true
		schedulerCompactor, _, _, _, _ := prepare(t, schedulerCfg, bucketClient)
		schedulerCompactor.bucketClient = bucketClient

		resp := httptest.NewRecorder()

		// Requesting the column explicitly must not bring it back: the owning compactor comes from the
		// ring, which doesn't decide who runs a job in scheduler mode.
		req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/?show_compactors=on", nil), map[string]string{"tenant": user})
		schedulerCompactor.PlannedJobsHandler(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)
		require.NotContains(t, resp.Body.String(), `id="show-compactors"`)
		require.NotContains(t, resp.Body.String(), "Compactor that owns this job")
	})

	t.Run("compaction jobs json", func(t *testing.T) {
		resp := httptest.NewRecorder()

		req := mux.SetURLVars(&http.Request{Header: headersWithJSONAccept}, map[string]string{
			"tenant":       user,
			"split_count":  "0",
			"merge_shards": "3",
		})
		c.PlannedJobsHandler(resp, req)

		require.Equal(t, http.StatusOK, resp.Code)

		require.Contains(t, resp.Body.String(), `"key":"0@17241709254077376921-merge--0-7200000"`)
		require.Contains(t, resp.Body.String(), `"key":"0@17241709254077376921-merge-1_of_3-86400000-172800000"`)
		require.Contains(t, resp.Body.String(), `"key":"0@17241709254077376921-merge-2_of_3-86400000-172800000"`)
	})
}
