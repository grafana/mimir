// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestIngester_PrepareInstanceRingDownscaleHandler(t *testing.T) {
	util_test.VerifyNoLeak(t)

	const target = "/ingester/prepare-instance-ring-downscale"

	type response struct {
		Timestamp int64 `json:"timestamp"`
	}

	setup := func(t *testing.T, startIngester bool) (*Ingester, *ring.Ring) {
		cfg := defaultIngesterTestConfig(t)
		ingestersRing := createAndStartRing(t, cfg.IngesterRing.ToRingConfig())

		i, _, err := prepareIngesterWithBlocksStorage(t, cfg, ingestersRing, nil)
		require.NoError(t, err)
		if startIngester {
			startAndWaitHealthy(t, i, ingestersRing)
		}

		return i, ingestersRing
	}

	t.Run("POST request should switch the instance ring entry to read-only", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(t, true)

		// Pre-condition: entry is not read-only.
		test.Poll(t, 10*time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)

		resp := response{}
		require.NoError(t, json.Unmarshal(res.Body.Bytes(), &resp))
		require.InDelta(t, time.Now().Unix(), resp.Timestamp, 10)

		// Post-condition: entry is read only.
		test.Poll(t, 10*time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly && inst.ReadOnlyUpdatedTimestamp == resp.Timestamp
		})

		// Second call to POST will not update the entry.
		res2 := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res2, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)

		resp2 := response{}
		require.NoError(t, json.Unmarshal(res2.Body.Bytes(), &resp2))
		// Verify that timestamps hasn't changed
		require.Equal(t, resp.Timestamp, resp2.Timestamp)
	})

	t.Run("DELETE request should switch the instance ring entry to not read-only", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(t, true)
		res := httptest.NewRecorder()

		// Switch entry to read-only.
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)
		test.Poll(t, 10*time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})

		// Now switch back to read-write.
		res = httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, target, nil))
		require.Equal(t, http.StatusOK, res.Code)

		resp := response{}
		require.NoError(t, json.Unmarshal(res.Body.Bytes(), &resp))
		require.Equal(t, int64(0), resp.Timestamp)

		// Post-condition: entry is not read only.
		test.Poll(t, 10*time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})
	})

	t.Run("should return ServiceUnavailable when service is not running", func(t *testing.T) {
		t.Parallel()

		ingester, _ := setup(t, false)

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusServiceUnavailable, res.Code)

		// Following is not part of the test, but a workaround for hanging listener goroutines (started when ingester is created, even before starting)
		// By stopping services watched by ingester.subservicesWatcher, we make sure that all listeners (goroutines) attached to those services
		// are stopped.
		ingester.lifecycler.StopAsync()
		if ingester.ownedSeriesService != nil {
			ingester.ownedSeriesService.StopAsync()
		}
		ingester.compactionService.StopAsync()
		ingester.metricsUpdaterService.StopAsync()
		ingester.metadataPurgerService.StopAsync()
	})

	t.Run("should return MethodNotAllowed when ingest storage is enabled", func(t *testing.T) {
		t.Parallel()

		cfg := defaultIngesterTestConfig(t)
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, nil, nil, nil, util_test.NewTestingLogger(t))
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester))
		})

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusMethodNotAllowed, res.Code)
	})

	t.Run("should return Conflict when read-only and any compaction is in progress", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(t, true)
		res := httptest.NewRecorder()

		// Switch entry to read-only.
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)
		test.Poll(t, 10*time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})

		// Simulate a compaction in progress.
		ingester.numCompactionsInProgress.Inc()
		defer ingester.numCompactionsInProgress.Dec()

		// Try to switch back to read-write while compaction is in progress
		res = httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, target, nil))
		require.Equal(t, http.StatusConflict, res.Code)

		// Post-condition: entry should still be read-only
		test.Poll(t, 10*time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})
	})

	t.Run("should return OK when not read-only and any compaction is in progress", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(t, true)

		// Pre-condition: entry is not read-only.
		test.Poll(t, 10*time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})

		// Simulate a compaction in progress.
		ingester.numCompactionsInProgress.Inc()
		defer ingester.numCompactionsInProgress.Dec()

		// Call DELETE while compaction is in progress
		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodDelete, target, nil))
		require.Equal(t, http.StatusOK, res.Code)

		resp := response{}
		require.NoError(t, json.Unmarshal(res.Body.Bytes(), &resp))
		require.Equal(t, int64(0), resp.Timestamp)

		// Post-condition: entry is still not read only.
		test.Poll(t, 10*time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.ingesterID)
			require.NoError(t, err)
			return inst.ReadOnly
		})
	})
}
