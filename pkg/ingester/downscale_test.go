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
)

func TestIngester_PrepareInstanceRingDownscaleHandler(t *testing.T) {
	const target = "/ingester/prepare-instance-ring-downscale"

	type response struct {
		Timestamp int64 `json:"timestamp"`
	}

	setup := func(startIngester bool) (*Ingester, *ring.Ring) {
		cfg := defaultIngesterTestConfig(t)
		ingestersRing := createAndStartRing(t, cfg.IngesterRing.ToRingConfig())

		i, err := prepareIngesterWithBlocksStorage(t, cfg, ingestersRing, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
		})
		if startIngester {
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

			// Wait until it's healthy
			test.Poll(t, 5*time.Second, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})
		}

		return i, ingestersRing
	}

	t.Run("POST request should switch the instance ring entry to read-only", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(true)

		// Pre-condition: entry is not read-only.
		test.Poll(t, time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
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
		test.Poll(t, time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
			require.NoError(t, err)
			return inst.ReadOnly && inst.ReadOnlyUpdatedTimestamp == resp.Timestamp
		})
	})

	t.Run("DELETE request should switch the instance ring entry to not read-only", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(true)
		res := httptest.NewRecorder()

		// Switch entry to read-only.
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)
		test.Poll(t, time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
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
		test.Poll(t, time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
			require.NoError(t, err)
			return inst.ReadOnly
		})
	})

	t.Run("should return ServiceUnavailable when service is not running", func(t *testing.T) {
		t.Parallel()

		ingester, _ := setup(false)

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusServiceUnavailable, res.Code)
	})

	t.Run("should return MethodNotAllowed when ingest storage is enabled", func(t *testing.T) {
		t.Parallel()

		cfg := defaultIngesterTestConfig(t)
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, nil, nil)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester))
		})

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusMethodNotAllowed, res.Code)
	})
}
