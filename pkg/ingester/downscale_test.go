// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
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

	setup := func(t *testing.T, cfg Config) (*Ingester, *ring.Ring) {
		ingestersRing := createAndStartRing(t, cfg.IngesterRing.ToRingConfig())
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingestersRing))
		})

		i, err := prepareIngesterWithBlocksStorage(t, cfg, ingestersRing, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
		})

		// Wait until it's healthy
		test.Poll(t, 5*time.Second, 1, func() interface{} {
			return i.lifecycler.HealthyInstancesCount()
		})

		return i, ingestersRing
	}

	t.Run("POST request should switch the instance ring entry to read-only", func(t *testing.T) {
		t.Parallel()

		ingester, r := setup(t, defaultIngesterTestConfig(t))

		// Pre-condition: entry is not read-only.
		test.Poll(t, time.Second, false, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
			require.NoError(t, err)
			return inst.ReadOnly
		})

		res := httptest.NewRecorder()
		ingester.PrepareInstanceRingDownscaleHandler(res, httptest.NewRequest(http.MethodPost, target, nil))
		require.Equal(t, http.StatusOK, res.Code)

		// Pre-condition: entry is read only.
		test.Poll(t, time.Second, true, func() interface{} {
			inst, err := r.GetInstance(ingester.lifecycler.ID)
			require.NoError(t, err)
			return inst.ReadOnly
		})
	})
}
