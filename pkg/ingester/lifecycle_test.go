// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/lifecycle_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/validation"
)

const userID = "1"

func defaultIngesterTestConfig(t testing.TB) Config {
	t.Helper()

	consul, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	flagext.DefaultValues(&cfg.BlocksStorageConfig)
	flagext.DefaultValues(&cfg.IngestStorageConfig)
	cfg.IngesterRing.KVStore.Mock = consul
	cfg.IngesterRing.NumTokens = 1
	cfg.IngesterRing.ListenPort = 0
	cfg.IngesterRing.InstanceAddr = "localhost"
	cfg.IngesterRing.InstanceID = "localhost"
	cfg.IngesterRing.FinalSleep = 0
	cfg.IngesterRing.MinReadyDuration = 100 * time.Millisecond
	cfg.IngesterRing.HeartbeatPeriod = defaultHeartbeatPeriod
	cfg.ActiveSeriesMetrics.Enabled = true
	return cfg
}

func defaultClientTestConfig() client.Config {
	clientConfig := client.Config{}
	flagext.DefaultValues(&clientConfig)
	return clientConfig
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

// TestIngesterRestart tests a restarting ingester doesn't keep adding more tokens.
func TestIngesterRestart(t *testing.T) {
	config := defaultIngesterTestConfig(t)
	limits := defaultLimitsTestConfig()
	config.IngesterRing.UnregisterOnShutdown = false

	{
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, config, limits, nil, "", nil)
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		time.Sleep(100 * time.Millisecond)
		// Doesn't actually unregister due to UnregisterFromRing: false.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	}

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.IngesterRing.KVStore.Mock, "localhost", IngesterRingKey)
	})

	{
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, config, limits, nil, "", nil)
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		time.Sleep(100 * time.Millisecond)
		// Doesn't actually unregister due to UnregisterFromRing: false.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
	}

	time.Sleep(200 * time.Millisecond)

	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return numTokens(config.IngesterRing.KVStore.Mock, "localhost", IngesterRingKey)
	})
}

func TestIngester_ShutdownHandler(t *testing.T) {
	for _, unregister := range []bool{false, true} {
		t.Run(fmt.Sprintf("unregister=%t", unregister), func(t *testing.T) {
			config := defaultIngesterTestConfig(t)
			limits := defaultLimitsTestConfig()
			config.IngesterRing.UnregisterOnShutdown = unregister

			ing, err := prepareIngesterWithBlocksStorageAndLimits(t, config, limits, nil, "", nil)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

			// Make sure the ingester has been added to the ring.
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return numTokens(config.IngesterRing.KVStore.Mock, "localhost", IngesterRingKey)
			})

			recorder := httptest.NewRecorder()
			ing.ShutdownHandler(recorder, nil)
			require.Equal(t, http.StatusNoContent, recorder.Result().StatusCode)

			// Make sure the ingester has been removed from the ring even when UnregisterFromRing is false.
			test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
				return numTokens(config.IngesterRing.KVStore.Mock, "localhost", IngesterRingKey)
			})
		})
	}
}

// numTokens determines the number of tokens owned by the specified
// address
func numTokens(c kv.Client, name, ringKey string) int {
	ringDesc, err := c.Get(context.Background(), ringKey)
	// The ringDesc may be null if the lifecycler hasn't stored the ring
	// to the KVStore yet.
	if ringDesc == nil || err != nil {
		return 0
	}
	rd := ringDesc.(*ring.Desc)
	if ing := rd.GetIngester(name); ing != nil {
		return len(ing.Tokens)
	}
	return 0
}
