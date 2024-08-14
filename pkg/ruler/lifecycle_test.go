// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/lifecycle_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/require"
)

// TestRulerShutdown tests shutting down ruler unregisters correctly
func TestRulerShutdown(t *testing.T) {
	ctx := context.Background()

	config := defaultRulerConfig(t)
	r := prepareRuler(t, config, newMockRuleStore(mockRules))

	kvStore := config.Ring.Common.KVStore.Mock

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

	// Wait until the tokens are registered in the ring
	test.Poll(t, 100*time.Millisecond, config.Ring.NumTokens, func() interface{} {
		return numTokens(kvStore, "localhost", RulerRingKey)
	})

	require.Equal(t, ring.ACTIVE, r.lifecycler.GetState())

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))

	// Wait until the tokens are unregistered from the ring
	test.Poll(t, 100*time.Millisecond, 0, func() interface{} {
		return numTokens(kvStore, "localhost", RulerRingKey)
	})
}

func TestRuler_RingLifecyclerShouldAutoForgetUnhealthyInstances(t *testing.T) {
	const unhealthyInstanceID = "unhealthy-id"
	const heartbeatTimeout = time.Minute

	ctx := context.Background()
	cfg := defaultRulerConfig(t)
	cfg.Ring.Common.HeartbeatPeriod = 100 * time.Millisecond
	cfg.Ring.Common.HeartbeatTimeout = heartbeatTimeout

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

	ringClient := cfg.Ring.Common.KVStore.Mock

	// Add an unhealthy instance to the ring.
	require.NoError(t, ringClient.CAS(ctx, RulerRingKey, func(in interface{}) (interface{}, bool, error) {
		ringDesc := ring.GetOrCreateRingDesc(in)

		instance := ringDesc.AddIngester(unhealthyInstanceID, "1.1.1.1", "", generateSortedTokens(cfg.Ring.NumTokens), ring.ACTIVE, time.Now(), false, time.Time{})
		instance.Timestamp = time.Now().Add(-(ringAutoForgetUnhealthyPeriods + 1) * heartbeatTimeout).Unix()
		ringDesc.Ingesters[unhealthyInstanceID] = instance

		return ringDesc, true, nil
	}))

	// Ensure the unhealthy instance is removed from the ring.
	test.Poll(t, time.Second*5, false, func() interface{} {
		d, err := ringClient.Get(ctx, RulerRingKey)
		if err != nil {
			return err
		}

		_, ok := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
		return ok
	})
}

func generateSortedTokens(numTokens int) ring.Tokens {
	tokens := ring.NewRandomTokenGenerator().GenerateTokens(numTokens, nil)

	// Ensure generated tokens are sorted.
	sort.Slice(tokens, func(i, j int) bool {
		return tokens[i] < tokens[j]
	})

	return tokens
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
	return len(rd.Ingesters[name].Tokens)
}
