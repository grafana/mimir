// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/lifecycle_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"cmp"
	"context"
	"slices"
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

func TestRulerShutdown_ShouldLeaveRingBeforeStoppingRuleEvaluation(t *testing.T) {
	for name, handoverPeriod := range map[string]time.Duration{
		"without handover period": 0,
		"with handover period":    500 * time.Millisecond,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			cfg := defaultRulerConfig(t)
			cfg.ShutdownHandoverPeriod = handoverPeriod
			r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

			kvStore := cfg.Ring.Common.KVStore.Mock

			var (
				managerStoppedAt    time.Time
				tokensAtManagerStop int
			)
			r.manager = &onStopManager{
				MultiTenantManager: r.manager,
				onStop: func() {
					managerStoppedAt = time.Now()
					tokensAtManagerStop = numTokens(kvStore, "localhost", RulerRingKey)
				},
			}

			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

			// Wait until the tokens are registered in the ring.
			test.Poll(t, 100*time.Millisecond, cfg.Ring.NumTokens, func() interface{} {
				return numTokens(kvStore, "localhost", RulerRingKey)
			})

			stopStartedAt := time.Now()
			require.NoError(t, services.StopAndAwaitTerminated(ctx, r))

			require.Equal(t, 0, tokensAtManagerStop, "the ruler should leave the ring before it stops evaluating rule groups")
			require.GreaterOrEqual(t, managerStoppedAt.Sub(stopStartedAt), handoverPeriod, "the ruler should keep evaluating rule groups for the handover period after leaving the ring")
		})
	}
}

// onStopManager is a MultiTenantManager that calls onStop before stopping the wrapped manager.
type onStopManager struct {
	MultiTenantManager
	onStop func()
}

func (m *onStopManager) Stop() {
	m.onStop()
	m.MultiTenantManager.Stop()
}

func TestRuler_RingLifecyclerShouldAutoForgetUnhealthyInstances(t *testing.T) {
	const unhealthyInstanceID = "unhealthy-id"
	const heartbeatTimeout = time.Minute

	ctx := context.Background()
	cfg := defaultRulerConfig(t)
	cfg.Ring.Common.HeartbeatPeriod = 100 * time.Millisecond
	cfg.Ring.Common.HeartbeatTimeout = heartbeatTimeout
	cfg.Ring.AutoForgetUnhealthyPeriods = 10

	r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

	ringClient := cfg.Ring.Common.KVStore.Mock

	// Add an unhealthy instance to the ring.
	require.NoError(t, ringClient.CAS(ctx, RulerRingKey, func(in interface{}) (interface{}, bool, error) {
		ringDesc := ring.GetOrCreateRingDesc(in)

		instance := ringDesc.AddIngester(unhealthyInstanceID, "1.1.1.1", "", generateSortedTokens(cfg.Ring.NumTokens), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		instance.Timestamp = time.Now().Add(-time.Duration(cfg.Ring.AutoForgetUnhealthyPeriods+1) * heartbeatTimeout).Unix()
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
	slices.SortFunc(tokens, func(a, b uint32) int {
		return cmp.Compare(a, b)
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
