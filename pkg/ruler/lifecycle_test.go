// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/lifecycle_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
		"with handover period":    time.Minute,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cfg := defaultRulerConfig(t)
				cfg.ShutdownHandoverPeriod = handoverPeriod
				gated := &gatedKV{Client: cfg.Ring.Common.KVStore.Mock, release: make(chan struct{})}
				cfg.Ring.Common.KVStore.Mock = gated
				r := prepareRuler(t, cfg, newMockRuleStore(mockRules))

				var (
					managerStoppedAt time.Time
					tokensAtStop     int
					rulesAtStop      int
				)
				managerStopped := make(chan struct{})
				r.manager = &onStopManager{
					MultiTenantManager: r.manager,
					onStop: func() {
						managerStoppedAt = time.Now()
						tokensAtStop = numTokens(gated.Client, "localhost", RulerRingKey)
						rulesAtStop = len(r.manager.GetRules("user1"))
						close(managerStopped)
					},
				}

				require.NoError(t, services.StartAndAwaitRunning(t.Context(), r))
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(gated.release) }) }
				defer release()

				gated.armed.Store(true)
				r.StopAsync()
				synctest.Wait()
				select {
				case <-managerStopped:
					release()
					require.NoError(t, r.AwaitTerminated(t.Context()))
					t.Fatal("the ruler stopped the rule managers before it left the ring")
				default:
				}

				// Keep the leave blocked for a while.
				// Then the handover period must start when the leave ends, not when the stop starts.
				// The leave ends when the CAS calls are released, because the fake clock does not move until all goroutines block.
				time.Sleep(10 * time.Second)
				leftAt := time.Now()
				release()
				require.NoError(t, r.AwaitTerminated(t.Context()))
				require.False(t, managerStoppedAt.IsZero(), "the rule managers were never stopped")
				require.Zero(t, tokensAtStop, "the ruler should leave the ring before it stops evaluating rule groups")
				require.Equal(t, handoverPeriod, managerStoppedAt.Sub(leftAt))
				require.NotZero(t, rulesAtStop, "the rule managers should still hold rule groups at the end of the handover")
			})
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

// gatedKV blocks CAS calls while armed, until release is closed.
type gatedKV struct {
	kv.Client
	armed   atomic.Bool
	release chan struct{}
}

func (g *gatedKV) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	if g.armed.Load() {
		<-g.release
	}
	return g.Client.CAS(ctx, key, f)
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
