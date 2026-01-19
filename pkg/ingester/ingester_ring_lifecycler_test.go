// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestLifecycler_FlushOnShutdown(t *testing.T) {
	tests := map[string]struct {
		initialFlushOnShutdown bool
		callSetFlushOnShutdown bool
		expectFlushCalled      bool
	}{
		"should not call flush if not initialized with flush on shutdown": {
			initialFlushOnShutdown: false,
			callSetFlushOnShutdown: false,
			expectFlushCalled:      false,
		},
		"should call flush if initialized with flush on shutdown": {
			initialFlushOnShutdown: true,
			callSetFlushOnShutdown: false,
			expectFlushCalled:      true,
		},
		"should call flush if SetFlushOnShutdown(true) is called": {
			initialFlushOnShutdown: false,
			callSetFlushOnShutdown: true,
			expectFlushCalled:      true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for _, implName := range []string{"classic Lifecycler", "tokenlessLifecycler"} {
				t.Run(implName, func(t *testing.T) {
					cfg := defaultIngesterTestConfig(t)
					kvClient := cfg.IngesterRing.KVStore.Mock
					testRing := createTestRing(t, cfg.IngesterRing)
					flusher := newTestFlushTransferer()

					var lifecycler ingesterLifecycler
					switch implName {
					case "classic Lifecycler":
						lifecycler = createClassicLifecycler(t, cfg.IngesterRing, tc.initialFlushOnShutdown, flusher, nil)
					case "tokenlessLifecycler":
						cfg.IngesterRing.NumTokens = 0
						lifecycler = createTokenlessLifecycler(t, cfg.IngesterRing, kvClient, tc.initialFlushOnShutdown, flusher.Flush, nil)
					}
					flusher.lifecycler = lifecycler

					ctx := context.Background()

					// Start and wait for ACTIVE.
					require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
					require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

					// Pre-condition: instance is ACTIVE.
					require.Equal(t, ring.ACTIVE, lifecycler.GetState())

					// Verify token count matches expected for this lifecycler type.
					tokenCount, exists := getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
					require.True(t, exists)
					require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

					// Optionally call SetFlushOnShutdown.
					if tc.callSetFlushOnShutdown {
						lifecycler.SetFlushOnShutdown(true)
					}

					// Stop lifecycler.
					require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

					// Verify flush behavior.
					require.Equal(t, tc.expectFlushCalled, flusher.flushCalled.Load())
					if tc.expectFlushCalled {
						require.Equal(t, ring.LEAVING, flusher.stateOnFlush.Load().(ring.InstanceState))
					}
				})
			}
		})
	}
}

func TestLifecycler_ReadOnlyState(t *testing.T) {
	tests := map[string]struct {
		setReadOnly         bool
		expectReadOnly      bool
		expectedMetricValue float64
	}{
		"should start with read-only disabled": {
			setReadOnly:         false,
			expectReadOnly:      false,
			expectedMetricValue: 0,
		},
		"should enable read-only state": {
			setReadOnly:         true,
			expectReadOnly:      true,
			expectedMetricValue: 1,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for _, implName := range []string{"classic Lifecycler", "tokenlessLifecycler"} {
				t.Run(implName, func(t *testing.T) {
					cfg := defaultIngesterTestConfig(t)
					kvClient := cfg.IngesterRing.KVStore.Mock
					testRing := createTestRing(t, cfg.IngesterRing)
					reg := prometheus.NewRegistry()

					var lifecycler ingesterLifecycler
					switch implName {
					case "classic Lifecycler":
						lifecycler = createClassicLifecycler(t, cfg.IngesterRing, false, &noopFlushTransferer{}, reg)
					case "tokenlessLifecycler":
						cfg.IngesterRing.NumTokens = 0
						lifecycler = createTokenlessLifecycler(t, cfg.IngesterRing, kvClient, false, nil, reg)
					}

					ctx := context.Background()

					// Start and wait for ACTIVE.
					require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
					require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

					// Verify token count matches expected for this lifecycler type.
					tokenCount, exists := getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
					require.True(t, exists)
					require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

					// Initial state: not read-only.
					readOnly, _ := lifecycler.GetReadOnlyState()
					require.False(t, readOnly)

					// Change read-only state if requested.
					if tc.setReadOnly {
						require.NoError(t, lifecycler.ChangeReadOnlyState(ctx, true))
					}

					// Verify read-only state.
					readOnly, ts := lifecycler.GetReadOnlyState()
					require.Equal(t, tc.expectReadOnly, readOnly)
					if tc.expectReadOnly {
						require.False(t, ts.IsZero(), "read-only timestamp should be set")
					}

					// Verify the lifecycler_read_only metric.
					require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
						# HELP lifecycler_read_only Set to 1 if this lifecycler's instance entry is in read-only state.
						# TYPE lifecycler_read_only gauge
						lifecycler_read_only{name="ingester"} %v
					`, tc.expectedMetricValue)), "lifecycler_read_only"))

					// Stop lifecycler.
					require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
				})
			}
		})
	}
}

func TestLifecycler_UnregisterOnShutdown(t *testing.T) {
	tests := map[string]struct {
		callSetUnregisterOnShutdown *bool // nil = don't call, true/false = call with value
		expectUnregisterOnShutdown  bool
	}{
		"should default to unregister on shutdown": {
			callSetUnregisterOnShutdown: nil,
			expectUnregisterOnShutdown:  true, // Default is true (unregister on shutdown)
		},
		"should unregister when SetUnregisterOnShutdown(true) called": {
			callSetUnregisterOnShutdown: boolPtr(true),
			expectUnregisterOnShutdown:  true,
		},
		"should not unregister when SetUnregisterOnShutdown(false) called": {
			callSetUnregisterOnShutdown: boolPtr(false),
			expectUnregisterOnShutdown:  false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			for _, implName := range []string{"classic Lifecycler", "tokenlessLifecycler"} {
				t.Run(implName, func(t *testing.T) {
					cfg := defaultIngesterTestConfig(t)
					kvClient := cfg.IngesterRing.KVStore.Mock
					testRing := createTestRing(t, cfg.IngesterRing)

					var lifecycler ingesterLifecycler
					switch implName {
					case "classic Lifecycler":
						lifecycler = createClassicLifecycler(t, cfg.IngesterRing, false, &noopFlushTransferer{}, nil)
					case "tokenlessLifecycler":
						cfg.IngesterRing.NumTokens = 0
						lifecycler = createTokenlessLifecycler(t, cfg.IngesterRing, kvClient, false, nil, nil)
					}

					ctx := context.Background()

					// Start and wait for ACTIVE.
					require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
					require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

					// Pre-condition: instance is ACTIVE.
					require.Equal(t, ring.ACTIVE, lifecycler.GetState())

					// Verify token count matches expected for this lifecycler type.
					tokenCount, exists := getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
					require.True(t, exists)
					require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

					// Optionally call SetUnregisterOnShutdown.
					if tc.callSetUnregisterOnShutdown != nil {
						lifecycler.SetUnregisterOnShutdown(*tc.callSetUnregisterOnShutdown)
					}

					// Verify getter returns expected value.
					require.Equal(t, tc.expectUnregisterOnShutdown, lifecycler.ShouldUnregisterOnShutdown())

					// Stop lifecycler.
					require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))

					// Verify instance presence in ring after shutdown.
					state, exists := getInstanceStateInRing(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
					if tc.expectUnregisterOnShutdown {
						require.False(t, exists, "instance should be removed from ring after shutdown")
					} else {
						// Instance should still be in the ring, but in LEAVING state.
						require.True(t, exists, "instance should exist in ring")
						require.Equal(t, ring.LEAVING, state, "instance should be in LEAVING state after shutdown")
					}
				})
			}
		})
	}
}

func TestLifecycler_CheckReady(t *testing.T) {
	const minReadyDuration = 1 * time.Second

	for _, implName := range []string{"classic Lifecycler", "tokenlessLifecycler"} {
		t.Run(implName, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.MinReadyDuration = minReadyDuration
			kvClient := cfg.IngesterRing.KVStore.Mock
			testRing := createTestRing(t, cfg.IngesterRing)

			var lifecycler ingesterLifecycler
			switch implName {
			case "classic Lifecycler":
				lifecycler = createClassicLifecycler(t, cfg.IngesterRing, false, &noopFlushTransferer{}, nil)
			case "tokenlessLifecycler":
				cfg.IngesterRing.NumTokens = 0
				lifecycler = createTokenlessLifecycler(t, cfg.IngesterRing, kvClient, false, nil, nil)
			}

			ctx := context.Background()

			// Before starting: CheckReady should return an error.
			require.Error(t, lifecycler.CheckReady(ctx))

			// Start the lifecycler.
			require.NoError(t, services.StartAndAwaitRunning(ctx, lifecycler))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, lifecycler))
			})

			// Wait for instance to be ACTIVE in the ring.
			require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

			// Verify token count matches expected for this lifecycler type.
			tokenCount, exists := getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
			require.True(t, exists)
			require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

			// Immediately after ACTIVE: CheckReady should return error because
			// MinReadyDuration hasn't elapsed yet.
			require.ErrorContains(t, lifecycler.CheckReady(ctx), "waiting for")

			// Wait for MinReadyDuration to fully elapse.
			time.Sleep(minReadyDuration)

			// After MinReadyDuration: CheckReady should return nil.
			require.NoError(t, lifecycler.CheckReady(ctx))

			// Change instance to read-only. The ready state should be latched,
			// so CheckReady should still return nil.
			require.NoError(t, lifecycler.ChangeReadOnlyState(ctx, true))

			// CheckReady should still return nil (latched ready state).
			require.NoError(t, lifecycler.CheckReady(ctx))
		})
	}
}

func TestTokenlessLifecycler_RemovesPreExistingTokens(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	kvClient := cfg.IngesterRing.KVStore.Mock
	testRing := createTestRing(t, cfg.IngesterRing)
	ctx := context.Background()

	// Step 1: Pre-register the instance with tokens using a classic lifecycler.
	classicLifecycler := createClassicLifecycler(t, cfg.IngesterRing, false, &noopFlushTransferer{}, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, classicLifecycler))
	require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

	// Verify instance has tokens.
	tokenCount, exists := getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
	require.True(t, exists)
	require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

	// Stop the classic lifecycler but don't unregister, keeping the instance in the ring with tokens.
	classicLifecycler.SetUnregisterOnShutdown(false)
	require.NoError(t, services.StopAndAwaitTerminated(ctx, classicLifecycler))

	// Verify instance still has tokens after classic lifecycler stopped.
	tokenCount, exists = getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
	require.True(t, exists)
	require.Equal(t, cfg.IngesterRing.NumTokens, tokenCount)

	// Step 2: Start tokenless lifecycler for the same instance.
	cfg.IngesterRing.NumTokens = 0
	tokenlessLifecycler := createTokenlessLifecycler(t, cfg.IngesterRing, kvClient, false, nil, nil)
	require.NoError(t, services.StartAndAwaitRunning(ctx, tokenlessLifecycler))
	require.NoError(t, ring.WaitInstanceState(ctx, testRing, cfg.IngesterRing.InstanceID, ring.ACTIVE))

	// Step 3: Verify tokens have been removed.
	tokenCount, exists = getInstanceTokenCount(t, kvClient, IngesterRingKey, cfg.IngesterRing.InstanceID)
	require.True(t, exists)
	require.Equal(t, 0, tokenCount)

	// Cleanup.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, tokenlessLifecycler))
}

func boolPtr(b bool) *bool {
	return &b
}

// noopFlushTransferer implements ring.FlushTransferer with no-op methods.
type noopFlushTransferer struct{}

func (f *noopFlushTransferer) Flush()                              {}
func (f *noopFlushTransferer) TransferOut(_ context.Context) error { return ring.ErrTransferDisabled }

func getInstanceStateInRing(t *testing.T, kvClient kv.Client, ringKey, instanceID string) (ring.InstanceState, bool) {
	ctx := context.Background()

	val, err := kvClient.Get(ctx, ringKey)
	require.NoError(t, err)
	if val == nil {
		return ring.PENDING, false
	}

	ringDesc, ok := val.(*ring.Desc)
	require.True(t, ok, "expected ring.Desc, got %T", val)

	instance, exists := ringDesc.Ingesters[instanceID]
	if !exists {
		return ring.PENDING, false
	}
	return instance.State, true
}

func getInstanceTokenCount(t *testing.T, kvClient kv.Client, ringKey, instanceID string) (int, bool) {
	ctx := context.Background()

	val, err := kvClient.Get(ctx, ringKey)
	require.NoError(t, err)
	if val == nil {
		return 0, false
	}

	ringDesc, ok := val.(*ring.Desc)
	require.True(t, ok, "expected ring.Desc, got %T", val)

	instance, exists := ringDesc.Ingesters[instanceID]
	if !exists {
		return 0, false
	}
	return len(instance.Tokens), true
}

func createTestRing(t *testing.T, cfg RingConfig) *ring.Ring {
	r, err := ring.New(cfg.ToRingConfig(), IngesterRingName, IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), r))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), r))
	})
	return r
}

// testFlushTransferer implements ring.FlushTransferer for testing lifecyclers.
type testFlushTransferer struct {
	flushCalled  *atomic.Bool
	stateOnFlush *atomic.Value
	lifecycler   ingesterLifecycler
}

func newTestFlushTransferer() *testFlushTransferer {
	return &testFlushTransferer{
		flushCalled:  &atomic.Bool{},
		stateOnFlush: &atomic.Value{},
	}
}

func (f *testFlushTransferer) Flush() {
	f.flushCalled.Store(true)
	if f.lifecycler != nil {
		f.stateOnFlush.Store(f.lifecycler.GetState())
	}
}

func (f *testFlushTransferer) TransferOut(_ context.Context) error {
	return ring.ErrTransferDisabled
}

func createClassicLifecycler(
	t *testing.T,
	cfg RingConfig,
	flushOnShutdown bool,
	flusher ring.FlushTransferer,
	reg prometheus.Registerer,
) ingesterLifecycler {
	lifecycler, err := ring.NewLifecycler(
		cfg.ToLifecyclerConfig(),
		flusher,
		IngesterRingName,
		IngesterRingKey,
		flushOnShutdown,
		log.NewNopLogger(),
		reg,
	)
	require.NoError(t, err)

	return lifecycler
}

func createTokenlessLifecycler(
	t *testing.T,
	cfg RingConfig,
	kvClient kv.Client,
	flushOnShutdown bool,
	flush func(),
	reg prometheus.Registerer,
) ingesterLifecycler {
	basicCfg, err := cfg.ToTokenlessBasicLifecyclerConfig(log.NewNopLogger())
	require.NoError(t, err)

	lc, err := newTokenlessLifecycler(
		basicCfg,
		IngesterRingName,
		IngesterRingKey,
		kvClient,
		cfg.MinReadyDuration,
		cfg.FinalSleep,
		flushOnShutdown,
		flush,
		log.NewNopLogger(),
		reg,
	)
	require.NoError(t, err)

	return lc
}
