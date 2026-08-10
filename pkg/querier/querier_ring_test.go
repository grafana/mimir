// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type mockInstance struct {
	state     ring.InstanceState
	versions  ring.InstanceVersions
	unhealthy bool
}

func TestRingQueryPlanVersionProvider(t *testing.T) {
	versionsMap := func(v uint64) map[uint64]uint64 {
		return map[uint64]uint64{MaximumSupportedQueryPlanVersion: v}
	}

	testCases := map[string]struct {
		instances       []mockInstance
		expectedVersion planning.QueryPlanVersion
		expectedError   string
	}{
		"no instances in the ring": {
			expectedError: "could not compute maximum supported query plan version: could not get all queriers from the ring: empty ring",
		},
		"one instance in the ring, has version but is unhealthy": {
			instances: []mockInstance{
				{
					state:     ring.ACTIVE,
					versions:  versionsMap(123),
					unhealthy: true,
				},
			},
			expectedError: "could not compute maximum supported query plan version: no healthy queriers in the ring",
		},
		"many instances in the ring, all have versions but all are unhealthy": {
			instances: []mockInstance{
				{
					state:     ring.ACTIVE,
					versions:  versionsMap(123),
					unhealthy: true,
				},
				{
					state:     ring.LEAVING,
					versions:  versionsMap(124),
					unhealthy: true,
				},
			},
			expectedError: "could not compute maximum supported query plan version: no healthy queriers in the ring",
		},
		"many instances in the ring, only the unhealthy ones have a lower version": {
			instances: []mockInstance{
				{
					state:     ring.ACTIVE,
					versions:  versionsMap(122),
					unhealthy: true,
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"one instance in the ring, has no version": {
			instances: []mockInstance{
				{
					state: ring.ACTIVE,
				},
			},
			expectedError: "could not compute maximum supported query plan version: at least one querier in the ring is not reporting a supported query plan version",
		},
		"one instance in the ring, has version and is active": {
			instances: []mockInstance{
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"one instance in the ring, has version and is joining": {
			instances: []mockInstance{
				{
					state:    ring.JOINING,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"one instance in the ring, has version and is leaving": {
			instances: []mockInstance{
				{
					state:    ring.LEAVING,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"one instance in the ring, has version and is pending": {
			instances: []mockInstance{
				{
					state:    ring.PENDING,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"many instances in the ring, all have no version": {
			instances: []mockInstance{
				{
					state: ring.ACTIVE,
				},
				{
					state: ring.ACTIVE,
				},
				{
					state: ring.ACTIVE,
				},
			},
			expectedError: "could not compute maximum supported query plan version: at least one querier in the ring is not reporting a supported query plan version",
		},
		"many instances in the ring, some have no version": {
			instances: []mockInstance{
				{
					state: ring.ACTIVE,
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
				{
					state: ring.ACTIVE,
				},
			},
			expectedError: "could not compute maximum supported query plan version: at least one querier in the ring is not reporting a supported query plan version",
		},
		"many instances in the ring, all have the same version": {
			instances: []mockInstance{
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
			},
			expectedVersion: 123,
		},
		"many instances in the ring, each have different versions": {
			instances: []mockInstance{
				{
					state:    ring.ACTIVE,
					versions: versionsMap(122),
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(123),
				},
				{
					state:    ring.ACTIVE,
					versions: versionsMap(124),
				},
			},
			expectedVersion: 122,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			desc := ring.NewDesc()

			// Shuffle the order of the instances to make sure we're not depending on the order.
			// If this test is flaky, this is likely why and it indicates a bug in our logic somewhere.
			rand.Shuffle(len(testCase.instances), func(i, j int) {
				testCase.instances[i], testCase.instances[j] = testCase.instances[j], testCase.instances[i]
			})

			for idx, instance := range testCase.instances {
				id := fmt.Sprintf("querier-%d", idx)
				added := desc.AddIngester(id, fmt.Sprintf("127.0.0.%d", idx), "", []uint32{uint32(idx)}, instance.state, time.Now(), false, time.Time{}, instance.versions)

				if instance.unhealthy {
					// AddIngester always sets a fresh heartbeat, so backdate it beyond the ring's heartbeat timeout.
					added.Timestamp = time.Now().Add(-time.Hour).Unix()
					desc.Ingesters[id] = added
				}
			}

			cfg := ring.Config{
				ReplicationFactor: 1,
				HeartbeatTimeout:  time.Minute,
			}
			store := &mockStore{desc: desc}
			reg := prometheus.NewPedanticRegistry()
			logger := log.NewNopLogger()
			r, err := ring.NewWithStoreClientAndStrategy(cfg, "querier-test", "queriers", store, ring.NewDefaultReplicationStrategy(), reg, logger)
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, r) })

			versionProvider := NewRingQueryPlanVersionProvider(r, reg, log.NewNopLogger())
			version, err := versionProvider.GetMaximumSupportedQueryPlanVersion(ctx)
			if testCase.expectedError != "" {
				require.EqualError(t, err, testCase.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedVersion, version)
			}

			expectedMetricValue := float64(testCase.expectedVersion)
			if testCase.expectedError != "" {
				expectedMetricValue = -1
			}

			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_query_frontend_querier_ring_calculated_maximum_supported_query_plan_version The maximum supported query plan version calculated from the querier ring.
				# TYPE cortex_query_frontend_querier_ring_calculated_maximum_supported_query_plan_version gauge
				cortex_query_frontend_querier_ring_calculated_maximum_supported_query_plan_version %v

				# HELP cortex_query_frontend_querier_ring_expected_maximum_supported_query_plan_version The maximum supported query plan version this process was compiled to support.
				# TYPE cortex_query_frontend_querier_ring_expected_maximum_supported_query_plan_version gauge
				cortex_query_frontend_querier_ring_expected_maximum_supported_query_plan_version %v
			`, expectedMetricValue, planning.MaximumSupportedQueryPlanVersion)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_query_frontend_querier_ring_calculated_maximum_supported_query_plan_version", "cortex_query_frontend_querier_ring_expected_maximum_supported_query_plan_version"))
		})
	}
}

type mockStore struct {
	desc *ring.Desc
}

func (m *mockStore) List(ctx context.Context, prefix string) ([]string, error) {
	panic("not supported")
}

func (m *mockStore) Get(ctx context.Context, key string) (interface{}, error) {
	if key == "queriers" {
		return m.desc, nil
	}

	return nil, fmt.Errorf("unknown key %q", key)
}

func (m *mockStore) Delete(ctx context.Context, key string) error {
	panic("not supported")
}

func (m *mockStore) CAS(ctx context.Context, key string, f func(in interface{}) (out interface{}, retry bool, err error)) error {
	panic("not supported")
}

func (m *mockStore) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	// Do nothing, just wait for the test to stop.
	<-ctx.Done()
}

func (m *mockStore) WatchPrefix(ctx context.Context, prefix string, f func(string, interface{}) bool) {
	panic("not supported")
}

func TestRingService_WaitsForQueriersInRing(t *testing.T) {
	setup := func(t *testing.T, waitTimeout time.Duration) (*consul.Client, *ring.Ring, services.Service) {
		store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { require.NoError(t, closer.Close()) })

		cfg := ring.Config{ReplicationFactor: 1, HeartbeatTimeout: time.Minute}
		r, err := ring.NewWithStoreClientAndStrategy(cfg, "querier-test", querierRingKey, store, ring.NewDefaultReplicationStrategy(), prometheus.NewPedanticRegistry(), log.NewNopLogger())
		require.NoError(t, err)

		svc := NewRingService(r, waitTimeout, log.NewNopLogger())
		// Stopping must outlive the test context, otherwise it returns before the service is terminated.
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), svc)) })

		return store, r, svc
	}

	t.Run("doesn't become running until a querier joins the ring", func(t *testing.T) {
		store, _, svc := setup(t, time.Minute)

		require.NoError(t, svc.StartAsync(t.Context()))

		// The service must stay in the starting state while the ring is empty.
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, services.Starting, svc.State())

		require.NoError(t, store.CAS(t.Context(), querierRingKey, func(interface{}) (interface{}, bool, error) {
			desc := ring.NewDesc()
			desc.AddIngester("querier-0", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, ring.InstanceVersions{MaximumSupportedQueryPlanVersion: uint64(planning.MaximumSupportedQueryPlanVersion)})
			return desc, true, nil
		}))

		require.NoError(t, svc.AwaitRunning(t.Context()))
	})

	t.Run("doesn't become running while the ring holds only unhealthy queriers", func(t *testing.T) {
		store, _, svc := setup(t, time.Minute)

		require.NoError(t, store.CAS(t.Context(), querierRingKey, func(interface{}) (interface{}, bool, error) {
			desc := ring.NewDesc()
			inst := desc.AddIngester("querier-0", "127.0.0.1", "", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, ring.InstanceVersions{MaximumSupportedQueryPlanVersion: uint64(planning.MaximumSupportedQueryPlanVersion)})
			// AddIngester always sets a fresh heartbeat, so backdate it beyond the ring's heartbeat timeout.
			inst.Timestamp = time.Now().Add(-time.Hour).Unix()
			desc.Ingesters["querier-0"] = inst
			return desc, true, nil
		}))

		require.NoError(t, svc.StartAsync(t.Context()))

		time.Sleep(500 * time.Millisecond)
		require.Equal(t, services.Starting, svc.State())
	})

	t.Run("becomes running once the wait times out, even if the ring is empty", func(t *testing.T) {
		_, _, svc := setup(t, 100*time.Millisecond)

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), svc))
	})

	t.Run("fails immediately if the ring client fails while still waiting", func(t *testing.T) {
		_, r, _ := setup(t, time.Minute)
		require.NoError(t, services.StartAndAwaitRunning(t.Context(), r))
		t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), r) })

		// Send on the failure channel unbuffered, exactly like services.FailureWatcher does: if the
		// wait doesn't drain it, this send blocks until the wait times out a minute from now.
		ringFailures := make(chan error)
		waitErr := make(chan error, 1)
		go func() {
			waitErr <- waitForQueriersInRing(t.Context(), r, time.Minute, ringFailures, log.NewNopLogger())
		}()

		select {
		case ringFailures <- errors.New("ring client exploded"):
		case <-time.After(time.Second):
			t.Fatal("timed out sending the ring failure: the wait isn't draining the failure channel")
		}

		select {
		case err := <-waitErr:
			require.EqualError(t, err, "querier ring client failed: ring client exploded")
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for waitForQueriersInRing to return")
		}
	})

	t.Run("terminates cleanly and stops the ring client when stopped while still waiting", func(t *testing.T) {
		_, r, svc := setup(t, time.Minute)

		require.NoError(t, svc.StartAsync(t.Context()))

		time.Sleep(500 * time.Millisecond)
		require.Equal(t, services.Starting, svc.State())

		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), svc))
		require.Equal(t, services.Terminated, svc.State())
		require.Equal(t, services.Terminated, r.State())
	})
}
