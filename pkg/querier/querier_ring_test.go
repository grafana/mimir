// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
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
	state    ring.InstanceState
	versions ring.InstanceVersions
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
				desc.AddIngester(fmt.Sprintf("querier-%d", idx), fmt.Sprintf("127.0.0.%d", idx), "", []uint32{uint32(idx)}, instance.state, time.Now(), false, time.Time{}, instance.versions)
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
	setup := func(t *testing.T, waitTimeout time.Duration) (*consul.Client, services.Service) {
		store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { require.NoError(t, closer.Close()) })

		cfg := ring.Config{ReplicationFactor: 1, HeartbeatTimeout: time.Minute}
		r, err := ring.NewWithStoreClientAndStrategy(cfg, "querier-test", querierRingKey, store, ring.NewDefaultReplicationStrategy(), prometheus.NewPedanticRegistry(), log.NewNopLogger())
		require.NoError(t, err)

		svc := NewRingService(r, waitTimeout, log.NewNopLogger())
		// Stopping must outlive the test context, otherwise it returns before the service is terminated.
		t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), svc) })

		return store, svc
	}

	t.Run("doesn't become running until a querier joins the ring", func(t *testing.T) {
		store, svc := setup(t, time.Minute)

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

	t.Run("becomes running once the wait times out, even if the ring is empty", func(t *testing.T) {
		_, svc := setup(t, 100*time.Millisecond)

		require.NoError(t, services.StartAndAwaitRunning(t.Context(), svc))
	})
}
