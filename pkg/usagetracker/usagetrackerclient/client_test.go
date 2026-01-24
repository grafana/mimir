// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/usagetracker"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

// mockLimitsProvider is a mock implementation of usagetrackerclient.limitsProvider.
type mockLimitsProvider struct {
	limits map[string]int
}

func newMockLimitsProvider() *mockLimitsProvider {
	return &mockLimitsProvider{
		limits: make(map[string]int),
	}
}

func (m *mockLimitsProvider) MaxActiveOrGlobalSeriesPerUser(userID string) int {
	if limit, ok := m.limits[userID]; ok {
		return limit
	}
	return 0 // No limit
}

type noopUsageTrackerRejectionObserver struct{}

func (n *noopUsageTrackerRejectionObserver) ObserveUsageTrackerRejection(_ string) {
}

var _ usagetrackerclient.UsageTrackerRejectionObserver = (*noopUsageTrackerRejectionObserver)(nil)

var noOpObserver = &noopUsageTrackerRejectionObserver{}

type testRejectionObserver struct {
	rejections map[string]int
}

func (o *testRejectionObserver) ObserveUsageTrackerRejection(userID string) {
	o.rejections[userID]++
}

var _ usagetrackerclient.UsageTrackerRejectionObserver = (*testRejectionObserver)(nil)

// prepareTestRings is a helper function that sets up the rings needed for testing.
func prepareTestRings(t testing.TB, ctx context.Context) (*ring.MultiPartitionInstanceRing, *ring.Ring, prometheus.Registerer) {
	logger := log.NewNopLogger()
	registerer := prometheus.NewPedanticRegistry()

	consulConfig := consul.Config{
		MaxCasRetries: 100,
		CasRetryDelay: 10 * time.Millisecond,
	}

	// Setup the in-memory KV store used for the ring.
	instanceRingStore, instanceRingCloser := consul.NewInMemoryClientWithConfig(ring.GetCodec(), consulConfig, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, instanceRingCloser.Close()) })

	partitionRingStore, partitionRingCloser := consul.NewInMemoryClientWithConfig(ring.GetPartitionRingCodec(), consulConfig, log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, partitionRingCloser.Close()) })

	// Add few usage-tracker instances to the instance ring.
	require.NoError(t, instanceRingStore.CAS(ctx, usagetracker.InstanceRingKey, func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		d.AddIngester("usage-tracker-zone-a-1", "1.1.1.1", "zone-a", []uint32{1}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		d.AddIngester("usage-tracker-zone-a-2", "2.2.2.2", "zone-a", []uint32{2}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		d.AddIngester("usage-tracker-zone-b-1", "3.3.3.3", "zone-b", []uint32{3}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		d.AddIngester("usage-tracker-zone-b-2", "4.4.4.4", "zone-b", []uint32{4}, ring.ACTIVE, time.Now(), false, time.Time{}, nil)

		return d, true, nil
	}))

	// Add partitions to the partition ring.
	require.NoError(t, partitionRingStore.CAS(ctx, usagetracker.PartitionRingKey, func(interface{}) (interface{}, bool, error) {
		d := ring.NewPartitionRingDesc()
		d.AddPartition(1, ring.PartitionActive, time.Now())
		d.AddPartition(2, ring.PartitionActive, time.Now())
		d.AddOrUpdateOwner("usage-tracker-zone-a-1", ring.OwnerActive, 1, time.Now())
		d.AddOrUpdateOwner("usage-tracker-zone-a-2", ring.OwnerActive, 2, time.Now())
		d.AddOrUpdateOwner("usage-tracker-zone-b-1", ring.OwnerActive, 1, time.Now())
		d.AddOrUpdateOwner("usage-tracker-zone-b-2", ring.OwnerActive, 2, time.Now())

		return d, true, nil
	}))

	serverCfg := createTestServerConfig()
	serverCfg.InstanceRing.KVStore.Mock = instanceRingStore
	serverCfg.PartitionRing.KVStore.Mock = partitionRingStore

	// Create the instance ring.
	instanceRing, err := usagetracker.NewInstanceRingClient(serverCfg.InstanceRing, logger, registerer)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, instanceRing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, instanceRing))
	})

	// Pre-condition check: all instances should be healthy.
	set, err := instanceRing.GetAllHealthy(usagetrackerclient.TrackSeriesOp)
	require.NoError(t, err)
	require.Len(t, set.Instances, 4)

	// Create the partition ring.
	partitionRingWatcher := usagetracker.NewPartitionRingWatcher(partitionRingStore, logger, registerer)
	require.NoError(t, services.StartAndAwaitRunning(ctx, partitionRingWatcher))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, partitionRingWatcher))
	})

	partitionRing := ring.NewMultiPartitionInstanceRing(partitionRingWatcher, instanceRing, serverCfg.InstanceRing.HeartbeatTimeout)

	// Pre-condition check: all partitions should be active.
	require.Equal(t, []int32{1, 2}, partitionRingWatcher.PartitionRing().ActivePartitionIDs())

	return partitionRing, instanceRing, registerer
}

func TestUsageTrackerClient_TrackSeries(t *testing.T) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
		userID = "user-1"
	)

	prepareTest := func() (*ring.MultiPartitionInstanceRing, *ring.Ring, prometheus.Registerer) {
		return prepareTestRings(t, ctx)
	}

	t.Run("should track series to usage-trackers running in the preferred zone if available (series are sharded to 2 partitions)", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithSuccessfulResponse(),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
		series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition2, series5Partition2})
		require.NoError(t, err)
		require.Empty(t, rejected)

		// Should have tracked series only to usage-tracker replicas in the preferred zone.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 1)

		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

		req = instances["usage-tracker-zone-b-2"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series4Partition2, series5Partition2}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(2), req.(*usagetrackerpb.TrackSeriesRequest).Partition)
	})

	t.Run("should track series to usage-trackers running in the preferred zone if available (series are sharded to 1 partition)", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithSuccessfulResponse(),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)

		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1})
		require.NoError(t, err)
		require.Empty(t, rejected)

		// Should have tracked series only to usage-tracker replicas in the preferred zone.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 0)

		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)
	})

	t.Run("should fallback to the other zone if a usage-tracker instance in the preferred zone is failing", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithResponse(nil, errors.New("failing instance")),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithSuccessfulResponse(),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
		series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition2, series5Partition2})
		require.NoError(t, err)
		require.Empty(t, rejected)

		// Should have tracked series only to usage-tracker replicas in the preferred zone.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 1)

		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

		req = instances["usage-tracker-zone-b-2"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series4Partition2, series5Partition2}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(2), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

		// Fallback.
		req = instances["usage-tracker-zone-a-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)
	})

	for _, returnRejectedSeries := range []bool{true, false} {
		testName := map[bool]string{
			true:  "should return rejected series",
			false: "should not return rejected series",
		}[returnRejectedSeries]

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			partitionRing, instanceRing, registerer := prepareTest()

			// Generate the series hashes so that we can predict in which partition they're sharded to.
			partitions := partitionRing.PartitionRing().Partitions()
			require.Len(t, partitions, 2)
			slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

			require.Equal(t, int32(1), partitions[0].Id)
			require.Equal(t, int32(2), partitions[1].Id)

			series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
			series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
			series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
			series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
			series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

			// Mock the usage-tracker server.
			instances := map[string]*usageTrackerMock{
				"usage-tracker-zone-a-1": newUsageTrackerMockWithSuccessfulResponse(),
				"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),

				// Return rejected series only from zone-b to ensure the response is picked up from this zone.
				"usage-tracker-zone-b-1": newUsageTrackerMockWithResponse(&usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: []uint64{series2Partition1}}, nil),
				"usage-tracker-zone-b-2": newUsageTrackerMockWithResponse(&usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: []uint64{series4Partition2, series5Partition2}}, nil),
			}

			clientCfg := createTestClientConfig()
			clientCfg.IgnoreRejectedSeries = !returnRejectedSeries
			clientCfg.PreferAvailabilityZone = "zone-b"

			clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
				mock, ok := instances[instance.Id]
				if ok {
					return mock, nil
				}

				return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
			})

			c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
			require.NoError(t, services.StartAndAwaitRunning(ctx, c))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
			})

			rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition2, series5Partition2})
			require.NoError(t, err)
			if returnRejectedSeries {
				require.ElementsMatch(t, []uint64{series2Partition1, series4Partition2, series5Partition2}, rejected)
			} else {
				require.Empty(t, rejected)
			}

			// Should have tracked series only to usage-tracker replicas in the preferred zone.
			instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeries", 0)
			instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeries", 0)
			instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
			instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 1)

			req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
			require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
			require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

			req = instances["usage-tracker-zone-b-2"].Calls[0].Arguments.Get(1)
			require.ElementsMatch(t, []uint64{series4Partition2, series5Partition2}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
			require.Equal(t, int32(2), req.(*usagetrackerpb.TrackSeriesRequest).Partition)
		})
	}

	t.Run("should hedge requests to the other zone if a usage-tracker instance in the preferred zone is slow", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.RequestsHedgingDelay = 250 * time.Millisecond

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
		series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			// Return rejected series only from this instance, to ensure the response comes from here.
			"usage-tracker-zone-a-1": newUsageTrackerMockWithResponse(&usagetrackerpb.TrackSeriesResponse{RejectedSeriesHashes: []uint64{series2Partition1}}, nil),

			"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithSlowSuccessfulResponse(clientCfg.RequestsHedgingDelay * 2),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithSuccessfulResponse(),
		}

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition2, series5Partition2})
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{series2Partition1}, rejected)

		// Should have tracked series only to usage-tracker replicas in the preferred zone.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeries", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 1)

		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

		req = instances["usage-tracker-zone-b-2"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series4Partition2, series5Partition2}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(2), req.(*usagetrackerpb.TrackSeriesRequest).Partition)

		// Hedged request.
		req = instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		require.ElementsMatch(t, []uint64{series1Partition1, series2Partition1, series3Partition1}, req.(*usagetrackerpb.TrackSeriesRequest).SeriesHashes)
		require.Equal(t, int32(1), req.(*usagetrackerpb.TrackSeriesRequest).Partition)
	})

	t.Run("should be a no-op if there are no series to track", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.RequestsHedgingDelay = 250 * time.Millisecond

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithSuccessfulResponse(),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithSuccessfulResponse(),
		}

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{})
		require.NoError(t, err)
		require.Empty(t, rejected)
	})

	t.Run("should ignore errors when IgnoreErrors is enabled", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server with failing instances.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithResponse(nil, errors.New("failing instance")),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithResponse(nil, errors.New("failing instance")),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithResponse(nil, errors.New("failing instance")),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithResponse(nil, errors.New("failing instance")),
		}

		clientCfg := createTestClientConfig()
		clientCfg.IgnoreErrors = true
		clientCfg.PreferAvailabilityZone = "zone-b"

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, noOpObserver)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
		series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

		// Despite all instances failing, the client should not return an error when IgnoreErrors is enabled.
		rejected, err := c.TrackSeries(user.InjectOrgID(ctx, userID), userID, []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition2, series5Partition2})
		require.NoError(t, err)
		require.Empty(t, rejected)

		// All instances should have been called despite failing.
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeries", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeries", 1)
	})
}

func createTestClientConfig() usagetrackerclient.Config {
	cfg := usagetrackerclient.Config{}
	flagext.DefaultValues(&cfg)

	// No hedging in tests by default.
	cfg.RequestsHedgingDelay = time.Hour

	return cfg
}

func createTestServerConfig() usagetracker.Config {
	cfg := usagetracker.Config{}
	flagext.DefaultValues(&cfg)

	return cfg
}

type usageTrackerMock struct {
	mock.Mock

	usagetrackerpb.UsageTrackerClient
	grpc_health_v1.HealthClient
}

func newUsageTrackerMockWithSuccessfulResponse() *usageTrackerMock {
	return newUsageTrackerMockWithResponse(&usagetrackerpb.TrackSeriesResponse{}, nil)
}

func newUsageTrackerMockWithSlowSuccessfulResponse(delay time.Duration) *usageTrackerMock {
	m := &usageTrackerMock{}
	m.On("TrackSeries", mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
		time.Sleep(delay)
	}).Return(&usagetrackerpb.TrackSeriesResponse{}, nil)

	return m
}

func newUsageTrackerMockWithResponse(res *usagetrackerpb.TrackSeriesResponse, err error) *usageTrackerMock {
	m := &usageTrackerMock{}
	m.On("TrackSeries", mock.Anything, mock.Anything).Return(res, err)

	return m
}

func newUsageTrackerMockWithBatchResponse(res *usagetrackerpb.TrackSeriesBatchResponse, err error) *usageTrackerMock {
	m := &usageTrackerMock{}
	m.On("TrackSeriesBatch", mock.Anything, mock.Anything).Return(res, err)
	// Also set up TrackSeries for backward compatibility
	m.On("TrackSeries", mock.Anything, mock.Anything).Return(&usagetrackerpb.TrackSeriesResponse{}, nil)

	return m
}

func (m *usageTrackerMock) TrackSeries(ctx context.Context, req *usagetrackerpb.TrackSeriesRequest, _ ...grpc.CallOption) (*usagetrackerpb.TrackSeriesResponse, error) {
	args := m.Called(ctx, req)

	if args.Get(0) != nil {
		return args.Get(0).(*usagetrackerpb.TrackSeriesResponse), args.Error(1)
	}

	return nil, args.Error(1)
}

func (m *usageTrackerMock) TrackSeriesBatch(ctx context.Context, req *usagetrackerpb.TrackSeriesBatchRequest, _ ...grpc.CallOption) (*usagetrackerpb.TrackSeriesBatchResponse, error) {
	args := m.Called(ctx, req)

	if args.Get(0) != nil {
		return args.Get(0).(*usagetrackerpb.TrackSeriesBatchResponse), args.Error(1)
	}

	return nil, args.Error(1)
}

func (m *usageTrackerMock) GetUsersCloseToLimit(ctx context.Context, req *usagetrackerpb.GetUsersCloseToLimitRequest, _ ...grpc.CallOption) (*usagetrackerpb.GetUsersCloseToLimitResponse, error) {
	// Check if there's a configured expectation
	if len(m.ExpectedCalls) > 0 {
		for _, call := range m.ExpectedCalls {
			if call.Method == "GetUsersCloseToLimit" {
				args := m.Called(ctx, req)
				if args.Get(0) != nil {
					return args.Get(0).(*usagetrackerpb.GetUsersCloseToLimitResponse), args.Error(1)
				}
				return &usagetrackerpb.GetUsersCloseToLimitResponse{
					SortedUserIds: []string{},
					Partition:     req.Partition,
				}, args.Error(1)
			}
		}
	}

	// Return empty list by default for tests that don't need to check async tracking behavior
	return &usagetrackerpb.GetUsersCloseToLimitResponse{
		SortedUserIds: []string{},
		Partition:     req.Partition,
	}, nil
}

func (m *usageTrackerMock) Close() error {
	return nil
}

func TestUsageTrackerClient_CanTrackAsync(t *testing.T) {
	tests := []struct {
		name                           string
		userID                         string
		usersCloseToLimit              []string
		userLimit                      int
		minSeriesLimitForAsyncTracking int
		expectedCanTrackAsync          bool
	}{
		{
			name:                           "user not in close to limit list, no min limit check",
			userID:                         "user-1",
			usersCloseToLimit:              []string{"user-2", "user-3"},
			userLimit:                      100000,
			minSeriesLimitForAsyncTracking: 0,
			expectedCanTrackAsync:          true,
		},
		{
			name:                           "user in close to limit list",
			userID:                         "user-2",
			usersCloseToLimit:              []string{"user-1", "user-2", "user-3"},
			userLimit:                      100000,
			minSeriesLimitForAsyncTracking: 0,
			expectedCanTrackAsync:          false,
		},
		{
			name:                           "user with limit below min threshold",
			userID:                         "user-1",
			usersCloseToLimit:              []string{},
			userLimit:                      50000,
			minSeriesLimitForAsyncTracking: 100000,
			expectedCanTrackAsync:          false,
		},
		{
			name:                           "user with limit at min threshold",
			userID:                         "user-1",
			usersCloseToLimit:              []string{},
			userLimit:                      100000,
			minSeriesLimitForAsyncTracking: 100000,
			expectedCanTrackAsync:          true,
		},
		{
			name:                           "user with limit above min threshold",
			userID:                         "user-1",
			usersCloseToLimit:              []string{},
			userLimit:                      150000,
			minSeriesLimitForAsyncTracking: 100000,
			expectedCanTrackAsync:          true,
		},
		{
			name:                           "user with no limit (0) and min threshold set",
			userID:                         "user-1",
			usersCloseToLimit:              []string{},
			userLimit:                      0,
			minSeriesLimitForAsyncTracking: 100000,
			expectedCanTrackAsync:          true,
		},
		{
			name:                           "user below min threshold and in close to limit list",
			userID:                         "user-1",
			usersCloseToLimit:              []string{"user-1", "user-2"},
			userLimit:                      50000,
			minSeriesLimitForAsyncTracking: 100000,
			expectedCanTrackAsync:          false,
		},
		{
			name:                           "empty close to limit list",
			userID:                         "user-1",
			usersCloseToLimit:              []string{},
			userLimit:                      100000,
			minSeriesLimitForAsyncTracking: 0,
			expectedCanTrackAsync:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				ctx    = context.Background()
				logger = log.NewNopLogger()
			)

			partitionRing, instanceRing, registerer := prepareTestRings(t, ctx)

			// Create mock limits provider
			limitsProvider := newMockLimitsProvider()
			limitsProvider.limits[tt.userID] = tt.userLimit

			// Mock the usage-tracker server to return the users close to limit
			instances := map[string]*usageTrackerMock{
				"usage-tracker-zone-a-1": newUsageTrackerMockWithUsersCloseToLimit(tt.usersCloseToLimit),
				"usage-tracker-zone-a-2": newUsageTrackerMockWithUsersCloseToLimit(tt.usersCloseToLimit),
				"usage-tracker-zone-b-1": newUsageTrackerMockWithUsersCloseToLimit(tt.usersCloseToLimit),
				"usage-tracker-zone-b-2": newUsageTrackerMockWithUsersCloseToLimit(tt.usersCloseToLimit),
			}

			clientCfg := createTestClientConfig()
			clientCfg.MinSeriesLimitForAsyncTracking = tt.minSeriesLimitForAsyncTracking

			clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
				mock, ok := instances[instance.Id]
				if ok {
					return mock, nil
				}
				return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
			})

			// Create and start the client
			// StartAndAwaitRunning ensures that starting() has completed, which populates the cache
			c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, limitsProvider, logger, registerer, noOpObserver)
			require.NoError(t, services.StartAndAwaitRunning(ctx, c))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
			})

			// Test CanTrackAsync
			result := c.CanTrackAsync(tt.userID)
			assert.Equal(t, tt.expectedCanTrackAsync, result)
		})
	}
}

// newUsageTrackerMockWithUsersCloseToLimit creates a mock that returns a specific list of users close to limit.
func newUsageTrackerMockWithUsersCloseToLimit(userIDs []string) *usageTrackerMock {
	m := &usageTrackerMock{}
	m.On("TrackSeries", mock.Anything, mock.Anything).Return(&usagetrackerpb.TrackSeriesResponse{}, nil)

	// Clone and sort the user IDs since CanTrackAsync expects a sorted list for binary search
	sortedUserIDs := slices.Clone(userIDs)
	slices.Sort(sortedUserIDs)

	m.On("GetUsersCloseToLimit", mock.Anything, mock.Anything).Return(&usagetrackerpb.GetUsersCloseToLimitResponse{
		SortedUserIds: sortedUserIDs,
		Partition:     1,
	}, nil)
	return m
}

func TestUsageTrackerClient_TrackSeriesBatch(t *testing.T) {
	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	prepareTest := func() (*ring.MultiPartitionInstanceRing, *ring.Ring, prometheus.Registerer) {
		return prepareTestRings(t, ctx)
	}

	t.Run("should track series batch to usage-trackers running in the preferred zone if available", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.UseBatchedTracking = true
		clientCfg.BatchDelay = 1_000 * time.Hour // Effectively disable timed batch flushing - we'll flush manually.

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		r := &testRejectionObserver{
			rejections: make(map[string]int),
		}

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, r)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition1 := uint64(partitions[0].Tokens[0] - 2)
		series5Partition1 := uint64(partitions[0].Tokens[1] - 2)

		err := c.TrackSeriesAsync(t.Context(), "user-1", []uint64{series1Partition1, series2Partition1, series3Partition1})
		require.NoError(t, err)
		err = c.TrackSeriesAsync(t.Context(), "user-2", []uint64{series4Partition1, series5Partition1})
		require.NoError(t, err)

		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		batchClient := c.BatchClient()
		batchClient.TestFlush()

		// Should have tracked series only to usage-tracker replicas in the preferred zone.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		require.Equal(t, 0, r.rejections["user-1"])
		require.Equal(t, 0, r.rejections["user-2"])

		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		batchReq := req.(*usagetrackerpb.TrackSeriesBatchRequest)
		require.Len(t, batchReq.Partitions, 1)
		require.Equal(t, int32(1), batchReq.Partitions[0].Partition)
		require.Len(t, batchReq.Partitions[0].Users, 2)
		require.EqualValues(t, batchReq.Partitions[0].Users,
			[]*usagetrackerpb.TrackSeriesBatchUser{
				{
					UserID:       "user-1",
					SeriesHashes: []uint64{series1Partition1, series2Partition1, series3Partition1},
				},
				{
					UserID:       "user-2",
					SeriesHashes: []uint64{series4Partition1, series5Partition1},
				},
			},
		)
	})

	t.Run("should fallback to the other zone if a usage-tracker instance in the preferred zone is failing", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithBatchResponse(nil, errors.New("failing instance")),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.UseBatchedTracking = true
		clientCfg.BatchDelay = 1_000 * time.Hour // Effectively disable timed batch flushing - we'll flush manually.

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		r := &testRejectionObserver{
			rejections: make(map[string]int),
		}

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, r)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition2 := uint64(partitions[1].Tokens[0] - 1)
		series5Partition2 := uint64(partitions[1].Tokens[1] - 1)

		err := c.TrackSeriesAsync(t.Context(), "user-1", []uint64{series1Partition1, series2Partition1, series3Partition1})
		require.NoError(t, err)
		err = c.TrackSeriesAsync(t.Context(), "user-2", []uint64{series4Partition2, series5Partition2})
		require.NoError(t, err)

		require.Equal(t, 0, r.rejections["user-1"])
		require.Equal(t, 0, r.rejections["user-2"])

		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		batchClient := c.BatchClient()
		batchClient.TestFlush()

		// Should have attempted to track series to usage-tracker replicas in the preferred zone first.
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1)

		// Should have fallen back to the other zone for partition 1.
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		// Verify the fallback request for partition 1.
		req := instances["usage-tracker-zone-a-1"].Calls[0].Arguments.Get(1)
		batchReq := req.(*usagetrackerpb.TrackSeriesBatchRequest)
		require.Len(t, batchReq.Partitions, 1)
		require.Equal(t, int32(1), batchReq.Partitions[0].Partition)
		require.Len(t, batchReq.Partitions[0].Users, 1)
		require.EqualValues(t, batchReq.Partitions[0].Users,
			[]*usagetrackerpb.TrackSeriesBatchUser{
				{
					UserID:       "user-1",
					SeriesHashes: []uint64{series1Partition1, series2Partition1, series3Partition1},
				},
			},
		)

		// Verify the successful request for partition 2 (zone-b-2 worked).
		req = instances["usage-tracker-zone-b-2"].Calls[0].Arguments.Get(1)
		batchReq = req.(*usagetrackerpb.TrackSeriesBatchRequest)
		require.Len(t, batchReq.Partitions, 1)
		require.Equal(t, int32(2), batchReq.Partitions[0].Partition)
		require.Len(t, batchReq.Partitions[0].Users, 1)
		require.EqualValues(t, batchReq.Partitions[0].Users,
			[]*usagetrackerpb.TrackSeriesBatchUser{
				{
					UserID:       "user-2",
					SeriesHashes: []uint64{series4Partition2, series5Partition2},
				},
			},
		)
	})

	t.Run("should flush batch when max series count is exceeded", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Mock the usage-tracker server.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.UseBatchedTracking = true
		clientCfg.MaxBatchSeries = 5             // Set a low threshold to trigger flush
		clientCfg.BatchDelay = 1_000 * time.Hour // Effectively disable timed batch flushing

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		r := &testRejectionObserver{
			rejections: make(map[string]int),
		}

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, r)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition1 := uint64(partitions[0].Tokens[0] - 2)
		series5Partition1 := uint64(partitions[0].Tokens[1] - 2)
		series6Partition1 := uint64(partitions[0].Tokens[2] - 2) // This will exceed the threshold

		// Add series that will exceed the max batch series threshold (5)
		// First 3 series should not trigger flush
		err := c.TrackSeriesAsync(t.Context(), "user-1", []uint64{series1Partition1, series2Partition1, series3Partition1})
		require.NoError(t, err)

		// Verify no flush yet
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		// Add 3 more series (total 6, exceeding threshold of 5)
		err = c.TrackSeriesAsync(t.Context(), "user-2", []uint64{series4Partition1, series5Partition1, series6Partition1})
		require.NoError(t, err)

		// Wait a bit for the async flush to complete

		require.Eventually(t, func() bool {
			// Should have automatically flushed when threshold was exceeded
			return (instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0) &&
				instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0) &&
				instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1) &&
				instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0))
		}, 5*time.Second, 10*time.Millisecond)

		require.Equal(t, 0, r.rejections["user-1"])
		require.Equal(t, 0, r.rejections["user-2"])

		// Verify the flushed batch contains all 6 series
		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		batchReq := req.(*usagetrackerpb.TrackSeriesBatchRequest)
		require.Len(t, batchReq.Partitions, 1)
		require.Equal(t, int32(1), batchReq.Partitions[0].Partition)
		require.Len(t, batchReq.Partitions[0].Users, 2)
		require.EqualValues(t, batchReq.Partitions[0].Users,
			[]*usagetrackerpb.TrackSeriesBatchUser{
				{
					UserID:       "user-1",
					SeriesHashes: []uint64{series1Partition1, series2Partition1, series3Partition1},
				},
				{
					UserID:       "user-2",
					SeriesHashes: []uint64{series4Partition1, series5Partition1, series6Partition1},
				},
			},
		)
	})

	t.Run("should observe rejections when user exceeds their limit", func(t *testing.T) {
		t.Parallel()

		partitionRing, instanceRing, registerer := prepareTest()

		// Generate the series hashes so that we can predict in which partition they're sharded to.
		partitions := partitionRing.PartitionRing().Partitions()
		require.Len(t, partitions, 2)
		slices.SortFunc(partitions, func(a, b ring.PartitionDesc) int { return int(a.Id - b.Id) })

		require.Equal(t, int32(1), partitions[0].Id)
		require.Equal(t, int32(2), partitions[1].Id)

		series1Partition1 := uint64(partitions[0].Tokens[0] - 1)
		series2Partition1 := uint64(partitions[0].Tokens[1] - 1)
		series3Partition1 := uint64(partitions[0].Tokens[2] - 1)
		series4Partition1 := uint64(partitions[0].Tokens[0] - 2)
		series5Partition1 := uint64(partitions[0].Tokens[1] - 2)

		// Mock the usage-tracker server to return rejections for user-1 when they exceed their limit.
		instances := map[string]*usageTrackerMock{
			"usage-tracker-zone-a-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{
				Rejections: []*usagetrackerpb.TrackSeriesBatchRejection{
					{
						Partition: 1,
						Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
							{
								UserID:               "user-1",
								RejectedSeriesHashes: []uint64{series3Partition1, series4Partition1},
							},
						},
					},
				},
			}, nil),
			"usage-tracker-zone-a-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
			"usage-tracker-zone-b-1": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{
				Rejections: []*usagetrackerpb.TrackSeriesBatchRejection{
					{
						Partition: 1,
						Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
							{
								UserID:               "user-1",
								RejectedSeriesHashes: []uint64{series3Partition1, series4Partition1},
							},
						},
					},
				},
			}, nil),
			"usage-tracker-zone-b-2": newUsageTrackerMockWithBatchResponse(&usagetrackerpb.TrackSeriesBatchResponse{}, nil),
		}

		clientCfg := createTestClientConfig()
		clientCfg.PreferAvailabilityZone = "zone-b"
		clientCfg.UseBatchedTracking = true
		clientCfg.BatchDelay = 1_000 * time.Hour // Effectively disable timed batch flushing - we'll flush manually.

		clientCfg.ClientFactory = ring_client.PoolInstFunc(func(instance ring.InstanceDesc) (ring_client.PoolClient, error) {
			mock, ok := instances[instance.Id]
			if ok {
				return mock, nil
			}

			return nil, fmt.Errorf("usage-tracker with ID %s not found", instance.Id)
		})

		r := &testRejectionObserver{
			rejections: make(map[string]int),
		}

		c := usagetrackerclient.NewUsageTrackerClient("test", clientCfg, partitionRing, instanceRing, newMockLimitsProvider(), logger, registerer, r)
		require.NoError(t, services.StartAndAwaitRunning(ctx, c))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
		})

		// Track series for user-1, some of which will be rejected
		err := c.TrackSeriesAsync(t.Context(), "user-1", []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition1, series5Partition1})
		require.NoError(t, err)

		// Track series for user-2, which should not be rejected
		err = c.TrackSeriesAsync(t.Context(), "user-2", []uint64{series1Partition1})
		require.NoError(t, err)

		// Verify no calls yet before flush
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		// No rejections observed yet
		require.Equal(t, 0, r.rejections["user-1"])
		require.Equal(t, 0, r.rejections["user-2"])

		// Flush the batch
		batchClient := c.BatchClient()
		batchClient.TestFlush()

		// Should have tracked series to usage-tracker replicas in the preferred zone
		instances["usage-tracker-zone-a-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-a-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)
		instances["usage-tracker-zone-b-1"].AssertNumberOfCalls(t, "TrackSeriesBatch", 1)
		instances["usage-tracker-zone-b-2"].AssertNumberOfCalls(t, "TrackSeriesBatch", 0)

		// Verify rejections were observed for user-1 (1 Track call had rejection(s))
		require.Equal(t, 1, r.rejections["user-1"])
		require.Equal(t, 0, r.rejections["user-2"])

		// Verify the batch request contains all series
		req := instances["usage-tracker-zone-b-1"].Calls[0].Arguments.Get(1)
		batchReq := req.(*usagetrackerpb.TrackSeriesBatchRequest)
		require.Len(t, batchReq.Partitions, 1)
		require.Equal(t, int32(1), batchReq.Partitions[0].Partition)
		require.Len(t, batchReq.Partitions[0].Users, 2)
		require.EqualValues(t, batchReq.Partitions[0].Users,
			[]*usagetrackerpb.TrackSeriesBatchUser{
				{
					UserID:       "user-1",
					SeriesHashes: []uint64{series1Partition1, series2Partition1, series3Partition1, series4Partition1, series5Partition1},
				},
				{
					UserID:       "user-2",
					SeriesHashes: []uint64{series1Partition1},
				},
			},
		)
	})
}

func TestRejectionString(t *testing.T) {
	tests := []struct {
		name       string
		rejections []*usagetrackerpb.TrackSeriesBatchRejection
		expected   string
	}{
		{
			name:       "no rejections",
			rejections: []*usagetrackerpb.TrackSeriesBatchRejection{},
			expected:   "",
		},
		{
			name: "multiple different users",
			rejections: []*usagetrackerpb.TrackSeriesBatchRejection{
				{
					Partition: 1,
					Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
						{
							UserID:               "user-1",
							RejectedSeriesHashes: []uint64{1, 2, 3},
						},
						{
							UserID:               "user-2",
							RejectedSeriesHashes: []uint64{4, 5, 6},
						},
					},
				},
			},
			expected: "user-1 (3), user-2 (3)",
		},
		{
			name: "same user with multiple rejections",
			rejections: []*usagetrackerpb.TrackSeriesBatchRejection{
				{
					Partition: 1,
					Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
						{
							UserID:               "user-1",
							RejectedSeriesHashes: []uint64{1, 2, 3},
						},
						{
							UserID:               "user-1",
							RejectedSeriesHashes: []uint64{4, 5, 6},
						},
					},
				},
			},
			expected: "user-1 (6)",
		},
		{
			name: "same user across partitions",
			rejections: []*usagetrackerpb.TrackSeriesBatchRejection{
				{
					Partition: 1,
					Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
						{
							UserID:               "user-1",
							RejectedSeriesHashes: []uint64{1, 2, 3},
						},
					},
				},
				{
					Partition: 2,
					Users: []*usagetrackerpb.TrackSeriesBatchRejectionUser{
						{
							UserID:               "user-1",
							RejectedSeriesHashes: []uint64{4, 5, 6},
						},
					},
				},
			},
			expected: "user-1 (6)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, usagetrackerclient.RejectionString(tt.rejections))
		})
	}
}

func BenchmarkPartitionBatcher_TrackSeries(b *testing.B) {
	logger := log.NewNopLogger()
	stopping := make(chan struct{})
	defer close(stopping)

	// Create partitionBatcher with high thresholds to avoid flushes during benchmark
	batcher := usagetrackerclient.NewPartitionBatcher(
		1,               // partition
		0,               // never flush due to size threshold
		1_000*time.Hour, // batchDelay (very long)
		logger,
		nil, // trackerClient (not needed if no flushes)
		nil, // clientsPool (not needed if no flushes)
		stopping,
	)

	// Generate 100 series hashes
	series := make([]uint64, 100)
	for i := range series {
		series[i] = uint64(i)
	}

	b.ResetTimer()
	for b.Loop() {
		batcher.TrackSeries("user-1", series)
	}
}
