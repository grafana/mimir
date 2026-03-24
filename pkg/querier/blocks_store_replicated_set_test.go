// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_replicated_set_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
)

func newBlock(id ulid.ULID, minT time.Time, maxT time.Time) *bucketindex.Block {
	return &bucketindex.Block{
		ID:      id,
		MinTime: minT.UnixMilli(),
		MaxTime: maxT.UnixMilli(),
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	blockID1 := ulid.MustNew(1, nil) // hash: 283204220
	blockID2 := ulid.MustNew(2, nil) // hash: 444110359
	blockID3 := ulid.MustNew(5, nil) // hash: 2931974232
	blockID4 := ulid.MustNew(6, nil) // hash: 3092880371

	block1Hash := mimir_tsdb.HashBlockID(blockID1)
	block2Hash := mimir_tsdb.HashBlockID(blockID2)
	block3Hash := mimir_tsdb.HashBlockID(blockID3)
	block4Hash := mimir_tsdb.HashBlockID(blockID4)

	minT := time.Now().Add(-5 * time.Hour)
	maxT := minT.Add(2 * time.Hour)

	block1 := newBlock(blockID1, minT, maxT)
	block2 := newBlock(blockID2, minT, maxT)
	block3 := newBlock(blockID3, minT, maxT)
	block4 := newBlock(blockID4, minT, maxT)

	userID := "user-A"
	registeredAt := time.Now()

	tests := map[string]struct {
		tenantShardSize   int
		replicationFactor int
		setup             func(*ring.Desc)
		queryBlocks       bucketindex.Blocks
		exclude           map[ulid.ULID][]string
		expectedClients   map[string][]ulid.ULID
		expectedErr       error
	}{
		"shard size 0, single instance in the ring with RF = 1": {
			tenantShardSize:   0,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2},
			},
		},
		"shard size 0, single instance in the ring with RF = 1 but excluded": {
			tenantShardSize:   0,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			exclude: map[ulid.ULID][]string{
				blockID1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", blockID1.String()),
		},
		"shard size 0, single instance in the ring with RF = 1 but excluded for non queried block": {
			tenantShardSize:   0,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			exclude: map[ulid.ULID][]string{
				blockID3: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2},
			},
		},
		"shard size 0, single instance in the ring with RF = 2": {
			tenantShardSize:   0,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2},
			},
		},
		"shard size 0, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1": {
			tenantShardSize:   0,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1},
				"127.0.0.3": {blockID3},
				"127.0.0.4": {blockID4},
			},
		},
		"shard size 0, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 1 but excluded": {
			tenantShardSize:   0,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				blockID3: {"127.0.0.3"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", blockID3.String()),
		},
		"shard size 0, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2": {
			tenantShardSize:   0,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1},
				"127.0.0.3": {blockID3},
				"127.0.0.4": {blockID4},
			},
		},
		"shard size 0, multiple instances in the ring with multiple requested blocks belonging to the same store-gateway and RF = 2": {
			tenantShardSize:   0,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2, block3, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID4},
				"127.0.0.2": {blockID2, blockID3},
			},
		},
		"shard size 0, multiple instances in the ring with each requested block belonging to a different store-gateway and RF = 2 and some blocks excluded but with replacement available": {
			tenantShardSize:   0,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block3, block4},
			exclude: map[ulid.ULID][]string{
				blockID3: {"127.0.0.3"},
				blockID1: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.2": {blockID1},
				"127.0.0.4": {blockID3, blockID4},
			},
		},
		"shard size 0, multiple instances in the ring are JOINING, the requested block + its replicas only belongs to JOINING instances": {
			tenantShardSize:   0,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.JOINING, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.JOINING, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.JOINING, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.4": {blockID1},
			},
		},
		"shard size 1, single instance in the ring with RF = 1": {
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2},
			},
		},
		"shard size 1, single instance in the ring with RF = 1, but store-gateway excluded": {
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			exclude: map[ulid.ULID][]string{
				blockID1: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", blockID1.String()),
		},
		"shard size 2, single instance in the ring with RF = 2": {
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2},
			},
		},
		"shard size 1, multiple instances in the ring with RF = 1": {
			tenantShardSize:   1,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID2, blockID4},
			},
		},
		"shard size 2, shuffle sharding, multiple instances in the ring with RF = 1": {
			tenantShardSize:   2,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1, blockID4},
				"127.0.0.3": {blockID2},
			},
		},
		"shard size 4, multiple instances in the ring with RF = 1": {
			tenantShardSize:   4,
			replicationFactor: 1,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2, block4},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.1": {blockID1},
				"127.0.0.2": {blockID2},
				"127.0.0.4": {blockID4},
			},
		},
		"shard size 2, multiple instances in the ring with RF = 2, with excluded blocks but some replacement available": {
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			exclude: map[ulid.ULID][]string{
				blockID1: {"127.0.0.1"},
				blockID2: {"127.0.0.1"},
			},
			expectedClients: map[string][]ulid.ULID{
				"127.0.0.3": {blockID1, blockID2},
			},
		},
		"shard size 2, multiple instances in the ring with RF = 2, SS = 2 with excluded blocks and no replacement available": {
			tenantShardSize:   2,
			replicationFactor: 2,
			setup: func(d *ring.Desc) {
				d.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-3", "127.0.0.3", "", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				d.AddIngester("instance-4", "127.0.0.4", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			queryBlocks: []*bucketindex.Block{block1, block2},
			exclude: map[ulid.ULID][]string{
				blockID1: {"127.0.0.1", "127.0.0.3"},
				blockID2: {"127.0.0.1"},
			},
			expectedErr: fmt.Errorf("no store-gateway instance left after checking exclude for block %s", blockID1.String()),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()

			// Setup the ring state.
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			require.NoError(t, ringStore.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setup(d)
				return d, true, nil
			}))

			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = testData.replicationFactor

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

			limits := &blocksStoreLimitsMock{
				storeGatewayTenantShardSize: testData.tenantShardSize,
			}

			reg := prometheus.NewPedanticRegistry()
			s, err := newBlocksStoreReplicationSet(r, noLoadBalancing, storegateway.NewNopDynamicReplication(ringCfg.ReplicationFactor), nil, limits, StoreGatewayClientConfig{}, log.NewNopLogger(), reg)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, s))
			defer services.StopAndAwaitTerminated(ctx, s) //nolint:errcheck

			// Wait until the ring client has initialised the state.
			test.Poll(t, time.Second, true, func() interface{} {
				all, err := r.GetAllHealthy(storegateway.BlocksRead)
				return err == nil && len(all.Instances) > 0
			})

			clients, err := s.GetClientsFor(userID, testData.queryBlocks, testData.exclude)
			assert.Equal(t, testData.expectedErr, err)
			defer func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			}()

			if testData.expectedErr == nil {
				assert.Equal(t, testData.expectedClients, getStoreGatewayClientAddrs(clients))

				assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_storegateway_clients The current number of store-gateway clients in the pool.
					# TYPE cortex_storegateway_clients gauge
					cortex_storegateway_clients{client="querier"} %d
				`, len(testData.expectedClients))), "cortex_storegateway_clients"))
			}
		})
	}
}

func TestBlocksStoreReplicationSet_GetClientsFor_ShouldSupportRandomLoadBalancingStrategy(t *testing.T) {
	const (
		numRuns      = 1000
		numInstances = 3
	)

	ctx := context.Background()
	userID := "user-A"
	registeredAt := time.Now()

	minT := time.Now().Add(-5 * time.Hour)
	maxT := minT.Add(2 * time.Hour)
	blockID1 := ulid.MustNew(1, nil)
	block1 := newBlock(blockID1, minT, maxT)

	// Create a ring.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, ringStore.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		for n := 1; n <= numInstances; n++ {
			d.AddIngester(fmt.Sprintf("instance-%d", n), fmt.Sprintf("127.0.0.%d", n), fmt.Sprintf("zone-%d", n), []uint32{uint32(n)}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
		}
		return d, true, nil
	}))

	// Configure a replication factor equal to the number of instances, so that every store-gateway gets all blocks.
	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = numInstances

	setupBlocksStoreReplicationSet := func(t *testing.T, preferredZones []string) *blocksStoreReplicationSet {
		r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
		require.NoError(t, err)

		limits := &blocksStoreLimitsMock{storeGatewayTenantShardSize: 0}
		reg := prometheus.NewPedanticRegistry()
		s, err := newBlocksStoreReplicationSet(r, randomLoadBalancing, storegateway.NewNopDynamicReplication(ringCfg.ReplicationFactor), preferredZones, limits, StoreGatewayClientConfig{}, log.NewNopLogger(), reg)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, s))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, s))
		})

		// Wait until the ring client has initialised the state.
		test.Poll(t, time.Second, true, func() interface{} {
			all, err := r.GetAllHealthy(storegateway.BlocksRead)
			return err == nil && len(all.Instances) > 0
		})

		return s
	}

	t.Run("no preferred zone configured", func(t *testing.T) {
		s := setupBlocksStoreReplicationSet(t, nil)

		// Request the same block multiple times and ensure the distribution of
		// requests across store-gateways is balanced.
		distribution := map[string]int{}

		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)

			for addr := range getStoreGatewayClientAddrs(clients) {
				distribution[addr]++
			}
		}

		assert.Len(t, distribution, numInstances)
		for addr, count := range distribution {
			// Ensure that the number of times each client is returned is above
			// the 80% of the perfect even distribution.
			assert.Greaterf(t, float64(count), (float64(numRuns)/float64(numInstances))*0.8, "store-gateway address: %s", addr)
		}
	})

	t.Run("preferred zone configured", func(t *testing.T) {
		const preferredZone = "zone-1"
		s := setupBlocksStoreReplicationSet(t, []string{preferredZone})

		// Request the same block multiple times. We expect we always get the store-gateway in the preferred zone.
		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)
			for client := range clients {
				require.Equal(t, preferredZone, client.RemoteZone(), "should always return store-gateway from preferred zone")
			}
		}

		// Request the same block multiple times, excluding the store-gateway running in the preferred zone.
		// We expect the distribution of requests across other store-gateway zones to be balanced.
		distribution := map[string]int{}

		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, map[ulid.ULID][]string{block1.ID: {"127.0.0.1"}})
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)

			for addr := range getStoreGatewayClientAddrs(clients) {
				distribution[addr]++
			}
		}

		assert.Len(t, distribution, numInstances-1)
		for addr, count := range distribution {
			// Ensure that the number of times each client is returned is above
			// the 80% of the perfect even distribution.
			assert.Greaterf(t, float64(count), (float64(numRuns)/float64(numInstances-1))*0.8, "store-gateway address: %s", addr)
		}
	})

	t.Run("multiple preferred zones configured", func(t *testing.T) {
		preferredZones := []string{"zone-1", "zone-2"}
		s := setupBlocksStoreReplicationSet(t, preferredZones)

		// Count which zones are hit across multiple requests.
		zoneDistribution := make(map[string]int)

		// Request the same block multiple times. We expect we always get a store-gateway in one of the preferred zones.
		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)
			for client := range clients {
				zone := client.RemoteZone()
				zoneDistribution[zone]++

				// We should always get one of the preferred zones
				assert.Contains(t, preferredZones, zone, "should always query a store-gateway in one of the preferred zones")
			}
		}

		// Verify only the preferred zones were used (zone-3 should never be selected)
		assert.Len(t, zoneDistribution, 2, "only the 2 preferred zones should be selected")
		assert.NotContains(t, zoneDistribution, "zone-3", "non-preferred zone-3 should never be selected")

		// Verify both preferred zones get approximately equal distribution (due to random shuffling)
		zone1Hits := zoneDistribution["zone-1"]
		zone2Hits := zoneDistribution["zone-2"]
		require.Greater(t, zone1Hits, 0, "zone-1 should be selected at least once")
		require.Greater(t, zone2Hits, 0, "zone-2 should be selected at least once")

		ratio := float64(zone1Hits) / float64(zone2Hits)
		// Allow reasonable deviation from perfect 1:1 ratio due to randomness
		assert.Greater(t, ratio, 0.5, "zone-1 and zone-2 should have roughly equal distribution")
		assert.Less(t, ratio, 2.0, "zone-1 and zone-2 should have roughly equal distribution")

		// Request the same block multiple times, excluding zone-1 (127.0.0.1).
		// We expect to always get zone-2 (the remaining preferred zone).
		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, map[ulid.ULID][]string{block1.ID: {"127.0.0.1"}})
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)
			for client := range clients {
				require.Equal(t, "zone-2", client.RemoteZone(), "with zone-1 excluded, should always return zone-2 (remaining preferred zone)")
			}
		}

		// Request the same block multiple times, excluding both preferred zones (zone-1 and zone-2).
		// We expect to fall back to zone-3 (non-preferred zone).
		for n := 0; n < numRuns; n++ {
			clients, err := s.GetClientsFor(userID, []*bucketindex.Block{block1}, map[ulid.ULID][]string{block1.ID: {"127.0.0.1", "127.0.0.2"}})
			require.NoError(t, err)
			t.Cleanup(func() {
				// Close all clients to ensure no goroutines are leaked.
				for c := range clients {
					c.(io.Closer).Close() //nolint:errcheck
				}
			})

			require.Len(t, clients, 1)
			for client := range clients {
				require.Equal(t, "zone-3", client.RemoteZone(), "with both preferred zones excluded, should fall back to zone-3")
			}
		}
	})
}

// TestBlocksStoreReplicationSet_GetClientsFor_BufferReuseSafety verifies that reusing
// ring buffers across multiple block lookups in GetClientsFor doesn't corrupt instance data.
func TestBlocksStoreReplicationSet_GetClientsFor_BufferReuseSafety(t *testing.T) {
	// Use block IDs with different hash values to ensure they map to different instances.
	blockID1 := ulid.MustNew(1, nil) // hash: 283204220
	blockID2 := ulid.MustNew(2, nil) // hash: 444110359
	blockID3 := ulid.MustNew(5, nil) // hash: 2931974232
	blockID4 := ulid.MustNew(6, nil) // hash: 3092880371

	block1Hash := mimir_tsdb.HashBlockID(blockID1)
	block2Hash := mimir_tsdb.HashBlockID(blockID2)
	block3Hash := mimir_tsdb.HashBlockID(blockID3)
	block4Hash := mimir_tsdb.HashBlockID(blockID4)

	minT := time.Now().Add(-5 * time.Hour)
	maxT := minT.Add(2 * time.Hour)

	blocks := bucketindex.Blocks{
		newBlock(blockID1, minT, maxT),
		newBlock(blockID2, minT, maxT),
		newBlock(blockID3, minT, maxT),
		newBlock(blockID4, minT, maxT),
	}

	userID := "user-A"
	registeredAt := time.Now()
	ctx := context.Background()

	// Setup the ring with instances in different zones.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	require.NoError(t, ringStore.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		// Each instance owns one block and is in a distinct zone.
		d.AddIngester("instance-1", "127.0.0.1", "zone-a", []uint32{block1Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
		d.AddIngester("instance-2", "127.0.0.2", "zone-b", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
		d.AddIngester("instance-3", "127.0.0.3", "zone-c", []uint32{block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
		d.AddIngester("instance-4", "127.0.0.4", "zone-d", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
		return d, true, nil
	}))

	ringCfg := ring.Config{}
	flagext.DefaultValues(&ringCfg)
	ringCfg.ReplicationFactor = 1

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, "test", "test", ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, r))
	})

	limits := &blocksStoreLimitsMock{storeGatewayTenantShardSize: 0}

	reg := prometheus.NewPedanticRegistry()
	s, err := newBlocksStoreReplicationSet(r, noLoadBalancing, storegateway.NewNopDynamicReplication(ringCfg.ReplicationFactor), nil, limits, StoreGatewayClientConfig{}, log.NewNopLogger(), reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, s))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, s))
	})

	// Wait until the ring client has initialised the state.
	test.Poll(t, time.Second, true, func() interface{} {
		all, err := r.GetAllHealthy(storegateway.BlocksRead)
		return err == nil && len(all.Instances) == 4
	})

	// Query all blocks at once. This exercises the buffer reuse across multiple block lookups.
	clients, err := s.GetClientsFor(userID, blocks, nil)
	require.NoError(t, err)
	defer func() {
		for c := range clients {
			c.(io.Closer).Close() //nolint:errcheck
		}
	}()

	// Verify we got the expected number of clients (one per block since RF=1 and each instance owns one block).
	assert.Len(t, clients, 4)

	// Build a map from address to zone for verification.
	expectedZoneByAddr := map[string]string{
		"127.0.0.1": "zone-a",
		"127.0.0.2": "zone-b",
		"127.0.0.3": "zone-c",
		"127.0.0.4": "zone-d",
	}

	// Verify that each client has the correct zone information.
	// If buffer reuse corrupted the instance data, the zones would be wrong.
	for client := range clients {
		addr := client.RemoteAddress()
		expectedZone, ok := expectedZoneByAddr[addr]
		require.True(t, ok, "unexpected client address: %s", addr)
		assert.Equal(t, expectedZone, client.RemoteZone(), "zone mismatch for client at %s - this could indicate buffer reuse corruption", addr)
	}

	// Verify the block assignment is correct.
	clientAddrs := getStoreGatewayClientAddrs(clients)
	expectedClients := map[string][]ulid.ULID{
		"127.0.0.1": {blockID1},
		"127.0.0.2": {blockID2},
		"127.0.0.3": {blockID3},
		"127.0.0.4": {blockID4},
	}
	assert.Equal(t, expectedClients, clientAddrs)
}

func BenchmarkBlocksStoreReplicationSet_GetClientsFor(b *testing.B) {
	const (
		numInstancesPerZone = 300
		numTokens           = 512
		numBlocks           = 10
		replicationFactor   = 3
		userID              = "user-1"
	)

	zones := []string{"zone-1", "zone-2", "zone-3"}

	// Create recent blocks (triggers dynamic replication when enabled).
	now := time.Now()
	blocks := make(bucketindex.Blocks, numBlocks)
	for i := range numBlocks {
		blocks[i] = &bucketindex.Block{
			ID:      ulid.MustNew(uint64(i+1), nil),
			MinTime: now.Add(-2 * time.Hour).UnixMilli(),
			MaxTime: now.Add(-1 * time.Hour).UnixMilli(),
		}
	}

	testCases := []struct {
		name               string
		zone3State         ring.InstanceState
		dynamicReplication bool
	}{
		{"all zones ACTIVE", ring.ACTIVE, false},
		{"one zone JOINING", ring.JOINING, false},
		{"one zone LEAVING", ring.LEAVING, false},
		{"all zones ACTIVE with dynamic replication", ring.ACTIVE, true},
		{"one zone JOINING with dynamic replication", ring.JOINING, true},
		{"one zone LEAVING with dynamic replication", ring.LEAVING, true},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			registeredAt := time.Now()

			// Setup ring store.
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			b.Cleanup(func() { _ = closer.Close() })

			// Populate ring.
			err := ringStore.CAS(ctx, storegateway.RingKey, func(interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				tokenGen := ring.NewRandomTokenGeneratorWithSeed(42)

				for _, zone := range zones {
					state := ring.ACTIVE
					if zone == "zone-3" {
						state = tc.zone3State
					}
					for i := 0; i < numInstancesPerZone; i++ {
						id := fmt.Sprintf("%s-instance-%d", zone, i)
						d.AddIngester(id, id, zone, tokenGen.GenerateTokens(numTokens, nil), state, registeredAt, false, time.Time{}, nil)
					}
				}
				return d, true, nil
			})
			require.NoError(b, err)

			// Create the ring client.
			ringCfg := ring.Config{}
			flagext.DefaultValues(&ringCfg)
			ringCfg.ReplicationFactor = replicationFactor
			ringCfg.ZoneAwarenessEnabled = true

			r, err := ring.NewWithStoreClientAndStrategy(ringCfg, storegateway.RingNameForClient, storegateway.RingKey, ringStore, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
			require.NoError(b, err)
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(ctx, r))
			})

			// Create dynamic replication.
			var dynRepl storegateway.DynamicReplication
			if tc.dynamicReplication {
				dynRepl = storegateway.NewMaxTimeDynamicReplication(storegateway.Config{
					ShardingRing:       storegateway.RingConfig{ReplicationFactor: replicationFactor},
					DynamicReplication: storegateway.DynamicReplicationConfig{Enabled: true, MaxTimeThreshold: 25 * time.Hour, Multiple: 5},
				}, 0)
			} else {
				dynRepl = storegateway.NewNopDynamicReplication(replicationFactor)
			}

			// Create blocksStoreReplicationSet.
			limits := &blocksStoreLimitsMock{storeGatewayTenantShardSize: 0}
			s, err := newBlocksStoreReplicationSet(r, noLoadBalancing, dynRepl, nil, limits, StoreGatewayClientConfig{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(b, err)
			require.NoError(b, services.StartAndAwaitRunning(ctx, s))
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(ctx, s))
			})

			// Wait for ring client initialization.
			test.Poll(b, 5*time.Second, numInstancesPerZone*len(zones), func() interface{} {
				all, err := r.GetAllHealthy(storegateway.BlocksOwnerSync)
				if err != nil {
					return 0
				}
				return len(all.Instances)
			})

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				clients, err := s.GetClientsFor(userID, blocks, nil)
				if err != nil {
					b.Fatal(err)
				}
				for c := range clients {
					_ = c.(io.Closer).Close()
				}
			}
		})
	}
}

func getStoreGatewayClientAddrs(clients map[BlocksStoreClient][]ulid.ULID) map[string][]ulid.ULID {
	addrs := map[string][]ulid.ULID{}
	for c, blockIDs := range clients {
		addrs[c.RemoteAddress()] = blockIDs
	}
	return addrs
}
