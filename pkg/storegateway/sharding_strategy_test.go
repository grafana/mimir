// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/sharding_strategy_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/extprom"
)

func TestShuffleShardingStrategy(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371
	numAllBlocks := 4

	block1Hash := mimir_tsdb.HashBlockID(block1)
	block2Hash := mimir_tsdb.HashBlockID(block2)
	block3Hash := mimir_tsdb.HashBlockID(block3)
	block4Hash := mimir_tsdb.HashBlockID(block4)

	userID := "user-A"
	registeredAt := time.Now()

	type usersExpectation struct {
		instanceID   string
		instanceAddr string
		users        []string
		err          error
	}

	type blocksExpectation struct {
		instanceID   string
		instanceAddr string
		blocks       []ulid.ULID
	}

	tests := map[string]struct {
		replicationFactor  int
		dynamicReplication bool
		limits             ShardingLimits
		setupRing          func(*ring.Desc)
		prevLoadedBlocks   map[string]map[ulid.ULID]struct{}
		expectedUsers      []usersExpectation
		expectedBlocks     []blocksExpectation
	}{
		"one ACTIVE instance in the ring with RF = 1 and SS = 1": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"one ACTIVE instance in the ring with RF = 2 and SS = 1 (should still sync blocks on the only available instance)": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"one ACTIVE instance in the ring with RF = 2 and SS = 2 (should still sync blocks on the only available instance)": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 1 and SS = 1 (should sync blocks on 1 instance because of the shard size)": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 1 and SS = 2 (should sync blocks on 2 instances because of the shard size)": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2, block4}},
			},
		},
		"two ACTIVE instances in the ring with RF = 2 and SS = 1 (should sync blocks on 1 instance because of the shard size)": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 1},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{}},
			},
		},
		"two ACTIVE instances in the ring with RF = 2 and SS = 2 (should sync all blocks on 2 instances)": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block1, block2, block3, block4}},
			},
		},
		"multiple ACTIVE instances in the ring with RF = 2 and SS = 3": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3 /* replicated: */, block2, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2 /* replicated: */, block1}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4 /* replicated: */, block3}},
			},
		},
		"multiple ACTIVE instances in the ring with RF = 1 and SS = 3 and DR = true": {
			replicationFactor:  1,
			dynamicReplication: true,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3 /* extended replication: */, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4}},
			},
		},
		"one unhealthy instance in the ring with RF = 1, SS = 3 and NO previously loaded blocks": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				// No shard has the blocks of the unhealthy instance.
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{}},
			},
		},
		"one unhealthy instance in the ring with RF = 2, SS = 3 and NO previously loaded blocks": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 3},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3 /* replicated: */, block2, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2 /* replicated: */, block1}},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{}},
			},
		},
		"one unhealthy instance in the ring with RF = 2, SS = 2 and NO previously loaded blocks": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block3Hash + 1},
				}
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{ /* no blocks because unhealthy */ }},
			},
		},
		"one unhealthy instance in the ring with RF = 2, SS = 2 and some previously loaded blocks": {
			replicationFactor:  2,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				r.Ingesters["instance-3"] = ring.InstanceDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block3Hash + 1},
				}
			},
			prevLoadedBlocks: map[string]map[ulid.ULID]struct{}{
				"instance-3": {block2: struct{}{}, block4: struct{}{}},
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", err: errStoreGatewayUnhealthy},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3, block4}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block2, block4 /* keeping the previously loaded blocks */}},
			},
		},
		"LEAVING instance in the ring should continue to keep its shard blocks and they should NOT be replicated to another instance": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.LEAVING, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4}},
			},
		},
		"JOINING instance in the ring should get its shard blocks and they should not be replicated to another instance": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 2},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.JOINING, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: nil},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block2, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{ /* no blocks because not belonging to the shard */ }},
				{instanceID: "instance-3", instanceAddr: "127.0.0.3", blocks: []ulid.ULID{block4}},
			},
		},
		"SS = 0 disables shuffle sharding": {
			replicationFactor:  1,
			dynamicReplication: false,
			limits:             &shardingLimitsMock{storeGatewayTenantShardSize: 0},
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expectedUsers: []usersExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", users: []string{userID}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", users: []string{userID}},
			},
			expectedBlocks: []blocksExpectation{
				{instanceID: "instance-1", instanceAddr: "127.0.0.1", blocks: []ulid.ULID{block1, block3}},
				{instanceID: "instance-2", instanceAddr: "127.0.0.2", blocks: []ulid.ULID{block2, block4}},
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			now := time.Now()
			ctx := context.Background()
			store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Initialize dynamic replication if enabled
			var dynamicReplication DynamicReplication = NewNopDynamicReplication(testData.replicationFactor)
			if testData.dynamicReplication {
				dynamicReplication = NewMaxTimeDynamicReplication(Config{
					ShardingRing: RingConfig{ReplicationFactor: testData.replicationFactor},
					DynamicReplication: DynamicReplicationConfig{
						Enabled:          true,
						MaxTimeThreshold: 25 * time.Hour,
						Multiple:         2,
					},
				}, 45*time.Minute)
			}

			// Initialize the ring state.
			require.NoError(t, store.CAS(ctx, "test", func(interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setupRing(d)
				return d, true, nil
			}))

			cfg := ring.Config{
				ReplicationFactor:    testData.replicationFactor,
				HeartbeatTimeout:     time.Minute,
				SubringCacheDisabled: true,
			}

			r, err := ring.NewWithStoreClientAndStrategy(cfg, "test", "test", store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(ctx, r)
			})

			// Wait until the ring client has synced.
			require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-1", ring.ACTIVE))

			// Assert on filter users.
			for _, expected := range testData.expectedUsers {
				filter := NewShuffleShardingStrategy(r, expected.instanceID, expected.instanceAddr, dynamicReplication, testData.limits, log.NewNopLogger())
				actualUsers, err := filter.FilterUsers(ctx, []string{userID})
				assert.Equal(t, expected.err, err)
				assert.Equal(t, expected.users, actualUsers)
			}

			// Assert on filter blocks.
			for _, expected := range testData.expectedBlocks {
				filter := NewShuffleShardingStrategy(r, expected.instanceID, expected.instanceAddr, dynamicReplication, testData.limits, log.NewNopLogger())
				synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
				synced.WithLabelValues(shardExcludedMeta).Set(0)

				metas := map[ulid.ULID]*block.Meta{
					block1: {
						BlockMeta: tsdb.BlockMeta{
							MinTime: now.Add(-5 * 24 * time.Hour).UnixMilli(),
							MaxTime: now.Add(-4 * 24 * time.Hour).UnixMilli(),
						},
					},
					block2: {
						BlockMeta: tsdb.BlockMeta{
							MinTime: now.Add(-4 * 24 * time.Hour).UnixMilli(),
							MaxTime: now.Add(-3 * 24 * time.Hour).UnixMilli(),
						},
					},
					block3: {
						BlockMeta: tsdb.BlockMeta{
							MinTime: now.Add(-3 * 24 * time.Hour).UnixMilli(),
							MaxTime: now.Add(-2 * 24 * time.Hour).UnixMilli(),
						},
					},
					block4: {
						BlockMeta: tsdb.BlockMeta{
							MinTime: now.Add(-2 * 24 * time.Hour).UnixMilli(),
							MaxTime: now.Add(-1 * 24 * time.Hour).UnixMilli(),
						},
					},
				}

				err = filter.FilterBlocks(ctx, userID, metas, testData.prevLoadedBlocks[expected.instanceID], synced)
				require.NoError(t, err)

				var actualBlocks []ulid.ULID
				for id := range metas {
					actualBlocks = append(actualBlocks, id)
				}

				assert.ElementsMatch(t, expected.blocks, actualBlocks)

				// Assert on the metric used to keep track of the blocks filtered out.
				synced.Submit()
				assert.Equal(t, float64(numAllBlocks-len(expected.blocks)), testutil.ToFloat64(synced))
			}
		})
	}
}

type shardingLimitsMock struct {
	storeGatewayTenantShardSize        int
	storeGatewayTenantShardSizePerZone int
}

func (m *shardingLimitsMock) StoreGatewayTenantShardSize(_ string) int {
	return m.storeGatewayTenantShardSize
}

func (m *shardingLimitsMock) StoreGatewayTenantShardSizePerZone(_ string) int {
	return m.storeGatewayTenantShardSizePerZone
}

// TestShuffleShardingStrategy_RFMigration tests that during zone-aware replication factor migrations,
// blocks are never reshuffled in the zones we don't touch. This is critical because:
// 1. Queriers will try to query blocks from wrong store-gateways and queries will fail
// 2. Store-gateways will have to do a bunch of work (data transfer and writes to disk) to download index-headers
//
// For RF3 to RF4 migration (scaling up):
// 1. Start with RF=3, 3 zones (zone-a, zone-b, zone-c)
// 2. Increase RF to 4 (without adding instances)
// 3. Add zone-d instances
// 4. Remove zone-c instances
// 5. Re-add zone-c instances with new tokens
//
// we expect that throughout this process, blocks owned by zone-a and zone-b instances should never change.
//
// For RF4 to RF3 migration (scaling down):
// 1. Start with RF=4, 4 zones (zone-a, zone-b, zone-c, zone-d)
// 2. Remove zone-d instances
// 3. Decrease RF to 3
// Throughout this process, blocks owned by zone-a, zone-b, and zone-c instances should never change.
func TestShuffleShardingStrategy_RFMigration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name               string
		shardSize          int
		shardSizePerZone   int
		instancesPerZone   int
		dynamicReplication bool
	}{
		{
			name:             "shuffle sharding disabled, 3 instances per zone",
			shardSize:        0,
			instancesPerZone: 3,
		},
		{
			name:               "shuffle sharding disabled with dynamic replication, 3 instances per zone",
			shardSize:          0,
			instancesPerZone:   3,
			dynamicReplication: true,
		},
		{
			name:             "shuffle sharding enabled with shard size = 3, 3 instances per zone",
			shardSize:        3,
			instancesPerZone: 3,
		},
		{
			name:             "shuffle sharding enabled with shard size = 6, 3 instances per zone",
			shardSize:        6,
			instancesPerZone: 3,
		},
		{
			name:             "shuffle sharding enabled with per-zone shard size = 3, 5 instances per zone",
			shardSizePerZone: 3,
			instancesPerZone: 5,
		},
		{
			name:               "shuffle sharding enabled with per-zone shard size = 3, with dynamic replication, 5 instances per zone",
			shardSizePerZone:   3,
			instancesPerZone:   5,
			dynamicReplication: true,
		},
	}

	for _, tc := range testCases {
		t.Run("RF3_to_RF4_scaling_up/"+tc.name, func(t *testing.T) {
			t.Parallel()
			runRF3toRF4MigrationTest(t, tc.shardSize, tc.shardSizePerZone, tc.instancesPerZone, tc.dynamicReplication)
		})

		t.Run("RF4_to_RF3_scaling_down/"+tc.name, func(t *testing.T) {
			t.Parallel()
			runRF4toRF3MigrationTest(t, tc.shardSize, tc.shardSizePerZone, tc.instancesPerZone, tc.dynamicReplication)
		})
	}
}

func runRF3toRF4MigrationTest(t *testing.T, shardSize, shardSizePerZone, instancesPerZone int, dynamicReplication bool) {
	t.Helper()

	ctx := context.Background()
	registeredAt := time.Now()

	// Initialize a random number generator with a constant seed for reproducibility.
	random := rand.New(rand.NewSource(0))

	// Generate a large number of block IDs for comprehensive testing.
	const numBlocks = 100
	blocks := make([]ulid.ULID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks[i] = ulid.MustNew(0, random)
	}

	// Generate multiple user IDs to test FilterUsers as well.
	const numUsers = 10
	userIDs := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		userIDs[i] = fmt.Sprintf("user-%d", i)
	}

	// Create initial instances for 3 zones.
	zoneAInstances := createZoneInstances(random, "zone-a", instancesPerZone)
	zoneBInstances := createZoneInstances(random, "zone-b", instancesPerZone)
	zoneCInstances := createZoneInstances(random, "zone-c", instancesPerZone)

	// Create KV store.
	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	limits := &shardingLimitsMock{
		storeGatewayTenantShardSize:        shardSize,
		storeGatewayTenantShardSizePerZone: shardSizePerZone,
	}

	t.Log("Step 1: Setting up initial ring with 3 zones, RF=3")

	require.NoError(t, store.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		addInstancesToRing(d, zoneAInstances, registeredAt)
		addInstancesToRing(d, zoneBInstances, registeredAt)
		addInstancesToRing(d, zoneCInstances, registeredAt)
		return d, true, nil
	}))

	var gatewayCfg Config
	flagext.DefaultValues(&gatewayCfg)
	ringCfg := gatewayCfg.ShardingRing.ToRingConfig()
	ringCfg.ZoneAwarenessEnabled = true
	require.Equal(t, 3, ringCfg.ReplicationFactor, "pre-condition check: the initial replication factor must be 3")

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))

	// Track the current ring for cleanup. We'll update this as we create new rings.
	currentRing := r
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, currentRing) })

	// Wait until the ring client has synced.
	require.NoError(t, ring.WaitInstanceState(ctx, r, zoneAInstances[0].id, ring.ACTIVE))

	// Capture baseline state for all instances.
	var allInitialInstances []instanceDef
	allInitialInstances = append(allInitialInstances, zoneAInstances...)
	allInitialInstances = append(allInitialInstances, zoneBInstances...)
	allInitialInstances = append(allInitialInstances, zoneCInstances...)

	baselineState := captureState(t, ctx, r, allInitialInstances, 3, limits, userIDs, blocks, dynamicReplication)

	// Ensure every instance gets some blocks when shuffle sharding is disabled.
	if shardSize == 0 && shardSizePerZone == 0 {
		for instance, state := range baselineState {
			require.NotEmpty(t, state.blocksPerUser, "no blocks owned by instance %s", instance)
		}
	}

	t.Logf("Baseline captured: %d instances", len(baselineState))

	t.Log("Step 2: Changing RF from 3 to 4 (no instance changes)")

	// Stop the old ring and create a new one with RF=4.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, r))

	ringCfg.ReplicationFactor = 4
	r, err = ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	currentRing = r // Update for cleanup

	// Wait for ring to sync.
	require.NoError(t, ring.WaitInstanceState(ctx, r, zoneAInstances[0].id, ring.ACTIVE))

	// Verify all instances have the same blocks as before.
	stateAfterRFChange := captureState(t, ctx, r, allInitialInstances, ringCfg.ReplicationFactor, limits, userIDs, blocks, dynamicReplication)
	assertStateUnchanged(t, baselineState, stateAfterRFChange,
		getInstanceIDs(zoneAInstances, zoneBInstances, zoneCInstances))

	t.Log("Step 2 passed: RF change did not reshuffle blocks")

	t.Log("Step 3: Adding zone-d instances")

	// Simulate the gradual addition of zone-d instances, like would happen in the real world
	// when a new zone is added (different instances don't join the ring at the same exact time).
	for _, numZoneDInstances := range []int{1, instancesPerZone / 2, instancesPerZone} {
		t.Run(fmt.Sprintf("zone-d instances=%d", numZoneDInstances), func(t *testing.T) {
			zoneDInstances := createZoneInstances(random, "zone-d", numZoneDInstances)

			require.NoError(t, store.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				d := in.(*ring.Desc)
				addInstancesToRing(d, zoneDInstances, registeredAt)
				return d, true, nil
			}))

			// Wait for zone-d instances to appear in the ring.
			require.NoError(t, ring.WaitInstanceState(ctx, r, zoneDInstances[0].id, ring.ACTIVE))

			// Verify zone-a, zone-b, zone-c instances have the same blocks as baseline.
			stateAfterZoneDAdded := captureState(t, ctx, r, allInitialInstances, ringCfg.ReplicationFactor, limits, userIDs, blocks, dynamicReplication)
			assertStateUnchanged(t, baselineState, stateAfterZoneDAdded,
				getInstanceIDs(zoneAInstances, zoneBInstances, zoneCInstances))

			// In case there's only 1 zone-d instance, we expect that instance to own all blocks.
			if numZoneDInstances == 1 {
				require.Len(t, zoneDInstances, 1)
				zoneDState := captureState(t, ctx, r, zoneDInstances, ringCfg.ReplicationFactor, limits, userIDs, blocks, dynamicReplication)
				instState, ok := zoneDState[zoneDInstances[0].id]
				require.True(t, ok, "zone-d instance %s not found in captured state", zoneDInstances[0].id)
				require.NotEmpty(t, instState.blocksPerUser, "zone-d instance %s should own blocks for users", zoneDInstances[0].id)
				for user, userBlocks := range instState.blocksPerUser {
					require.Len(t, userBlocks, numBlocks, "user %s has wrong number of blocks", user)
				}
			}
		})
	}

	t.Log("Step 3 passed: Adding zone-d did not reshuffle blocks in existing zones")

	t.Log("Step 4: Removing zone-c instances")

	require.NoError(t, store.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
		d := in.(*ring.Desc)
		removeInstancesFromRing(d, zoneCInstances)
		return d, true, nil
	}))

	// Wait for the ring to update (zone-c instances should be gone).
	// We wait for zone-a instance to still be active as a proxy for ring sync.
	require.Eventually(t, func() bool {
		replicationSet, err := r.GetAllHealthy(BlocksOwnerSync)
		if err != nil {
			return false
		}
		// Check that zone-c instances are gone.
		for _, inst := range zoneCInstances {
			if replicationSet.Includes(inst.addr) {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "zone-c instances should be removed from ring")

	// Verify zone-a and zone-b instances have the same blocks as baseline.
	var zoneABInstances []instanceDef
	zoneABInstances = append(zoneABInstances, zoneAInstances...)
	zoneABInstances = append(zoneABInstances, zoneBInstances...)

	stateAfterZoneCRemoved := captureState(t, ctx, r, zoneABInstances, 4, limits, userIDs, blocks, dynamicReplication)
	assertStateUnchanged(t, baselineState, stateAfterZoneCRemoved,
		getInstanceIDs(zoneAInstances, zoneBInstances))

	t.Log("Step 4 passed: Removing zone-c did not reshuffle blocks in zone-a and zone-b")

	t.Log("Step 5: Re-adding zone-c instances with new tokens")

	// Create new zone-c instances with different tokens.
	newZoneCInstances := createZoneInstances(random, "zone-c", instancesPerZone)

	require.NoError(t, store.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
		d := in.(*ring.Desc)
		addInstancesToRing(d, newZoneCInstances, registeredAt)
		return d, true, nil
	}))

	// Wait for new zone-c instances to appear in the ring.
	require.NoError(t, ring.WaitInstanceState(ctx, r, newZoneCInstances[0].id, ring.ACTIVE))

	// Verify zone-a and zone-b instances have the same blocks as baseline.
	stateAfterZoneCReAdded := captureState(t, ctx, r, zoneABInstances, 4, limits, userIDs, blocks, dynamicReplication)
	assertStateUnchanged(t, baselineState, stateAfterZoneCReAdded,
		getInstanceIDs(zoneAInstances, zoneBInstances))

	t.Log("Step 5 passed: Re-adding zone-c with new tokens did not reshuffle blocks in zone-a and zone-b")

	t.Log("All migration steps completed successfully - blocks were never reshuffled in untouched zones")
}

func runRF4toRF3MigrationTest(t *testing.T, shardSize, shardSizePerZone, instancesPerZone int, dynamicReplication bool) {
	t.Helper()

	ctx := context.Background()
	registeredAt := time.Now()

	// Initialize a random number generator with a constant seed for reproducibility.
	random := rand.New(rand.NewSource(0))

	// Generate a large number of block IDs for comprehensive testing.
	const numBlocks = 100
	blocks := make([]ulid.ULID, numBlocks)
	for i := 0; i < numBlocks; i++ {
		blocks[i] = ulid.MustNew(0, random)
	}

	// Generate multiple user IDs to test FilterUsers as well.
	const numUsers = 10
	userIDs := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		userIDs[i] = fmt.Sprintf("user-%d", i)
	}

	// Create initial instances for 4 zones.
	zoneAInstances := createZoneInstances(random, "zone-a", instancesPerZone)
	zoneBInstances := createZoneInstances(random, "zone-b", instancesPerZone)
	zoneCInstances := createZoneInstances(random, "zone-c", instancesPerZone)
	zoneDInstances := createZoneInstances(random, "zone-d", instancesPerZone)

	// Create KV store.
	store, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	limits := &shardingLimitsMock{
		storeGatewayTenantShardSize:        shardSize,
		storeGatewayTenantShardSizePerZone: shardSizePerZone,
	}

	t.Log("Step 1: Setting up initial ring with 4 zones, RF=4")

	require.NoError(t, store.CAS(ctx, RingKey, func(interface{}) (interface{}, bool, error) {
		d := ring.NewDesc()
		addInstancesToRing(d, zoneAInstances, registeredAt)
		addInstancesToRing(d, zoneBInstances, registeredAt)
		addInstancesToRing(d, zoneCInstances, registeredAt)
		addInstancesToRing(d, zoneDInstances, registeredAt)
		return d, true, nil
	}))

	var gatewayCfg Config
	flagext.DefaultValues(&gatewayCfg)
	ringCfg := gatewayCfg.ShardingRing.ToRingConfig()
	ringCfg.ZoneAwarenessEnabled = true
	ringCfg.ReplicationFactor = 4

	r, err := ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))

	currentRing := r
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(ctx, currentRing) })

	require.NoError(t, ring.WaitInstanceState(ctx, r, zoneAInstances[0].id, ring.ACTIVE))

	// Capture baseline state for all instances.
	var allInitialInstances []instanceDef
	allInitialInstances = append(allInitialInstances, zoneAInstances...)
	allInitialInstances = append(allInitialInstances, zoneBInstances...)
	allInitialInstances = append(allInitialInstances, zoneCInstances...)
	allInitialInstances = append(allInitialInstances, zoneDInstances...)

	baselineState := captureState(t, ctx, r, allInitialInstances, 4, limits, userIDs, blocks, dynamicReplication)

	if shardSize == 0 && shardSizePerZone == 0 {
		for instance, state := range baselineState {
			require.NotEmpty(t, state.blocksPerUser, "no blocks owned by instance %s", instance)
		}
	}

	t.Logf("Baseline captured: %d instances", len(baselineState))

	t.Log("Step 2: Removing zone-d instances")

	require.NoError(t, store.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
		d := in.(*ring.Desc)
		removeInstancesFromRing(d, zoneDInstances)
		return d, true, nil
	}))

	require.Eventually(t, func() bool {
		replicationSet, err := r.GetAllHealthy(BlocksOwnerSync)
		if err != nil {
			return false
		}
		for _, inst := range zoneDInstances {
			if replicationSet.Includes(inst.addr) {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond, "zone-d instances should be removed from ring")

	// Verify zone-a, zone-b, zone-c instances have the same blocks as baseline.
	var zoneABCInstances []instanceDef
	zoneABCInstances = append(zoneABCInstances, zoneAInstances...)
	zoneABCInstances = append(zoneABCInstances, zoneBInstances...)
	zoneABCInstances = append(zoneABCInstances, zoneCInstances...)

	stateAfterZoneDRemoved := captureState(t, ctx, r, zoneABCInstances, 4, limits, userIDs, blocks, dynamicReplication)
	assertStateUnchanged(t, baselineState, stateAfterZoneDRemoved,
		getInstanceIDs(zoneAInstances, zoneBInstances, zoneCInstances))

	t.Log("Step 2 passed: Removing zone-d did not reshuffle blocks in zone-a, zone-b, zone-c")

	t.Log("Step 3: Changing RF from 4 to 3")

	require.NoError(t, services.StopAndAwaitTerminated(ctx, r))

	ringCfg.ReplicationFactor = 3
	r, err = ring.NewWithStoreClientAndStrategy(ringCfg, RingNameForServer, RingKey, store, ring.NewIgnoreUnhealthyInstancesReplicationStrategy(), nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, r))
	currentRing = r

	require.NoError(t, ring.WaitInstanceState(ctx, r, zoneAInstances[0].id, ring.ACTIVE))

	// Verify zone-a, zone-b, zone-c instances have the same blocks as baseline.
	stateAfterRFChange := captureState(t, ctx, r, zoneABCInstances, ringCfg.ReplicationFactor, limits, userIDs, blocks, dynamicReplication)
	assertStateUnchanged(t, baselineState, stateAfterRFChange,
		getInstanceIDs(zoneAInstances, zoneBInstances, zoneCInstances))

	t.Log("Step 3 passed: RF change did not reshuffle blocks")

	t.Log("All migration steps completed successfully - blocks were never reshuffled in untouched zones")
}

// instanceDef defines an instance in the ring for migration tests.
type instanceDef struct {
	id     string
	addr   string
	zone   string
	tokens []uint32
}

// instanceState captures the state of an instance for comparison.
// We store blocks per-user to ensure we're tracking the exact same block ownership,
// not just aggregate counts which could mask redistribution issues.
type instanceState struct {
	users         []string
	blocksPerUser map[string][]ulid.ULID // userID -> blocks owned for that user
}

func createZoneInstances(random *rand.Rand, zone string, numInstances int) []instanceDef {
	generator := ring.NewRandomTokenGeneratorWithSeed(random.Int63())
	instances := make([]instanceDef, numInstances)
	for i := 0; i < numInstances; i++ {
		id := fmt.Sprintf("store-gateway-zone-%s-%d", zone, i+1)
		instances[i] = instanceDef{
			id:     id,
			addr:   id,
			zone:   zone,
			tokens: generator.GenerateTokens(ringNumTokensDefault, nil),
		}
	}
	return instances
}

// createDynamicReplication creates a DynamicReplication based on configuration.
func createDynamicReplication(withDynamicReplication bool, rf int) DynamicReplication {
	if !withDynamicReplication {
		return NewNopDynamicReplication(rf)
	}

	return NewMaxTimeDynamicReplication(Config{
		ShardingRing: RingConfig{ReplicationFactor: rf},
		DynamicReplication: DynamicReplicationConfig{
			Enabled:          true,
			MaxTimeThreshold: 25 * time.Hour,
			Multiple:         2,
		},
	}, 45*time.Minute)
}

// createBlockMetas creates block metadata. When dynamic replication is enabled, only 40% of blocks
// are marked as "recent" (within the MaxTimeThreshold), the rest are older blocks.
func createBlockMetas(withDynamicReplication bool, blocks []ulid.ULID) map[ulid.ULID]*block.Meta {
	metas := make(map[ulid.ULID]*block.Meta, len(blocks))
	for i, blockID := range blocks {
		var maxTime int64
		if withDynamicReplication && i >= len(blocks)*40/100 {
			// Old block: 48 hours ago (beyond the 25 hour threshold)
			maxTime = time.Now().Add(-48 * time.Hour).UnixMilli()
		} else {
			// Recent block: now
			maxTime = time.Now().UnixMilli()
		}
		metas[blockID] = &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    blockID,
				MaxTime: maxTime,
			},
		}
	}
	return metas
}

// captureState captures the state for all instances.
func captureState(t *testing.T, ctx context.Context, r *ring.Ring, instances []instanceDef, replicationFactor int, limits ShardingLimits, userIDs []string, blocks []ulid.ULID, dynamicReplication bool) map[string]instanceState {
	t.Helper()

	state := make(map[string]instanceState)

	for _, inst := range instances {
		strategy := NewShuffleShardingStrategy(r, inst.id, inst.addr, createDynamicReplication(dynamicReplication, replicationFactor), limits, log.NewNopLogger())

		filteredUsers, err := strategy.FilterUsers(ctx, userIDs)
		require.NoError(t, err, "FilterUsers failed for instance %s", inst.id)

		blocksPerUser := make(map[string][]ulid.ULID)
		for _, userID := range userIDs {
			metas := createBlockMetas(dynamicReplication, blocks)

			synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
			err := strategy.FilterBlocks(ctx, userID, metas, nil, synced)
			require.NoError(t, err, "FilterBlocks failed for instance %s, user %s", inst.id, userID)

			for blockID := range metas {
				blocksPerUser[userID] = append(blocksPerUser[userID], blockID)
			}
		}

		state[inst.id] = instanceState{
			users:         filteredUsers,
			blocksPerUser: blocksPerUser,
		}
	}

	return state
}

// assertStateUnchanged asserts that the state is unchanged for specified instances.
func assertStateUnchanged(t *testing.T, baseline, current map[string]instanceState, instanceIDs []string) {
	t.Helper()

	for _, id := range instanceIDs {
		baselineState, ok := baseline[id]
		require.True(t, ok, "baseline state not found for instance %s", id)

		currentState, ok := current[id]
		require.True(t, ok, "current state not found for instance %s", id)

		assert.ElementsMatch(t, baselineState.users, currentState.users,
			"FilterUsers result changed for instance %s", id)

		require.Equal(t, len(baselineState.blocksPerUser), len(currentState.blocksPerUser),
			"Number of users with blocks changed for instance %s", id)

		for userID, baselineBlocks := range baselineState.blocksPerUser {
			currentBlocks, ok := currentState.blocksPerUser[userID]
			require.True(t, ok, "user %s blocks not found in current state for instance %s", userID, id)
			assert.ElementsMatch(t, baselineBlocks, currentBlocks,
				"FilterBlocks result changed for instance %s, user %s", id, userID)
		}
	}
}

// addInstancesToRing adds instances to a ring descriptor.
func addInstancesToRing(d *ring.Desc, instances []instanceDef, registeredAt time.Time) {
	for _, inst := range instances {
		d.AddIngester(inst.id, inst.addr, inst.zone, inst.tokens, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
	}
}

// removeInstancesFromRing removes instances from a ring descriptor.
func removeInstancesFromRing(d *ring.Desc, instances []instanceDef) {
	for _, inst := range instances {
		d.RemoveIngester(inst.id)
	}
}

// getInstanceIDs extracts instance IDs from instance definitions.
func getInstanceIDs(instances ...[]instanceDef) []string {
	var ids []string
	for _, instList := range instances {
		for _, inst := range instList {
			ids = append(ids, inst.id)
		}
	}
	return ids
}
