// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_store_replicated_set.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"context"
	"fmt"
	"math/rand"
	"slices"

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
)

type loadBalancingStrategy int

const (
	noLoadBalancing = loadBalancingStrategy(iota)
	randomLoadBalancing
)

// BlocksStoreSet implementation used when the blocks are sharded and replicated across
// a set of store-gateway instances.
type blocksStoreReplicationSet struct {
	services.Service

	storesRing         *ring.Ring
	clientsPool        *client.Pool
	balancingStrategy  loadBalancingStrategy
	dynamicReplication storegateway.DynamicReplication
	limits             BlocksStoreLimits

	transitions *storegateway.ShardSizeTransitions

	// When non empty, the querier prioritises querying blocks from store-gateways in these zones with equal priority.
	preferredZones []string

	// Subservices manager.
	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

func newBlocksStoreReplicationSet(
	storesRing *ring.Ring,
	clientsPool *client.Pool,
	balancingStrategy loadBalancingStrategy,
	dynamicReplication storegateway.DynamicReplication,
	preferredZones []string,
	limits BlocksStoreLimits,
	transitions *storegateway.ShardSizeTransitions,
) (*blocksStoreReplicationSet, error) {
	s := &blocksStoreReplicationSet{
		storesRing:         storesRing,
		clientsPool:        clientsPool,
		dynamicReplication: dynamicReplication,
		balancingStrategy:  balancingStrategy,
		preferredZones:     preferredZones,
		limits:             limits,
		transitions:        transitions,
		subservicesWatcher: services.NewFailureWatcher(),
	}

	// The clients pool is shared across compartments and managed by the queryable, so this set manages
	// only its ring.
	var err error
	s.subservices, err = services.NewManager(s.storesRing)
	if err != nil {
		return nil, err
	}

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)

	return s, nil
}

func (s *blocksStoreReplicationSet) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return errors.Wrap(err, "unable to start blocks store set subservices")
	}

	return nil
}

func (s *blocksStoreReplicationSet) running(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.subservicesWatcher.Chan():
			return errors.Wrap(err, "blocks store set subservice failed")
		}
	}
}

func (s *blocksStoreReplicationSet) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
}

func (s *blocksStoreReplicationSet) GetClientsFor(userID string, blocks bucketindex.Blocks, exclude map[ulid.ULID][]string) (map[BlocksStoreClient][][]ulid.ULID, error) {
	blocksByAddr := make(map[string][]ulid.ULID)
	instances := make(map[string]ring.InstanceDesc)
	userRing, previousUserRing := storegateway.GetShuffleShardingSubrings(s.storesRing, userID, s.limits, s.transitions)

	// It's safe to reuse buffers across iterations, as far as we don't retain the ReplicationSet.Instances slice
	// (retaining individual instances from the slice is fine).
	ringBuffersOpt := ring.WithBuffers(ring.MakeBuffersForGet())

	// Find the replication set of each block we need to query.
	for _, block := range blocks {
		ringOpts := []ring.Option{ringBuffersOpt}
		if eligible, replicationFactor := s.dynamicReplication.EligibleForQuerying(block); eligible {
			ringOpts = append(ringOpts, ring.WithReplicationFactor(replicationFactor))
		}

		// During a transition, try the new tenant shard first and alternate on each retry of the block, because the new
		// owners may not have loaded it yet. The attempt number is the count of excluded instances. A shard with no
		// instance left falls back to the other one.
		userRings := [2]ring.ReadRing{userRing, previousUserRing}
		numUserRings := 1
		if previousUserRing != nil {
			numUserRings = 2
			if len(exclude[block.ID])%2 == 1 {
				userRings[0], userRings[1] = userRings[1], userRings[0]
			}
		}

		var (
			inst   *ring.InstanceDesc
			getErr error
		)
		for _, r := range userRings[:numUserRings] {
			set, err := r.GetWithOptions(mimir_tsdb.HashBlockID(block.ID), storegateway.BlocksRead, ringOpts...)
			if err != nil {
				getErr = err
				continue
			}

			// Pick a non excluded store-gateway instance.
			// We copy the instance descriptor, and we don't use the replication set's slice buffers any more beyond this point,
			// so that it's safe to reuse ring buffers across iterations.
			if inst = getNonExcludedInstance(set, exclude[block.ID], s.balancingStrategy, s.preferredZones); inst != nil {
				break
			}
		}
		if inst == nil {
			if getErr != nil {
				return nil, errors.Wrapf(getErr, "failed to get store-gateway replication set owning the block %s", block.ID)
			}
			return nil, fmt.Errorf("no store-gateway instance left after checking exclude for block %s", block.ID)
		}

		instances[inst.Addr] = *inst
		blocksByAddr[inst.Addr] = append(blocksByAddr[inst.Addr], block.ID)
	}

	clients := map[BlocksStoreClient][][]ulid.ULID{}
	maxBlocksPerClient := s.limits.MaxBlocksPerStoreRequest(userID)

	// Get the client for each store-gateway.
	for addr, instance := range instances {
		c, err := s.clientsPool.GetClientForInstance(instance)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get store-gateway client for %s %s", instance.Id, addr)
		}

		clients[c.(BlocksStoreClient)] = partitionBlocks(blocksByAddr[addr], maxBlocksPerClient)
	}

	return clients, nil
}

func (s *blocksStoreReplicationSet) InShardSizeTransition(userID string) bool {
	return s.transitions.InTransition(userID)
}

// partitionBlocks splits blocks into consecutive partitions of at most max blocks each.
// When max is 0 the function returns all blocks in a single partition.
func partitionBlocks(blocks []ulid.ULID, max int) [][]ulid.ULID {
	if max == 0 {
		return [][]ulid.ULID{blocks}
	}
	return slices.Collect(slices.Chunk(blocks, max))
}

func getNonExcludedInstance(set ring.ReplicationSet, exclude []string, balancingStrategy loadBalancingStrategy, preferredZones []string) *ring.InstanceDesc {
	if balancingStrategy == randomLoadBalancing {
		// Randomize the list of instances to not always query the same one.
		rand.Shuffle(len(set.Instances), func(i, j int) {
			set.Instances[i], set.Instances[j] = set.Instances[j], set.Instances[i]
		})
	}

	if len(preferredZones) > 0 {
		// Move all instances from preferred zones to the front.
		// This gives equal priority to all preferred zones (since they were already shuffled).
		nextPos := 0
		for idx, instance := range set.Instances {
			if slices.Contains(preferredZones, instance.Zone) {
				set.Instances[nextPos], set.Instances[idx] = set.Instances[idx], set.Instances[nextPos]
				nextPos++
			}
		}
	}

	for _, instance := range set.Instances {
		if !slices.Contains(exclude, instance.Addr) {
			return &instance
		}
	}

	return nil
}
