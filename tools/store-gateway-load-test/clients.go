package main

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/querier"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
)

type storeGatewayClients map[querier.BlocksStoreClient][]ulid.ULID

type storeGatewaySelector struct {
	storesRing  *ring.Ring
	clientsPool *client.Pool
	limits      querier.BlocksStoreLimits
}

func newStoreGatewaySelector(storesRing *ring.Ring, clientConfig querier.ClientConfig, limits querier.BlocksStoreLimits, logger log.Logger, reg prometheus.Registerer) *storeGatewaySelector {
	return &storeGatewaySelector{
		storesRing:  storesRing,
		clientsPool: querier.NewStoreGatewayClientPool(client.NewRingServiceDiscovery(storesRing), clientConfig, logger, reg),
		limits:      limits,
	}
}

func (s *storeGatewaySelector) GetZonalClientsFor(userID string, blockIDs []ulid.ULID) (map[string]storeGatewayClients, error) {
	perZoneInstances := map[string]map[string][]ulid.ULID{}
	perZoneClients := map[string]storeGatewayClients{}

	userRing := storegateway.GetShuffleShardingSubring(s.storesRing, userID, s.limits)

	// Find the replication set of each block we need to query.
	for _, blockID := range blockIDs {
		// Do not reuse the same buffer across multiple Get() calls because we do retain the
		// returned replication set.
		bufDescs, bufHosts, bufZones := ring.MakeBuffersForGet()

		set, err := userRing.Get(mimir_tsdb.HashBlockID(blockID), storegateway.BlocksRead, bufDescs, bufHosts, bufZones)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get store-gateway replication set owning the block %s", blockID.String())
		}

		// Keep track of the store-gateways owning the block for each zone.
		for _, instance := range set.Instances {
			if _, ok := perZoneInstances[instance.Zone]; !ok {
				perZoneInstances[instance.Zone] = map[string][]ulid.ULID{}
			}

			perZoneInstances[instance.Zone][instance.Addr] = append(perZoneInstances[instance.Zone][instance.Addr], blockID)
		}
	}

	// Get the client for each store-gateway.
	for zone, instances := range perZoneInstances {
		perZoneClients[zone] = make(storeGatewayClients)

		for addr, blockIDs := range instances {
			c, err := s.clientsPool.GetClientFor(addr)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get store-gateway client for %s", addr)
			}

			perZoneClients[zone][c.(querier.BlocksStoreClient)] = blockIDs
		}
	}

	return perZoneClients, nil
}
