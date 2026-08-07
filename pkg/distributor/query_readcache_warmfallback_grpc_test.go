// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

// stubReadcacheServer is a real gRPC ingester service. Serving these
// cases over an actual transport is the whole point: a server-streaming
// handler that returns an error before its first Send delivers that
// error to the client on Recv, not from the QueryStream call. An
// in-process mock client returns it from QueryStream instead, which is
// the branch production never reaches.
type stubReadcacheServer struct {
	ingester_client.UnimplementedIngesterServer

	// queryErr, when set, is returned by the QueryStream handler
	// before any message is sent.
	queryErr error

	mu    sync.Mutex
	calls int
}

func (s *stubReadcacheServer) QueryStream(_ *ingester_client.QueryRequest, srv ingester_client.Ingester_QueryStreamServer) error {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()

	if s.queryErr != nil {
		return s.queryErr
	}
	return srv.Send(&ingester_client.QueryStreamResponse{IsEndOfSeriesStream: true})
}

func (s *stubReadcacheServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// startReadcaches serves one gRPC readcache per instance ID. IDs listed
// in warming reject every query the way a readcache does while it
// replays its head from Kafka.
func startReadcaches(t *testing.T, partitionID int32, warming []string, ids ...string) (map[string]*stubReadcacheServer, string) {
	t.Helper()

	isWarming := make(map[string]bool, len(warming))
	for _, id := range warming {
		isWarming[id] = true
	}

	servers := make(map[string]*stubReadcacheServer, len(ids))
	addresses := make([]string, 0, len(ids))
	for _, id := range ids {
		stub := &stubReadcacheServer{}
		if isWarming[id] {
			stub.queryErr = status.Errorf(codes.Unavailable, "readcache:still_warming partition=%d", partitionID)
		}

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		srv := grpc.NewServer()
		ingester_client.RegisterIngesterServer(srv, stub)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)

		servers[id] = stub
		addresses = append(addresses, id+"="+lis.Addr().String())
	}

	return servers, strings.Join(addresses, ",")
}

func newWarmFallbackDistributor(t *testing.T, now time.Time, addresses string, ignoreReplicaMap bool) *Distributor {
	t.Helper()

	d := &Distributor{
		now: func() time.Time { return now },
		log: log.NewNopLogger(),
		limits: validation.NewOverrides(validation.Limits{
			ReadcacheReadRouting: validation.ReadcacheReadRoutingNautilus,
			QueryIngestersWithin: model.Duration(13 * time.Hour),
		}, nil),
	}
	// Readcache only ever runs on top of ingest storage: partitions
	// come from Kafka, and quorum is one instance per partition.
	d.cfg.IngestStorageConfig.Enabled = true
	d.cfg.Readcache = ReadcacheConfig{
		Addresses:                  addresses,
		IgnoreReplicaMapForQueries: ignoreReplicaMap,
	}

	pool, err := newReadcachePool(d.cfg.Readcache, nil, "", prometheus.NewPedanticRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	d.readcachePool = pool
	t.Cleanup(func() { _ = pool.Close() })

	return d
}

func queryReadcacheSets(ctx context.Context, d *Distributor, sets []ring.ReplicationSet, partitionByInstance map[string]int32) error {
	_, err := d.queryIngesterStream(
		ctx,
		sets,
		partitionByInstance,
		newReadcacheHitTracker(),
		&ingester_client.QueryRequest{},
		stats.NewQueryMetrics(prometheus.NewPedanticRegistry()),
	)
	return err
}

func readcacheQueryContext(t *testing.T, userID string) context.Context {
	t.Helper()

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	return limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry()))
}

// TestQueryIngesterStream_StillWarmingFallback pins the warm-up
// fallback end to end. Its unit-level pieces (the fallback state
// machine, the previous-owner lookup, replication-set construction)
// were each covered in isolation, so the wiring between them went
// untested and the fallback never actually fired in production: the
// still_warming reply arrives on the first Recv, not from QueryStream.
func TestQueryIngesterStream_StillWarmingFallback(t *testing.T) {
	const (
		userID     = "user-1"
		partition  = int32(0)
		newOwnerID = "readcache-new"
		oldOwnerID = "readcache-old"
	)

	// A partition that just moved: the previous owner's lease is
	// truncated at the end of the move safety window and overlaps the
	// new owner's lease, which is what makes it the warm fallback.
	leases := func(now time.Time) []readcacheassignment.LogEntry {
		return []readcacheassignment.LogEntry{
			{PartitionID: partition, InstanceID: oldOwnerID, From: now.Add(-time.Hour), To: now.Add(2 * time.Minute)},
			{PartitionID: partition, InstanceID: newOwnerID, From: now.Add(-time.Minute), To: now.Add(15 * time.Minute)},
		}
	}

	t.Run("RF1 falls back to the previous lease owner", func(t *testing.T) {
		now := time.Now()
		servers, addresses := startReadcaches(t, partition, []string{newOwnerID}, newOwnerID, oldOwnerID)
		d := newWarmFallbackDistributor(t, now, addresses, true)
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries(leases(now)), nil)

		ctx := readcacheQueryContext(t, userID)

		// Asserted separately so a failure of the query below can only
		// mean the fallback was never attempted, not that the lookup
		// had nothing to return.
		_, prevID, ok := d.previousReadcacheClientForPartition(ctx, partition, newOwnerID)
		require.True(t, ok, "the truncated previous lease must still resolve to a dialable owner")
		require.Equal(t, oldOwnerID, prevID)

		instanceID := readcacheSyntheticInstanceID(newOwnerID, partition)
		sets := []ring.ReplicationSet{{Instances: []ring.InstanceDesc{{Id: instanceID, Addr: newOwnerID}}}}

		err := queryReadcacheSets(ctx, d, sets, map[string]int32{instanceID: partition})
		require.NoError(t, err, "a still_warming reply must fall back to the previous lease owner instead of failing the query")
		assert.Equal(t, 1, servers[newOwnerID].callCount(), "the current owner must be tried first")
		assert.Equal(t, 1, servers[oldOwnerID].callCount(), "the previous lease owner must serve the read while the current owner warms")
	})

	t.Run("a still_warming reply with no previous owner surfaces the error", func(t *testing.T) {
		now := time.Now()
		servers, addresses := startReadcaches(t, partition, []string{newOwnerID}, newOwnerID)
		d := newWarmFallbackDistributor(t, now, addresses, true)
		// Only the current owner has ever held the partition.
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries([]readcacheassignment.LogEntry{
			{PartitionID: partition, InstanceID: newOwnerID, From: now.Add(-time.Minute), To: now.Add(15 * time.Minute)},
		}), nil)

		instanceID := readcacheSyntheticInstanceID(newOwnerID, partition)
		sets := []ring.ReplicationSet{{Instances: []ring.InstanceDesc{{Id: instanceID, Addr: newOwnerID}}}}

		err := queryReadcacheSets(readcacheQueryContext(t, userID), d, sets, map[string]int32{instanceID: partition})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "still_warming", "the original warming error must not be masked")
		assert.Equal(t, 1, servers[newOwnerID].callCount())
	})

	// The fallback deliberately sits below the quorum layer and waits
	// for every current mirror: a previous-owner response satisfies
	// quorum, so serving one before a warm peer has been tried would
	// answer from stale frozen data.
	t.Run("RF2 with one warming mirror is served by its peer", func(t *testing.T) {
		now := time.Now()
		newA, newB := "readcache-new-zone-a", "readcache-new-zone-b"
		oldA, oldB := "readcache-old-zone-a", "readcache-old-zone-b"
		servers, addresses := startReadcaches(t, partition, []string{newA}, newA, newB, oldA, oldB)

		d := newWarmFallbackDistributor(t, now, addresses, false)
		replicaMap := readcacheassignment.ReplicaMap{
			newOwnerID: {{InstanceID: newA, Zone: "zone-a"}, {InstanceID: newB, Zone: "zone-b"}},
			oldOwnerID: {{InstanceID: oldA, Zone: "zone-a"}, {InstanceID: oldB, Zone: "zone-b"}},
		}
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries(leases(now)), replicaMap)

		set := readcacheReplicationSetForOwner(replicaMap, partition, newOwnerID, false)
		partitionByInstance := map[string]int32{}
		for _, inst := range set.Instances {
			partitionByInstance[inst.Id] = partition
		}

		err := queryReadcacheSets(readcacheQueryContext(t, userID), d, []ring.ReplicationSet{set}, partitionByInstance)
		require.NoError(t, err)
		assert.Equal(t, 0, servers[oldA].callCount()+servers[oldB].callCount(),
			"a warm peer must answer before any previous-owner fallback is attempted")
	})

	t.Run("RF2 with both mirrors warming falls back once", func(t *testing.T) {
		now := time.Now()
		newA, newB := "readcache-new-zone-a", "readcache-new-zone-b"
		oldA, oldB := "readcache-old-zone-a", "readcache-old-zone-b"
		servers, addresses := startReadcaches(t, partition, []string{newA, newB}, newA, newB, oldA, oldB)

		d := newWarmFallbackDistributor(t, now, addresses, false)
		replicaMap := readcacheassignment.ReplicaMap{
			newOwnerID: {{InstanceID: newA, Zone: "zone-a"}, {InstanceID: newB, Zone: "zone-b"}},
			oldOwnerID: {{InstanceID: oldA, Zone: "zone-a"}, {InstanceID: oldB, Zone: "zone-b"}},
		}
		d.setReadcacheAssignment(readcacheassignment.NewLogFromEntries(leases(now)), replicaMap)

		set := readcacheReplicationSetForOwner(replicaMap, partition, newOwnerID, false)
		partitionByInstance := map[string]int32{}
		for _, inst := range set.Instances {
			partitionByInstance[inst.Id] = partition
		}

		err := queryReadcacheSets(readcacheQueryContext(t, userID), d, []ring.ReplicationSet{set}, partitionByInstance)
		require.NoError(t, err)
		assert.Equal(t, 1, servers[oldA].callCount()+servers[oldB].callCount(),
			"exactly one previous-owner fallback may be claimed per replication set")
	})
}
