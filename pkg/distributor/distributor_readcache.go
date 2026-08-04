// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
	"github.com/grafana/mimir/pkg/util/validation"
)

// errReadcacheRoutingUnavailable is returned by the nautilus read
// path when a tenant is configured for readcache_read_routing
// nautilus-only but the distributor cannot resolve the query to a
// readcache instance: either no live assignment/readcache log
// snapshot has been received, or a resolved partition has no current
// readcache owner. There is intentionally no ingester fallback for
// nautilus-only tenants (their data lives on the nautilus_ingest
// topic, which ingesters do not consume), so this surfaces to the
// caller as a query failure, mirroring the write-side
// nautilusRoutingUnavailableError semantics.
type errReadcacheRoutingUnavailable struct {
	reason string
}

func newReadcacheRoutingUnavailableError(reason string) errReadcacheRoutingUnavailable {
	return errReadcacheRoutingUnavailable{reason: reason}
}

func (e errReadcacheRoutingUnavailable) Error() string {
	return fmt.Sprintf("readcache read routing required but unavailable: %s", e.reason)
}

// readcacheHitTracker accumulates the set of distinct readcache
// instance IDs the distributor committed to using for a single
// query (typically one Distributor.QueryStream call). It is
// thread-safe because queryIngesterStream fans out to per-replica
// goroutines via ring.DoMultiUntilQuorumWithoutSuccessfulContextCancellation,
// and each goroutine may independently resolve a readcache instance
// via queryClientForInstance.
//
// At the end of a metric-name-scoped readcache query the tracker's
// count feeds the
// cortex_distributor_query_readcache_instances_hit_per_query
// histogram. Queries whose metric names cannot be reduced to a finite
// set are tracked by the separate full-fanout counter.
type readcacheHitTracker struct {
	mu        sync.Mutex
	instances map[string]struct{}
}

func newReadcacheHitTracker() *readcacheHitTracker {
	return &readcacheHitTracker{instances: make(map[string]struct{})}
}

// record marks instanceID as having served (part of) this query.
// Safe to call concurrently; duplicates are ignored.
func (t *readcacheHitTracker) record(instanceID string) {
	if t == nil || instanceID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.instances[instanceID] = struct{}{}
}

// count returns the number of distinct readcache instances recorded
// so far. Reading is also safe under the mutex so callers don't
// observe a torn map size during concurrent record() calls.
func (t *readcacheHitTracker) count() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.instances)
}

// readcacheAssignmentState is the atomically-published pair of
// assignment log + replica map from one WatchReadcacheAssignments
// message. Storing them together prevents querier/ruler read-path
// goroutines from observing a new logical-ID log against a stale map.
type readcacheAssignmentState struct {
	log        *readcacheassignment.Log
	replicaMap readcacheassignment.ReplicaMap
}

// GetReadcacheLog returns the current (partition -> readcache
// instance) log streamed from the rebalancer, or nil if no snapshot
// has been received yet (cold start or rebalancer unreachable).
func (d *Distributor) GetReadcacheLog() *readcacheassignment.Log {
	if s := d.loadReadcacheAssignment(); s != nil {
		return s.log
	}
	return nil
}

// GetReadcacheReplicaMap returns the current logical->concrete
// readcache replica map streamed from the rebalancer. A nil or empty
// map means identity: the instance IDs in the assignment log are
// themselves the concrete pods to dial (RF=1 / legacy).
func (d *Distributor) GetReadcacheReplicaMap() readcacheassignment.ReplicaMap {
	if s := d.loadReadcacheAssignment(); s != nil {
		return s.replicaMap
	}
	return nil
}

// loadReadcacheAssignment returns the atomically-published log+map
// pair, or nil when no snapshot has arrived yet.
func (d *Distributor) loadReadcacheAssignment() *readcacheAssignmentState {
	return d.readcacheAssignment.Load()
}

// setReadcacheAssignment publishes log and replicaMap together.
// replicaMap may be nil (identity). Used by the watch loop and tests.
func (d *Distributor) setReadcacheAssignment(log *readcacheassignment.Log, replicaMap readcacheassignment.ReplicaMap) {
	d.readcacheAssignment.Store(&readcacheAssignmentState{
		log:        log,
		replicaMap: replicaMap.Clone(),
	})
}

// watchReadcacheAssignments mirrors watchNautilusAssignments for the
// readcache (partition -> instance) log. It reuses the existing
// nautilusRebalancerConn (the rebalancer serves both streams over
// the same gRPC server) and the same reconnect / backoff loop
// structure.
func (d *Distributor) watchReadcacheAssignments(ctx context.Context) {
	conn, ok := d.nautilusRebalancerConn.(*grpc.ClientConn)
	if !ok || conn == nil {
		return
	}
	client := rebalancer.NewNautilusRebalancerClient(conn)

	const minBackoff = readcacheMinBackoff
	const maxBackoff = readcacheMaxBackoff
	backoff := minBackoff

	for ctx.Err() == nil {
		stream, err := client.WatchReadcacheAssignments(ctx, &rebalancer.WatchReadcacheAssignmentsRequest{SupportsDeltas: true})
		if err != nil {
			level.Warn(d.log).Log("msg", "failed to open readcache WatchReadcacheAssignments stream", "err", err, "backoff", backoff)
			d.sleepWithCtx(ctx, backoff)
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}
		backoff = minBackoff

		if err := d.consumeReadcacheStream(stream); err != nil && ctx.Err() == nil {
			level.Warn(d.log).Log("msg", "readcache WatchReadcacheAssignments stream ended", "err", err, "backoff", backoff)
		}
		if ctx.Err() != nil {
			return
		}
		d.sleepWithCtx(ctx, backoff)
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

// consumeReadcacheStream mirrors consumeNautilusStream's
// snapshot/delta handling; see there for the protocol notes.
func (d *Distributor) consumeReadcacheStream(stream rebalancer.NautilusRebalancer_WatchReadcacheAssignmentsClient) error {
	first := true
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		entries := rebalancer.ReadcacheEntriesFromProto(resp.Entries)
		var log *readcacheassignment.Log
		if resp.Reset_ || first {
			log = readcacheassignment.NewLogFromEntries(entries)
		} else if prev := d.GetReadcacheLog(); prev != nil {
			log = prev.MergedWithEntries(entries)
		} else {
			log = readcacheassignment.NewLogFromEntries(entries)
		}
		if resp.PruneBeforeUnixMs > 0 {
			log.Prune(time.UnixMilli(resp.PruneBeforeUnixMs))
		}
		first = false
		// Publish log + map atomically so RF≥2 expansion always
		// matches the lease IDs from the same message.
		d.setReadcacheAssignment(log, rebalancer.ReplicaMapFromProto(resp.ReplicaSets))
		if d.readcacheInitialSync != nil {
			d.readcacheInitialSyncOnce.Do(func() {
				close(d.readcacheInitialSync)
			})
		}
	}
}

const (
	// Mirrors the WatchAssignments backoff values; defined here so
	// the readcache subscription is independent of any future
	// adjustments to the nautilus side.
	readcacheMinBackoff = 250 * time.Millisecond
	readcacheMaxBackoff = 8 * time.Second
)

// resolveReadcacheClientForPartition looks up the readcache instance
// currently owning partitionID and returns a typed gRPC client for
// it along with the resolved instance ID. Returns ok=false when:
//
//   - the readcache log has no snapshot yet (cold start, rebalancer
//     unreachable);
//   - the lease for partitionID has expired without a successor;
//   - no address is configured for the resolved instance; or
//   - the connection cannot be established.
//
// On ok=false the caller falls back to the ingester pool. The
// fallback is deliberate during phase 2C so a transient gap in
// readcache coverage degrades to "served by ingester" rather than
// "served by nothing".
//
// The returned instanceID is the readcache instance the client
// dials; callers use it for per-query observability (which
// readcache pods served this query) rather than for routing.
//
// In ok=false cases where err is non-nil, the caller may log it; nil
// err with ok=false simply means the partition is not currently
// covered by a readcache lease.
func (d *Distributor) resolveReadcacheClientForPartition(ctx context.Context, partitionID int32) (cli client.IngesterClient, instanceID string, ok bool, err error) {
	if d.readcachePool == nil {
		return nil, "", false, nil
	}
	rcState := d.loadReadcacheAssignment()
	if rcState == nil || rcState.log == nil {
		return nil, "", false, nil
	}
	owners := rcState.log.Lookup(d.now(), partitionID)
	if len(owners) == 0 {
		return nil, "", false, nil
	}
	// The log records one logical owner per partition; multi-owner
	// mode from readcacheassignment.Log is reserved for drain/handoff
	// windows. Pick the first owner deterministically, then expand it
	// to its concrete zone replicas and dial them in order until one
	// connects — under RF=1 the expansion is the identity, so this
	// reduces to dialing the logged owner.
	for _, rep := range expandReadcacheReplicasForQuery(rcState.replicaMap, owners[0], d.cfg.Readcache.IgnoreReplicaMapForQueries) {
		cli, err = d.readcachePool.GetClientForInstance(ctx, rep.InstanceID)
		if err != nil {
			continue
		}
		return cli, rep.InstanceID, true, nil
	}
	if err == nil {
		err = fmt.Errorf("logical readcache owner %q of partition %d has no concrete replica", owners[0], partitionID)
	}
	return nil, "", false, err
}

// previousReadcacheOwnerForPartition returns the readcache instance
// that owned partitionID in the lease immediately preceding the
// current one, if such a lease is still in the log (i.e. its
// To timestamp has not yet been pruned past `now`).
//
// The plan calls for the distributor to fall back to this previous
// owner when the current owner replies with a "still warming" error,
// since after a partition move the previous owner still has a fully
// warm head and its lease just got truncated at the round boundary.
//
// Returns "", false when no eligible previous owner exists.
func (d *Distributor) previousReadcacheOwnerForPartition(partitionID int32) (string, bool) {
	log := d.GetReadcacheLog()
	if log == nil {
		return "", false
	}
	return previousReadcacheOwnerFromLog(log, d.now(), partitionID)
}

func previousReadcacheOwnerFromLog(log *readcacheassignment.Log, now time.Time, partitionID int32) (string, bool) {
	currentOwners := map[string]struct{}{}
	for _, id := range log.Lookup(now, partitionID) {
		currentOwners[id] = struct{}{}
	}
	// Walk every entry for this partition, ignoring future-only
	// leases (From > now). Pick the entry with the largest To that
	// is *not* still active for `now` and whose InstanceID is not
	// the current owner. That entry's owner is the one whose lease
	// was just truncated by the move.
	var best readcacheassignment.LogEntry
	bestFound := false
	for _, e := range log.Entries() {
		if e.PartitionID != partitionID {
			continue
		}
		if e.From.After(now) {
			continue
		}
		if _, isCurrent := currentOwners[e.InstanceID]; isCurrent {
			continue
		}
		if !bestFound || e.To.After(best.To) {
			best = e
			bestFound = true
		}
	}
	if !bestFound {
		return "", false
	}
	return best.InstanceID, true
}

// readcacheSyntheticInstanceID is the routing key the read path uses
// for one concrete readcache pod serving one partition. A readcache
// owns many partitions and a partition may be served by several pods
// (zone replicas of one logical slot, or successive owners across a
// move), so neither dimension alone is unique.
func readcacheSyntheticInstanceID(concreteID string, partitionID int32) string {
	return fmt.Sprintf("%s/p%d", concreteID, partitionID)
}

// expandReadcacheReplicasForQuery returns the concrete pods the read
// path should dial for logicalOwner.
//
// When ignoreMap is true (RF=1→RF=2 warm stage), expansion is the
// identity so queriers keep dialing the legacy non-zonal pod while
// zone mirrors consume via the real replica map.
//
// When ignoreMap is false and the map lists any zoned replica,
// non-zonal entries are dropped so a dual-fleet cutover can leave the
// old STS consuming without serving. An explicitly empty map entry
// (both mirrors down) is handled by the caller before Expand.
func expandReadcacheReplicasForQuery(replicaMap readcacheassignment.ReplicaMap, logicalOwner string, ignoreMap bool) []readcacheassignment.Replica {
	if logicalOwner == "" {
		return nil
	}
	if ignoreMap {
		return []readcacheassignment.Replica{{InstanceID: logicalOwner}}
	}
	replicas := replicaMap.Expand(logicalOwner)
	zoned := make([]readcacheassignment.Replica, 0, len(replicas))
	for _, rep := range replicas {
		if rep.Zone != "" {
			zoned = append(zoned, rep)
		}
	}
	if len(zoned) > 0 {
		return zoned
	}
	return replicas
}

// readcacheReplicationSetForOwner builds the ring.ReplicationSet the
// read path uses for one (partition, logical owner) pair: one
// InstanceDesc per concrete zone replica of the logical slot, with
// InstanceDesc.Addr carrying the pod to dial and Id carrying the
// synthetic routing key.
//
// Zone replicas of a slot consume the same partitions and therefore
// hold identical data, so the read only needs one of them to answer.
// With distinct zones that is expressed with zone awareness, which
// makes DoUntilQuorum return as soon as one zone succeeds and spill
// to the other zone only on failure. With an empty replica map the
// expansion is the identity and the set degenerates to the
// single-instance, no-tolerance shape used under RF=1.
//
// ignoreMap forces identity expansion (legacy pod) even when a
// replica map is present — used for dual-fleet warm before query cutover.
func readcacheReplicationSetForOwner(replicaMap readcacheassignment.ReplicaMap, partitionID int32, logicalOwner string, ignoreMap bool) ring.ReplicationSet {
	if !ignoreMap {
		if reps, known := replicaMap[logicalOwner]; known && len(reps) == 0 {
			// The rebalancer knows this logical slot but has no live pod
			// for it (both mirrors left the ring). Returning an empty set
			// makes the caller fail the slot explicitly instead of
			// dialing the logical ID, which is not a routable address.
			return ring.ReplicationSet{}
		}
	}
	replicas := expandReadcacheReplicasForQuery(replicaMap, logicalOwner, ignoreMap)
	set := ring.ReplicationSet{Instances: make([]ring.InstanceDesc, 0, len(replicas))}
	zones := make(map[string]struct{}, len(replicas))
	for _, rep := range replicas {
		if rep.InstanceID == "" {
			continue
		}
		set.Instances = append(set.Instances, ring.InstanceDesc{
			Id:   readcacheSyntheticInstanceID(rep.InstanceID, partitionID),
			Addr: rep.InstanceID,
			Zone: rep.Zone,
		})
		if rep.Zone != "" {
			zones[rep.Zone] = struct{}{}
		}
	}
	switch {
	case len(zones) > 1:
		set.ZoneAwarenessEnabled = true
		set.MaxUnavailableZones = len(zones) - 1
	case len(set.Instances) > 1:
		// Several replicas but no distinct zone labels: express the
		// same "any one replica serves the read" tolerance with
		// MaxErrors, which is mutually exclusive with zone awareness.
		set.MaxErrors = len(set.Instances) - 1
	}
	return set
}

// queryClientForInstance returns the gRPC client the distributor's
// read path should use for the given instance, and whether that
// client is a readcache client (viaReadcache).
//
// For nautilus-only tenants (shouldRouteReadToReadcache), the read
// path routes exclusively to readcache: the replication sets were
// built from the assignment log by getReadcacheReplicationSetsForQuery,
// so every instance maps to a partition with a current readcache
// owner. Any inability to resolve a reachable owner is a hard
// failure — there is deliberately no ingester fallback, because the
// tenant's data lives on the nautilus_ingest topic that ingesters
// do not consume. The returned error surfaces to the caller (a 503,
// matching a full ingester outage); the warm-up previous-owner
// fallback is handled by the caller on errStillWarming.
//
// For all other tenants the ingester client from the shared pool is
// returned with viaReadcache=false.
//
// When a readcache client is returned and hits is non-nil, the
// chosen readcache instance ID is recorded in hits so the caller can
// emit a per-query histogram observation. The ingester branch never
// records into hits.
func (d *Distributor) queryClientForInstance(ctx context.Context, ing ring.InstanceDesc, partitionByInstance map[string]int32, hits *readcacheHitTracker, _ log.Logger) (client.IngesterClient, bool, error) {
	if d.shouldRouteReadToReadcache(ctx) {
		if d.readcachePool == nil {
			return nil, false, newReadcacheRoutingUnavailableError("readcache pool is not configured")
		}
		// getReadcacheReplicationSetsForQuery resolved the specific
		// owner for this (owner, partition) pair and carried it in
		// Addr. Dial that instance directly: with interval-aware
		// fan-out a partition can map to several owners across the
		// query window, so we must not collapse back to a single
		// current owner here.
		if ing.Addr != "" {
			rcClient, err := d.readcachePool.GetClientForInstance(ctx, ing.Addr)
			if err != nil {
				return nil, false, err
			}
			hits.record(ing.Addr)
			return rcClient, true, nil
		}
		partID, ok := partitionByInstance[ing.Id]
		if !ok {
			return nil, false, newReadcacheRoutingUnavailableError(fmt.Sprintf("instance %q has no resolved partition", ing.Id))
		}
		rcClient, instanceID, ok, err := d.resolveReadcacheClientForPartition(ctx, partID)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, newReadcacheRoutingUnavailableError(fmt.Sprintf("partition %d has no reachable readcache owner", partID))
		}
		hits.record(instanceID)
		return rcClient, true, nil
	}

	c, err := d.ingesterPool.GetClientForInstance(ing)
	if err != nil {
		return nil, false, err
	}
	return c.(client.IngesterClient), false, nil
}

// previousReadcacheClientForPartition returns the gRPC client for
// the readcache instance that owned partitionID in the lease
// immediately preceding the current one, along with that instance
// ID. Used by the caller to fall back when the current owner
// returns errStillWarming.
//
// Returns ok=false if there is no recoverable previous owner (no
// log, lease pruned, address unconfigured, or dial error). The
// instance ID is empty in that case.
func (d *Distributor) previousReadcacheClientForPartition(ctx context.Context, partitionID int32) (client.IngesterClient, string, bool) {
	if d.readcachePool == nil {
		return nil, "", false
	}
	rcState := d.loadReadcacheAssignment()
	if rcState == nil || rcState.log == nil {
		return nil, "", false
	}
	prevID, ok := previousReadcacheOwnerFromLog(rcState.log, d.now(), partitionID)
	if !ok {
		return nil, "", false
	}
	// prevID is a logical slot under RF≥2; expand and take the first
	// replica that dials.
	for _, rep := range expandReadcacheReplicasForQuery(rcState.replicaMap, prevID, d.cfg.Readcache.IgnoreReplicaMapForQueries) {
		cli, err := d.readcachePool.GetClientForInstance(ctx, rep.InstanceID)
		if err != nil {
			continue
		}
		return cli, rep.InstanceID, true
	}
	return nil, "", false
}

// shouldRouteReadToReadcache reports whether the per-tenant read
// routing knob asks the distributor to serve this tenant from
// readcache. The check is skipped (returns false) when the tenant
// can't be resolved from the context — read-path callers always
// have a tenant, so this only fires in tests.
func (d *Distributor) shouldRouteReadToReadcache(ctx context.Context) bool {
	if d.limits == nil {
		return false
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return false
	}
	return d.limits.ReadcacheReadRouting(userID) == validation.ReadcacheReadRoutingNautilus
}
