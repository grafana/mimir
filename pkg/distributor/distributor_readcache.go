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
// At the end of a query the tracker's count feeds the
// cortex_distributor_query_readcache_instances_hit_per_query
// histogram. A count of 0 is a load-bearing datapoint: it means the
// query was served entirely from ingesters (either because the
// tenant isn't on nautilus-only routing, the assignment log was
// empty, or every partition's readcache owner failed to dial and
// the path fell back to ingesters). Tracking that "0" lets us
// observe the migration as the value drifts upward.
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

// GetReadcacheLog returns the current (partition -> readcache
// instance) log streamed from the rebalancer, or nil if no snapshot
// has been received yet (cold start or rebalancer unreachable).
func (d *Distributor) GetReadcacheLog() *readcacheassignment.Log {
	return d.readcacheLog.Load()
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
		stream, err := client.WatchReadcacheAssignments(ctx, &rebalancer.WatchReadcacheAssignmentsRequest{})
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

func (d *Distributor) consumeReadcacheStream(stream rebalancer.NautilusRebalancer_WatchReadcacheAssignmentsClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		log := readcacheassignment.NewLogFromEntries(rebalancer.ReadcacheEntriesFromProto(resp.Entries))
		d.readcacheLog.Store(log)
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
	log := d.readcacheLog.Load()
	if log == nil {
		return nil, "", false, nil
	}
	owners := log.Lookup(d.now(), partitionID)
	if len(owners) == 0 {
		return nil, "", false, nil
	}
	// Phase 2C is single-owner-per-partition; multi-owner mode
	// from readcacheassignment.Log is reserved for future drain/
	// handoff work. Pick the first owner deterministically.
	cli, err = d.readcachePool.GetClientForInstance(ctx, owners[0])
	if err != nil {
		return nil, "", false, err
	}
	return cli, owners[0], true, nil
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
	log := d.readcacheLog.Load()
	if log == nil {
		return "", false
	}
	now := d.now()
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
		// e.From <= now and e.InstanceID != current owner.
		// Acceptable as a fallback if its To is "recent enough".
		// The rebalancer's cool-down policy (≈ 2x warmup) caps how
		// stale a previous-owner can be; we trust the log's
		// retention rather than checking explicit recency here.
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
// records into hits (a readcache count of zero means "served
// entirely from ingesters", which is the migration baseline).
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
	prevID, ok := d.previousReadcacheOwnerForPartition(partitionID)
	if !ok {
		return nil, "", false
	}
	cli, err := d.readcachePool.GetClientForInstance(ctx, prevID)
	if err != nil {
		return nil, "", false
	}
	return cli, prevID, true
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
