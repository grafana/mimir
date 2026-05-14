// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
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
// it. Returns ok=false when:
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
// In ok=false cases where err is non-nil, the caller may log it; nil
// err with ok=false simply means the partition is not currently
// covered by a readcache lease.
func (d *Distributor) resolveReadcacheClientForPartition(ctx context.Context, partitionID int32) (client.IngesterClient, bool, error) {
	if d.readcachePool == nil {
		return nil, false, nil
	}
	log := d.readcacheLog.Load()
	if log == nil {
		return nil, false, nil
	}
	owners := log.Lookup(d.now(), partitionID)
	if len(owners) == 0 {
		return nil, false, nil
	}
	// Phase 2C is single-owner-per-partition; multi-owner mode
	// from readcacheassignment.Log is reserved for future drain/
	// handoff work. Pick the first owner deterministically.
	cli, err := d.readcachePool.GetClientForInstance(ctx, owners[0])
	if err != nil {
		return nil, false, err
	}
	return cli, true, nil
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

// queryClientForInstance returns the gRPC client the
// distributor's read path should use for the given ingester
// instance. When a readcache pod owns the partition that mapped
// the instance and a readcache pool is configured, the readcache
// client is returned; otherwise the ingester client from the
// shared pool is returned.
//
// Errors from readcache resolution are logged but never bubble up:
// a failed readcache dial degrades to "served by ingester".
// Transport errors observed mid-stream (after the dial succeeded)
// surface to the caller in the usual way and are handled by the
// normal ring-quorum retry machinery.
func (d *Distributor) queryClientForInstance(ctx context.Context, ing ring.InstanceDesc, partitionByInstance map[string]int32, logger log.Logger) (client.IngesterClient, error) {
	if d.shouldRouteReadToReadcache(ctx) && partitionByInstance != nil && d.readcachePool != nil {
		if partID, ok := partitionByInstance[ing.Id]; ok {
			rcClient, ok, err := d.resolveReadcacheClientForPartition(ctx, partID)
			if err != nil {
				level.Warn(logger).Log("msg", "readcache resolution failed; falling back to ingester", "partition", partID, "err", err)
			} else if ok {
				return rcClient, nil
			}
		}
	}
	c, err := d.ingesterPool.GetClientForInstance(ing)
	if err != nil {
		return nil, err
	}
	return c.(client.IngesterClient), nil
}

// previousReadcacheClientForPartition returns the gRPC client for
// the readcache instance that owned partitionID in the lease
// immediately preceding the current one. Used by the caller to
// fall back when the current owner returns errStillWarming.
//
// Returns ok=false if there is no recoverable previous owner (no
// log, lease pruned, address unconfigured, or dial error).
func (d *Distributor) previousReadcacheClientForPartition(ctx context.Context, partitionID int32) (client.IngesterClient, bool) {
	if d.readcachePool == nil {
		return nil, false
	}
	prevID, ok := d.previousReadcacheOwnerForPartition(partitionID)
	if !ok {
		return nil, false
	}
	cli, err := d.readcachePool.GetClientForInstance(ctx, prevID)
	if err != nil {
		return nil, false
	}
	return cli, true
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
