// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
)

// Reconciliation check outcome label values.
const (
	checkInSync   = "in_sync"
	checkRepaired = "repaired"
	checkNoChange = "no_change"
	checkSkipped  = "skipped"
	checkFailed   = "failed"
)

// silenceResyncWorkers is how many tenants are resynced from peers at once during silence reconciliation.
const silenceResyncWorkers = 8

// newSilenceReconcileChecksTotal creates the counter for per-tenant silences digest comparisons
// with peers. Every tenant requested from an answering peer lands in exactly one outcome.
func newSilenceReconcileChecksTotal(reg prometheus.Registerer) *prometheus.CounterVec {
	c := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_alertmanager_silence_reconcile_checks_total",
		Help: "Number of per-tenant silences digest comparisons with a peer during silence reconciliation, by outcome.",
	}, []string{"outcome"})
	for _, o := range []string{checkInSync, checkRepaired, checkNoChange, checkSkipped, checkFailed} {
		c.WithLabelValues(o)
	}
	return c
}

// reconcileSilences resyncs any owned tenant whose silences digest differs from an owning peer's.
func (am *MultitenantAlertmanager) reconcileSilences(ctx context.Context) {
	am.alertmanagersMtx.Lock()
	userIDs := make([]string, 0, len(am.alertmanagers))
	for userID := range am.alertmanagers {
		userIDs = append(userIDs, userID)
	}
	am.alertmanagersMtx.Unlock()

	// Different tenants can be owned by different replica sets under the hash ring, so group them
	// by which peer to ask and send one batched request per peer instead of one per tenant.
	byPeer := make(map[string][]string)
	for _, userID := range userIDs {
		set, err := am.ring.Get(shardByUser(userID), RingOp, nil, nil, nil)
		if err != nil {
			am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
			level.Warn(am.logger).Log("msg", "Failed to look up tenant's replicas for silence reconciliation", "user", userID, "err", err)
			continue
		}

		for _, addr := range set.GetAddressesWithout(am.ringLifecycler.GetInstanceAddr()) {
			byPeer[addr] = append(byPeer[addr], userID)
		}
	}

	// Compare digests with every peer concurrently and hand diverged tenants to resync workers as they
	// come in. A per-tenant lock keeps a tenant from being resynced from two peers at once.
	resyncs := make(chan pendingResync)
	var comparing sync.WaitGroup
	for peer, tenantIDs := range byPeer {
		comparing.Add(1)
		go func() {
			defer comparing.Done()
			for _, p := range am.compareWithPeer(ctx, peer, tenantIDs) {
				resyncs <- p
			}
		}()
	}
	go func() {
		comparing.Wait()
		close(resyncs)
	}()

	// Locks are created for diverged tenants only and never deleted during the run, so every worker
	// resyncing a tenant shares the same one.
	var tenantLocks sync.Map
	var resyncing sync.WaitGroup
	for range silenceResyncWorkers {
		resyncing.Add(1)
		go func() {
			defer resyncing.Done()
			for p := range resyncs {
				v, _ := tenantLocks.LoadOrStore(p.userID, &sync.Mutex{})
				mtx := v.(*sync.Mutex)
				mtx.Lock()
				am.resyncFromPeer(ctx, p)
				mtx.Unlock()
			}
		}()
	}
	resyncing.Wait()
}

// pendingResync is a tenant to resync from a peer whose silences digest differs.
type pendingResync struct {
	userID     string
	userAM     *Alertmanager
	addr       string
	client     Client
	peerDigest uint64
}

// compareWithPeer asks addr for its digest of each of userIDs, counts the tenants that match or can't
// be compared, and returns the ones whose silences digest differs.
func (am *MultitenantAlertmanager) compareWithPeer(ctx context.Context, addr string, userIDs []string) []pendingResync {
	c, err := am.alertmanagerClientsPool.GetClientFor(addr)
	if err != nil {
		am.silenceReconcilePeerErrorsTotal.Inc()
		level.Warn(am.logger).Log("msg", "Failed to get client for silence reconciliation", "peer", addr, "err", err)
		return nil
	}

	digestCtx, cancel := context.WithTimeout(ctx, am.cfg.AlertmanagerClient.RemoteTimeout)
	defer cancel()
	// The client's gRPC instrumentation requires an org ID, which ReadTenantDigests ignores.
	resp, err := c.ReadTenantDigests(user.InjectOrgID(digestCtx, ""), &alertmanagerpb.TenantDigestsRequest{UserIds: userIDs})
	if err != nil {
		am.silenceReconcilePeerErrorsTotal.Inc()
		level.Warn(am.logger).Log("msg", "Failed to read tenant digests for silence reconciliation", "peer", addr, "err", err)
		return nil
	}

	answered := make(map[string]struct{}, len(resp.Digests))
	for _, d := range resp.Digests {
		answered[d.UserId] = struct{}{}
	}
	for _, userID := range userIDs {
		if _, ok := answered[userID]; !ok {
			am.silenceReconcileChecksTotal.WithLabelValues(checkSkipped).Inc()
			level.Debug(am.logger).Log("msg", "Skipping silence reconciliation check, peer didn't answer for the tenant", "user", userID, "peer", addr)
		}
	}

	var resyncs []pendingResync
	for _, d := range resp.Digests {
		if !d.Found {
			am.silenceReconcileChecksTotal.WithLabelValues(checkSkipped).Inc()
			level.Debug(am.logger).Log("msg", "Skipping silence reconciliation check, peer doesn't hold the tenant", "user", d.UserId, "peer", addr)
			continue
		}

		peerDigest, sent := silencesDigestFrom(d)
		if !sent {
			am.silenceReconcileChecksTotal.WithLabelValues(checkSkipped).Inc()
			level.Debug(am.logger).Log("msg", "Skipping silence reconciliation check, peer hasn't synced the tenant yet", "user", d.UserId, "peer", addr)
			continue
		}

		am.alertmanagersMtx.Lock()
		userAM, ok := am.alertmanagers[d.UserId]
		am.alertmanagersMtx.Unlock()
		if !ok {
			am.silenceReconcileChecksTotal.WithLabelValues(checkSkipped).Inc()
			level.Debug(am.logger).Log("msg", "Skipping silence reconciliation check, tenant's Alertmanager no longer active here", "user", d.UserId, "peer", addr)
			continue
		}
		// Pulling from a synced peer is safe even if this replica's own initial sync failed, and that
		// replica is the one most likely to have diverged, so only skip while the sync is still running.
		if userAM.state.State() != services.Running {
			am.silenceReconcileChecksTotal.WithLabelValues(checkSkipped).Inc()
			level.Debug(am.logger).Log("msg", "Skipping silence reconciliation check, tenant's initial sync hasn't finished here", "user", d.UserId, "peer", addr)
			continue
		}

		localDigest, err := userAM.silencesDigest(ctx)
		if err != nil {
			am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
			level.Error(am.logger).Log("msg", "Failed to compute local silences digest while reconciling", "user", d.UserId, "err", err)
			continue
		}
		if localDigest == peerDigest {
			am.silenceReconcileChecksTotal.WithLabelValues(checkInSync).Inc()
			continue
		}

		resyncs = append(resyncs, pendingResync{userID: d.UserId, userAM: userAM, addr: addr, client: c, peerDigest: peerDigest})
	}
	return resyncs
}

// resyncFromPeer resyncs p's tenant from its peer if their silences digests still differ, in a single
// attempt. The next run is the retry.
func (am *MultitenantAlertmanager) resyncFromPeer(ctx context.Context, p pendingResync) {
	userAM, c, addr, peerDigest := p.userAM, p.client, p.addr, p.peerDigest
	// An earlier resync from another peer may have brought the tenant in sync.
	localDigest, err := userAM.silencesDigest(ctx)
	if err != nil {
		am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
		level.Error(am.logger).Log("msg", "Failed to compute local silences digest while reconciling", "user", p.userID, "err", err)
		return
	}
	if localDigest == peerDigest {
		am.silenceReconcileChecksTotal.WithLabelValues(checkInSync).Inc()
		return
	}

	// Digests differ, so resync this tenant's silences from the peer.
	resyncCtx, cancel := context.WithTimeout(ctx, defaultSettleReadTimeout)
	readResp, err := c.ReadState(user.InjectOrgID(resyncCtx, p.userID), &alertmanagerpb.ReadStateRequest{OnlySilences: true})
	cancel()
	if err == nil && readResp.Status != alertmanagerpb.ReadStateStatus_READ_OK {
		err = fmt.Errorf("unexpected read status %s: %s", readResp.Status, readResp.Error)
	}
	if err != nil {
		am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
		level.Warn(am.logger).Log("msg", "Failed to read silences while reconciling", "user", p.userID, "peer", addr, "err", err)
		return
	}
	if err := userAM.state.MergeFullStates([]*clusterpb.FullState{readResp.State}); err != nil {
		am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
		level.Error(am.logger).Log("msg", "Failed to merge silences while reconciling", "user", p.userID, "peer", addr, "err", err)
		return
	}
	// The peer only sends a digest once its own initial sync succeeded, so after merging its silences
	// this replica's are authoritative too, even if its own initial sync failed.
	userAM.state.markInitialSyncDone()

	// An unchanged digest means the peer was the stale one.
	after, err := userAM.silencesDigest(ctx)
	if err != nil {
		am.silenceReconcileChecksTotal.WithLabelValues(checkFailed).Inc()
		level.Error(am.logger).Log("msg", "Failed to compute local silences digest while reconciling", "user", p.userID, "peer", addr, "err", err)
		return
	}
	if after == localDigest {
		am.silenceReconcileChecksTotal.WithLabelValues(checkNoChange).Inc()
		level.Debug(am.logger).Log("msg", "Silences digest diverged from peer but reconciling changed nothing locally", "user", p.userID, "peer", addr, "digest", localDigest, "peer_digest", peerDigest)
		return
	}
	am.silenceReconcileChecksTotal.WithLabelValues(checkRepaired).Inc()
	level.Info(am.logger).Log("msg", "Repaired silences from peer", "user", p.userID, "peer", addr, "previous_digest", localDigest, "new_digest", after)
}
