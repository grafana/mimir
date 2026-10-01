// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/prometheus/alertmanager/cluster"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
)

// activeAndPendingSilences returns the tenant's active and pending silences sorted by ID.
func (am *Alertmanager) activeAndPendingSilences(ctx context.Context) ([]*silencepb.Silence, error) {
	sils, _, err := am.silences.Query(ctx, silence.QState(silence.SilenceStateActive, silence.SilenceStatePending))
	if err != nil {
		return nil, err
	}
	sort.Slice(sils, func(i, j int) bool { return sils[i].Id < sils[j].Id })
	return sils, nil
}

// silencesDigestCache memoizes one tenant's digest.
type silencesDigestCache struct {
	mtx        sync.Mutex
	computed   bool
	generation uint64
	deadline   time.Time
	value      uint64
}

// generationTrackingState moves generation after every merge into the wrapped state.
type generationTrackingState struct {
	cluster.State
	generation *atomic.Uint64
}

func (g *generationTrackingState) Merge(b []byte) error {
	// Moved after merging, and even on error, since a merge can fail after applying some entries.
	// Moving it before would let a concurrent digest cache the pre-merge silences under the new generation.
	defer g.generation.Inc()
	return g.State.Merge(b)
}

// silencesDigest hashes (ID, UpdatedAt) pairs, so it changes on a content edit as well as a
// silence appearing or disappearing, not just the latter.
//
// The result is cached, valid until either silencesGeneration moves (a write or merge happened) or
// 'now' reaches the deadline - the soonest EndsAt (expiration) among the hashed silences.
//
// The cache isn't keyed on silences.Version(), which only moves when a silence with a new ID is
// added, not when one is updated or expired.
func (am *Alertmanager) silencesDigest(ctx context.Context) (uint64, error) {
	c := &am.silencesDigestCache
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// Read before querying, so a write landing during the query leaves the cached generation behind.
	generation := am.silencesGeneration.Load()
	if c.computed && c.generation == generation && (c.deadline.IsZero() || time.Now().Before(c.deadline)) {
		return c.value, nil
	}

	sils, err := am.activeAndPendingSilences(ctx)
	if err != nil {
		return 0, err
	}

	h := fnv.New64a()
	var deadline time.Time
	var buf []byte
	// The hash is order-sensitive, so this relies on sils being sorted by ID: replicas holding the
	// same silences must feed them in the same order.
	for _, sil := range sils {
		// 255 terminates the ID. It can't occur in a UUID, and the fixed-width timestamp after it
		// needs no delimiter, so two different sets of silences can't produce the same bytes.
		buf = append(buf[:0], sil.Id...)
		buf = append(buf, 255)
		buf = binary.BigEndian.AppendUint64(buf, uint64(sil.GetUpdatedAt().AsTime().UnixNano()))
		_, _ = h.Write(buf)
		if endsAt := sil.GetEndsAt().AsTime(); deadline.IsZero() || endsAt.Before(deadline) {
			deadline = endsAt
		}
	}

	c.value = h.Sum64()
	c.generation = generation
	c.deadline = deadline
	c.computed = true
	return c.value, nil
}

// tenantDigestsWorkers is how many tenant digests ReadTenantDigests computes at once.
const tenantDigestsWorkers = 8

// tenantDigestsMaxReplyMargin caps how much of a request's deadline ReadTenantDigests leaves for its
// answer to reach the peer.
const tenantDigestsMaxReplyMargin = time.Second

// ReadTenantDigests implements the Alertmanager service. It lets a peer check whether its view of
// a batch of tenants' silences matches this replica's, without transferring full state. If the
// request has a deadline, it returns the digests computed by then, minus a margin of 1/4 of the
// remaining time up to tenantDigestsMaxReplyMargin, and leaves the rest out. Digests still being
// computed then finish in the background.
func (am *MultitenantAlertmanager) ReadTenantDigests(ctx context.Context, req *alertmanagerpb.TenantDigestsRequest) (*alertmanagerpb.TenantDigestsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var cutoff <-chan time.Time
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		timer := time.NewTimer(remaining - min(remaining/4, tenantDigestsMaxReplyMargin))
		defer timer.Stop()
		cutoff = timer.C
	}

	// Buffered so workers never block on a handler that already returned.
	results := make(chan *alertmanagerpb.TenantDigest, len(req.UserIds))
	// Canceling stops workers from starting new tenants once the handler returns.
	workCtx, stopWork := context.WithCancelCause(ctx)
	defer stopWork(errors.New("tenant digests request handled"))

	go func() {
		_ = concurrency.ForEachJob(workCtx, len(req.UserIds), tenantDigestsWorkers, func(jobCtx context.Context, idx int) error {
			results <- am.tenantDigest(jobCtx, req.UserIds[idx])
			return nil
		})
	}()

	resp := &alertmanagerpb.TenantDigestsResponse{Digests: make([]*alertmanagerpb.TenantDigest, 0, len(req.UserIds))}
	for range req.UserIds {
		select {
		case td := <-results:
			resp.Digests = append(resp.Digests, td)
		case <-cutoff:
			// Keep digests that finished at the same time as the cutoff.
			for len(results) > 0 {
				resp.Digests = append(resp.Digests, <-results)
			}
			level.Warn(am.logger).Log("msg", "Returning partial tenant digests before the request deadline", "requested", len(req.UserIds), "answered", len(resp.Digests))
			return resp, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return resp, nil
}

// tenantDigest returns userID's entry for a ReadTenantDigests response.
func (am *MultitenantAlertmanager) tenantDigest(ctx context.Context, userID string) *alertmanagerpb.TenantDigest {
	am.alertmanagersMtx.Lock()
	userAM, ok := am.alertmanagers[userID]
	am.alertmanagersMtx.Unlock()

	if !ok {
		return &alertmanagerpb.TenantDigest{UserId: userID, Found: false}
	}

	// Found means the tenant is here, not that its state is trustworthy. An unsynced tenant
	// sends no digest rather than a hash a peer would try to converge on.
	td := &alertmanagerpb.TenantDigest{UserId: userID, Found: true}
	if userAM.initialStateSynced() {
		digest, err := userAM.silencesDigest(ctx)
		if err != nil {
			level.Warn(am.logger).Log("msg", "Failed to compute silences digest", "user", userID, "err", err)
		} else {
			td.Digests = append(td.Digests, &alertmanagerpb.Digest{
				Type:  alertmanagerpb.DigestType_DIGEST_TYPE_SILENCES,
				Value: digest,
			})
		}
	}
	return td
}

// silencesDigestFrom returns the peer's silences digest and whether it sent one at all. An absent
// digest must not be read as zero, which is indistinguishable from a real hash.
func silencesDigestFrom(d *alertmanagerpb.TenantDigest) (uint64, bool) {
	for _, digest := range d.Digests {
		if digest.Type == alertmanagerpb.DigestType_DIGEST_TYPE_SILENCES {
			return digest.Value, true
		}
	}
	return 0, false
}
