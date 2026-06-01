// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

// distributorSpotlight intervals. The poller runs more frequently
// than the emitter so the spotlight set is fresh enough that
// freshly-flagged ranges start collecting observations on their
// first write batch, but the emit interval is wide enough that one
// log line summarizes meaningful sample volume per (spotlight,
// partition) pair.
const (
	distributorSpotlightPollInterval = 10 * time.Second
	distributorSpotlightEmitInterval = 30 * time.Second
	distributorSpotlightPollTimeout  = 2 * time.Second

	// distributorSpotlightSoftCap is a safety log threshold: if the
	// active spotlight count ever exceeds it, per-write linear
	// scanning has become an unbudgeted cost on the hot path and
	// the rebalancer is likely misbehaving. Emit a one-time warning
	// per poll round and continue (rather than truncate) — the
	// alternative drops data and obscures the underlying bug.
	distributorSpotlightSoftCap = 200
)

// distributorSpotlightTracker holds the distributor-side cache of
// the rebalancer's spotlight set plus an accumulator of per-
// (spotlight, partition) sample counts observed since the last emit.
//
// The accumulator is keyed by (trace_id, partition_id). A separate
// observe path tallies into a per-call local map and merges under
// the tracker mutex once — keeping the hot-path write barrier short
// even when a single batch routes thousands of series across many
// partitions.
type distributorSpotlightTracker struct {
	mu         sync.RWMutex
	spotlights []rebalancer.SpotlightedRange

	// observations[traceID][partitionID] is the sample count routed
	// to that partition for that spotlighted range since the last
	// emit. Nil until first observation; reset to nil on each emit.
	observations map[string]map[int32]int64

	// lastEmitAt is the wall-clock time of the previous emitAndReset
	// (or tracker construction, on the first emit). emitAndReset
	// divides the per-(spotlight, partition) sample counter by the
	// time elapsed since lastEmitAt to derive a samples/sec rate;
	// using measured elapsed (rather than the nominal ticker
	// interval) keeps the rate honest if the emit goroutine fires
	// late or if shutdown flushes a partial interval.
	lastEmitAt time.Time

	// nowFn is the clock the tracker consults for lastEmitAt and
	// elapsed-time computations. Defaults to time.Now and is
	// overridable by tests so emit-rate assertions are deterministic.
	nowFn func() time.Time
}

func newDistributorSpotlightTracker() *distributorSpotlightTracker {
	return &distributorSpotlightTracker{
		lastEmitAt: time.Now(),
		nowFn:      time.Now,
	}
}

// snapshot returns the currently cached spotlight set. The returned
// slice aliases internal state and must not be mutated; callers
// iterate it read-only on the write path.
func (t *distributorSpotlightTracker) snapshot() []rebalancer.SpotlightedRange {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.spotlights
}

// setSpotlights replaces the cached set. The new slice is taken
// over verbatim (no defensive copy); callers must not mutate it
// after handing it off.
func (t *distributorSpotlightTracker) setSpotlights(s []rebalancer.SpotlightedRange) {
	t.mu.Lock()
	t.spotlights = s
	t.mu.Unlock()
}

// observeWrite is the hot-path hook. For each spotlighted range,
// counts samples written to each partition this request routes to.
// Cheap when no spotlights are active: an RLock + len check + nil
// return.
//
// `keys` and `req.Timeseries` are parallel: keys[i] is the
// hash of req.Timeseries[i] for i < initialMetadataIndex.
// Metadata indexes (i >= initialMetadataIndex) are skipped because
// they don't carry samples.
func (t *distributorSpotlightTracker) observeWrite(keys []uint32, partitionKeys []ring.PartitionKeys, req *mimirpb.WriteRequest, initialMetadataIndex int) {
	if req == nil {
		return
	}
	t.mu.RLock()
	spotlights := t.spotlights
	t.mu.RUnlock()
	if len(spotlights) == 0 {
		return
	}

	// Local-then-merge so the hot path doesn't hold the tracker
	// mutex across O(partitions * keys) work. The local map almost
	// always has very few entries because most writes miss the
	// spotlight set entirely.
	type accKey struct {
		traceID     string
		partitionID int32
	}
	var local map[accKey]int64

	for _, pk := range partitionKeys {
		for _, idx := range pk.Indexes {
			if idx >= initialMetadataIndex {
				continue
			}
			if idx >= len(req.Timeseries) || idx >= len(keys) {
				// Defensive: getSeriesAndMetadataTokens guarantees
				// these are in range, but a future caller might
				// pass mismatched slices.
				continue
			}
			keyHash := keys[idx]
			var sampleCount int
			for _, sp := range spotlights {
				if keyHash < sp.Lo || keyHash > sp.Hi {
					continue
				}
				if sampleCount == 0 {
					ts := req.Timeseries[idx]
					sampleCount = len(ts.Samples) + len(ts.Histograms)
					if sampleCount == 0 {
						break
					}
				}
				if local == nil {
					local = make(map[accKey]int64)
				}
				local[accKey{traceID: sp.TraceId, partitionID: pk.PartitionID}] += int64(sampleCount)
			}
		}
	}

	if len(local) == 0 {
		return
	}

	t.mu.Lock()
	if t.observations == nil {
		t.observations = make(map[string]map[int32]int64, len(local))
	}
	for k, v := range local {
		m, ok := t.observations[k.traceID]
		if !ok {
			m = make(map[int32]int64, 1)
			t.observations[k.traceID] = m
		}
		m[k.partitionID] += v
	}
	t.mu.Unlock()
}

// emitAndReset writes one log line per (spotlight, partition) seen
// since the last emit, then clears the accumulator. Called by
// runPoller every distributorSpotlightEmitInterval.
//
// instanceID is logged so an operator inspecting multi-replica logs
// can see how a single spotlight's traffic is distributed across
// distributor pods.
//
// Each line carries both the cumulative `samples_routed` over the
// interval and `samples_per_sec` (samples_routed / elapsed). The
// rate is the more useful signal for rebalancer correlation —
// elapsed varies if the emit goroutine fires late or if shutdown
// flushes a partial interval — but we keep the absolute count too
// so adding two pods' logs reconstructs the true sample volume.
func (t *distributorSpotlightTracker) emitAndReset(logger log.Logger, instanceID string) {
	t.mu.Lock()
	spotlights := t.spotlights
	obs := t.observations
	t.observations = nil
	now := t.nowFn()
	elapsed := now.Sub(t.lastEmitAt)
	t.lastEmitAt = now
	t.mu.Unlock()
	if len(obs) == 0 {
		return
	}

	// Clamp elapsed below: a 0 or negative interval (clock skew on
	// the first emit after construction, or a test stub returning a
	// frozen time) would divide-by-zero. Reporting "all the volume
	// happened in 1ms" is misleading but easier to spot in a
	// dashboard than NaN/Inf.
	elapsedSec := elapsed.Seconds()
	if elapsedSec <= 0 {
		elapsedSec = 0.001
	}

	byID := make(map[string]rebalancer.SpotlightedRange, len(spotlights))
	for _, sp := range spotlights {
		byID[sp.TraceId] = sp
	}

	for traceID, perPart := range obs {
		sp, ok := byID[traceID]
		if !ok {
			// The spotlight expired between writes and emit;
			// drop silently. The decision-time log from the
			// rebalancer is still in Loki, so the trace_id is
			// not orphaned — there's just nothing useful left
			// to say about this interval.
			continue
		}
		for partID, samples := range perPart {
			level.Info(logger).Log(
				"msg", "nautilus spotlight: distributor observation",
				"spotlight_id", traceID,
				"reason", sp.Reason,
				"range_lo", sp.Lo,
				"range_hi", sp.Hi,
				"from_partition", sp.FromPartitionId,
				"to_partition", sp.ToPartitionId,
				"observed_partition", partID,
				"samples_routed", samples,
				"samples_per_sec", float64(samples)/elapsedSec,
				"interval_sec", elapsedSec,
				"instance_id", instanceID,
			)
		}
	}
}

// runSpotlightLoop polls the rebalancer for the active spotlight
// set and periodically emits accumulated observations. Exits when
// ctx is cancelled. Safe to call once at startup from a fresh
// goroutine; no-op if conn is nil.
func (d *Distributor) runSpotlightLoop(ctx context.Context) {
	conn, ok := d.nautilusRebalancerConn.(*grpc.ClientConn)
	if !ok || conn == nil {
		return
	}
	cli := rebalancer.NewNautilusRebalancerClient(conn)

	pollT := time.NewTicker(distributorSpotlightPollInterval)
	emitT := time.NewTicker(distributorSpotlightEmitInterval)
	defer pollT.Stop()
	defer emitT.Stop()

	// Best-effort instance ID: empty when the distributor isn't
	// wired into the ring (tests). The spotlight log lines stay
	// useful without it.
	instanceID := d.cfg.DistributorRing.Common.InstanceID

	// Prime once immediately so we don't wait the full poll interval
	// for the first set after startup.
	d.pollSpotlights(ctx, cli)

	for {
		select {
		case <-ctx.Done():
			// Flush remaining observations before exiting so
			// shutdown logs don't drop in-flight data.
			d.spotlights.emitAndReset(d.log, instanceID)
			return
		case <-pollT.C:
			d.pollSpotlights(ctx, cli)
		case <-emitT.C:
			d.spotlights.emitAndReset(d.log, instanceID)
		}
	}
}

// pollSpotlights issues one GetSpotlightedRanges call against the
// rebalancer and updates the local cache. Failures are logged at
// debug; the previous cache is preserved (better stale than empty
// during transient rebalancer outages).
func (d *Distributor) pollSpotlights(ctx context.Context, cli rebalancer.NautilusRebalancerClient) {
	rpcCtx, cancel := context.WithTimeout(ctx, distributorSpotlightPollTimeout)
	defer cancel()
	resp, err := cli.GetSpotlightedRanges(rpcCtx, &rebalancer.GetSpotlightedRangesRequest{})
	if err != nil {
		level.Debug(d.log).Log("msg", "nautilus spotlight: poll failed", "err", err)
		return
	}
	d.spotlights.setSpotlights(resp.Ranges)
	if n := len(resp.Ranges); n > distributorSpotlightSoftCap {
		level.Warn(d.log).Log(
			"msg", "nautilus spotlight: active set exceeds soft cap; per-write scanning may add measurable latency",
			"count", n,
			"soft_cap", distributorSpotlightSoftCap,
		)
	}
}
