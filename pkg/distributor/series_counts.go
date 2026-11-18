// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SeriesCountsResult is what the ingesters answered to a SeriesCounts request.
// When Hashed is false, adding up the responses' counts gives each series once;
// when true, the responses carry hashes and must be unioned.
type SeriesCountsResult struct {
	Responses []*ingester_client.SeriesCountsResponse
	// Hashed is set when the responses carry hashes, asked for or because counts can't be added up.
	Hashed bool
	// release gives back what the answers are charged to the context's memory tracker.
	release func()
}

// NewSeriesCountsResult returns a result whose Release calls release, for a source of answers other
// than Distributor.SeriesCounts that charges them to the memory consumption tracker itself.
func NewSeriesCountsResult(responses []*ingester_client.SeriesCountsResponse, hashed bool, release func()) SeriesCountsResult {
	return SeriesCountsResult{Responses: responses, Hashed: hashed, release: release}
}

// Release gives back the memory the answers were charged to the memory consumption tracker of the
// request's context. The caller calls it once it has merged Responses and no longer holds them.
func (r SeriesCountsResult) Release() {
	if r.release != nil {
		r.release()
	}
}

// ErrSeriesCountsOverMemoryLimit is wrapped by the error for answers that pass the limit of the context's memory consumption tracker.
var ErrSeriesCountsOverMemoryLimit = errors.New("the ingester replies held pass the memory limit")

// seriesCountsGroupBytes is what a group of an answer is charged on top of its value, counts and hashes.
const seriesCountsGroupBytes = 64

// logSeriesCountsFailure logs which ingester a call failed on, unless the request was cancelled or reached quorum.
func logSeriesCountsFailure(ctx context.Context, logger log.Logger, addr string, err error) {
	if ctx.Err() != nil {
		return
	}
	level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "series counts failed on an ingester", "ingester", addr, "err", err)
}

// SeriesCounts asks the ingesters to count series per value of one label over
// a window, from their heads' chunk metas, using a query's replication sets.
// Counts are kept only when each set has a single ingester or, under ingest
// storage, one ingester per zone, the partitions have been stable since before
// the window and no ingester saw the tenant's shard size change. Otherwise it asks
// for hashes, which the caller unions, with a second request when only the
// answers show the shard size change.
//
// The answers held are charged to the context's memory consumption tracker, if any, as they
// arrive. The caller gives the charge back with the result's Release.
func (d *Distributor) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *ingester_client.SeriesCountsRequest) (_ SeriesCountsResult, returnErr error) {
	replicationSets, partitionsStable, changedBefore, err := d.seriesCountsReplicationSets(ctx, matchers, req.StartMs)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	charge := &seriesCountsCharge{tracker: tracker}
	defer func() {
		if returnErr != nil {
			charge.close()
		}
	}()
	hashed := req.Hashes
	if !d.seriesCountsAddUp(replicationSets, partitionsStable) && !req.Hashes {
		hashed = true
		d.seriesCountsSwitchedToHashes.WithLabelValues("replication_sets").Inc()
	}
	// Only the ingesters know whether the shard size changed over the window.
	checkShardSize := !hashed && d.cfg.IngestStorageConfig.Enabled && len(replicationSets) > 1

	// Split max_index_bytes and max_hashes across the ingesters that will be asked.
	shared := *req
	shared.Hashes = hashed
	if checkShardSize {
		shared.ShardSizeUnchangedBeforeMs = changedBefore
	}
	calls := d.seriesCountsCalls(replicationSets)
	if calls > 1 {
		if req.MaxIndexBytes > 0 {
			shared.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(calls))
		}
		if req.MaxHashes > 0 {
			shared.MaxHashes = max(1, req.MaxHashes/int64(calls))
		}
	}

	perSet, spent, err := d.seriesCountsFanOut(ctx, replicationSets, &shared, charge)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	if checkShardSize && !shardSizeUnchanged(perSet, changedBefore) {
		// The answers of the first round are dropped.
		for _, answers := range perSet {
			for _, a := range answers {
				charge.sub(a.charged)
			}
		}
		hashed = true
		d.seriesCountsSwitchedToHashes.WithLabelValues("shard_size_changed").Inc()
		// A copy: a call of the first round cancelled at quorum may still hold shared.
		again := shared
		again.Hashes = true
		again.ShardSizeUnchangedBeforeMs = 0
		if req.MaxIndexBytes > 0 {
			// The second round gets what the first left of max_index_bytes.
			if again.MaxIndexBytes = (req.MaxIndexBytes - spent) / int64(max(1, calls)); again.MaxIndexBytes < 1 {
				return SeriesCountsResult{}, cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the tenant's shard size changed, so the ingesters are asked again for hashes, and the first round used %d of max_index_bytes %d", spent, req.MaxIndexBytes)
			}
		}
		if perSet, _, err = d.seriesCountsFanOut(ctx, replicationSets, &again, charge); err != nil {
			return SeriesCountsResult{}, err
		}
	}

	out := SeriesCountsResult{Hashed: hashed, release: charge.close}
	for i, answers := range perSet {
		if out.Hashed || !zoneAware(replicationSets[i]) {
			// Hashes union to the same count however many replicas sent them.
			for _, a := range answers {
				out.Responses = append(out.Responses, a.resp)
			}
			continue
		}
		out.Responses = append(out.Responses, maxOverZones(answersByZone(answers)))
	}
	return out, nil
}

// seriesCountsCalls is how many ingesters ring.DoUntilQuorum asks for sets when none of them fails.
func (d *Distributor) seriesCountsCalls(sets []ring.ReplicationSet) int {
	calls := 0
	for _, rs := range sets {
		switch {
		case !d.cfg.MinimizeIngesterRequests:
			calls += len(rs.Instances)
		case zoneAware(rs):
			perZone := map[string]int{}
			for _, instance := range rs.Instances {
				perZone[instance.Zone]++
			}
			sizes := slices.Sorted(maps.Values(perZone))
			// The zone sorter picks the zones, so assume the largest.
			for _, n := range sizes[min(len(sizes), max(0, rs.MaxUnavailableZones)):] {
				calls += n
			}
		default:
			calls += max(0, len(rs.Instances)-rs.MaxErrors)
		}
	}
	return calls
}

// seriesCountsFanOut sends req to the replication sets' ingesters, charging the answers to charge.
// It also returns what the ingesters called used of max_index_bytes: what each answer reports,
// DoUntilQuorum's discarded ones included, and the whole of req.MaxIndexBytes for a call that
// failed or was still running, since it does not say what it read.
func (d *Distributor) seriesCountsFanOut(ctx context.Context, replicationSets []ring.ReplicationSet, req *ingester_client.SeriesCountsRequest, charge *seriesCountsCharge) (_ [][]seriesCountsAnswer, _ int64, returnErr error) {
	ctx, span := tracer.Start(ctx, "Distributor.SeriesCounts fan-out")
	defer func() {
		if returnErr != nil {
			span.RecordError(returnErr)
		}
		span.End()
	}()
	span.SetAttributes(attribute.Int("replication_sets", len(replicationSets)), attribute.Bool("hashes", req.Hashes))
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	// A count walk is slow, so don't hedge it onto another zone.
	quorumConfig.HedgingDelay = 0
	// Another owner would refuse the same request.
	quorumConfig.IsTerminalError = isSeriesCountsRefusal
	perSet := make([][]seriesCountsAnswer, len(replicationSets))
	setErrs := make([]error, len(replicationSets))
	var called, succeeded, reported atomic.Int64
	err := concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, idx int) error {
		// An older owner fails the whole set, instead of whichever error DoUntilQuorum saw last.
		var (
			olderMtx sync.Mutex
			older    error
		)
		answers, err := ring.DoUntilQuorum[seriesCountsAnswer](ctx, replicationSets[idx], quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (seriesCountsAnswer, error) {
			called.Inc()
			a, err := d.seriesCountsCall(ctx, desc, req, charge)
			if err == nil {
				succeeded.Inc()
				reported.Add(a.resp.IndexBytes)
			}
			if err != nil && isOlderIngesterError(err) {
				olderMtx.Lock()
				if older == nil {
					older = err
				}
				olderMtx.Unlock()
			}
			return a, err
		}, func(a seriesCountsAnswer) { charge.sub(a.charged) })
		if err != nil && ctx.Err() == nil {
			olderMtx.Lock()
			if older != nil {
				err = older
			}
			olderMtx.Unlock()
		}
		perSet[idx] = answers
		setErrs[idx] = err
		return err
	})
	var answered, counted int64
	for _, answers := range perSet {
		for _, a := range answers {
			answered++
			counted += a.resp.SeriesCounted
		}
	}
	span.SetAttributes(attribute.Int64("ingesters_answered", answered), attribute.Int64("series_counted_summed_over_ingesters", counted))
	// The first set to fail cancels the others, so a later set's older owner may not be seen.
	for _, setErr := range setErrs {
		if setErr != nil && isOlderIngesterError(setErr) {
			return perSet, 0, setErr
		}
	}
	// succeeded is read before called, so a call that answers meanwhile is charged its whole share.
	unanswered := -succeeded.Load()
	unanswered += called.Load()
	return perSet, reported.Load() + unanswered*req.MaxIndexBytes, err
}

// seriesCountsCall sends req to one ingester and checks every group of its answer carries the counts asked for.
func (d *Distributor) seriesCountsCall(ctx context.Context, desc *ring.InstanceDesc, req *ingester_client.SeriesCountsRequest, charge *seriesCountsCharge) (seriesCountsAnswer, error) {
	poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
	if err != nil {
		return seriesCountsAnswer{}, err
	}
	stream, err := poolClient.(ingester_client.IngesterClient).SeriesCounts(ctx, req)
	if err != nil {
		logSeriesCountsFailure(ctx, d.log, desc.Addr, err)
		return seriesCountsAnswer{}, err
	}
	// The whole answer is held until the caller merged it, so each message is charged as it arrives.
	answer := seriesCountsAnswer{zone: desc.Zone}
	resp, err := cardinalitypb.ReceiveGroups(stream.Recv, func(msg *ingester_client.SeriesCountsResponse) error {
		var b uint64
		for _, g := range msg.Groups {
			b += uint64(seriesCountsGroupBytes + len(g.Value) + 8*len(g.Counts) + 8*len(g.Hashes))
		}
		if err := charge.add(b); err != nil {
			return fmt.Errorf("%w: %w", ErrSeriesCountsOverMemoryLimit, err)
		}
		answer.charged += b
		return nil
	})
	if err != nil {
		charge.sub(answer.charged)
		if !errors.Is(err, ErrSeriesCountsOverMemoryLimit) {
			logSeriesCountsFailure(ctx, d.log, desc.Addr, err)
		}
		return seriesCountsAnswer{}, err
	}
	// A check that fails below gives the charge back.
	checked := false
	defer func() {
		if !checked {
			charge.sub(answer.charged)
		}
	}()
	// A group of counts without one count is malformed.
	if !req.Hashes {
		for _, g := range resp.Groups {
			if len(g.Counts) != 1 {
				level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester returned a group without one count in a series counts answer", "ingester", desc.Addr, "group", g.Value, "counts", len(g.Counts))
				return seriesCountsAnswer{}, fmt.Errorf("an ingester returned %d counts for %q instead of one", len(g.Counts), g.Value)
			}
		}
	}
	checked = true
	answer.resp = resp
	return answer, nil
}

// isOlderIngesterError reports whether err shows an ingester that doesn't know the RPC.
func isOlderIngesterError(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := grpcutil.ErrorToStatus(e); ok {
			return st.Code() == codes.Unimplemented
		}
	}
	return false
}

// isSeriesCountsRefusal reports whether err is a limit refusal, an invalid request or a tenant with the estimate disabled.
func isSeriesCountsRefusal(err error) bool {
	if cardinality.RefusedBy(err) != "" || errors.Is(err, ErrSeriesCountsOverMemoryLimit) {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := grpcutil.ErrorToStatus(e); ok {
			return st.Code() == codes.InvalidArgument || st.Code() == codes.FailedPrecondition
		}
	}
	return false
}

// shardSizeUnchanged reports whether every ingester that counted something has
// seen the tenant's shard size unchanged since before changedBefore.
func shardSizeUnchanged(perSet [][]seriesCountsAnswer, changedBefore int64) bool {
	for _, answers := range perSet {
		for _, a := range answers {
			if a.resp.ShardSizeChanged {
				return false
			}
			if since := a.resp.ShardSizeUnchangedSinceMs; len(a.resp.Groups) > 0 && (since == 0 || since >= changedBefore) {
				return false
			}
		}
	}
	return true
}

// seriesCountsAddUp reports whether the counts of the ingesters in sets give
// each series once: every set has a single instance or, under ingest storage,
// one instance per zone, each a full copy of its partition, with the partitions
// adding up only when partitionsStable. With classic replication a write
// succeeds at quorum, so no zone is a full copy.
func (d *Distributor) seriesCountsAddUp(sets []ring.ReplicationSet, partitionsStable bool) bool {
	if len(sets) == 0 {
		return false
	}
	if len(sets) > 1 && (!d.cfg.IngestStorageConfig.Enabled || !partitionsStable) {
		return false
	}
	for _, rs := range sets {
		if len(rs.Instances) == 1 {
			continue
		}
		if !d.cfg.IngestStorageConfig.Enabled || !zoneAware(rs) || rs.ZoneCount() != len(rs.Instances) {
			return false
		}
	}
	return true
}

// partitionChangeSettleTime is how long a partition state change takes to reach every distributor.
const partitionChangeSettleTime = time.Minute

// seriesCountsReplicationSets returns the replication sets for matchers, and
// under ingest storage whether the partitions' counts add up over a window
// starting at startMs: every partition asked has been ACTIVE since before the
// window (allowing for the creation grace period and ring propagation) and the
// tenant writes to the partitions it is read from. The returned time is what
// the partitions' state timestamps were checked against, for the shard size check.
func (d *Distributor) seriesCountsReplicationSets(ctx context.Context, matchers []*labels.Matcher, startMs int64) ([]ring.ReplicationSet, bool, int64, error) {
	if !d.cfg.IngestStorageConfig.Enabled {
		sets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
		return sets, false, 0, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, false, 0, err
	}
	shardSize, lookbackPeriod := d.partitionShardForQuery(userID)
	now := time.Now()

	// A read shard larger than the write shard may be moving series between partitions.
	stable := shardSize <= 0 || max(0, d.limits.EffectiveIngestionPartitionsTenantWriteShardSize(userID)) == shardSize
	changedBefore := startMs - (d.limits.CreationGracePeriod(userID) + partitionChangeSettleTime).Milliseconds()

	compartmentIDs := []int{0}
	if d.cfg.Compartments.Enabled {
		compartmentIDs = d.compartmentRouter.CompartmentsForMatchers(userID, matchers)
	}
	var sets []ring.ReplicationSet
	for _, c := range compartmentIDs {
		r, err := d.partitionInstanceRings.Get(c).ShuffleShardWithLookback(userID, shardSize, lookbackPeriod, now)
		if err == nil {
			for _, p := range r.PartitionRing().Partitions() {
				// The state timestamp is in seconds.
				if !p.IsActive() || p.GetStateTimestamp()*1000 >= changedBefore {
					stable = false
				}
			}
			var cs []ring.ReplicationSet
			if cs, err = r.GetReplicationSetsForOperation(readNoExtend); err == nil {
				sets = append(sets, cs...)
			}
		}
		if err != nil {
			if d.cfg.Compartments.Enabled {
				return nil, false, 0, fmt.Errorf("read compartment %d: %w", c, err)
			}
			return nil, false, 0, err
		}
	}
	return sets, stable, changedBefore, nil
}

type seriesCountsAnswer struct {
	zone string
	resp *ingester_client.SeriesCountsResponse
	// charged is what resp is charged to the request's seriesCountsCharge.
	charged uint64
}

// seriesCountsCharge is what one SeriesCounts request's answers are charged to tracker, which may
// be nil. A call still running when the request returns finishes later, so after close, when the
// caller's own accounting goes on, it changes nothing.
type seriesCountsCharge struct {
	tracker *limiter.MemoryConsumptionTracker

	mtx    sync.Mutex
	held   uint64
	closed bool
}

func (c *seriesCountsCharge) add(b uint64) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.tracker == nil || c.closed {
		return nil
	}
	if err := c.tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
		return err
	}
	c.held += b
	return nil
}

// sub gives back b, which was added before.
func (c *seriesCountsCharge) sub(b uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.tracker == nil || c.closed || b == 0 {
		return
	}
	c.tracker.DecreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups)
	c.held -= b
}

// close gives back all that is held.
func (c *seriesCountsCharge) close() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if c.tracker != nil && c.held > 0 {
		c.tracker.DecreaseMemoryConsumption(c.held, limiter.CardinalityEstimateGroups)
	}
	c.held = 0
}

func answersByZone(answers []seriesCountsAnswer) map[string][]*ingester_client.SeriesCountsResponse {
	out := map[string][]*ingester_client.SeriesCountsResponse{}
	for _, a := range answers {
		out[a.zone] = append(out[a.zone], a.resp)
	}
	return out
}

// maxOverZones adds up each zone's responses and keeps the largest zone's count
// per group, since a zone can only miss series. min_time_ms is the
// latest start reported, ignoring ingesters that report 0 (nothing held).
func maxOverZones(byZone map[string][]*ingester_client.SeriesCountsResponse) *ingester_client.SeriesCountsResponse {
	merged := map[string][]int64{}
	out := &ingester_client.SeriesCountsResponse{}
	for _, resps := range byZone {
		zone := map[string][]int64{}
		var counted, minT int64
		for _, r := range resps {
			for _, g := range r.Groups {
				sum := zone[g.Value]
				if sum == nil {
					sum = make([]int64, len(g.Counts))
					zone[g.Value] = sum
				}
				for i := 0; i < len(sum) && i < len(g.Counts); i++ {
					sum[i] += g.Counts[i]
				}
			}
			counted += r.SeriesCounted
			minT = max(minT, r.MinTimeMs)
		}
		for v, sum := range zone {
			into := merged[v]
			if into == nil {
				merged[v] = sum
				continue
			}
			for i := range into {
				into[i] = max(into[i], sum[i])
			}
		}
		out.SeriesCounted = max(out.SeriesCounted, counted)
		out.MinTimeMs = max(out.MinTimeMs, minT)
	}
	values := make([]string, 0, len(merged))
	for v := range merged {
		values = append(values, v)
	}
	slices.Sort(values)
	for _, v := range values {
		out.Groups = append(out.Groups, &cardinalitypb.SeriesCountGroup{Value: v, Counts: merged[v]})
	}
	return out
}

func zoneAware(rs ring.ReplicationSet) bool {
	return rs.ZoneAwarenessEnabled || rs.MaxUnavailableZones > 0
}
