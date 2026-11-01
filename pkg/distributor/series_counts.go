// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
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
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SeriesCountsResult is what the ingesters answered to a SeriesCounts request.
// When Hashed is false, adding up the responses' counts gives each series once;
// when true, the responses carry hashes and must be unioned.
type SeriesCountsResult struct {
	Responses []*ingester_client.SeriesCountsResponse
	// Hashed is set when the responses carry hashes, asked for or because counts can't be added up.
	Hashed bool
}

// ErrSeriesCountsNeedOneIngesterPerZone is returned for per-bucket counts when counts can't be added up.
var ErrSeriesCountsNeedOneIngesterPerZone = errors.New("per-bucket series counts need one ingester per zone: a series can be on several ingesters of a zone or in several partitions, so their counts can't be added up")

// logSeriesCountsFailure logs which ingester a call failed on, unless the request was cancelled or reached quorum.
func logSeriesCountsFailure(ctx context.Context, logger log.Logger, addr string, err error) {
	if ctx.Err() != nil {
		return
	}
	level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "series counts failed on an ingester", "ingester", addr, "err", err)
}

// ErrSeriesCountsOlderIngester is wrapped by the error for an answer that ignored window_ms or step_ms.
var ErrSeriesCountsOlderIngester = errors.New("it may run an older version, retry once every ingester is updated")

// SeriesCounts asks the ingesters to count series per value of one label over
// a window, from their heads' chunk metas, using a query's replication sets.
// Counts are kept only when one ingester per zone is asked for each set (and,
// under ingest storage, the partitions have been stable since before the
// window and no ingester saw the tenant's shard size change). Otherwise it asks
// for hashes, which the caller unions, with a second request when only the
// answers show the shard size change. Per-bucket counts can't be hashed, so
// they are refused with ErrSeriesCountsNeedOneIngesterPerZone.
func (d *Distributor) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *ingester_client.SeriesCountsRequest) (SeriesCountsResult, error) {
	replicationSets, partitionsStable, changedBefore, err := d.seriesCountsReplicationSets(ctx, matchers, req.StartTimestampMs)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	hashed := req.Hashes
	if !d.seriesCountsAddUp(replicationSets, partitionsStable) && !req.Hashes {
		if req.StepMs > 0 {
			return SeriesCountsResult{}, ErrSeriesCountsNeedOneIngesterPerZone
		}
		hashed = true
	}
	// The distributor can turn a request for counts into one for hashes.
	if hashed && req.SketchAbove <= 0 {
		return SeriesCountsResult{}, grpcstatus.Error(codes.InvalidArgument, cardinality.ErrHashesNeedSketchAbove.Error())
	}
	// Only the ingesters know whether the shard size changed over the window.
	checkShardSize := !hashed && d.cfg.IngestStorageConfig.Enabled && len(replicationSets) > 1

	// Split max_index_bytes and max_hashes across the ingesters that will be asked.
	shared := *req
	shared.Hashes = hashed
	if checkShardSize {
		shared.ShardSizeUnchangedBeforeMs = changedBefore
	}
	calls := 0
	for _, rs := range replicationSets {
		if zoneAware(rs) && rs.ZoneCount() > 0 {
			calls += max(1, len(rs.Instances)/rs.ZoneCount())
		} else {
			calls += len(rs.Instances)
		}
	}
	if calls > 1 {
		if req.MaxIndexBytes > 0 {
			shared.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(calls))
		}
		if req.MaxHashes > 0 {
			shared.MaxHashes = max(1, req.MaxHashes/int64(calls))
		}
	}

	perSet, err := d.seriesCountsFanOut(ctx, replicationSets, &shared)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	if checkShardSize && !shardSizeUnchanged(perSet, changedBefore) {
		if req.StepMs > 0 {
			return SeriesCountsResult{}, ErrSeriesCountsNeedOneIngesterPerZone
		}
		if req.SketchAbove <= 0 {
			return SeriesCountsResult{}, grpcstatus.Error(codes.InvalidArgument, cardinality.ErrHashesNeedSketchAbove.Error())
		}
		hashed = true
		shared.Hashes = true
		shared.ShardSizeUnchangedBeforeMs = 0
		if perSet, err = d.seriesCountsFanOut(ctx, replicationSets, &shared); err != nil {
			return SeriesCountsResult{}, err
		}
	}

	out := SeriesCountsResult{Hashed: hashed}
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

func (d *Distributor) seriesCountsFanOut(ctx context.Context, replicationSets []ring.ReplicationSet, req *ingester_client.SeriesCountsRequest) (_ [][]seriesCountsAnswer, returnErr error) {
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
	perSet := make([][]seriesCountsAnswer, len(replicationSets))
	setErrs := make([]error, len(replicationSets))
	err := concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, idx int) error {
		// An older owner fails the whole set, instead of whichever error DoUntilQuorum saw last.
		var (
			olderMtx sync.Mutex
			older    error
		)
		answers, err := ring.DoUntilQuorum[seriesCountsAnswer](ctx, replicationSets[idx], quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (seriesCountsAnswer, error) {
			a, err := d.seriesCountsCall(ctx, desc, req)
			if err != nil && isOlderIngesterError(err) {
				olderMtx.Lock()
				if older == nil {
					older = err
				}
				olderMtx.Unlock()
			}
			return a, err
		}, func(seriesCountsAnswer) {})
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
			return perSet, setErr
		}
	}
	return perSet, err
}

// seriesCountsCall sends req to one ingester and checks its answer followed window_ms and step_ms.
func (d *Distributor) seriesCountsCall(ctx context.Context, desc *ring.InstanceDesc, req *ingester_client.SeriesCountsRequest) (seriesCountsAnswer, error) {
	poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
	if err != nil {
		return seriesCountsAnswer{}, err
	}
	stream, err := poolClient.(ingester_client.IngesterClient).SeriesCounts(ctx, req)
	if err != nil {
		logSeriesCountsFailure(ctx, d.log, desc.Addr, err)
		return seriesCountsAnswer{}, err
	}
	resp, err := ingester_client.ReceiveSeriesCountsResponse(stream)
	if err != nil {
		logSeriesCountsFailure(ctx, d.log, desc.Addr, err)
		return seriesCountsAnswer{}, err
	}
	// An older ingester counts adjacent step-wide buckets and echoes no window.
	if resp.WindowMs != req.WindowMs {
		level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester ignored window_ms in a series counts request", "ingester", desc.Addr, "window_ms", req.WindowMs, "got", resp.WindowMs)
		return seriesCountsAnswer{}, fmt.Errorf("an ingester counted with window_ms %d instead of the %d asked for; %w", resp.WindowMs, req.WindowMs, ErrSeriesCountsOlderIngester)
	}
	// An older ingester that ignores step_ms answers one count per group.
	if want := seriesCountsBuckets(req); want > 0 {
		for _, g := range resp.Groups {
			if len(g.Counts) != want {
				level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester ignored step_ms in a series counts request", "ingester", desc.Addr, "step_ms", req.StepMs, "buckets", want, "got", len(g.Counts))
				return seriesCountsAnswer{}, fmt.Errorf("an ingester returned %d buckets for %q instead of the %d asked for with step_ms; %w", len(g.Counts), g.Value, want, ErrSeriesCountsOlderIngester)
			}
		}
	} else if !req.Hashes {
		// A group with no counts is malformed.
		for _, g := range resp.Groups {
			if len(g.Counts) == 0 {
				level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester returned a group with no counts in a series counts answer", "ingester", desc.Addr, "group", g.Value)
				return seriesCountsAnswer{}, fmt.Errorf("an ingester returned no counts for %q", g.Value)
			}
		}
	}
	return seriesCountsAnswer{zone: desc.Zone, resp: resp}, nil
}

// isOlderIngesterError reports whether err shows an ingester that doesn't know the RPC or ignored window_ms or step_ms.
func isOlderIngesterError(err error) bool {
	if errors.Is(err, ErrSeriesCountsOlderIngester) {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := grpcutil.ErrorToStatus(e); ok {
			return st.Code() == codes.Unimplemented
		}
	}
	return false
}

// seriesCountsBuckets is how many counts each group of an answer to req carries, 0 for hashes or no step.
func seriesCountsBuckets(req *ingester_client.SeriesCountsRequest) int {
	if req.StepMs <= 0 || req.Hashes {
		return 0
	}
	n, err := cardinality.Buckets(req.StartTimestampMs, req.EndTimestampMs, req.StepMs, req.WindowMs)
	if err != nil {
		return 0
	}
	return n
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
// each series once: every set has a single instance, or is zone-aware with one
// instance per zone and every zone a full copy. Under ingest storage the sets
// are partitions, which add up only when partitionsStable.
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
		if !zoneAware(rs) || rs.ZoneCount() != len(rs.Instances) {
			return false
		}
		if !d.cfg.IngestStorageConfig.Enabled && rs.ZoneCount() > d.ingestersRing.ReplicationFactor() {
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
}

func answersByZone(answers []seriesCountsAnswer) map[string][]*ingester_client.SeriesCountsResponse {
	out := map[string][]*ingester_client.SeriesCountsResponse{}
	for _, a := range answers {
		out[a.zone] = append(out[a.zone], a.resp)
	}
	return out
}

// maxOverZones adds up each zone's responses and keeps the largest zone's count
// per group and bucket, since a zone can only miss series. min_time_ms is the
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
			out.WindowMs = r.WindowMs
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
