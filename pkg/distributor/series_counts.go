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
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// SeriesCountsResult is what the ingesters answered to a SeriesCounts
// request, already chosen so the caller can combine it without knowing the
// replication: when Hashed is false, adding up the responses' counts gives
// each series once; when it is true, the responses carry hashes and must be
// unioned.
type SeriesCountsResult struct {
	Responses []*ingester_client.SeriesCountsResponse
	// Hashed is set when the responses carry hashes, either because the
	// request asked for them or because more than one ingester of a zone is
	// asked, where counts can't be added up.
	Hashed bool
}

// ErrSeriesCountsNeedOneIngesterPerZone is returned for per-bucket counts
// when the replication can't give each series once by adding up counts.
var ErrSeriesCountsNeedOneIngesterPerZone = errors.New("per-bucket series counts need one ingester per zone: a series can be on several ingesters of a zone or in several partitions, so their counts can't be added up")

// logSeriesCountsFailure logs which ingester a SeriesCounts call failed on.
// The querier returns the error to the caller without the address, which a
// gRPC transport error carries in its text. A call that ended because the
// request was cancelled or quorum was reached is not logged.
func logSeriesCountsFailure(ctx context.Context, logger log.Logger, addr string, err error) {
	if ctx.Err() != nil {
		return
	}
	level.Warn(spanlogger.FromContext(ctx, logger)).Log("msg", "series counts failed on an ingester", "ingester", addr, "err", err)
}

// ErrSeriesCountsOlderIngester is wrapped by the error for an answer that
// shows its ingester runs an older version, one that ignored window_ms or
// step_ms. The querier refuses the request with 422, as for an ingester that
// doesn't know the RPC.
var ErrSeriesCountsOlderIngester = errors.New("it may run an older version, retry once every ingester is updated")

// SeriesCounts asks the ingesters to count series per value of one label
// over a window, from their heads' chunk metas. It takes the same
// replication sets as a query. Counts are kept only when one ingester per
// zone is asked for each replication set: then it takes the largest zone's
// count per group and bucket, since a zone that answered in full holds
// every series once, and under ingest storage the caller adds up the
// partitions, as long as they have been stable since before the window
// (seriesCountsReplicationSets) and every ingester that counted something
// has seen the tenant's shard size unchanged since then. Otherwise it asks
// for hashes and the caller unions them; when only the ingesters' answers
// show the change, that is a second request. Several ingesters of a zone,
// or partitions whose state or shard changed inside the window, are not
// known to hold disjoint series: a series written before the change stays
// in the old owner's head while its new samples go to the new one.
// Per-bucket counts, which hashes don't carry, are refused then with
// ErrSeriesCountsNeedOneIngesterPerZone.
func (d *Distributor) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *ingester_client.SeriesCountsRequest) (SeriesCountsResult, error) {
	replicationSets, partitionsStable, changedBefore, err := d.seriesCountsReplicationSets(ctx, matchers, req.StartTimestampMs)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	hashed := req.Hashes
	if !d.seriesCountsAddUp(replicationSets, partitionsStable) && !req.Hashes {
		if req.StepMs > 0 {
			return SeriesCountsResult{}, ErrSeriesCountsNeedOneIngesterPerZone
		}
		hashed = true
	}
	// Counts added up across partitions also need every partition's shard
	// size unchanged over the window, which only the ingesters know.
	checkShardSize := !hashed && d.cfg.IngestStorageConfig.Enabled && len(replicationSets) > 1

	// Split max_index_bytes across the ingesters that will be asked: one
	// zone's instances per replication set when zone-aware, every instance
	// otherwise. With hedging or a failed zone more may be asked, each with
	// the same share.
	shared := *req
	shared.Hashes = hashed
	if checkShardSize {
		shared.ShardSizeUnchangedBeforeMs = changedBefore
	}
	if req.MaxIndexBytes > 0 {
		calls := 0
		for _, rs := range replicationSets {
			if zoneAware(rs) && rs.ZoneCount() > 0 {
				calls += max(1, len(rs.Instances)/rs.ZoneCount())
			} else {
				calls += len(rs.Instances)
			}
		}
		if calls > 1 {
			shared.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(calls))
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
			// Hashes union to the same count however many replicas sent
			// them, and without zones counts are kept only for a single
			// ingester.
			for _, a := range answers {
				out.Responses = append(out.Responses, a.resp)
			}
			continue
		}
		out.Responses = append(out.Responses, maxOverZones(answersByZone(answers)))
	}
	return out, nil
}

func (d *Distributor) seriesCountsFanOut(ctx context.Context, replicationSets []ring.ReplicationSet, req *ingester_client.SeriesCountsRequest) ([][]seriesCountsAnswer, error) {
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	perSet := make([][]seriesCountsAnswer, len(replicationSets))
	setErrs := make([]error, len(replicationSets))
	err := concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, idx int) error {
		// DoUntilQuorum returns the failure that made quorum impossible, so
		// with one owner too old and another failing for another reason the
		// status would depend on which answered last. An older owner makes
		// the whole set fail as one, the way an older store-gateway does. A
		// set that ran out of time, or was cancelled, keeps that error, and
		// an older owner that answers after quorum failed may not be seen.
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
	// The first set to fail cancels the others, so under ingest storage,
	// with a set per partition, an older owner in a later set is seen only
	// if its set failed before it was cancelled.
	for _, setErr := range setErrs {
		if setErr != nil && isOlderIngesterError(setErr) {
			return perSet, setErr
		}
	}
	return perSet, err
}

// seriesCountsCall sends req to one ingester and checks that its answer
// followed window_ms and step_ms.
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
	// An ingester that doesn't know window_ms, such as an older
	// version during a rolling update, counts adjacent step-wide
	// buckets instead and echoes no window.
	if resp.WindowMs != req.WindowMs {
		level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester ignored window_ms in a series counts request", "ingester", desc.Addr, "window_ms", req.WindowMs, "got", resp.WindowMs)
		return seriesCountsAnswer{}, fmt.Errorf("an ingester counted with window_ms %d instead of the %d asked for; %w", resp.WindowMs, req.WindowMs, ErrSeriesCountsOlderIngester)
	}
	// The same for step_ms: a version that ignores it answers one
	// count per group, which maxOverZones can't merge with the
	// buckets of the other zones.
	if want := seriesCountsBuckets(req); want > 0 {
		for _, g := range resp.Groups {
			if len(g.Counts) != want {
				level.Warn(spanlogger.FromContext(ctx, d.log)).Log("msg", "ingester ignored step_ms in a series counts request", "ingester", desc.Addr, "step_ms", req.StepMs, "buckets", want, "got", len(g.Counts))
				return seriesCountsAnswer{}, fmt.Errorf("an ingester returned %d buckets for %q instead of the %d asked for with step_ms; %w", len(g.Counts), g.Value, want, ErrSeriesCountsOlderIngester)
			}
		}
	}
	return seriesCountsAnswer{zone: desc.Zone, resp: resp}, nil
}

// isOlderIngesterError reports whether err shows that an ingester runs a
// version older than the request: one that doesn't know the RPC, or one
// whose answer ignored window_ms or step_ms. It reads the first gRPC status
// in the chain, as the querier's classifyIngesterError does.
func isOlderIngesterError(err error) bool {
	if errors.Is(err, ErrSeriesCountsOlderIngester) {
		return true
	}
	for e := err; e != nil; e = errors.Unwrap(e) {
		if st, ok := grpcstatus.FromError(e); ok {
			return st.Code() == codes.Unimplemented
		}
	}
	return false
}

// seriesCountsBuckets is how many counts each group of an answer to req
// carries: the buckets of its step and window, or 0 when it asks for hashes or
// has no step.
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

// shardSizeUnchanged reports whether every ingester that counted something
// has seen the tenant's shard size unchanged since before changedBefore.
// An ingester that refused to count says so with shard_size_changed. One
// with no TSDB for the tenant answers no groups and can't count a series
// twice; one too old to know the field answers 0.
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

// seriesCountsAddUp reports whether the counts of the ingesters in sets
// give each series once. Every set must have a single instance, or be
// zone-aware with one instance per zone and every zone a full copy: under
// ingest storage each zone of a partition is; with classic replication only
// when there are no more zones than the replication factor. Classic
// replication must also have one set. Under ingest storage there is one set
// per partition, and their counts add up only when partitionsStable says no
// series is in two of them over the window.
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

// partitionChangeSettleTime is how long after a partition's state change
// distributors may still write to the partitions they wrote to before,
// until the change has reached them through the ring.
const partitionChangeSettleTime = time.Minute

// seriesCountsReplicationSets returns the replication sets a query over
// matchers asks, as getIngesterReplicationSetsForQuery does, and under
// ingest storage whether the partitions' counts add up over a window that
// starts at startMs. They do when the tenant's partitions took every sample
// of the window that the ingesters still hold, each series in the one
// partition its labels map to, so no series is in two partitions' heads or
// local blocks with a sample in the window: every partition asked has been
// ACTIVE since before the window, allowing for samples written ahead of
// time (the creation grace period) and for the change to reach every
// distributor, and the tenant writes to the partitions it is read from. The
// partitions are checked on the same subrings the sets come from. The start
// is not clamped to the ingesters' lookback: they drop a local block by its
// end, so they can hold samples from before it. A change of the tenant's
// partition shard size is not in the ring: the returned time is what the
// state timestamps are checked against, and SeriesCounts checks the
// ingesters' shard size against it too.
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

	// A tenant read from a shard of partitions but written to fewer of them
	// may be moving series between them, which the ring doesn't record. A
	// read of every partition (shard size 0) covers any write shard.
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

// maxOverZones adds up each zone's responses and keeps, for every group and
// bucket, the largest zone's count. Each zone that answered in full holds
// every series of the replication set, but a write acknowledged by a quorum
// may not have reached every zone yet, and a zone can only miss series, so
// the largest count is the closest. The series counts and min_time_ms are
// taken the same way.
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
			if r.MinTimeMs > 0 && (minT == 0 || r.MinTimeMs < minT) {
				minT = r.MinTimeMs
			}
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
		out.Groups = append(out.Groups, &ingester_client.SeriesCountGroup{Value: v, Counts: merged[v]})
	}
	return out
}

func zoneAware(rs ring.ReplicationSet) bool {
	return rs.ZoneAwarenessEnabled || rs.MaxUnavailableZones > 0
}
