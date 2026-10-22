// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"slices"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
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

// SeriesCounts asks the ingesters to count series per value of one label
// over a window, from their heads' chunk metas. It takes the same
// replication sets as a query. Counts are kept only when one ingester per
// zone is asked: then it takes the largest zone's count per group and
// bucket, since a zone that answered in full holds every series once.
// Otherwise it asks for hashes and the caller unions them. Several
// ingesters of a zone, or several partitions under ingest storage, are not
// known to hold disjoint series: after the tenant's partitions or the ring
// change inside the lookback, a series written before stays in the old
// owner's head while its new samples go to the new one. Per-bucket counts,
// which hashes don't carry, are refused then with ErrSeriesCountsNeedOneIngesterPerZone.
func (d *Distributor) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *ingester_client.SeriesCountsRequest) (SeriesCountsResult, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	out := SeriesCountsResult{Hashed: req.Hashes}
	if !d.seriesCountsAddUp(replicationSets) && !req.Hashes {
		if req.StepMs > 0 {
			return SeriesCountsResult{}, ErrSeriesCountsNeedOneIngesterPerZone
		}
		withHashes := *req
		withHashes.Hashes = true
		req = &withHashes
		out.Hashed = true
	}

	// Split max_index_bytes across the ingesters that will be asked: one
	// zone's instances per replication set when zone-aware, every instance
	// otherwise. With hedging or a failed zone more may be asked, each with
	// the same share.
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
			split := *req
			split.MaxIndexBytes = max(1, req.MaxIndexBytes/int64(calls))
			req = &split
		}
	}

	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	perSet := make([][]seriesCountsAnswer, len(replicationSets))
	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, idx int) error {
		answers, err := ring.DoUntilQuorum[seriesCountsAnswer](ctx, replicationSets[idx], quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (seriesCountsAnswer, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
			if err != nil {
				return seriesCountsAnswer{}, err
			}
			resp, err := poolClient.(ingester_client.IngesterClient).SeriesCounts(ctx, req)
			if err != nil {
				return seriesCountsAnswer{}, err
			}
			return seriesCountsAnswer{zone: desc.Zone, resp: resp}, nil
		}, func(seriesCountsAnswer) {})
		perSet[idx] = answers
		return err
	})
	if err != nil {
		return SeriesCountsResult{}, err
	}

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

// seriesCountsAddUp reports whether the counts of the ingesters in sets
// give each series once: there is one replication set, and either it has a
// single instance or it is zone-aware with one instance per zone and every
// zone holds a full copy. Under ingest storage each zone of a partition
// does; with classic replication only when there are no more zones than
// the replication factor.
func (d *Distributor) seriesCountsAddUp(sets []ring.ReplicationSet) bool {
	if len(sets) != 1 {
		return false
	}
	rs := sets[0]
	if len(rs.Instances) == 1 {
		return true
	}
	if !zoneAware(rs) || rs.ZoneCount() != len(rs.Instances) {
		return false
	}
	return d.cfg.IngestStorageConfig.Enabled || rs.ZoneCount() <= d.ingestersRing.ReplicationFactor()
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
		var counted, walked, minT int64
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
			walked += r.SeriesWalked
			if r.MinTimeMs > 0 && (minT == 0 || r.MinTimeMs < minT) {
				minT = r.MinTimeMs
			}
			out.HeadMinTimeMs = max(out.HeadMinTimeMs, r.HeadMinTimeMs)
			out.HeadMaxTimeMs = max(out.HeadMaxTimeMs, r.HeadMaxTimeMs)
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
		out.SeriesWalked = max(out.SeriesWalked, walked)
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
