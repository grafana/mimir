// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
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
	// request asked for them or because the replication places a series on
	// several ingesters of one zone, where counts can't be added up.
	Hashed bool
}

// errSeriesCountsNeedZones is returned for per-bucket counts when the
// replication can't give each series once by adding up counts.
type errSeriesCountsNeedZones struct{ replicationFactor int }

func (e errSeriesCountsNeedZones) Error() string {
	return "per-bucket series counts need ingest storage or zone-aware replication: with a replication factor above 1 in one zone, counts from several ingesters can't be added up"
}

// SeriesCounts asks the ingesters to count series per value of one label
// over a window, from their heads' chunk metas. It takes the same
// replication sets as a query. In zone-aware mode, which includes ingest
// storage where each replication set is one partition, it keeps the
// responses of one zone per replication set: a zone that answered in full
// holds every series of that set once, so the counts add up exactly across
// sets. Without zones and with a replication factor above 1, a series is on
// several ingesters, so it asks for hashes instead and the caller unions
// them.
func (d *Distributor) SeriesCounts(ctx context.Context, matchers []*labels.Matcher, req *ingester_client.SeriesCountsRequest) (SeriesCountsResult, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
	if err != nil {
		return SeriesCountsResult{}, err
	}
	if !d.cfg.IngestStorageConfig.Enabled && len(replicationSets) == 1 && replicationSets[0].ZoneCount() == 1 {
		replicationSets[0].MaxErrors = 0
	}

	out := SeriesCountsResult{Hashed: req.Hashes}
	needHashes := false
	for _, rs := range replicationSets {
		if !zoneAware(rs) && d.ingestersRing.ReplicationFactor() > 1 && len(rs.Instances) > 1 {
			needHashes = true
		}
	}
	if needHashes && !req.Hashes {
		if req.StepMs > 0 {
			return SeriesCountsResult{}, errSeriesCountsNeedZones{d.ingestersRing.ReplicationFactor()}
		}
		withHashes := *req
		withHashes.Hashes = true
		req = &withHashes
		out.Hashed = true
	}

	type answer struct {
		zone string
		resp *ingester_client.SeriesCountsResponse
	}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	perSet := make([][]answer, len(replicationSets))
	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, idx int) error {
		answers, err := ring.DoUntilQuorum[answer](ctx, replicationSets[idx], quorumConfig, func(ctx context.Context, desc *ring.InstanceDesc) (answer, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*desc)
			if err != nil {
				return answer{}, err
			}
			resp, err := poolClient.(ingester_client.IngesterClient).SeriesCounts(ctx, req)
			if err != nil {
				return answer{}, err
			}
			return answer{zone: desc.Zone, resp: resp}, nil
		}, func(answer) {})
		perSet[idx] = answers
		return err
	})
	if err != nil {
		return SeriesCountsResult{}, err
	}

	for i, answers := range perSet {
		if out.Hashed || !zoneAware(replicationSets[i]) {
			// Hashes union to the same count however many replicas sent
			// them, and without zones and a replication factor of 1 each
			// series is on one ingester.
			for _, a := range answers {
				out.Responses = append(out.Responses, a.resp)
			}
			continue
		}
		// Every zone DoUntilQuorum returns answered for all its instances;
		// take the first by name so repeated requests pick the same one.
		zones := make([]string, 0, len(answers))
		for _, a := range answers {
			zones = append(zones, a.zone)
		}
		slices.Sort(zones)
		for _, a := range answers {
			if len(zones) > 0 && a.zone == zones[0] {
				out.Responses = append(out.Responses, a.resp)
			}
		}
	}
	return out, nil
}

func zoneAware(rs ring.ReplicationSet) bool {
	return rs.ZoneAwarenessEnabled || rs.MaxUnavailableZones > 0
}
