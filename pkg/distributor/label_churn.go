// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
)

const defaultLabelChurnLookback = 30 * time.Minute

// LabelChurnResponse is the label churn of a tenant, summed from the partitions that responded.
type LabelChurnResponse struct {
	// QueriedPartitions is the number of partitions that the distributor queried.
	QueriedPartitions int `json:"queried_partitions"`
	// RespondedPartitions is the number of partitions that returned scores.
	// The scores do not include the series of the other partitions.
	RespondedPartitions int `json:"responded_partitions"`
	// RecentlyCompactedPartitions is the number of partitions without scores,
	// because no record exists after the last compaction that removed series.
	RecentlyCompactedPartitions int `json:"recently_compacted_partitions"`
	// NoRecordPartitions is the number of partitions without scores,
	// because no record exists at or after the requested start. For example, the ingesters started recently.
	NoRecordPartitions int `json:"no_record_partitions"`
	// FailedPartitions is the number of partitions in which all ingesters returned an error.
	FailedPartitions int `json:"failed_partitions"`
	// OldestSinceTimestampMs and NewestSinceTimestampMs are the earliest and the latest start
	// of the time ranges that the partitions returned scores for.
	// Each partition can start its time range later than the requested start.
	OldestSinceTimestampMs int64                             `json:"oldest_since_timestamp_ms,omitempty"`
	NewestSinceTimestampMs int64                             `json:"newest_since_timestamp_ms,omitempty"`
	Scores                 []ingester_client.LabelChurnScore `json:"scores"`
}

type zonedLabelChurnResponse struct {
	zone     string
	response *ingester_client.LabelChurnResponse
}

// errLabelChurnRequiresIngestStorage is the error when the ingest storage is disabled.
// Without the ingest storage, each series is in more than one ingester of a zone, thus the sum of the scores is not correct.
var errLabelChurnRequiresIngestStorage = errors.New("label churn detection requires the ingest storage")

// errLabelChurnNoScores makes the distributor query the other ingesters of the partition,
// because the ingesters of different zones can compact and start at different times.
var errLabelChurnNoScores = errors.New("the ingester returned no label churn scores")

// LabelChurnHandler returns the label churn scores of the tenant in the request path.
// The optional "lookback" parameter sets the start of the time range. The default is 30m.
func (d *Distributor) LabelChurnHandler(w http.ResponseWriter, r *http.Request) {
	if !d.cfg.IngestStorageConfig.Enabled {
		http.Error(w, errLabelChurnRequiresIngestStorage.Error(), http.StatusBadRequest)
		return
	}

	tenantID := mux.Vars(r)["tenant"]
	if err := tenant.ValidTenantID(tenantID); err != nil {
		http.Error(w, fmt.Sprintf("invalid tenant: %s", err), http.StatusBadRequest)
		return
	}

	lookback := defaultLabelChurnLookback
	if lookbackParameter := r.FormValue("lookback"); lookbackParameter != "" {
		parsedLookback, err := model.ParseDuration(lookbackParameter)
		if err != nil || time.Duration(parsedLookback) < ingester_client.LabelChurnRecordInterval {
			http.Error(w, fmt.Sprintf("the lookback parameter must be a duration of at least %s", ingester_client.LabelChurnRecordInterval), http.StatusBadRequest)
			return
		}
		lookback = time.Duration(parsedLookback)
	}

	response, err := d.LabelChurn(user.InjectOrgID(r.Context(), tenantID), time.Now().Add(-lookback))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, response)
}

// LabelChurn returns the label churn scores of the tenant, summed from the partitions that responded.
// The score of a label name is the number of series that have a value of the label which did not exist before since.
// LabelChurn is for incidents in which some ingesters are overloaded.
// Thus it returns an error only when all partitions fail.
func (d *Distributor) LabelChurn(ctx context.Context, since time.Time) (*LabelChurnResponse, error) {
	if !d.cfg.IngestStorageConfig.Enabled {
		return nil, errLabelChurnRequiresIngestStorage
	}

	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.LabelChurnRequest{SinceTimestampMs: since.UnixMilli()}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	responsesByReplicationSet := make([][]zonedLabelChurnResponse, len(replicationSets))
	// noScoresStatusesByReplicationSet has one bit for each status that the ingesters of the partition returned without scores.
	noScoresStatusesByReplicationSet := make([]atomic.Int32, len(replicationSets))
	errsByReplicationSet := make([]error, len(replicationSets))

	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, replicationSetIdx int) error {
		replicationSet := replicationSets[replicationSetIdx]
		responses, err := ring.DoUntilQuorum(ctx, replicationSet, quorumConfig, func(ctx context.Context, instance *ring.InstanceDesc) (zonedLabelChurnResponse, error) {
			response, err := d.labelChurnFromIngester(ctx, instance, req)
			if err != nil {
				if !grpcutil.IsCanceled(err) {
					level.Warn(d.log).Log("msg", "an ingester did not return label churn scores", "ingester", instance.Addr, "zone", instance.Zone, "err", err)
				}
				return zonedLabelChurnResponse{}, err
			}
			if response.Status != ingester_client.LABEL_CHURN_OK {
				noScoresStatusesByReplicationSet[replicationSetIdx].Or(1 << response.Status)
				return zonedLabelChurnResponse{}, errLabelChurnNoScores
			}
			return zonedLabelChurnResponse{zone: instance.Zone, response: response}, nil
		}, func(zonedLabelChurnResponse) {})

		// Each job writes to a different index, thus no lock is necessary.
		if err == nil {
			responsesByReplicationSet[replicationSetIdx] = responses
		} else {
			errsByReplicationSet[replicationSetIdx] = err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// DoUntilQuorum returns an error only after all ingesters of the partition responded.
	// Thus no ingester changes the status of a partition with an error after DoUntilQuorum returns.
	merged := mergeLabelChurnResponses(responsesByReplicationSet)
	var lastErr error
	for replicationSetIdx, err := range errsByReplicationSet {
		if err == nil {
			continue
		}
		// Report COMPACTED_TOO_RECENTLY before NO_RECORD, because it tells the user that a shorter lookback can return scores.
		noScoresStatuses := noScoresStatusesByReplicationSet[replicationSetIdx].Load()
		switch {
		case noScoresStatuses&(1<<ingester_client.LABEL_CHURN_COMPACTED_TOO_RECENTLY) != 0:
			merged.RecentlyCompactedPartitions++
		case noScoresStatuses&(1<<ingester_client.LABEL_CHURN_NO_RECORD) != 0:
			merged.NoRecordPartitions++
		default:
			merged.FailedPartitions++
			lastErr = err
		}
	}
	if merged.FailedPartitions == merged.QueriedPartitions && lastErr != nil {
		return nil, fmt.Errorf("all partitions failed to return label churn scores: %w", lastErr)
	}
	return merged, nil
}

func (d *Distributor) labelChurnFromIngester(ctx context.Context, instance *ring.InstanceDesc, req *ingester_client.LabelChurnRequest) (*ingester_client.LabelChurnResponse, error) {
	poolClient, err := d.ingesterPool.GetClientForInstance(*instance)
	if err != nil {
		return nil, err
	}
	return poolClient.(ingester_client.IngesterClient).LabelChurn(ctx, req)
}

// mergeLabelChurnResponses sums the scores of all replication sets, and sorts them in descending order.
// A replication set without responses counts as a partition that did not respond.
// For each replication set, it uses the zone with the earliest since, because that zone covers the longest time range.
func mergeLabelChurnResponses(responsesByReplicationSet [][]zonedLabelChurnResponse) *LabelChurnResponse {
	scorePerLabelName := map[string]uint64{}
	sampledSeriesPerLabelNameAndMetricName := map[string]map[string]uint64{}
	merged := &LabelChurnResponse{QueriedPartitions: len(responsesByReplicationSet)}

	for _, responses := range responsesByReplicationSet {
		if len(responses) == 0 {
			continue
		}
		merged.RespondedPartitions++

		// All zones of a replication set have the same series, thus the responses of one zone are sufficient.
		zone := zoneWithEarliestLabelChurnSince(responses)
		for _, zonedResponse := range responses {
			if zonedResponse.zone != zone {
				continue
			}

			if since := zonedResponse.response.SinceTimestampMs; since > 0 {
				if merged.OldestSinceTimestampMs == 0 || since < merged.OldestSinceTimestampMs {
					merged.OldestSinceTimestampMs = since
				}
				merged.NewestSinceTimestampMs = max(merged.NewestSinceTimestampMs, since)
			}
			for _, score := range zonedResponse.response.Scores {
				scorePerLabelName[score.LabelName] += score.Score

				sampledSeriesPerMetricName := sampledSeriesPerLabelNameAndMetricName[score.LabelName]
				if sampledSeriesPerMetricName == nil {
					sampledSeriesPerMetricName = map[string]uint64{}
					sampledSeriesPerLabelNameAndMetricName[score.LabelName] = sampledSeriesPerMetricName
				}
				for _, metricName := range score.MetricNames {
					sampledSeriesPerMetricName[metricName.MetricName] += metricName.SampledSeries
				}
			}
		}
	}

	merged.Scores = make([]ingester_client.LabelChurnScore, 0, len(scorePerLabelName))
	for labelName, score := range scorePerLabelName {
		merged.Scores = append(merged.Scores, ingester_client.LabelChurnScore{
			LabelName:   labelName,
			Score:       score,
			MetricNames: ingester_client.TopLabelChurnMetricNames(sampledSeriesPerLabelNameAndMetricName[labelName], ingester_client.LabelChurnMetricNamesPerLabel),
		})
	}
	slices.SortFunc(merged.Scores, func(a, b ingester_client.LabelChurnScore) int {
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.LabelName, b.LabelName))
	})
	return merged
}

// zoneWithEarliestLabelChurnSince returns the zone of the response with the earliest since.
// A response without since has no records, thus it is used only when no other response exists.
func zoneWithEarliestLabelChurnSince(responses []zonedLabelChurnResponse) string {
	earliest := responses[0]
	for _, candidate := range responses[1:] {
		candidateSince, earliestSince := candidate.response.SinceTimestampMs, earliest.response.SinceTimestampMs
		if candidateSince > 0 && (earliestSince == 0 || candidateSince < earliestSince) {
			earliest = candidate
		}
	}
	return earliest.zone
}
