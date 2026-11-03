// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
)

const (
	defaultLabelChurnLookback = 30 * time.Minute

	// labelChurnMetricNamesPerLabel is the maximum number of metric names in the result for each label name.
	labelChurnMetricNamesPerLabel = 20
)

// LabelChurnResponse is the label churn of a tenant, summed from the partitions that responded.
// Without the ingest storage, all ingesters count as one partition.
type LabelChurnResponse struct {
	// QueriedPartitions is the number of partitions that the distributor queried.
	QueriedPartitions int `json:"queried_partitions"`
	// RespondedPartitions is the number of partitions that returned scores.
	// The scores do not include the series of the other partitions.
	RespondedPartitions int `json:"responded_partitions"`
	// RecentlyCompactedPartitions is the number of partitions that returned no scores,
	// because no record exists after their last compaction that removed series.
	RecentlyCompactedPartitions int `json:"recently_compacted_partitions"`
	// FailedPartitions is the number of partitions that returned an error.
	FailedPartitions int `json:"failed_partitions"`
	// SinceTimestampMs is the start of the time range that the scores are for.
	// It can be later than the requested start.
	SinceTimestampMs int64                             `json:"since_timestamp_ms,omitempty"`
	Scores           []ingester_client.LabelChurnScore `json:"scores"`
}

type zonedLabelChurnResponse struct {
	zone     string
	response *ingester_client.LabelChurnResponse
}

// errLabelChurnCompactedTooRecently makes the distributor query the other ingesters of the partition,
// because the ingesters of different zones can compact at different times.
var errLabelChurnCompactedTooRecently = errors.New("the ingester compacted too recently")

// LabelChurnHandler returns the label churn scores of the tenant in the "tenant" parameter.
// The optional "lookback" parameter sets the start of the time range. The default is 30m.
func (d *Distributor) LabelChurnHandler(w http.ResponseWriter, r *http.Request) {
	tenantID := r.FormValue("tenant")
	if err := tenant.ValidTenantID(tenantID); err != nil {
		http.Error(w, fmt.Sprintf("invalid tenant parameter: %s", err), http.StatusBadRequest)
		return
	}

	lookback := defaultLabelChurnLookback
	if lookbackParameter := r.FormValue("lookback"); lookbackParameter != "" {
		parsedLookback, err := model.ParseDuration(lookbackParameter)
		if err != nil || parsedLookback <= 0 {
			http.Error(w, "the lookback parameter must be a positive duration", http.StatusBadRequest)
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
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.LabelChurnRequest{SinceTimestampMs: since.UnixMilli()}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	responsesByReplicationSet := make([][]zonedLabelChurnResponse, len(replicationSets))
	compactedByReplicationSet := make([]bool, len(replicationSets))
	errsByReplicationSet := make([]error, len(replicationSets))

	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, replicationSetIdx int) error {
		replicationSet := replicationSets[replicationSetIdx]
		responses, err := ring.DoUntilQuorum(ctx, replicationSet, quorumConfig, func(ctx context.Context, instance *ring.InstanceDesc) (zonedLabelChurnResponse, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*instance)
			if err != nil {
				return zonedLabelChurnResponse{}, err
			}

			response, err := poolClient.(ingester_client.IngesterClient).LabelChurn(ctx, req)
			if err != nil {
				return zonedLabelChurnResponse{}, err
			}
			if response.CompactedTooRecently {
				return zonedLabelChurnResponse{}, errLabelChurnCompactedTooRecently
			}
			return zonedLabelChurnResponse{zone: instance.Zone, response: response}, nil
		}, func(zonedLabelChurnResponse) {})

		// Each job writes to a different index, thus no lock is necessary.
		switch {
		case errors.Is(err, errLabelChurnCompactedTooRecently):
			compactedByReplicationSet[replicationSetIdx] = true
		case err != nil:
			level.Warn(d.log).Log("msg", "a partition did not return label churn scores", "ingesters", strings.Join(replicationSet.GetAddresses(), ","), "err", err)
			errsByReplicationSet[replicationSetIdx] = err
		default:
			responsesByReplicationSet[replicationSetIdx] = responses
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	merged := mergeLabelChurnResponses(responsesByReplicationSet)
	for _, compacted := range compactedByReplicationSet {
		if compacted {
			merged.RecentlyCompactedPartitions++
		}
	}
	merged.FailedPartitions = merged.QueriedPartitions - merged.RespondedPartitions - merged.RecentlyCompactedPartitions
	if merged.FailedPartitions > 0 && merged.FailedPartitions == merged.QueriedPartitions {
		return nil, fmt.Errorf("all partitions failed to return label churn scores: %w", errsByReplicationSet[0])
	}
	return merged, nil
}

// mergeLabelChurnResponses sums the scores of all replication sets, and sorts them in descending order.
// A replication set without responses counts as a partition that did not respond.
// The returned since is the latest since of the responses.
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
		zone := responses[0].zone
		for _, zonedResponse := range responses {
			if zonedResponse.zone != zone {
				continue
			}

			merged.SinceTimestampMs = max(merged.SinceTimestampMs, zonedResponse.response.SinceTimestampMs)
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
			MetricNames: ingester_client.TopLabelChurnMetricNames(sampledSeriesPerLabelNameAndMetricName[labelName], labelChurnMetricNamesPerLabel),
		})
	}
	slices.SortFunc(merged.Scores, func(a, b ingester_client.LabelChurnScore) int {
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.LabelName, b.LabelName))
	})
	return merged
}
