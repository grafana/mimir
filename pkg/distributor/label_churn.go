// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"
	"time"

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

type zonedLabelChurnResponse struct {
	zone     string
	response *ingester_client.LabelChurnResponse
}

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

// LabelChurn returns the label churn scores of the tenant, summed from all partitions.
// The score of a label name is the number of series that have a value of the label which did not exist before since.
func (d *Distributor) LabelChurn(ctx context.Context, since time.Time) (*ingester_client.LabelChurnResponse, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.LabelChurnRequest{SinceTimestampMs: since.UnixMilli()}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	responsesByReplicationSet := make([][]zonedLabelChurnResponse, len(replicationSets))

	err = concurrency.ForEachJob(ctx, len(replicationSets), 0, func(ctx context.Context, replicationSetIdx int) error {
		responses, err := ring.DoUntilQuorum(ctx, replicationSets[replicationSetIdx], quorumConfig, func(ctx context.Context, instance *ring.InstanceDesc) (zonedLabelChurnResponse, error) {
			poolClient, err := d.ingesterPool.GetClientForInstance(*instance)
			if err != nil {
				return zonedLabelChurnResponse{}, err
			}

			response, err := poolClient.(ingester_client.IngesterClient).LabelChurn(ctx, req)
			if err != nil {
				return zonedLabelChurnResponse{}, err
			}
			return zonedLabelChurnResponse{zone: instance.Zone, response: response}, nil
		}, func(zonedLabelChurnResponse) {})

		// Each job writes to a different index, thus no lock is necessary.
		responsesByReplicationSet[replicationSetIdx] = responses
		return err
	})
	if err != nil {
		return nil, err
	}

	return mergeLabelChurnResponses(responsesByReplicationSet), nil
}

// mergeLabelChurnResponses sums the scores of all replication sets, and sorts them in descending order.
// The returned since is the latest since of the responses.
func mergeLabelChurnResponses(responsesByReplicationSet [][]zonedLabelChurnResponse) *ingester_client.LabelChurnResponse {
	scorePerLabelName := map[string]uint64{}
	sampledSeriesPerLabelNameAndMetricName := map[string]map[string]uint64{}
	merged := &ingester_client.LabelChurnResponse{}

	for _, responses := range responsesByReplicationSet {
		if len(responses) == 0 {
			continue
		}

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
