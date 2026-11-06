// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic" //lint:ignore faillint go.uber.org/atomic has no Or method.
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

const (
	defaultLabelChurnLookback = 30 * time.Minute
	defaultLabelChurnLimit    = 30

	// labelChurnIngesterLimitFactor is how many times more scores the distributor requests from each ingester than it returns.
	// A label name with a high score for the tenant usually has a high score in each partition too.
	// Thus the additional scores make it more probable that each partition returns the label names of the result.
	// Then labelChurnMerger removes fewer label names from the result.
	labelChurnIngesterLimitFactor = 3

	// labelChurnConcurrency is the maximum number of partitions that the distributor queries at the same time.
	// It limits the number of responses that the distributor keeps in memory before it merges them.
	labelChurnConcurrency = 64
)

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
	OldestSinceTimestampMs int64 `json:"oldest_since_timestamp_ms,omitempty"`
	NewestSinceTimestampMs int64 `json:"newest_since_timestamp_ms,omitempty"`
	// Scores are the highest scores in descending order.
	// Scores can have fewer scores than the limit. See labelChurnMerger.
	Scores []ingester_client.LabelChurnScore `json:"scores"`
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
// The optional "limit" parameter sets the maximum number of scores. The default is 30.
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

	limit := defaultLabelChurnLimit
	if limitParameter := r.FormValue("limit"); limitParameter != "" {
		parsedLimit, err := strconv.ParseInt(limitParameter, 10, 32)
		if err != nil || parsedLimit < 1 {
			http.Error(w, "the limit parameter must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = int(parsedLimit)
	}

	response, err := d.LabelChurn(user.InjectOrgID(r.Context(), tenantID), time.Now().Add(-lookback), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	util.WriteJSONResponse(w, response)
}

// LabelChurn returns the limit highest label churn scores of the tenant, summed from the partitions that responded.
// The score of a label name is the number of series that have a value of the label which did not exist before since.
// LabelChurn can return fewer than limit scores. See labelChurnMerger. A limit of 0 or less returns all scores.
// LabelChurn is for incidents in which some ingesters are overloaded.
// Thus it returns an error only when all partitions fail.
func (d *Distributor) LabelChurn(ctx context.Context, since time.Time, limit int) (*LabelChurnResponse, error) {
	if !d.cfg.IngestStorageConfig.Enabled {
		return nil, errLabelChurnRequiresIngestStorage
	}

	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, nil)
	if err != nil {
		return nil, err
	}

	req := &ingester_client.LabelChurnRequest{
		SinceTimestampMs: since.UnixMilli(),
		Limit:            int64(max(limit, 0)) * labelChurnIngesterLimitFactor,
	}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	merger := newLabelChurnMerger(len(replicationSets))

	err = concurrency.ForEachJob(ctx, len(replicationSets), labelChurnConcurrency, func(ctx context.Context, replicationSetIdx int) error {
		replicationSet := replicationSets[replicationSetIdx]
		// noScoresStatuses has one bit for each status that the ingesters of the partition returned without scores.
		var noScoresStatuses atomic.Int32
		responses, err := ring.DoUntilQuorum(ctx, replicationSet, quorumConfig, func(ctx context.Context, instance *ring.InstanceDesc) (zonedLabelChurnResponse, error) {
			response, err := d.labelChurnFromIngester(ctx, instance, req)
			if err != nil {
				if !grpcutil.IsCanceled(err) {
					level.Warn(d.log).Log("msg", "an ingester did not return label churn scores", "ingester", instance.Addr, "zone", instance.Zone, "err", err)
				}
				return zonedLabelChurnResponse{}, err
			}
			if response.Status != ingester_client.LABEL_CHURN_OK {
				noScoresStatuses.Or(1 << response.Status)
				return zonedLabelChurnResponse{}, errLabelChurnNoScores
			}
			return zonedLabelChurnResponse{zone: instance.Zone, response: response}, nil
		}, func(zonedLabelChurnResponse) {})

		if err != nil {
			// DoUntilQuorum returns an error only after all ingesters of the partition responded.
			// Thus no ingester changes noScoresStatuses after DoUntilQuorum returns.
			merger.addPartitionWithoutScores(noScoresStatuses.Load(), err)
			return nil
		}
		merger.addPartition(responses)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return merger.result(limit)
}

func (d *Distributor) labelChurnFromIngester(ctx context.Context, instance *ring.InstanceDesc, req *ingester_client.LabelChurnRequest) (*ingester_client.LabelChurnResponse, error) {
	poolClient, err := d.ingesterPool.GetClientForInstance(*instance)
	if err != nil {
		return nil, err
	}
	return poolClient.(ingester_client.IngesterClient).LabelChurn(ctx, req)
}

// mergedLabelChurnScore is the sum of the scores that the partitions returned for one label name.
type mergedLabelChurnScore struct {
	labelName string
	// score is the lowest possible score of the label name. See labelChurnMerger.
	score uint64
	// sumOfMaxDroppedScoresOfReturningPartitions is the sum of the max dropped scores of the partitions that returned the label name.
	sumOfMaxDroppedScoresOfReturningPartitions uint64
	sampledSeriesPerMetricName                 map[string]uint64
}

// highestPossibleScore returns the highest real score that the label name can have. See labelChurnMerger.
// sumOfMaxDroppedScores is the sum of the max dropped scores of all partitions that responded.
func (s *mergedLabelChurnScore) highestPossibleScore(sumOfMaxDroppedScores uint64) uint64 {
	return s.score + sumOfMaxDroppedScores - s.sumOfMaxDroppedScoresOfReturningPartitions
}

// labelChurnMerger sums the scores of the partitions when they respond.
// Thus the distributor does not keep the responses of all partitions in memory at the same time.
// The merger keeps the sums of all returned label names, because a partition that responds later can change the order of the label names.
// result returns at most limit scores in descending order.
// labelChurnMerger is safe for concurrent use.
//
// Each ingester returns only its highest scores and its max dropped score.
// The max dropped score is the score of the label with the highest score that the ingester did not return due to the <limits> parameter.
// Thus the sum of the returned scores can be lower than the real score of a label name.
// result calculates two scores for each label name:
//   - The lowest possible score is the sum of the scores that the partitions returned.
//   - The highest possible score is the lowest possible score plus the max dropped score of each partition that did not return the label name.
//
// A label name that no partition returned has a highest possible score that is the sum of all max dropped scores.
// result sorts the label names by their lowest possible score, and keeps the first <limit> label names.
// Then it removes label names from the end of the list.
// It stops when no label name outside the list can have a higher score than the last label name in the list.
// Thus the real score of each label name in the list is not lower than the real score of each label name outside the list.
// The scores in the list can be lower than the real scores, thus the order in the list can be different from the real order, but it should be a good approximation.
//
// Example: The limit is 2, and each ingester returns only its 2 highest scores.
// In reality, the distributor requests 3 times the limit (labelChurnIngesterLimitFactor) from each ingester.
// The distributor does this to prevent the problem that this example shows.
// These are the real scores:
//
//	label name   partition 0   partition 1   partition 2   real score
//	pod                  100           100           100          300
//	instance              50            50            50          150
//	job                   60             5             5           70
//	node                   0            55             0           55
//	zone                   0             0            55           55
//
// The ingesters return these scores:
//   - Partition 0 returns pod=100 and job=60. Its max dropped score is 50 (instance).
//   - Partition 1 returns pod=100 and node=55. Its max dropped score is 50 (instance).
//   - Partition 2 returns pod=100 and zone=55. Its max dropped score is 50 (instance).
//
// result calculates these scores:
//   - pod: All partitions returned pod. Thus the lowest and the highest possible score are both 300.
//   - job: The lowest possible score is 60. The highest possible score is 60+50+50=160.
//   - node and zone: The lowest possible score is 55. The highest possible score is 55+50+50=155.
//   - instance: No partition returned instance. Thus its highest possible score is 50+50+50=150.
//
// Sorted by the lowest possible score, the first 2 label names are pod=300 and job=60.
// Outside the list, node and zone can have a score of 155. This is higher than 60, thus result removes job.
// Then job is also outside the list, and job can have a score of 160. This is lower than 300, thus pod stays.
// The result is pod=300, which has fewer label names than the limit.
// But the result does not show job as the second label name. That would be incorrect, because instance has a higher real score.
//
// With 3 times the limit, each ingester returns up to 6 scores. In the example, each partition then returns all its scores.
// Thus the max dropped score of each partition is 0, and the result is pod=300 and instance=150.
type labelChurnMerger struct {
	mtx sync.Mutex
	// merged has the partition counts and the time range. result sets the scores.
	merged                  LabelChurnResponse
	mergedScorePerLabelName map[string]*mergedLabelChurnScore
	sumOfMaxDroppedScores   uint64
	lastErr                 error
}

func newLabelChurnMerger(queriedPartitions int) *labelChurnMerger {
	return &labelChurnMerger{
		merged:                  LabelChurnResponse{QueriedPartitions: queriedPartitions},
		mergedScorePerLabelName: map[string]*mergedLabelChurnScore{},
	}
}

// addPartition adds the responses of the ingesters of one partition.
// It uses the zone with the earliest since, because that zone covers the longest time range.
func (m *labelChurnMerger) addPartition(responses []zonedLabelChurnResponse) {
	if len(responses) == 0 {
		return
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.merged.RespondedPartitions++

	// All zones of a replication set have the same series, thus the responses of one zone are sufficient.
	zone := zoneWithEarliestLabelChurnSince(responses)
	for _, zonedResponse := range responses {
		if zonedResponse.zone != zone {
			continue
		}

		if since := zonedResponse.response.SinceTimestampMs; since > 0 {
			if m.merged.OldestSinceTimestampMs == 0 || since < m.merged.OldestSinceTimestampMs {
				m.merged.OldestSinceTimestampMs = since
			}
			m.merged.NewestSinceTimestampMs = max(m.merged.NewestSinceTimestampMs, since)
		}
		maxDroppedScore := zonedResponse.response.MaxDroppedScore
		m.sumOfMaxDroppedScores += maxDroppedScore
		for _, score := range zonedResponse.response.Scores {
			mergedScore := m.mergedScorePerLabelName[score.LabelName]
			if mergedScore == nil {
				mergedScore = &mergedLabelChurnScore{labelName: score.LabelName, sampledSeriesPerMetricName: map[string]uint64{}}
				m.mergedScorePerLabelName[score.LabelName] = mergedScore
			}
			mergedScore.score += score.Score
			mergedScore.sumOfMaxDroppedScoresOfReturningPartitions += maxDroppedScore
			for _, metricName := range score.MetricNames {
				mergedScore.sampledSeriesPerMetricName[metricName.MetricName] += metricName.SampledSeries
			}
		}
	}
}

// addPartitionWithoutScores counts a partition in which no ingester returned scores.
// noScoresStatuses has one bit for each status that the ingesters of the partition returned without scores.
// err is the error that the distributor got from the partition.
func (m *labelChurnMerger) addPartitionWithoutScores(noScoresStatuses int32, err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// Report COMPACTED_TOO_RECENTLY before NO_RECORD, because it tells the user that a shorter lookback can return scores.
	switch {
	case noScoresStatuses&(1<<ingester_client.LABEL_CHURN_COMPACTED_TOO_RECENTLY) != 0:
		m.merged.RecentlyCompactedPartitions++
	case noScoresStatuses&(1<<ingester_client.LABEL_CHURN_NO_RECORD) != 0:
		m.merged.NoRecordPartitions++
	default:
		m.merged.FailedPartitions++
		m.lastErr = err
	}
}

// result returns the merged response with at most limit scores. A limit of 0 or less returns all scores.
// result returns an error when all partitions failed.
func (m *labelChurnMerger) result(limit int) (*LabelChurnResponse, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.merged.FailedPartitions == m.merged.QueriedPartitions && m.lastErr != nil {
		return nil, fmt.Errorf("all partitions failed to return label churn scores: %w", m.lastErr)
	}

	// A label name that no partition returned can have a score of sumOfMaxDroppedScores.
	highestPossibleScoreOutsideList := m.sumOfMaxDroppedScores
	topScores := util.NewTopK(limit, func(a, b *mergedLabelChurnScore) int {
		return cmp.Or(cmp.Compare(b.score, a.score), cmp.Compare(a.labelName, b.labelName))
	})
	for _, mergedScore := range m.mergedScorePerLabelName {
		if dropped, isDropped := topScores.Push(mergedScore); isDropped {
			highestPossibleScoreOutsideList = max(highestPossibleScoreOutsideList, dropped.highestPossibleScore(m.sumOfMaxDroppedScores))
		}
	}
	list := topScores.Sorted()
	for len(list) > 0 && list[len(list)-1].score < highestPossibleScoreOutsideList {
		removed := list[len(list)-1]
		highestPossibleScoreOutsideList = max(highestPossibleScoreOutsideList, removed.highestPossibleScore(m.sumOfMaxDroppedScores))
		list = list[:len(list)-1]
	}

	merged := m.merged
	merged.Scores = make([]ingester_client.LabelChurnScore, 0, len(list))
	for _, mergedScore := range list {
		merged.Scores = append(merged.Scores, ingester_client.LabelChurnScore{
			LabelName:   mergedScore.labelName,
			Score:       mergedScore.score,
			MetricNames: ingester_client.TopLabelChurnMetricNames(mergedScore.sampledSeriesPerMetricName, ingester_client.LabelChurnMetricNamesPerLabel),
		})
	}
	return &merged, nil
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
