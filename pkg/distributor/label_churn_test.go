// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

func TestDistributor_LabelChurn(t *testing.T) {
	since := time.UnixMilli(1000)

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		expectedResponse    *LabelChurnResponse
		expectedErr         error
	}{
		"all partitions respond": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:      2,
				RespondedPartitions:    2,
				OldestSinceTimestampMs: since.UnixMilli(),
				NewestSinceTimestampMs: since.UnixMilli(),
				Scores:                 []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of each partition fails": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:      2,
				RespondedPartitions:    2,
				OldestSinceTimestampMs: since.UnixMilli(),
				NewestSinceTimestampMs: since.UnixMilli(),
				Scores:                 []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"all ingesters of one partition fail": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:      2,
				RespondedPartitions:    1,
				FailedPartitions:       1,
				OldestSinceTimestampMs: since.UnixMilli(),
				NewestSinceTimestampMs: since.UnixMilli(),
				Scores:                 []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of a partition compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:      2,
				RespondedPartitions:    2,
				OldestSinceTimestampMs: since.UnixMilli(),
				NewestSinceTimestampMs: since.UnixMilli(),
				Scores:                 []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"all ingesters of one partition compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:           2,
				RespondedPartitions:         1,
				RecentlyCompactedPartitions: 1,
				OldestSinceTimestampMs:      since.UnixMilli(),
				NewestSinceTimestampMs:      since.UnixMilli(),
				Scores:                      []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one partition fails and the other partition compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateCompactedTooRecently}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateCompactedTooRecently}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:           2,
				RespondedPartitions:         0,
				RecentlyCompactedPartitions: 1,
				FailedPartitions:            1,
				Scores:                      []ingester_client.LabelChurnScore{},
			},
		},
		"all ingesters of one partition have no record": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateNoLabelChurnRecord}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateNoLabelChurnRecord}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:      2,
				RespondedPartitions:    1,
				NoRecordPartitions:     1,
				OldestSinceTimestampMs: since.UnixMilli(),
				NewestSinceTimestampMs: since.UnixMilli(),
				Scores:                 []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of a partition has no record and the other compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateNoLabelChurnRecord}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:           2,
				RespondedPartitions:         1,
				RecentlyCompactedPartitions: 1,
				OldestSinceTimestampMs:      since.UnixMilli(),
				NewestSinceTimestampMs:      since.UnixMilli(),
				Scores:                      []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of a partition compacted too recently and the other has no record": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateNoLabelChurnRecord}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:           2,
				RespondedPartitions:         1,
				RecentlyCompactedPartitions: 1,
				OldestSinceTimestampMs:      since.UnixMilli(),
				NewestSinceTimestampMs:      since.UnixMilli(),
				Scores:                      []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of a partition fails and the other compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:           2,
				RespondedPartitions:         1,
				RecentlyCompactedPartitions: 1,
				OldestSinceTimestampMs:      since.UnixMilli(),
				NewestSinceTimestampMs:      since.UnixMilli(),
				Scores:                      []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"all partitions fail": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateFailed}},
			},
			expectedErr: errFail,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ds, ingesters, _, _ := prepare(t, prepConfig{
				ingesterStateByZone:  testData.ingesterStateByZone,
				numDistributors:      1,
				ingestStorageEnabled: true,
				// The distributor queries zone-a first, and zone-b only for the partitions without scores in zone-a.
				// Thus the status of a partition does not depend on which zone responds first.
				configure: func(cfg *Config) {
					cfg.MinimizeIngesterRequests = true
					cfg.MinimiseIngesterRequestsHedgingDelay = time.Hour
					cfg.PreferAvailabilityZones = []string{"zone-a"}
				},
			})

			response, err := ds[0].LabelChurn(user.InjectOrgID(t.Context(), "test"), since, defaultLabelChurnLimit)

			labelChurnCalls := 0
			for _, ingester := range ingesters {
				labelChurnCalls += ingester.countCalls("LabelChurn")
				ingester.assertCalledFunc("LabelChurn", func(args ...any) {
					require.Equal(t, int64(labelChurnIngesterLimitFactor*defaultLabelChurnLimit), args[0].(*ingester_client.LabelChurnRequest).Limit)
				})
			}
			require.Positive(t, labelChurnCalls)

			if testData.expectedErr != nil {
				require.ErrorIs(t, err, testData.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testData.expectedResponse, response)
		})
	}
}

func TestDistributor_LabelChurnHandler_InvalidLookback(t *testing.T) {
	for _, lookback := range []string{"30s", "0", "-5m", "abc"} {
		t.Run(lookback, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/querier/label_churn/test?lookback="+lookback, nil)
			request = mux.SetURLVars(request, map[string]string{"tenant": "test"})

			distributor := &Distributor{}
			distributor.cfg.IngestStorageConfig.Enabled = true
			distributor.LabelChurnHandler(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), "the lookback parameter must be a duration of at least 1m0s")
		})
	}
}

func TestDistributor_LabelChurnHandler_InvalidLimit(t *testing.T) {
	for _, limit := range []string{"0", "-1", "1.5", "abc", "99999999999"} {
		t.Run(limit, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/querier/label_churn/test?limit="+limit, nil)
			request = mux.SetURLVars(request, map[string]string{"tenant": "test"})

			distributor := &Distributor{}
			distributor.cfg.IngestStorageConfig.Enabled = true
			distributor.LabelChurnHandler(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), "the limit parameter must be a positive integer")
		})
	}
}

func TestDistributor_LabelChurnHandler_RequiresIngestStorage(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/querier/label_churn/test", nil)
	request = mux.SetURLVars(request, map[string]string{"tenant": "test"})

	(&Distributor{}).LabelChurnHandler(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), errLabelChurnRequiresIngestStorage.Error())

	_, err := (&Distributor{}).LabelChurn(t.Context(), time.Now(), defaultLabelChurnLimit)
	require.ErrorIs(t, err, errLabelChurnRequiresIngestStorage)
}

func TestMergeLabelChurnResponses(t *testing.T) {
	partitionZeroResponse := &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 1000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 10, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
			{LabelName: "job", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
		},
	}
	partitionOneResponse := &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 2000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 4, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "other", SampledSeries: 10}, {MetricName: "up", SampledSeries: 3}}},
		},
	}

	// Zone-b of partition 0 compacted later, thus its time range is shorter and its score is lower.
	partitionZeroShorterResponse := &ingester_client.LabelChurnResponse{
		SinceTimestampMs: 1500,
		Scores:           []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1}},
	}

	merged := mergeLabelChurnResponses([][]zonedLabelChurnResponse{
		// Both zones of partition 0 respond. Only the zone with the earliest since counts.
		{{zone: "zone-b", response: partitionZeroShorterResponse}, {zone: "zone-a", response: partitionZeroResponse}},
		{{zone: "zone-b", response: partitionOneResponse}},
		{},
	}, defaultLabelChurnLimit)

	require.Equal(t, &LabelChurnResponse{
		// The third partition did not respond.
		QueriedPartitions:      3,
		RespondedPartitions:    2,
		OldestSinceTimestampMs: 1000,
		NewestSinceTimestampMs: 2000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 14, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "other", SampledSeries: 10}, {MetricName: "up", SampledSeries: 8}}},
			{LabelName: "job", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
		},
	}, merged)
}

func TestMergeLabelChurnResponses_Limit(t *testing.T) {
	partition := func(maxDroppedScore uint64, scores ...ingester_client.LabelChurnScore) []zonedLabelChurnResponse {
		return []zonedLabelChurnResponse{{zone: "zone-a", response: &ingester_client.LabelChurnResponse{
			SinceTimestampMs: 1000,
			MaxDroppedScore:  maxDroppedScore,
			Scores:           scores,
		}}}
	}
	score := func(labelName string, score uint64) ingester_client.LabelChurnScore {
		return ingester_client.LabelChurnScore{LabelName: labelName, Score: score}
	}
	mergedScore := func(labelName string, score uint64) ingester_client.LabelChurnScore {
		return ingester_client.LabelChurnScore{LabelName: labelName, Score: score, MetricNames: []ingester_client.LabelChurnMetricName{}}
	}

	// These partitions have the real scores of the example in the comment of mergeLabelChurnResponses.
	partitionsWithAllScores := [][]zonedLabelChurnResponse{
		partition(0, score("pod", 100), score("job", 60), score("instance", 50)),
		partition(0, score("pod", 100), score("node", 55), score("instance", 50), score("job", 5)),
		partition(0, score("pod", 100), score("zone", 55), score("instance", 50), score("job", 5)),
	}

	tests := map[string]struct {
		responsesByReplicationSet [][]zonedLabelChurnResponse
		limit                     int
		expectedScores            []ingester_client.LabelChurnScore
	}{
		"each ingester returns its 2 highest scores": {
			responsesByReplicationSet: [][]zonedLabelChurnResponse{
				partition(50, score("pod", 100), score("job", 60)),
				partition(50, score("pod", 100), score("node", 55)),
				partition(50, score("pod", 100), score("zone", 55)),
			},
			limit:          2,
			expectedScores: []ingester_client.LabelChurnScore{mergedScore("pod", 300)},
		},
		"each ingester returns all its scores": {
			responsesByReplicationSet: partitionsWithAllScores,
			limit:                     2,
			expectedScores:            []ingester_client.LabelChurnScore{mergedScore("pod", 300), mergedScore("instance", 150)},
		},
		"no limit": {
			responsesByReplicationSet: partitionsWithAllScores,
			limit:                     0,
			expectedScores: []ingester_client.LabelChurnScore{
				mergedScore("pod", 300), mergedScore("instance", 150), mergedScore("job", 70), mergedScore("node", 55), mergedScore("zone", 55),
			},
		},
		"a label name that a partition did not return stays when no label name outside the list can have a higher score": {
			// b can have a score of 90+40=130, and c can have a score of 50+10=60.
			// A label name that no partition returned can have a score of 10+40=50.
			responsesByReplicationSet: [][]zonedLabelChurnResponse{
				partition(10, score("a", 100), score("b", 90)),
				partition(40, score("a", 100), score("c", 50)),
			},
			limit:          2,
			expectedScores: []ingester_client.LabelChurnScore{mergedScore("a", 200), mergedScore("b", 90)},
		},
		"a label name that no partition returned can have a higher score than all returned label names": {
			responsesByReplicationSet: [][]zonedLabelChurnResponse{
				partition(100, score("a", 50)),
				partition(100, score("b", 50)),
			},
			limit:          1,
			expectedScores: []ingester_client.LabelChurnScore{},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			merged := mergeLabelChurnResponses(testData.responsesByReplicationSet, testData.limit)
			require.Equal(t, testData.expectedScores, merged.Scores)
		})
	}
}
