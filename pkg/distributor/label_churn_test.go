// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"testing"
	"time"

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
				QueriedPartitions:   2,
				RespondedPartitions: 2,
				SinceTimestampMs:    since.UnixMilli(),
				Scores:              []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of each partition fails": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:   2,
				RespondedPartitions: 2,
				SinceTimestampMs:    since.UnixMilli(),
				Scores:              []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"all ingesters of one partition fail": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:   2,
				RespondedPartitions: 1,
				FailedPartitions:    1,
				SinceTimestampMs:    since.UnixMilli(),
				Scores:              []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 1, MetricNames: []ingester_client.LabelChurnMetricName{}}},
			},
		},
		"one ingester of a partition compacted too recently": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateCompactedTooRecently}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy}},
			},
			expectedResponse: &LabelChurnResponse{
				QueriedPartitions:   2,
				RespondedPartitions: 2,
				SinceTimestampMs:    since.UnixMilli(),
				Scores:              []ingester_client.LabelChurnScore{{LabelName: "pod", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{}}},
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
				SinceTimestampMs:            since.UnixMilli(),
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
				RecentlyCompactedPartitions: 1,
				FailedPartitions:            1,
				Scores:                      []ingester_client.LabelChurnScore{},
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

			ds, _, _, _ := prepare(t, prepConfig{
				ingesterStateByZone:  testData.ingesterStateByZone,
				numDistributors:      1,
				ingestStorageEnabled: true,
			})

			response, err := ds[0].LabelChurn(user.InjectOrgID(context.Background(), "test"), since)
			if testData.expectedErr != nil {
				require.ErrorIs(t, err, testData.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testData.expectedResponse, response)
		})
	}
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

	merged := mergeLabelChurnResponses([][]zonedLabelChurnResponse{
		// Both zones of partition 0 respond with the same series, thus only one of the responses counts.
		{{zone: "zone-a", response: partitionZeroResponse}, {zone: "zone-b", response: partitionZeroResponse}},
		{{zone: "zone-b", response: partitionOneResponse}},
		{},
	})

	require.Equal(t, &LabelChurnResponse{
		// The third partition did not respond.
		QueriedPartitions:   3,
		RespondedPartitions: 2,
		SinceTimestampMs:    2000,
		Scores: []ingester_client.LabelChurnScore{
			{LabelName: "pod", Score: 14, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "other", SampledSeries: 10}, {MetricName: "up", SampledSeries: 8}}},
			{LabelName: "job", Score: 2, MetricNames: []ingester_client.LabelChurnMetricName{{MetricName: "up", SampledSeries: 5}}},
		},
	}, merged)
}
