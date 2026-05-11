// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

func TestDistributor_SearchLabelNames_FanOutAndMerge(t *testing.T) {
	replicaResponses := map[int][]string{
		0: {"foo", "bar"},
		1: {"foo", "footer"},
		2: {"bar", "baz", "foobar"},
	}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      3,
		happyIngesters:    3,
		replicationFactor: 1,
		searchLabelNamesHook: func(ingesterIdx int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return makeSearchBatches(replicaResponses[ingesterIdx])
		},
	})
	params := &streaminglabelvalues.Params{
		Terms:         []string{"fo"},
		CaseSensitive: true,
		FuzzAlg:       streaminglabelvalues.FuzzAlgJaroWinkler,
		FuzzThreshold: 0,
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
	require.NoError(t, err)
	hints := &storage.SearchHints{Filter: filter, OrderBy: storage.OrderByValueAsc, Limit: 100}

	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		model.Earliest,
		model.Latest,
		params,
		hints,
		[]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "metric")},
	)
	defer rs.Close()
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	// "fo" rejects "bar"/"baz"; emits "foo"/"footer"/"foobar" at prefix score 1.0.
	assert.Equal(t, []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}, got)
}

func makeSearchBatches(values []string) []*client.SearchResultBatch {
	results := make([]client.SearchResultBatch_Result, len(values))
	for i, v := range values {
		results[i] = client.SearchResultBatch_Result{Value: v}
	}
	return []*client.SearchResultBatch{{Results: results}}
}

func TestDistributor_SearchLabelNames_AllReplicasFail(t *testing.T) {
	ds, _, _, _ := prepare(t, prepConfig{numDistributors: 1, numIngesters: 3, happyIngesters: 0})
	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		model.Earliest,
		model.Latest,
		nil,
		&storage.SearchHints{Limit: 10},
		nil,
	)
	defer rs.Close()
	assert.False(t, rs.Next())
	assert.Error(t, rs.Err())
}

func TestDistributor_SearchLabelNames_WarningsPropagated(t *testing.T) {
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      1,
		happyIngesters:    1,
		replicationFactor: 1,
		searchLabelNamesHook: func(_ int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return []*client.SearchResultBatch{
				{Results: []client.SearchResultBatch_Result{{Value: "x"}}, Warnings: []string{"replica-warn"}},
			}
		},
	})
	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		model.Earliest,
		model.Latest,
		nil,
		&storage.SearchHints{Limit: 10},
		nil,
	)
	defer rs.Close()
	for rs.Next() {
		_ = rs.At()
	}
	require.NoError(t, rs.Err())
	msgs := make([]string, 0, 1)
	for _, w := range rs.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.Equal(t, []string{"replica-warn"}, msgs)
}

func TestDistributor_SearchLabelNames_QuorumShortCircuit(t *testing.T) {
	// RF=3 (the default), all 3 ingesters return overlapping data for the SAME shard.
	// DoUntilQuorum reads the first 2 (quorum=2) and short-circuits the 3rd.
	// Result must contain every value the first two returned, correctly merged.
	shared := []string{"alpha", "beta", "gamma"}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors: 1,
		numIngesters:    3,
		happyIngesters:  3,
		searchLabelNamesHook: func(_ int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return makeSearchBatches(shared)
		},
	})
	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		0, model.Time(time.Now().UnixMilli()),
		nil,
		&storage.SearchHints{Limit: 100, OrderBy: storage.OrderByValueAsc},
		nil,
	)
	defer rs.Close()
	var got []string
	for rs.Next() {
		got = append(got, rs.At().Value)
	}
	require.NoError(t, rs.Err())
	assert.ElementsMatch(t, shared, got, "every value returned by the quorum-reached replicas must survive merge+dedup")
}
