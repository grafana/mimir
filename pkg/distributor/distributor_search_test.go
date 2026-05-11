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
	// Each replica returns the values its ingester would actually emit:
	// already filtered against the wire SearchFilter (no "bar"/"baz"), and
	// scored at 1.0 because every emitted term is a prefix-match for "fo".
	// The distributor's job here is cross-replica dedup + ordering, NOT
	// re-applying the filter.
	replicaResponses := map[int][]scoredValue{
		0: {{"foo", 1.0}, {"footer", 1.0}},
		1: {{"foo", 1.0}, {"foobar", 1.0}},
		2: {{"foobar", 1.0}},
	}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      3,
		happyIngesters:    3,
		replicationFactor: 1,
		searchLabelNamesHook: func(ingesterIdx int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return makeScoredSearchBatches(replicaResponses[ingesterIdx])
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
	// Cross-replica dedup ("foo"/"foobar" appeared in multiple replicas);
	// scores carried verbatim from the leaf.
	assert.Equal(t, []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}, got)
}

// TestDistributor_SearchLabelNames_PreservesLeafScores locks in the
// score-preservation contract: non-1.0 scores computed by leaf ingesters
// (e.g., non-prefix substring matches at 0.9, fuzzy-alg matches at varying
// thresholds) propagate to the SearchResultSet unchanged. Without this,
// the merge layer could only emit prefix-match scores.
func TestDistributor_SearchLabelNames_PreservesLeafScores(t *testing.T) {
	// Each replica emits the same scored set — Spec invariant 3 requires
	// scores to be deterministic per (Value, Filter), so two replicas
	// running the same filter agree byte-for-byte. Here we use 0.7 to
	// represent a sub-prefix substring score; the test asserts that the
	// merge layer carries it through without re-applying any filter.
	scored := []scoredValue{{"alpha", 0.7}, {"beta", 1.0}}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      2,
		happyIngesters:    2,
		replicationFactor: 1,
		searchLabelNamesHook: func(_ int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return makeScoredSearchBatches(scored)
		},
	})
	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		model.Earliest,
		model.Latest,
		nil,
		&storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 10},
		nil,
	)
	defer rs.Close()
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, []storage.SearchResult{
		{Value: "alpha", Score: 0.7}, // non-1.0 leaf score survives the merge layer
		{Value: "beta", Score: 1.0},
	}, got)
}

// scoredValue is a tiny test-fixture pair to drive makeScoredSearchBatches.
type scoredValue struct {
	value string
	score float64
}

func makeScoredSearchBatches(values []scoredValue) []*client.SearchResultBatch {
	results := make([]client.SearchResultBatch_Result, len(values))
	for i, sv := range values {
		results[i] = client.SearchResultBatch_Result{Value: sv.value, Score: sv.score}
	}
	return []*client.SearchResultBatch{{Results: results}}
}

// makeSearchBatches is a convenience wrapper that emits each value with
// Score 1.0, suitable for tests that don't care about per-value scores.
func makeSearchBatches(values []string) []*client.SearchResultBatch {
	scored := make([]scoredValue, len(values))
	for i, v := range values {
		scored[i] = scoredValue{value: v, score: 1.0}
	}
	return makeScoredSearchBatches(scored)
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

func TestDistributor_SearchLabelValues_FanOutAndMerge(t *testing.T) {
	// Each replica returns the values its ingester would actually emit —
	// already filtered against the "prod" SearchFilter and scored 1.0 for
	// prefix matches.
	replicaResponses := map[int][]scoredValue{
		0: {{"prod-a", 1.0}, {"prod-b", 1.0}},
		1: {{"prod-b", 1.0}},
		2: {{"prod-c", 1.0}},
	}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      3,
		happyIngesters:    3,
		replicationFactor: 1,
		searchLabelValuesHook: func(ingesterIdx int, _ *client.SearchLabelValuesRequest) []*client.SearchResultBatch {
			return makeScoredSearchBatches(replicaResponses[ingesterIdx])
		},
	})
	params := &streaminglabelvalues.Params{
		Terms:         []string{"prod"},
		CaseSensitive: true,
	}
	filter, err := streaminglabelvalues.BuildFilter(params)
	require.NoError(t, err)
	hints := &storage.SearchHints{Filter: filter, OrderBy: storage.OrderByValueAsc, Limit: 100}

	rs := ds[0].SearchLabelValues(
		user.InjectOrgID(context.Background(), "user-1"),
		0, model.Time(time.Now().UnixMilli()),
		"env",
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
	// Cross-replica dedup carries scores verbatim from each leaf.
	assert.Equal(t, []storage.SearchResult{
		{Value: "prod-a", Score: 1.0},
		{Value: "prod-b", Score: 1.0},
		{Value: "prod-c", Score: 1.0},
	}, got)
}

func TestDistributor_SearchLabelValues_PassesLabelName(t *testing.T) {
	var observedName string
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      1,
		happyIngesters:    1,
		replicationFactor: 1,
		searchLabelValuesHook: func(_ int, req *client.SearchLabelValuesRequest) []*client.SearchResultBatch {
			observedName = req.Name
			return nil
		},
	})
	rs := ds[0].SearchLabelValues(
		user.InjectOrgID(context.Background(), "user-1"),
		0, model.Time(time.Now().UnixMilli()),
		"env",
		nil,
		&storage.SearchHints{Limit: 10},
		nil,
	)
	defer rs.Close()
	for rs.Next() {
		_ = rs.At()
	}
	require.NoError(t, rs.Err())
	assert.Equal(t, "env", observedName, "the wire request must carry the label name being searched")
}
