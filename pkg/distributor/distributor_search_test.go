// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

func TestDistributor_SearchLabelNames_FanOutAndMerge(t *testing.T) {
	// Replicas return pre-filtered, pre-scored results — the distributor
	// only does cross-replica dedup + ordering.
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
	// Cross-replica dedup; scores carried verbatim.
	assert.Equal(t, []storage.SearchResult{
		{Value: "foo", Score: 1.0},
		{Value: "foobar", Score: 1.0},
		{Value: "footer", Score: 1.0},
	}, got)
}

// TestDistributor_SearchLabelNames_PreservesLeafScores: non-1.0 leaf scores
// propagate to the SearchResultSet unchanged (no re-filtering at the merge).
func TestDistributor_SearchLabelNames_PreservesLeafScores(t *testing.T) {
	// 0.7 stands in for a sub-prefix substring score.
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

func TestIngesterSearchResultSet_AtIsIdempotent(t *testing.T) {
	stream := &mockSearchStream{batches: []*client.SearchResultBatch{
		{Results: []client.SearchResultBatch_Result{
			{Value: "a", Score: 1.0},
			{Value: "b", Score: 0.9},
		}},
	}}
	rs := newIngesterSearchResultSet(stream, func() {})
	require.True(t, rs.Next())
	first := rs.At()
	second := rs.At()
	assert.Equal(t, first, second)
	assert.Equal(t, "a", first.Value)
	require.True(t, rs.Next())
	assert.Equal(t, "b", rs.At().Value)
}

// scoredValue is a {value, score} test-fixture pair.
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

// makeSearchBatches emits each value with Score 1.0.
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
	// RF=3 with all replicas serving identical shard data — DoUntilQuorum
	// short-circuits the third. Quorum-reached replicas' values must
	// survive merge+dedup.
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
	// Replicas return pre-filtered, pre-scored results.
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

func TestParamsToProto(t *testing.T) {
	cases := []struct {
		name string
		in   *streaminglabelvalues.Params
		want *client.SearchFilter
	}{
		{name: "nil", in: nil, want: nil},
		{name: "empty terms", in: &streaminglabelvalues.Params{}, want: nil},
		{
			name: "case-sensitive inverts polarity to CaseInsensitive=false",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: true},
			want: &client.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: false, FuzzAlg: client.FUZZ_ALG_SUBSEQUENCE},
		},
		{
			name: "case-insensitive inverts polarity to CaseInsensitive=true",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: false},
			want: &client.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: true, FuzzAlg: client.FUZZ_ALG_SUBSEQUENCE},
		},
		{
			name: "JaroWinkler fuzz alg",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: true, FuzzAlg: streaminglabelvalues.FuzzAlgJaroWinkler, FuzzThreshold: 70},
			want: &client.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: false, FuzzAlg: client.FUZZ_ALG_JARO_WINKLER, FuzzThreshold: 70},
		},
		{
			name: "Subsequence is the default (zero-value FuzzAlg)",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: true, FuzzThreshold: 50},
			want: &client.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: false, FuzzAlg: client.FUZZ_ALG_SUBSEQUENCE, FuzzThreshold: 50},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, paramsToProto(tc.in))
		})
	}
}

func TestOrderingToProto(t *testing.T) {
	cases := []struct {
		name  string
		hints *storage.SearchHints
		want  client.SearchOrdering
	}{
		{name: "nil hints defaults to ValueAsc", hints: nil, want: client.ORDER_BY_VALUE_ASC},
		{name: "ValueAsc", hints: &storage.SearchHints{OrderBy: storage.OrderByValueAsc}, want: client.ORDER_BY_VALUE_ASC},
		{name: "ValueDesc", hints: &storage.SearchHints{OrderBy: storage.OrderByValueDesc}, want: client.ORDER_BY_VALUE_DESC},
		{name: "ScoreDesc", hints: &storage.SearchHints{OrderBy: storage.OrderByScoreDesc}, want: client.ORDER_BY_SCORE_DESC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, orderingToProto(tc.hints))
		})
	}
}

// closeCountingSet is a minimal storage.SearchResultSet whose only contract
// is to bump a counter on Close. Used by collectOrCleanupSearchSets tests
// to assert that survivor streams are properly torn down on the error path.
type closeCountingSet struct {
	closed *atomic.Int32
}

func (c *closeCountingSet) Next() bool                        { return false }
func (c *closeCountingSet) At() storage.SearchResult          { return storage.SearchResult{} }
func (c *closeCountingSet) Warnings() annotations.Annotations { return nil }
func (c *closeCountingSet) Err() error                        { return nil }
func (c *closeCountingSet) Close() error                      { c.closed.Inc(); return nil }

func TestCollectOrCleanupSearchSets_ClosesSurvivorsOnAnyError(t *testing.T) {
	// Two jobs: job 0 returns three mock sets; job 1 errors. After the
	// helper returns the error, all three mock sets must have been Close()d
	// — otherwise the original bug (ForEachJobMergeResults discarding the
	// accumulated slice on a sibling job's failure) would leak survivor
	// streams rooted in the caller's ctx.
	var closeCount atomic.Int32
	mk := func() storage.SearchResultSet { return &closeCountingSet{closed: &closeCount} }

	wantErr := errors.New("boom")
	got, err := collectOrCleanupSearchSets(context.Background(), 2, func(_ context.Context, idx int) ([]storage.SearchResultSet, error) {
		switch idx {
		case 0:
			return []storage.SearchResultSet{mk(), mk(), mk()}, nil
		default:
			return nil, wantErr
		}
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, got)
	assert.Equal(t, int32(3), closeCount.Load(), "every survivor set must be closed when a sibling job fails")
}

func TestCollectOrCleanupSearchSets_NoCloseOnSuccess(t *testing.T) {
	// All jobs succeed — the returned sets must NOT be closed by the helper.
	// The caller is responsible for Close on success; if the helper closed
	// here, the caller would receive zombie sets.
	var closeCount atomic.Int32
	mk := func() storage.SearchResultSet { return &closeCountingSet{closed: &closeCount} }

	got, err := collectOrCleanupSearchSets(context.Background(), 2, func(_ context.Context, idx int) ([]storage.SearchResultSet, error) {
		return []storage.SearchResultSet{mk()}, nil
	})
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, int32(0), closeCount.Load(), "successful sets must not be closed by the helper")
}

func TestCollectOrCleanupSearchSets_AllJobsErrorBeforeOpening(t *testing.T) {
	// No survivors to clean up, but the helper must still surface the error
	// cleanly without panicking on the empty slice.
	wantErr := errors.New("nothing opened")
	got, err := collectOrCleanupSearchSets(context.Background(), 3, func(_ context.Context, _ int) ([]storage.SearchResultSet, error) {
		return nil, wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, got)
}
