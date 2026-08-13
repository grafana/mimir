// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
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
	rs := newIngesterSearchResultSet(stream, nil, nil)
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

// TestDistributor_SearchLabelNames_WarningsPreservedUnderLimit pins the
// concurrentSearchResultSet warning-snapshot contract under hints.Limit:
// the merger short-circuits before draining all per-replica results, so the
// prefetcher's producer goroutine is still alive (blocked on ch <- r) when
// the caller reads Warnings(). Without the producer's incremental warnings
// snapshot, c.warns is still its zero value at this point and the warning
// is silently dropped.
func TestDistributor_SearchLabelNames_WarningsPreservedUnderLimit(t *testing.T) {
	// Need more results than the prefetch buffer (256) so the producer blocks
	// on ch <- r once the buffer fills and limit halts the consumer.
	const total = 1024
	scored := make([]scoredValue, total)
	for i := range total {
		scored[i] = scoredValue{value: fmt.Sprintf("v%04d", i), score: 1.0}
	}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      1,
		happyIngesters:    1,
		replicationFactor: 1,
		searchLabelNamesHook: func(_ int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			batches := makeScoredSearchBatches(scored)
			batches[0].Warnings = []string{"replica-warn"}
			return batches
		},
	})
	rs := ds[0].SearchLabelNames(
		user.InjectOrgID(context.Background(), "user-1"),
		model.Earliest, model.Latest,
		nil,
		&storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: 5},
		nil,
	)
	defer rs.Close()

	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	require.Len(t, got, 5, "merger must surface exactly hints.Limit results")

	// Read Warnings BEFORE Close — the bug is that c.warns is only populated
	// in the producer's terminal defer, which hasn't fired yet under limit.
	msgs := make([]string, 0, 1)
	for _, w := range rs.Warnings() {
		msgs = append(msgs, w.Error())
	}
	assert.Equal(t, []string{"replica-warn"}, msgs,
		"per-replica warnings must surface even when hints.Limit prevents the producer from exiting naturally")
}

// TestDistributor_SearchLabelNames_ContextCancelledMidStream verifies the
// distributor's stream lifecycle on caller-initiated cancellation: continued
// iteration must terminate, and Close must return promptly. Otherwise the
// prefetcher goroutines leak.
func TestDistributor_SearchLabelNames_ContextCancelledMidStream(t *testing.T) {
	// >256 results forces producer to block on ch <- r once the prefetch
	// channel fills; the cancel-cause path is the only way for it to exit.
	const total = 1024
	scored := make([]scoredValue, total)
	for i := range total {
		scored[i] = scoredValue{value: fmt.Sprintf("v%04d", i), score: 1.0}
	}
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors:   1,
		numIngesters:      1,
		happyIngesters:    1,
		replicationFactor: 1,
		searchLabelNamesHook: func(_ int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			return makeScoredSearchBatches(scored)
		},
	})

	ctx, cancel := context.WithCancel(user.InjectOrgID(context.Background(), "user-1"))
	rs := ds[0].SearchLabelNames(
		ctx,
		model.Earliest, model.Latest,
		nil,
		&storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: total + 1},
		nil,
	)

	// Read a few results, then cancel mid-stream.
	for range 5 {
		require.True(t, rs.Next())
	}
	cancel()

	// Continued iteration must terminate within a deadline; otherwise the
	// prefetcher would have leaked its producer goroutine indefinitely.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for rs.Next() { //nolint:revive // intentional drain after cancel
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("rs.Next did not terminate within 5s after ctx cancel")
	}

	// Close must not hang — it depends on the producer goroutine exiting.
	closeDone := make(chan error, 1)
	go func() { closeDone <- rs.Close() }()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("rs.Close hung after ctx cancel")
	}
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

// generateBenchmarkSearchValues returns numIngesters slices of valuesPerStream
// sorted strings. dedup controls cross-stream overlap:
//
//   - "disjoint": stream k holds values [k*N, (k+1)*N) — zero overlap; the
//     k-way merge emits k*N unique results.
//   - "half_overlap": each stream shares half its values with its neighbour;
//     overlap region rotates around the streams.
//   - "fully_overlapping": every stream holds the same values 0..N-1; the
//     merge emits N unique results with k-1 duplicates dropped per value.
//
// Values are pre-formatted as zero-padded decimals so alphabetic order
// equals numeric order — no surprises in alpha_asc result ordering.
func generateBenchmarkSearchValues(numIngesters, valuesPerStream int, dedup string) [][]string {
	streams := make([][]string, numIngesters)
	switch dedup {
	case "disjoint":
		for k := 0; k < numIngesters; k++ {
			s := make([]string, valuesPerStream)
			base := k * valuesPerStream
			for i := 0; i < valuesPerStream; i++ {
				s[i] = fmt.Sprintf("v%09d", base+i)
			}
			streams[k] = s
		}
	case "half_overlap":
		// Stream k holds [k*N/2, k*N/2 + N): adjacent streams share their
		// second/first half respectively.
		half := valuesPerStream / 2
		for k := 0; k < numIngesters; k++ {
			s := make([]string, valuesPerStream)
			base := k * half
			for i := 0; i < valuesPerStream; i++ {
				s[i] = fmt.Sprintf("v%09d", base+i)
			}
			streams[k] = s
		}
	case "fully_overlapping":
		shared := make([]string, valuesPerStream)
		for i := 0; i < valuesPerStream; i++ {
			shared[i] = fmt.Sprintf("v%09d", i)
		}
		for k := 0; k < numIngesters; k++ {
			// Hand each ingester its own slice header so an accidental
			// in-place mutation by one consumer can't poison the others.
			s := make([]string, valuesPerStream)
			copy(s, shared)
			streams[k] = s
		}
	default:
		panic("generateBenchmarkSearchValues: unknown dedup " + dedup)
	}
	return streams
}

// runDistributorSearchBenchmark drives ds[0].SearchLabelValues b.N times,
// draining every result set fully. The hook closure reads from prebuilt so
// per-iteration cost stays inside the timed loop.
func runDistributorSearchBenchmark(b *testing.B, ds *Distributor, hints *storage.SearchHints, name string) {
	ctx := user.InjectOrgID(context.Background(), "user-1")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs := ds.SearchLabelValues(ctx, 0, model.Latest, name, nil, hints, nil)
		for rs.Next() {
			_ = rs.At()
		}
		if err := rs.Err(); err != nil {
			b.Fatal(err)
		}
		rs.Close()
	}
}

// BenchmarkDistributor_SearchLabelValues exercises the distributor's
// fan-out + cross-replica MergeSearchResultSets merge against mock
// ingesters returning prebuilt SearchResultBatches. The mock-ingester
// hooks are populated outside the timed loop so per-iteration cost
// reflects the merge + adapter cost, not the data generation.
func BenchmarkDistributor_SearchLabelValues(b *testing.B) {
	type benchCase struct {
		numIngesters    int
		valuesPerStream int
		dedup           string
		ordering        storage.Ordering
	}

	cases := []benchCase{
		{3, 1000, "disjoint", storage.OrderByValueAsc},
		{3, 1000, "half_overlap", storage.OrderByValueAsc},
		{3, 1000, "fully_overlapping", storage.OrderByValueAsc},
		{3, 10_000, "disjoint", storage.OrderByValueAsc},
		{3, 10_000, "half_overlap", storage.OrderByValueAsc},
		{3, 10_000, "fully_overlapping", storage.OrderByValueAsc},
		{5, 10_000, "disjoint", storage.OrderByValueAsc},
		{5, 10_000, "half_overlap", storage.OrderByValueAsc},
		{5, 10_000, "fully_overlapping", storage.OrderByValueAsc},
		// Score-desc swaps the heap comparator; same data, different cost.
		{3, 10_000, "half_overlap", storage.OrderByScoreDesc},
	}

	// Group by numIngesters so prepare() only runs once per ring topology.
	for _, numIngesters := range []int{3, 5} {
		var (
			batchesByIngester [][]*client.SearchResultBatch
			lastIngesterCount int
		)
		ds, _, _, _ := prepare(b, prepConfig{
			numDistributors:   1,
			numIngesters:      numIngesters,
			happyIngesters:    numIngesters,
			replicationFactor: 1,
			searchLabelValuesHook: func(ingesterIdx int, _ *client.SearchLabelValuesRequest) []*client.SearchResultBatch {
				if ingesterIdx >= lastIngesterCount {
					return nil
				}
				return batchesByIngester[ingesterIdx]
			},
		})

		for _, c := range cases {
			if c.numIngesters != numIngesters {
				continue
			}
			c := c
			streams := generateBenchmarkSearchValues(c.numIngesters, c.valuesPerStream, c.dedup)
			batchesByIngester = make([][]*client.SearchResultBatch, c.numIngesters)
			for k, vals := range streams {
				batchesByIngester[k] = makeSearchBatches(vals)
			}
			lastIngesterCount = c.numIngesters

			name := fmt.Sprintf("ingesters=%d/values=%d/dedup=%s/order=%s",
				c.numIngesters, c.valuesPerStream, c.dedup, orderingShortName(c.ordering))
			b.Run(name, func(b *testing.B) {
				hints := &storage.SearchHints{OrderBy: c.ordering, Limit: c.numIngesters * c.valuesPerStream}
				runDistributorSearchBenchmark(b, ds[0], hints, "env")
			})
		}
	}
}

// BenchmarkDistributor_SearchLabelNames mirrors the SearchLabelValues
// benchmark with the label-names RPC. Label-name cardinality is small in
// practice so valuesPerStream stays modest; the merge cost characteristic
// is the same shape.
func BenchmarkDistributor_SearchLabelNames(b *testing.B) {
	type benchCase struct {
		numIngesters    int
		valuesPerStream int
		dedup           string
	}
	cases := []benchCase{
		{3, 100, "disjoint"},
		{3, 100, "half_overlap"},
		{3, 100, "fully_overlapping"},
		{3, 1000, "disjoint"},
		{3, 1000, "fully_overlapping"},
	}

	var (
		batchesByIngester [][]*client.SearchResultBatch
	)
	ds, _, _, _ := prepare(b, prepConfig{
		numDistributors:   1,
		numIngesters:      3,
		happyIngesters:    3,
		replicationFactor: 1,
		searchLabelNamesHook: func(ingesterIdx int, _ *client.SearchLabelNamesRequest) []*client.SearchResultBatch {
			if ingesterIdx >= len(batchesByIngester) {
				return nil
			}
			return batchesByIngester[ingesterIdx]
		},
	})

	for _, c := range cases {
		c := c
		streams := generateBenchmarkSearchValues(c.numIngesters, c.valuesPerStream, c.dedup)
		batchesByIngester = make([][]*client.SearchResultBatch, c.numIngesters)
		for k, vals := range streams {
			batchesByIngester[k] = makeSearchBatches(vals)
		}

		name := fmt.Sprintf("ingesters=%d/values=%d/dedup=%s", c.numIngesters, c.valuesPerStream, c.dedup)
		b.Run(name, func(b *testing.B) {
			ctx := user.InjectOrgID(context.Background(), "user-1")
			hints := &storage.SearchHints{OrderBy: storage.OrderByValueAsc, Limit: c.numIngesters * c.valuesPerStream}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs := ds[0].SearchLabelNames(ctx, 0, model.Latest, nil, hints, nil)
				for rs.Next() {
					_ = rs.At()
				}
				if err := rs.Err(); err != nil {
					b.Fatal(err)
				}
				rs.Close()
			}
		})
	}
}

func orderingShortName(o storage.Ordering) string {
	switch o {
	case storage.OrderByValueAsc:
		return "alpha_asc"
	case storage.OrderByValueDesc:
		return "alpha_desc"
	case storage.OrderByScoreDesc:
		return "score_desc"
	default:
		return "unknown"
	}
}
