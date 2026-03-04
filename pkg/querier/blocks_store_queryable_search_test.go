// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// reverseAlphaComparator sorts FilteredResults in reverse alphabetical order by Value.
// Used in tests to verify that the comparator is applied before the limit.
type reverseAlphaComparator struct{}

func (reverseAlphaComparator) Compare(a, b mimirstorage.FilteredResult) int {
	return strings.Compare(b.Value, a.Value)
}

// drainValueSet drains a SearcherValueSet and returns all values and any error.
func drainValueSet(vs mimirstorage.SearcherValueSet) ([]string, error) {
	var values []string
	for vs.Next() {
		values = append(values, vs.At().Value)
	}
	return values, vs.Err()
}

func TestBlocksStoreQuerier_SearchLabelNames(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	tests := map[string]struct {
		storeSetResponses []any
		finderErr         error
		hints             *mimirstorage.SearchHints
		expectedValues    []string
		expectedErrRegex  string
		unordered         bool
	}{
		"no blocks in storage": {
			expectedValues: nil,
		},
		"finder returns error": {
			finderErr:        errors.New("failed to find blocks"),
			expectedErrRegex: "failed to find blocks",
		},
		"single store-gateway returns all label names": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1, block2),
						},
					}: {block1, block2},
				},
			},
			expectedValues: []string{"__name__", "job", "namespace"},
		},
		"multiple store-gateways return overlapping label names": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "namespace"},
							ResponseHints: mockNamesResponseHints(block2),
						},
					}: {block2},
				},
			},
			// Results are deduplicated but arrival order from concurrent goroutines is not guaranteed.
			expectedValues: []string{"__name__", "job", "namespace"},
			unordered:      true,
		},
		"filter is applied to results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints: func() *mimirstorage.SearchHints {
				chain := streaminglabelvalues.NewFilterChains(false)
				c := streaminglabelvalues.NewFilterChain(streaminglabelvalues.Or, 1)
				c.AddFilter(streaminglabelvalues.NewFilterContains("job"))
				chain.AddFilterChain(c)
				return &mimirstorage.SearchHints{Filter: chain}
			}(),
			expectedValues: []string{"job"},
		},
		"limit is applied to results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints:          &mimirstorage.SearchHints{Limit: 2},
			expectedValues: []string{"__name__", "job"},
		},
		"store-gateway returns a non-retriable error": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr:          "1.1.1.1",
						mockedLabelNamesErr: status.Error(http.StatusUnprocessableEntity, "limit exceeded"),
					}: {block1},
				},
			},
			expectedErrRegex: "non-retriable error while fetching label names from store",
		},
		"finder returns error for label names": {
			finderErr:        errors.New("finder failed"),
			expectedErrRegex: "finder failed",
		},
		"comparator sorts results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints:          &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}},
			expectedValues: []string{"namespace", "job", "__name__"},
		},
		"comparator with limit applies limit after sort": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			},
			// Reverse-alpha order: namespace, job, __name__. Limit 2 keeps first two.
			hints:          &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}, Limit: 2},
			expectedValues: []string{"namespace", "job"},
		},
		"comparator with filter and limit": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelNamesResponse: &storepb.LabelNamesResponse{
							Names:         []string{"__name__", "job", "namespace"},
							ResponseHints: mockNamesResponseHints(block1),
						},
					}: {block1},
				},
			},
			// Filter keeps "job" and "namespace"; reverse-alpha gives namespace, job; limit 1 → namespace.
			hints: func() *mimirstorage.SearchHints {
				chain := streaminglabelvalues.NewFilterChains(false)
				c := streaminglabelvalues.NewFilterChain(streaminglabelvalues.Or, 1)
				c.AddFilter(streaminglabelvalues.NewFilterContains("e"))
				chain.AddFilterChain(c)
				return &mimirstorage.SearchHints{Filter: chain, Compare: reverseAlphaComparator{}, Limit: 1}
			}(),
			expectedValues: []string{"namespace"},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "user-1")
			reg := prometheus.NewPedanticRegistry()
			stores := &blocksStoreSetMock{mockedResponses: tc.storeSetResponses}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(
				blocksFromResponses(tc.storeSetResponses),
				&bucketindex.Metadata{},
				tc.finderErr,
			)

			q := &blocksStoreQuerier{
				minT:               minT,
				maxT:               maxT,
				finder:             finder,
				stores:             stores,
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, nil),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(reg),
				limits:             &blocksStoreLimitsMock{},
			}

			vs, err := q.SearchLabelNames(ctx, tc.hints)
			require.NoError(t, err)
			defer vs.Close()

			got, iterErr := drainValueSet(vs)
			if tc.expectedErrRegex != "" {
				require.Error(t, iterErr)
				assert.Regexp(t, tc.expectedErrRegex, iterErr.Error())
				return
			}
			require.NoError(t, iterErr)
			if tc.unordered {
				assert.ElementsMatch(t, tc.expectedValues, got)
			} else {
				assert.Equal(t, tc.expectedValues, got)
			}
		})
	}
}

func TestBlocksStoreQuerier_SearchLabelValues(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)

	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	tests := map[string]struct {
		storeSetResponses []any
		finderErr         error
		hints             *mimirstorage.SearchHints
		expectedValues    []string
		expectedErrRegex  string
		unordered         bool
	}{
		"no blocks in storage": {
			expectedValues: nil,
		},
		"single store-gateway returns label values": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block1, block2),
						},
					}: {block1, block2},
				},
			},
			expectedValues: []string{"metric_a", "metric_b", "metric_c"},
		},
		"multiple store-gateways return overlapping values": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
					&storeGatewayClientMock{
						remoteAddr: "2.2.2.2",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block2),
						},
					}: {block2},
				},
			},
			// Results are deduplicated but arrival order from concurrent goroutines is not guaranteed.
			expectedValues: []string{"metric_a", "metric_b", "metric_c"},
			unordered:      true,
		},
		"filter is applied to results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints: func() *mimirstorage.SearchHints {
				chain := streaminglabelvalues.NewFilterChains(false)
				c := streaminglabelvalues.NewFilterChain(streaminglabelvalues.Or, 1)
				c.AddFilter(streaminglabelvalues.NewFilterContains("_b"))
				chain.AddFilterChain(c)
				return &mimirstorage.SearchHints{Filter: chain}
			}(),
			expectedValues: []string{"metric_b"},
		},
		"limit is applied to results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints:          &mimirstorage.SearchHints{Limit: 1},
			expectedValues: []string{"metric_a"},
		},
		"store-gateway returns a non-retriable error": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr:           "1.1.1.1",
						mockedLabelValuesErr: status.Error(http.StatusUnprocessableEntity, "limit exceeded"),
					}: {block1},
				},
			},
			expectedErrRegex: "non-retriable error while fetching label values from store",
		},
		"finder returns error for label values": {
			finderErr:        errors.New("finder failed"),
			expectedErrRegex: "finder failed",
		},
		"comparator sorts results": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
				},
			},
			hints:          &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}},
			expectedValues: []string{"metric_c", "metric_b", "metric_a"},
		},
		"comparator with limit applies limit after sort": {
			storeSetResponses: []any{
				map[BlocksStoreClient][]ulid.ULID{
					&storeGatewayClientMock{
						remoteAddr: "1.1.1.1",
						mockedLabelValuesResponse: &storepb.LabelValuesResponse{
							Values:        []string{"metric_a", "metric_b", "metric_c"},
							ResponseHints: mockValuesResponseHints(block1),
						},
					}: {block1},
				},
			},
			// Reverse-alpha: metric_c, metric_b, metric_a. Limit 2 keeps first two.
			hints:          &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}, Limit: 2},
			expectedValues: []string{"metric_c", "metric_b"},
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "user-1")
			reg := prometheus.NewPedanticRegistry()
			stores := &blocksStoreSetMock{mockedResponses: tc.storeSetResponses}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(
				blocksFromResponses(tc.storeSetResponses),
				&bucketindex.Metadata{},
				tc.finderErr,
			)

			q := &blocksStoreQuerier{
				minT:               minT,
				maxT:               maxT,
				finder:             finder,
				stores:             stores,
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, nil),
				logger:             log.NewNopLogger(),
				metrics:            newBlocksStoreQueryableMetrics(reg),
				limits:             &blocksStoreLimitsMock{},
			}

			vs, err := q.SearchLabelValues(ctx, model.MetricNameLabel, tc.hints)
			require.NoError(t, err)
			defer vs.Close()

			got, iterErr := drainValueSet(vs)
			if tc.expectedErrRegex != "" {
				require.Error(t, iterErr)
				assert.Regexp(t, tc.expectedErrRegex, iterErr.Error())
				return
			}
			require.NoError(t, iterErr)
			if tc.unordered {
				assert.ElementsMatch(t, tc.expectedValues, got)
			} else {
				assert.Equal(t, tc.expectedValues, got)
			}
		})
	}
}


func TestLabelSearchStream(t *testing.T) {
	newStream := func(hints *mimirstorage.SearchHints, values ...string) (*labelSearchStream, context.CancelFunc) {
		ch := make(chan mimirstorage.FilteredResult, len(values)+1)
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel, hints: hints}
		for _, v := range values {
			ch <- mimirstorage.FilteredResult{Value: v, Score: -1}
		}
		close(ch)
		return s, cancel
	}

	t.Run("streams values in arrival order when no comparator", func(t *testing.T) {
		s, _ := newStream(nil, "b", "a", "c")
		got, err := drainValueSet(s)
		require.NoError(t, err)
		assert.Equal(t, []string{"b", "a", "c"}, got)
	})

	t.Run("sorted path buffers and sorts all values", func(t *testing.T) {
		s, _ := newStream(&mimirstorage.SearchHints{Compare: reverseAlphaComparator{}}, "b", "a", "c")
		got, err := drainValueSet(s)
		require.NoError(t, err)
		assert.Equal(t, []string{"c", "b", "a"}, got)
	})

	t.Run("sorted path applies limit after sort", func(t *testing.T) {
		s, _ := newStream(&mimirstorage.SearchHints{Compare: reverseAlphaComparator{}, Limit: 2}, "b", "a", "c")
		got, err := drainValueSet(s)
		require.NoError(t, err)
		assert.Equal(t, []string{"c", "b"}, got)
	})

	t.Run("Close while values are pending does not deadlock", func(t *testing.T) {
		ch := make(chan mimirstorage.FilteredResult, 10)
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel}
		for i := range 5 {
			ch <- mimirstorage.FilteredResult{Value: string([]byte{'a' + byte(i)})}
		}
		// Do not close ch — simulate a producer that is still running.
		// Close should cancel and drain without blocking.
		done := make(chan struct{})
		go func() {
			s.Close()
			// drain the remaining items that Close read from ch
			close(done)
		}()
		// Close the channel after a short yield so the drain loop can finish.
		close(ch)
		<-done
	})

	t.Run("context cancelled mid-stream stops Next and reports error", func(t *testing.T) {
		ch := make(chan mimirstorage.FilteredResult, 2)
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel}

		ch <- mimirstorage.FilteredResult{Value: "first"}

		require.True(t, s.Next())
		assert.Equal(t, "first", s.At().Value)

		// Cancel context before any more values arrive; channel stays open.
		cancel()

		assert.False(t, s.Next())
		assert.ErrorIs(t, s.Err(), context.Canceled)
	})

	t.Run("context cancelled during sorted drain stops collecting and reports error", func(t *testing.T) {
		ch := make(chan mimirstorage.FilteredResult) // unbuffered — no values will arrive
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel, hints: &mimirstorage.SearchHints{Compare: reverseAlphaComparator{}}}

		cancel() // cancel before Next() is ever called

		assert.False(t, s.Next())
		assert.ErrorIs(t, s.Err(), context.Canceled)
	})

	t.Run("Err returns nil after limit-triggered cancel", func(t *testing.T) {
		ch := make(chan mimirstorage.FilteredResult)
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel, limitReached: true}
		cancel() // simulate the dedup goroutine cancelling after limit
		close(ch)
		assert.NoError(t, s.Err())
	})

	t.Run("Err returns stored error when set", func(t *testing.T) {
		ch := make(chan mimirstorage.FilteredResult)
		ctx, cancel := context.WithCancel(context.Background())
		s := &labelSearchStream{ch: ch, ctx: ctx, cancel: cancel, err: errors.New("fetch failed")}
		close(ch)
		assert.EqualError(t, s.Err(), "fetch failed")
	})
}

// blocksFromResponses extracts the set of known blocks from mock storeSetResponses.
// This is used to set up the blocks finder mock to return the right blocks.
func blocksFromResponses(responses []any) bucketindex.Blocks {
	seen := map[ulid.ULID]struct{}{}
	var blocks bucketindex.Blocks
	for _, r := range responses {
		m, ok := r.(map[BlocksStoreClient][]ulid.ULID)
		if !ok {
			continue
		}
		for _, ids := range m {
			for _, id := range ids {
				if _, found := seen[id]; !found {
					seen[id] = struct{}{}
					blocks = append(blocks, &bucketindex.Block{ID: id})
				}
			}
		}
	}
	return blocks
}
