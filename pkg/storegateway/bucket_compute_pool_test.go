// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/workerpool"
)

// startComputePool returns a running pool of the given size, stopped on test cleanup.
func startComputePool(t *testing.T, size int) *workerpool.Pool {
	t.Helper()

	p, err := workerpool.New(workerpool.Config{Size: size}, "test", nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), p))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), p))
	})

	return p
}

// selectedSeries is the set of series the matchers are taken to have already selected.
var selectedSeries = []storage.SeriesRef{1, 2, 3}

// buildIntersectionInputs builds numValues label values where every even one intersects
// selectedSeries and every odd one does not, along with the expected matches. The postings are
// single-use iterators, so callers must rebuild them for each run.
func buildIntersectionInputs(numValues int) (allValues []streamindex.PostingListOffset, fetched []index.Postings, want []string) {
	allValues = make([]streamindex.PostingListOffset, numValues)
	fetched = make([]index.Postings, numValues)

	for i := 0; i < numValues; i++ {
		allValues[i] = streamindex.PostingListOffset{LabelValue: fmt.Sprintf("value_%04d", i)}
		if i%2 == 0 {
			fetched[i] = index.NewListPostings([]storage.SeriesRef{2})
			want = append(want, allValues[i].LabelValue)
		} else {
			fetched[i] = index.NewListPostings([]storage.SeriesRef{99})
		}
	}

	return allValues, fetched, want
}

func readerWithPool(pool *workerpool.Pool) *bucketIndexReader {
	return &bucketIndexReader{block: &bucketBlock{userID: "test", computeWorkerPool: pool}}
}

// TestIntersectLabelValuePostings asserts the pooled path agrees with the inline path, including
// across chunk boundaries, since the pooled path splits the values into chunks that may finish
// in any order.
func TestIntersectLabelValuePostings(t *testing.T) {
	ctx := context.Background()

	// Sizes either side of the chunk size, so a partial final chunk is covered, as is the
	// small-input case that deliberately stays inline.
	for _, numValues := range []int{0, 1, labelValuesPostingsChunkSize - 1, labelValuesPostingsChunkSize, labelValuesPostingsChunkSize + 1, 3*labelValuesPostingsChunkSize + 7} {
		t.Run(fmt.Sprintf("%d values", numValues), func(t *testing.T) {
			matchesFor := func(indexr *bucketIndexReader) []string {
				allValues, fetched, _ := buildIntersectionInputs(numValues)
				isMatch, err := intersectLabelValuePostings(ctx, indexr, selectedSeries, fetched, allValues)
				require.NoError(t, err)

				var got []string
				for i, v := range allValues {
					if isMatch[i] {
						got = append(got, v.LabelValue)
					}
				}
				return got
			}

			_, _, want := buildIntersectionInputs(numValues)

			require.Equal(t, want, matchesFor(readerWithPool(nil)), "inline path")
			require.Equal(t, want, matchesFor(readerWithPool(startComputePool(t, 4))), "pooled path")
		})
	}
}

// TestIntersectLabelValuePostings_PoolStopped asserts a stopped pool surfaces an error rather
// than quietly returning an incomplete result.
func TestIntersectLabelValuePostings_PoolStopped(t *testing.T) {
	pool, err := workerpool.New(workerpool.Config{Size: 2}, "test", nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), pool))
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), pool))

	// More values than the chunk size, so the pool is actually used.
	allValues, fetched, _ := buildIntersectionInputs(4 * labelValuesPostingsChunkSize)

	_, err = intersectLabelValuePostings(context.Background(), readerWithPool(pool), selectedSeries, fetched, allValues)
	require.ErrorIs(t, err, workerpool.ErrPoolStopped)
}

// TestIntersectLabelValuePostings_CancelledContext asserts cancellation is honoured rather than
// every chunk being run to completion.
func TestIntersectLabelValuePostings_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	allValues, fetched, _ := buildIntersectionInputs(4 * labelValuesPostingsChunkSize)

	_, err := intersectLabelValuePostings(ctx, readerWithPool(startComputePool(t, 2)), selectedSeries, fetched, allValues)
	require.ErrorIs(t, err, context.Canceled)
}

// TestBucketStore_ComputeWorkerPool_MatchesInlineResults asserts that routing the CPU seams
// through the pool does not change what the label endpoints return.
func TestBucketStore_ComputeWorkerPool_MatchesInlineResults(t *testing.T) {
	ctx := context.Background()

	namesFor := func(pool *workerpool.Pool) []string {
		bs := prepareSearchTestStore(t)
		for _, b := range bs.blockSet.blocks {
			b.computeWorkerPool = pool
		}

		// Matchers are required: without them these endpoints take a fast path straight off the
		// index header and never reach the postings seams the pool is meant to run.
		resp, err := bs.LabelNames(ctx, &storepb.LabelNamesRequest{
			End:      math.MaxInt64,
			Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "a", Value: ".+"}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Names, "test would be vacuous if nothing matched")
		return resp.Names
	}

	valuesFor := func(pool *workerpool.Pool) []string {
		bs := prepareSearchTestStore(t)
		for _, b := range bs.blockSet.blocks {
			b.computeWorkerPool = pool
		}

		resp, err := bs.LabelValues(ctx, &storepb.LabelValuesRequest{
			Label:    "a",
			End:      math.MaxInt64,
			Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_RE, Name: "a", Value: ".+"}},
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Values, "test would be vacuous if nothing matched")
		return resp.Values
	}

	pool := startComputePool(t, 4)
	require.Equal(t, namesFor(nil), namesFor(pool))
	require.Equal(t, valuesFor(nil), valuesFor(pool))
}

// TestIntersectLabelValuePostings_CancellationDoesNotWaitForDrain asserts a cancelled request
// stops waiting rather than blocking until every queued chunk has drained through a busy pool.
// This matters because the caller holds a process-wide block-gate slot for the duration: if a
// dead client could pin that slot, the gate meant to protect tenants from each other would
// itself block them.
func TestIntersectLabelValuePostings_CancellationDoesNotWaitForDrain(t *testing.T) {
	pool := startComputePool(t, 1)

	// Occupy the pool's only worker so submitted chunks have to queue behind it. Released in
	// cleanup, which runs before startComputePool's own stop cleanup (LIFO).
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	require.NoError(t, pool.Submit("test-hog", "other-tenant", func() { <-blocked }))

	allValues, fetched, _ := buildIntersectionInputs(8 * labelValuesPostingsChunkSize)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	began := time.Now()
	_, err := intersectLabelValuePostings(ctx, readerWithPool(pool), selectedSeries, fetched, allValues)
	took := time.Since(began)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, took, 5*time.Second, "should return when cancelled, not wait for the pool to drain")
}
