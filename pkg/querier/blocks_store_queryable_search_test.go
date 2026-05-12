// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// searchLabelNamesClientMock drains preset batches then surfaces err (if
// set) instead of EOF, letting tests inject a mid-stream Recv error.
//
// queriedBlockIDs is attached as SearchResponseHints to the final batch on
// the way out so the querier's consistency check credits the queried blocks.
type searchLabelNamesClientMock struct {
	grpc.ClientStream
	batches         []*storepb.SearchResultBatch
	err             error
	queriedBlockIDs []ulid.ULID
	idx             int
}

func (m *searchLabelNamesClientMock) Recv() (*storepb.SearchResultBatch, error) {
	if m.idx >= len(m.batches) {
		if m.err != nil {
			return nil, m.err
		}
		if len(m.queriedBlockIDs) > 0 {
			batch := &storepb.SearchResultBatch{ResponseHints: newSearchResponseHints(m.queriedBlockIDs)}
			m.queriedBlockIDs = nil
			return batch, nil
		}
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	if m.idx == len(m.batches) && len(m.queriedBlockIDs) > 0 && b.ResponseHints == nil {
		b.ResponseHints = newSearchResponseHints(m.queriedBlockIDs)
		m.queriedBlockIDs = nil
	}
	return b, nil
}

type searchLabelValuesClientMock struct {
	grpc.ClientStream
	batches         []*storepb.SearchResultBatch
	err             error
	queriedBlockIDs []ulid.ULID
	idx             int
}

func (m *searchLabelValuesClientMock) Recv() (*storepb.SearchResultBatch, error) {
	if m.idx >= len(m.batches) {
		if m.err != nil {
			return nil, m.err
		}
		if len(m.queriedBlockIDs) > 0 {
			batch := &storepb.SearchResultBatch{ResponseHints: newSearchResponseHints(m.queriedBlockIDs)}
			m.queriedBlockIDs = nil
			return batch, nil
		}
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	if m.idx == len(m.batches) && len(m.queriedBlockIDs) > 0 && b.ResponseHints == nil {
		b.ResponseHints = newSearchResponseHints(m.queriedBlockIDs)
		m.queriedBlockIDs = nil
	}
	return b, nil
}

func newSearchResponseHints(ids []ulid.ULID) *storepb.SearchResponseHints {
	hints := &storepb.SearchResponseHints{}
	for _, id := range ids {
		hints.AddQueriedBlock(id)
	}
	return hints
}

// searchStoreGatewayClientMock is a BlocksStoreClient with programmable
// SearchLabel{Names,Values} streams. lastSearchLabelValuesReq lets tests
// assert the wire request shape.
//
// queriedBlockIDs, when set, is attached as SearchResponseHints to the trailer
// batch so the querier's consistency check credits these blocks.
type searchStoreGatewayClientMock struct {
	storeGatewayClientMock
	searchLabelNamesBatches    []*storepb.SearchResultBatch
	searchLabelNamesErr        error
	searchLabelNamesStreamErr  error
	searchLabelValuesBatches   []*storepb.SearchResultBatch
	searchLabelValuesErr       error
	searchLabelValuesStreamErr error
	queriedBlockIDs            []ulid.ULID
	lastSearchLabelValuesReq   *storepb.SearchLabelValuesRequest
	calls                      atomicCounter
}

type atomicCounter struct {
	n int
}

func (c *atomicCounter) inc() { c.n++ }

func (m *searchStoreGatewayClientMock) SearchLabelNames(context.Context, *storepb.SearchLabelNamesRequest, ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelNamesClient, error) {
	m.calls.inc()
	if m.searchLabelNamesErr != nil {
		return nil, m.searchLabelNamesErr
	}
	return &searchLabelNamesClientMock{
		ClientStream:    grpcClientStreamMock{ctx: context.Background()},
		batches:         m.searchLabelNamesBatches,
		err:             m.searchLabelNamesStreamErr,
		queriedBlockIDs: m.queriedBlockIDs,
	}, nil
}

func (m *searchStoreGatewayClientMock) SearchLabelValues(_ context.Context, req *storepb.SearchLabelValuesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelValuesClient, error) {
	m.calls.inc()
	m.lastSearchLabelValuesReq = req
	if m.searchLabelValuesErr != nil {
		return nil, m.searchLabelValuesErr
	}
	return &searchLabelValuesClientMock{
		ClientStream:    grpcClientStreamMock{ctx: context.Background()},
		batches:         m.searchLabelValuesBatches,
		err:             m.searchLabelValuesStreamErr,
		queriedBlockIDs: m.queriedBlockIDs,
	}, nil
}

func newBlocksStoreSearchQuerier(t *testing.T, stores BlocksStoreSet, finder *blocksFinderMock, minT, maxT int64) *blocksStoreQuerier {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	return &blocksStoreQuerier{
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
}

func drainSearchResults(t *testing.T, rs storage.SearchResultSet) []storage.SearchResult {
	t.Helper()
	var got []storage.SearchResult
	for rs.Next() {
		got = append(got, rs.At())
	}
	require.NoError(t, rs.Err())
	return got
}

func TestBlocksStoreQuerier_SearchLabelNames_HappyPath(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	store1 := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "__name__", Score: 1.0},
				{Value: "instance", Score: 0.9},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	store2 := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "instance", Score: 0.9},
				{Value: "job", Score: 0.8},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block2},
	}

	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{
			store1: {block1},
			store2: {block2},
		},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
		{ID: block2},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	// Merger yields sorted, deduplicated values at score 1.0.
	require.Len(t, got, 3)
	assert.Equal(t, "__name__", got[0].Value)
	assert.Equal(t, "instance", got[1].Value)
	assert.Equal(t, "job", got[2].Value)
}

// TestBlocksStoreQuerier_SearchLabelNames_RetriesMissingBlock pins the
// consistency-check fix from the P1 review: when an SG fails to report a
// requested block in its SearchResponseHints (e.g. the block hadn't loaded
// yet on that SG), queryWithConsistencyCheck must retry for the missing
// block instead of silently returning partial results.
func TestBlocksStoreQuerier_SearchLabelNames_RetriesMissingBlock(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	// First attempt: asked for {block1, block2}, but the SG only ever loaded
	// block1, so it credits only block1.
	storePartial := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "from_block1", Score: 1.0}},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	// Retry: another SG owns block2 and credits it.
	storeRetry := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "from_block2", Score: 1.0}},
		}},
		queriedBlockIDs: []ulid.ULID{block2},
	}

	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storePartial: {block1, block2}},
		map[BlocksStoreClient][]ulid.ULID{storeRetry: {block2}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
		{ID: block2},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	require.Len(t, got, 2)
	assert.Equal(t, "from_block1", got[0].Value)
	assert.Equal(t, "from_block2", got[1].Value)
	assert.Equal(t, 1, storePartial.calls.n, "first SG must be tried once with both blocks")
	assert.Equal(t, 1, storeRetry.calls.n, "retry SG must be tried once for the missing block")
}

func TestBlocksStoreQuerier_SearchLabelNames_PartialReplicaFailureWithRetry(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	// First attempt: retriable error; consistency-check must retry.
	storeRetriable := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesErr:    status.Error(codes.Unavailable, "transient"),
	}
	// Second attempt: success.
	storeOK := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "namespace", Score: 1.0},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}

	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storeRetriable: {block1}},
		map[BlocksStoreClient][]ulid.ULID{storeOK: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	require.Len(t, got, 1)
	assert.Equal(t, "namespace", got[0].Value)
	assert.Equal(t, 1, storeRetriable.calls.n, "retriable store should be tried once")
	assert.Equal(t, 1, storeOK.calls.n, "second store should be queried after retry")
}

func TestBlocksStoreQuerier_SearchLabelNames_WarningsPropagated(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{
			{
				Results: []storepb.SearchResultBatch_Result{{Value: "env", Score: 1.0}},
			},
			{
				// Trailer batch carries a warning only.
				Warnings: []string{"sg-warning: partial results"},
			},
		},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{store: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	require.Len(t, got, 1)
	assert.Equal(t, "env", got[0].Value)
	warns := rs.Warnings()
	require.Len(t, warns, 1)
	// annotations.Annotations.AsErrors yields the underlying errors.
	gotWarnings := warns.AsErrors()
	require.Len(t, gotWarnings, 1)
	assert.Contains(t, gotWarnings[0].Error(), "sg-warning: partial results")
}

func TestBlocksStoreQuerier_SearchLabelNames_NoTenant(t *testing.T) {
	q := newBlocksStoreSearchQuerier(t, &blocksStoreSetMock{}, &blocksFinderMock{}, 10, 20)
	rs := q.SearchLabelNames(context.Background(), nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	assert.Error(t, rs.Err())
}

func TestBlocksStoreQuerier_SearchLabelNames_NonRetriableErrorBubblesUp(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)
	// 422 is non-retriable per shouldRetry — must short-circuit.
	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesErr:    status.Error(codes.Code(http.StatusUnprocessableEntity), "validation"),
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{store: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	assert.False(t, rs.Next())
	require.Error(t, rs.Err())
	assert.Contains(t, rs.Err().Error(), "non-retriable")
}

// TestBlocksStoreQuerier_SearchLabelNames_MidStreamRecvError pins the
// mid-stream-retry contract: a retriable error after partial delivery
// drops the failing SG's pre-error batches and re-runs against a
// different SG. This is "all-or-nothing per replica" — the goroutine
// bails before appending to nameSets, so the consistency tracker sees
// the blocks as missing and retries. SearchLabelValues inherits the
// same behaviour via drainSGSearchLabelStream.
func TestBlocksStoreQuerier_SearchLabelNames_MidStreamRecvError(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	// First SG: delivers one batch, then errors with Unavailable on the next
	// Recv — drainSGSearchLabelStream returns the error, shouldRetry classifies
	// it retriable, the goroutine logs and returns nil, no result is appended.
	storeMidStreamErr := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "pre_error_value", Score: 1.0},
			},
		}},
		searchLabelNamesStreamErr: status.Error(codes.Unavailable, "transient mid-stream"),
	}
	// Retry SG: clean drain with a different value.
	storeOK := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "retry_value", Score: 1.0},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}

	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storeMidStreamErr: {block1}},
		map[BlocksStoreClient][]ulid.ULID{storeOK: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	// Documented behaviour: the first SG's pre-error batch is DROPPED. Only
	// the retry SG's value appears in the final result.
	require.Len(t, got, 1)
	assert.Equal(t, "retry_value", got[0].Value)
	assert.NotContains(t, []string{got[0].Value}, "pre_error_value",
		"pre-error data from failed replica must NOT leak through")

	assert.Equal(t, 1, storeMidStreamErr.calls.n, "failing SG should be tried once")
	assert.Equal(t, 1, storeOK.calls.n, "retry SG should be tried once")
}

// TestBlocksStoreQuerier_SearchLabelValues_HappyPath mirrors the SearchLabelNames
// happy path: two SGs return overlapping values for label "env"; the querier
// merges, deduplicates and sorts. The retry / warnings / no-tenant /
// non-retriable / mid-stream-error patterns are inherited transitively from
// SearchLabelNames — SearchLabelValues reuses the same drain helper,
// fetch-from-store pattern and queryWithConsistencyCheck plumbing.
func TestBlocksStoreQuerier_SearchLabelValues_HappyPath(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)
	block2 := ulid.MustNew(2, nil)

	store1 := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelValuesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "dev", Score: 1.0},
				{Value: "prod", Score: 0.9},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	store2 := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelValuesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "prod", Score: 0.9},
				{Value: "staging", Score: 0.8},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block2},
	}

	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{
			store1: {block1},
			store2: {block2},
		},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
		{ID: block2},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelValues(ctx, "env", nil, nil)
	defer rs.Close()
	got := drainSearchResults(t, rs)

	require.Len(t, got, 3)
	assert.Equal(t, "dev", got[0].Value)
	assert.Equal(t, "prod", got[1].Value)
	assert.Equal(t, "staging", got[2].Value)
}

// TestBlocksStoreQuerier_SearchLabelValues_PassesLabelName asserts the label
// name argument is propagated through to the wire request. This is the only
// shape difference between SearchLabelNames and SearchLabelValues.
func TestBlocksStoreQuerier_SearchLabelValues_PassesLabelName(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelValuesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "prod", Score: 1.0},
			},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{store: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelValues(ctx, "env", nil, nil)
	defer rs.Close()
	_ = drainSearchResults(t, rs)

	require.NotNil(t, store.lastSearchLabelValuesReq, "SearchLabelValues must be called on the SG client")
	assert.Equal(t, "env", store.lastSearchLabelValuesReq.Label, "wire request must carry the label name")
}
