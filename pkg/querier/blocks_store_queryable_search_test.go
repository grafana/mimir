// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"net/http"
	"strconv"
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
	grpc_metadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// searchLabelNamesClientMock emits a header batch (carrying queriedBlockIDs in
// response_hints) as the first message — matching the SG-side wire contract
// in pkg/storegateway/bucket_search.go::streamBucketSearchResults — then walks
// through preset batches and finally surfaces err (if set) instead of EOF,
// letting tests inject a mid-stream Recv error.
//
// omitHeader skips the mandatory header batch so the protocol-violation path
// can be exercised. headerErr fires on the very first Recv to simulate an
// open-succeeded-but-header-Recv-failed condition.
type searchLabelNamesClientMock struct {
	grpc.ClientStream
	batches         []*storepb.SearchResultBatch
	err             error
	queriedBlockIDs []ulid.ULID
	omitHeader      bool
	headerErr       error
	headerSent      bool
	idx             int
}

func (m *searchLabelNamesClientMock) Recv() (*storepb.SearchResultBatch, error) {
	if !m.headerSent {
		m.headerSent = true
		if m.headerErr != nil {
			return nil, m.headerErr
		}
		if !m.omitHeader {
			return &storepb.SearchResultBatch{ResponseHints: newSearchResponseHints(m.queriedBlockIDs)}, nil
		}
	}
	if m.idx >= len(m.batches) {
		if m.err != nil {
			return nil, m.err
		}
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	return b, nil
}

type searchLabelValuesClientMock struct {
	grpc.ClientStream
	batches         []*storepb.SearchResultBatch
	err             error
	queriedBlockIDs []ulid.ULID
	omitHeader      bool
	headerErr       error
	headerSent      bool
	idx             int
}

func (m *searchLabelValuesClientMock) Recv() (*storepb.SearchResultBatch, error) {
	if !m.headerSent {
		m.headerSent = true
		if m.headerErr != nil {
			return nil, m.headerErr
		}
		if !m.omitHeader {
			return &storepb.SearchResultBatch{ResponseHints: newSearchResponseHints(m.queriedBlockIDs)}, nil
		}
	}
	if m.idx >= len(m.batches) {
		if m.err != nil {
			return nil, m.err
		}
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	return b, nil
}

// newSearchResponseHints always returns a non-nil hint struct (matching the
// SG-side contract) so a nil-or-empty ids slice still produces a valid header.
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
// queriedBlockIDs, when set, populates response_hints.queried_blocks on the
// header batch the mock client sends as its first message. omitHeader and
// headerErr drive protocol-violation and header-Recv-error coverage
// respectively.
type searchStoreGatewayClientMock struct {
	storeGatewayClientMock
	searchLabelNamesBatches    []*storepb.SearchResultBatch
	searchLabelNamesErr        error
	searchLabelNamesStreamErr  error
	searchLabelNamesHeaderErr  error
	searchLabelNamesOmitHeader bool
	searchLabelValuesBatches   []*storepb.SearchResultBatch
	searchLabelValuesErr       error
	searchLabelValuesStreamErr error
	queriedBlockIDs            []ulid.ULID
	lastSearchLabelNamesCtx    context.Context
	lastSearchLabelValuesCtx   context.Context
	lastSearchLabelValuesReq   *storepb.SearchLabelValuesRequest
	calls                      atomicCounter
}

type atomicCounter struct {
	n int
}

func (c *atomicCounter) inc() { c.n++ }

func (m *searchStoreGatewayClientMock) SearchLabelNames(ctx context.Context, _ *storepb.SearchLabelNamesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelNamesClient, error) {
	m.calls.inc()
	m.lastSearchLabelNamesCtx = ctx
	if m.searchLabelNamesErr != nil {
		return nil, m.searchLabelNamesErr
	}
	return &searchLabelNamesClientMock{
		ClientStream:    grpcClientStreamMock{ctx: context.Background()},
		batches:         m.searchLabelNamesBatches,
		err:             m.searchLabelNamesStreamErr,
		queriedBlockIDs: m.queriedBlockIDs,
		omitHeader:      m.searchLabelNamesOmitHeader,
		headerErr:       m.searchLabelNamesHeaderErr,
	}, nil
}

func (m *searchStoreGatewayClientMock) SearchLabelValues(ctx context.Context, req *storepb.SearchLabelValuesRequest, _ ...grpc.CallOption) (storegatewaypb.StoreGateway_SearchLabelValuesClient, error) {
	m.calls.inc()
	m.lastSearchLabelValuesCtx = ctx
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
// mid-stream-failure contract under the header-first wire protocol: once the
// SG's header has credited the requested blocks, the consistency tracker
// considers the SG's contribution accepted, so there is no block to refetch
// against another SG. A retriable Recv error mid-stream therefore propagates
// to the merger's Err() and fails the query — matching the ingester-path
// semantics in pkg/distributor/distributor_search.go (which also has no
// mid-stream-retry path). Pre-error rows that already crossed the merger
// boundary stay visible to the client; the failure is signalled via Err()
// after iteration ends.
func TestBlocksStoreQuerier_SearchLabelNames_MidStreamRecvError(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{
				{Value: "pre_error_value", Score: 1.0},
			},
		}},
		searchLabelNamesStreamErr: status.Error(codes.Unavailable, "transient mid-stream"),
		// Header credits block1 — consistency check accepts the SG's contribution.
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

	require.True(t, rs.Next(), "pre-error row should be emitted")
	assert.Equal(t, "pre_error_value", rs.At().Value)
	assert.False(t, rs.Next(), "mid-stream error must terminate iteration")
	require.Error(t, rs.Err())
	assert.Contains(t, rs.Err().Error(), "transient mid-stream")
	assert.Equal(t, 1, store.calls.n, "SG must be tried once; no block-level retry once the header has credited the block")
}

// TestBlocksStoreQuerier_SearchLabelNames_HeaderRecvErrorIsRetriable proves
// that a retriable error on the header Recv is treated like an open failure:
// the SG's contribution is dropped, the consistency tracker sees the blocks
// as missing, and another SG is tried. This is the only retry path the
// header-first wire contract preserves.
func TestBlocksStoreQuerier_SearchLabelNames_HeaderRecvErrorIsRetriable(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	storeHeaderErr := &searchStoreGatewayClientMock{
		storeGatewayClientMock:    storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesHeaderErr: status.Error(codes.Unavailable, "transient header"),
	}
	storeOK := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "2.2.2.2"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "retry_value", Score: 1.0}},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storeHeaderErr: {block1}},
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
	assert.Equal(t, "retry_value", got[0].Value)
	assert.Equal(t, 1, storeHeaderErr.calls.n, "header-failing SG should be tried once")
	assert.Equal(t, 1, storeOK.calls.n, "retry SG should be tried once")
}

// TestBlocksStoreQuerier_SearchLabelNames_MissingHeaderIsProtocolViolation
// asserts the non-retriable hard-fail when an SG emits result data without
// the mandatory header batch. The consistency tracker cannot run without the
// header, so we refuse to merge from a non-conforming SG.
func TestBlocksStoreQuerier_SearchLabelNames_MissingHeaderIsProtocolViolation(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	storeNoHeader := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "should_not_appear", Score: 1.0}},
		}},
		searchLabelNamesOmitHeader: true,
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storeNoHeader: {block1}},
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
	assert.Contains(t, rs.Err().Error(), "header")
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

// TestBlocksStoreQuerier_SearchLabelNames_PropagatesBucketStoreMetadata pins
// the gRPC outgoing-metadata contract: tenant ID and bucket-index updated-at
// must reach the SG via the per-stream ctx. Mirrors the propagation done by
// the legacy unary fetchLabelNamesFromStore path (which uses gCtx, rooted in
// reqCtx). The header live-streaming refactor rooted streams in the caller's
// ctx for lifecycle reasons; this test pins that the ctx used for stream open
// still carries the bucket-store metadata appended by
// grpcContextWithBucketStoreRequestMeta, so the SG can route to the right
// tenant and apply bucket-index versioning.
func TestBlocksStoreQuerier_SearchLabelNames_PropagatesBucketStoreMetadata(t *testing.T) {
	const (
		minT         = int64(10)
		maxT         = int64(20)
		indexUpdated = int64(1234567890)
	)
	block1 := ulid.MustNew(1, nil)

	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "a", Score: 1.0}},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{store: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{UpdatedAt: indexUpdated}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelNames(ctx, nil, nil)
	defer rs.Close()
	_ = drainSearchResults(t, rs)

	require.NotNil(t, store.lastSearchLabelNamesCtx, "mock must have captured the stream ctx")
	md, ok := grpc_metadata.FromOutgoingContext(store.lastSearchLabelNamesCtx)
	require.True(t, ok, "stream ctx must carry outgoing gRPC metadata")
	assert.Equal(t, []string{"user-1"}, md.Get(storegateway.GrpcContextMetadataTenantID),
		"tenant ID must be propagated as gRPC outgoing metadata")
	assert.Equal(t, []string{strconv.FormatInt(indexUpdated, 10)}, md.Get(storegateway.GrpcContextMetadataBucketIndexUpdatedAt),
		"bucket-index updated-at must be propagated as gRPC outgoing metadata")
}

// TestBlocksStoreQuerier_SearchLabelValues_PropagatesBucketStoreMetadata
// mirrors the SearchLabelNames variant for the SearchLabelValues RPC.
func TestBlocksStoreQuerier_SearchLabelValues_PropagatesBucketStoreMetadata(t *testing.T) {
	const (
		minT         = int64(10)
		maxT         = int64(20)
		indexUpdated = int64(987654321)
	)
	block1 := ulid.MustNew(1, nil)

	store := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelValuesBatches: []*storepb.SearchResultBatch{{
			Results: []storepb.SearchResultBatch_Result{{Value: "prod", Score: 1.0}},
		}},
		queriedBlockIDs: []ulid.ULID{block1},
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{store: {block1}},
	}}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", minT, maxT).Return(bucketindex.Blocks{
		{ID: block1},
	}, &bucketindex.Metadata{UpdatedAt: indexUpdated}, error(nil))

	q := newBlocksStoreSearchQuerier(t, stores, finder, minT, maxT)
	ctx := user.InjectOrgID(context.Background(), "user-1")

	rs := q.SearchLabelValues(ctx, "env", nil, nil)
	defer rs.Close()
	_ = drainSearchResults(t, rs)

	require.NotNil(t, store.lastSearchLabelValuesCtx, "mock must have captured the stream ctx")
	md, ok := grpc_metadata.FromOutgoingContext(store.lastSearchLabelValuesCtx)
	require.True(t, ok, "stream ctx must carry outgoing gRPC metadata")
	assert.Equal(t, []string{"user-1"}, md.Get(storegateway.GrpcContextMetadataTenantID),
		"tenant ID must be propagated as gRPC outgoing metadata")
	assert.Equal(t, []string{strconv.FormatInt(indexUpdated, 10)}, md.Get(storegateway.GrpcContextMetadataBucketIndexUpdatedAt),
		"bucket-index updated-at must be propagated as gRPC outgoing metadata")
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

func TestParamsToSGProto(t *testing.T) {
	cases := []struct {
		name string
		in   *streaminglabelvalues.Params
		want *storepb.SearchFilter
	}{
		{name: "nil", in: nil, want: nil},
		{name: "empty terms", in: &streaminglabelvalues.Params{}, want: nil},
		{
			name: "case-sensitive inverts polarity",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: true},
			want: &storepb.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: false, FuzzAlg: storepb.FUZZ_ALG_SUBSEQUENCE},
		},
		{
			name: "case-insensitive inverts polarity",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: false},
			want: &storepb.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: true, FuzzAlg: storepb.FUZZ_ALG_SUBSEQUENCE},
		},
		{
			name: "JaroWinkler",
			in:   &streaminglabelvalues.Params{Terms: []string{"foo"}, CaseSensitive: true, FuzzAlg: streaminglabelvalues.FuzzAlgJaroWinkler, FuzzThreshold: 70},
			want: &storepb.SearchFilter{Terms: []string{"foo"}, CaseInsensitive: false, FuzzAlg: storepb.FUZZ_ALG_JARO_WINKLER, FuzzThreshold: 70},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, paramsToSGProto(tc.in))
		})
	}
}

func TestOrderingToSGProto(t *testing.T) {
	cases := []struct {
		name  string
		hints *storage.SearchHints
		want  storepb.SearchOrdering
	}{
		{name: "nil hints defaults to ValueAsc", hints: nil, want: storepb.ORDER_BY_VALUE_ASC},
		{name: "ValueAsc", hints: &storage.SearchHints{OrderBy: storage.OrderByValueAsc}, want: storepb.ORDER_BY_VALUE_ASC},
		{name: "ValueDesc", hints: &storage.SearchHints{OrderBy: storage.OrderByValueDesc}, want: storepb.ORDER_BY_VALUE_DESC},
		{name: "ScoreDesc", hints: &storage.SearchHints{OrderBy: storage.OrderByScoreDesc}, want: storepb.ORDER_BY_SCORE_DESC},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, orderingToSGProto(tc.hints))
		})
	}
}

// TestBlocksStoreQuerier_SearchLabelNames_MalformedHeaderBlockHintHardFails
// pins a non-retriable hard fail when a header batch carries an unparseable
// queried-block ID. The consistency tracker would otherwise be fed a
// silently-dropped block and miscredit the SG's contribution.
func TestBlocksStoreQuerier_SearchLabelNames_MalformedHeaderBlockHintHardFails(t *testing.T) {
	const (
		minT = int64(10)
		maxT = int64(20)
	)
	block1 := ulid.MustNew(1, nil)

	// The header is sent on the first Recv but the mock's omitHeader=true
	// skips it and the first batch carries the malformed hint directly.
	// readSGSearchHeader reads the first message and rejects the bad block ID.
	storeBadHeader := &searchStoreGatewayClientMock{
		storeGatewayClientMock: storeGatewayClientMock{remoteAddr: "1.1.1.1"},
		searchLabelNamesBatches: []*storepb.SearchResultBatch{{
			ResponseHints: &storepb.SearchResponseHints{QueriedBlocks: []storepb.Block{{Id: "not-a-ulid"}}},
		}},
		searchLabelNamesOmitHeader: true,
	}
	stores := &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][]ulid.ULID{storeBadHeader: {block1}},
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
	assert.Contains(t, rs.Err().Error(), "queried block IDs")
}
