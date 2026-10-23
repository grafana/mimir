// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

// estimateRequest serves one request against q and returns the recorder.
func estimateRequest(t *testing.T, q *BlocksStoreQueryable, cfg CardinalityEstimateConfig, lbac bool, ctx context.Context, query string) *httptest.ResponseRecorder {
	t.Helper()
	h := CardinalityEstimateHandler(NewCardinalityEstimator(q, nil, nil), cfg, lbac)
	r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?"+query, nil)
	r = r.WithContext(user.InjectOrgID(ctx, "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestCardinalityEstimateHandler_LabelBasedAccessControl(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	var got *storegatewaypb.SeriesCountsRequest
	gateway := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	gateway.mockedMetricNameCounts = func(*storegatewaypb.MetricNameCountsRequest) (*storegatewaypb.MetricNameCountsResponse, error) {
		return nil, errors.New("the index-header path can't apply the policy and must not be used")
	}
	// The store set mock answers one GetClientsFor call per mocked response, so each case gets its own queryable.
	newQ := func() *BlocksStoreQueryable {
		return seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	}
	policy := func(selectors ...[]*labels.Matcher) context.Context {
		set := labelaccess.LabelPolicySet{}
		for _, sel := range selectors {
			set["user-1"] = append(set["user-1"], &labelaccess.LabelPolicy{Selector: sel})
		}
		return labelaccess.InjectLabelMatchersContext(context.Background(), set)
	}
	ns := labels.MustNewMatcher(labels.MatchEqual, "namespace", "a")

	t.Run("one policy selector is added to the matchers", func(t *testing.T) {
		rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, true, policy([]*labels.Matcher{ns}), "start=0&end=86400")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NotNil(t, got)
		require.Len(t, got.Matchers, 1)
		assert.Equal(t, "namespace", got.Matchers[0].Name)
		assert.Equal(t, "a", got.Matchers[0].Value)
		var out estimateResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, readFullIndex, out.Read, "a policy forces the full-index read")
	})

	t.Run("more than one policy selector is refused", func(t *testing.T) {
		rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, true, policy([]*labels.Matcher{ns}, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "namespace", "b")}), "start=0&end=86400")
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "more than one selector")
	})

	t.Run("no policy for the tenant leaves the request alone", func(t *testing.T) {
		got = nil
		rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, true, policy(), `start=0&end=86400&match[]={job="x"}`)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Len(t, got.Matchers, 1)
		assert.Equal(t, "job", got.Matchers[0].Name)
	})

	t.Run("with access control enabled and no policies in the context the request fails closed", func(t *testing.T) {
		rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, true, context.Background(), "start=0&end=86400")
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestCardinalityEstimateHandler_Status(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	oneDay := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gatewayFailing := func(err error) map[BlocksStoreClient][][]ulid.ULID {
		return map[BlocksStoreClient][][]ulid.ULID{
			&storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
				return nil, err
			}}: {{b1}},
		}
	}
	for name, tc := range map[string]struct {
		blocks  bucketindex.Blocks
		clients map[BlocksStoreClient][][]ulid.ULID
		cfg     CardinalityEstimateConfig
		query   string
		status  int
		body    string
	}{
		"too many buckets across block ranges is the caller's error": {
			blocks: bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}},
			query:  "start=0&end=172800&step=30m", status: http.StatusBadRequest, body: "capped at 48",
		},
		"a store-gateway refusing on max_index_bytes is 422": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.ResourceExhausted, "the request would read about 5000 index bytes, more than max_index_bytes 1000")),
			query: `start=0&end=86400&match[]={job="x"}&max_index_bytes=1000`, status: http.StatusUnprocessableEntity, body: "max_index_bytes",
		},
		"a store-gateway rejecting the request is 400": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.InvalidArgument, "step_ms doesn't divide the window")),
			query: `start=0&end=86400&match[]={job="x"}`, status: http.StatusBadRequest, body: "step_ms",
		},
		"a gRPC message over its size limit is a failure, not a refusal": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.ResourceExhausted, "grpc: received message larger than max (200000000 vs. 104857600)")),
			query: `start=0&end=86400&match[]={job="x"}`, status: http.StatusInternalServerError, body: "larger than max",
		},
		"a store-gateway failure is 500": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.Unavailable, "connection refused")),
			query: `start=0&end=86400&match[]={job="x"}`, status: http.StatusInternalServerError, body: "connection refused",
		},
		"compare=true is refused unless enabled": {
			blocks: oneDay, query: `start=0&end=86400&compare=true`, status: http.StatusBadRequest, body: "compare-enabled",
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := seriesCountsQueryable(0, 0, tc.blocks, tc.clients)
			q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(tc.blocks, &bucketindex.Metadata{}, nil)
			rec := estimateRequest(t, q, tc.cfg, false, context.Background(), tc.query)
			assert.Equal(t, tc.status, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.body)
		})
	}
}

func TestCardinalityEstimateHandler_MinCountAndStreamedCounts(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	clients := map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{
			{Value: "a", Counts: []int64{1}}, {Value: "b", Counts: []int64{4}}, {Value: "c", Counts: []int64{2}},
		}}): {{b1}},
	}
	q := seriesCountsQueryable(0, dayMs, blocks, clients)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}&min_count=2`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, []estimateGroup{{Value: "b", Count: 4}, {Value: "c", Count: 2}}, out.Counts, "a is below min_count")
	assert.Equal(t, int64(7), out.Total, "the total still counts every group")
	assert.Equal(t, 3, out.Groups)
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"counts":`), "the streamed body is one JSON object")
}

func TestBlocksStoreQueryable_SeriesCounts_MaxDedupSeries(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
	clients := map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}}): {{b1}},
		gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{3, 4}}}}):    {{b2}},
	}
	q := seriesCountsQueryable(0, 2*dayMs, days, clients)
	q.estimateCfg.MaxDedupSeries = 3
	_, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs})
	require.Error(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, estimateHTTPStatus(err))
	assert.Contains(t, err.Error(), "more than 3 series hashes")

	q = seriesCountsQueryable(0, 2*dayMs, days, clients)
	q.estimateCfg.MaxDedupSeries = 4
	res, err := q.SeriesCounts(context.Background(), "user-1", CardinalityEstimateRequest{MinT: 0, MaxT: 2 * dayMs})
	require.NoError(t, err)
	assert.Equal(t, map[string][]int64{"up": {4}}, res.Counts, "four distinct hashes fit the limit exactly")
}

// estimateDisabled is ingestersWithinLimit with the endpoint disabled for
// every tenant.
type estimateDisabled struct{ ingestersWithinLimit }

func (estimateDisabled) CardinalityEstimateEnabled(string) bool { return false }

func TestCardinalityEstimateHandler_DisabledForTenant(t *testing.T) {
	finder := &blocksFinderMock{}
	gateway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		return nil, errors.New("no store-gateway may be asked for a disabled tenant")
	}}
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{map[BlocksStoreClient][][]ulid.ULID{gateway: nil}}}}}}
	ing := &ingestersMock{}
	h := CardinalityEstimateHandler(NewCardinalityEstimator(q, ing, estimateDisabled{ingestersWithinLimit(13 * time.Hour)}), CardinalityEstimateConfig{}, false)
	r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?start=0&end=86400", nil)
	r = r.WithContext(user.InjectOrgID(context.Background(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "cardinality estimate is disabled for the tenant: user-1")
	finder.AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	assert.Nil(t, ing.gotSeriesCounts, "no ingester may be asked for a disabled tenant")
}
