// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
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
		absent  string
	}{
		"too many buckets answered one by one is a limit's refusal": {
			blocks: bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}},
			query:  "start=0&end=172800&step=30m", status: http.StatusUnprocessableEntity, body: "its 96 buckets would be answered one by one, more than 48 (the -querier.cardinality-estimate-max-buckets-answered-separately limit)",
		},
		"a store-gateway refusing on max_index_bytes is 422": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.ResourceExhausted, "the request would read about 5000 index bytes, more than max_index_bytes 1000")),
			query: `start=0&end=86400&match[]={job="x"}&max_index_bytes=1000`, status: http.StatusUnprocessableEntity, body: "max_index_bytes",
		},
		"an older store-gateway is 422 and not a split of max_index_bytes": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.Unimplemented, "unknown method SeriesCounts")),
			query: `start=0&end=86400&match[]={job="x"}&max_index_bytes=1000`, status: http.StatusUnprocessableEntity, body: "a store-gateway does not support cardinality estimates yet", absent: "split into",
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
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing: dial tcp 10.1.2.3:9095: connect: connection refused"`)),
			query: `start=0&end=86400&match[]={job="x"}`, status: http.StatusInternalServerError, body: "store-gateway: the call failed with Unavailable", absent: "10.1.2.3",
		},
		"compare=true is refused unless enabled": {
			blocks: oneDay, query: `start=0&end=86400&compare=true`, status: http.StatusBadRequest, body: "compare-enabled",
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := seriesCountsQueryable(0, 0, tc.blocks, tc.clients)
			q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(tc.blocks, &bucketindex.Metadata{}, nil)
			var logs bytes.Buffer
			q.logger = log.NewLogfmtLogger(&logs)
			rec := estimateRequest(t, q, tc.cfg, false, context.Background(), tc.query)
			assert.Equal(t, tc.status, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.body)
			assert.NotContains(t, rec.Body.String(), "1.1.1.1", "an error names the kind of component, not its address")
			if tc.absent != "" {
				assert.NotContains(t, rec.Body.String(), tc.absent)
			}
			if tc.status == http.StatusInternalServerError && tc.clients != nil {
				// The log line keeps what the body leaves out.
				line := logs.String()
				assert.Contains(t, line, "level=warn")
				assert.Contains(t, line, "store_gateway=1.1.1.1")
				if tc.absent != "" {
					assert.Contains(t, line, tc.absent)
				}
			}
		})
	}
}

// A bucket index error can name bucket paths or carry object storage's
// error text, so the caller sees only that the lookup failed; the log line
// keeps the rest.
func TestCardinalityEstimateHandler_BucketIndexErrorRedacted(t *testing.T) {
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), (*bucketindex.Metadata)(nil), errors.New("read bucket-index.json.gz: Get https://storage.example/private-bucket/user-1/bucket-index.json.gz: 403 Forbidden"))
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder}}}
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "the bucket index lookup failed")
	assert.NotContains(t, rec.Body.String(), "private-bucket")
	assert.Contains(t, logs.String(), "private-bucket")

	// A stale bucket index names no path and says what to do, so it is kept.
	stale := &blocksFinderMock{}
	stale.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), (*bucketindex.Metadata)(nil), newBucketIndexTooOldError(time.Unix(0, 0), time.Hour))
	q = &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: stale}}}
	rec = estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "the bucket index lookup: the bucket index is too old")

	// The deletion-mark lookup is redacted the same way.
	twoRanges := bucketindex.Blocks{{ID: ulid.MustNew(1, nil), MinTime: 0, MaxTime: dayMs / 2}, {ID: ulid.MustNew(2, nil), MinTime: 0, MaxTime: dayMs}}
	listed := &blocksFinderMock{}
	listed.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(twoRanges, &bucketindex.Metadata{}, nil)
	q = &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: &failingMarksFinder{blocksFinderMock: listed, err: errors.New("read deletion marks from private-bucket")}}}}
	rec = estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "the bucket index lookup failed")
	assert.NotContains(t, rec.Body.String(), "private-bucket")
}

// failingMarksFinder is a finder whose deletion-mark lookup fails.
type failingMarksFinder struct {
	*blocksFinderMock
	err error
}

func (m *failingMarksFinder) BlocksMarkedForDeletion(context.Context, string) (map[ulid.ULID]struct{}, error) {
	return nil, m.err
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
	assert.Equal(t, []estimateGroup{{Value: "b", Count: 4}, {Value: "c", Count: 2}}, out.Groups, "a is below min_count")
	assert.Equal(t, int64(7), out.Total, "the total still counts every group")
	assert.Equal(t, 3, out.GroupCount)
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"groups":`), "the streamed body is one JSON object")
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

func TestSetEstimateCacheControl(t *testing.T) {
	for name, tc := range map[string]struct {
		out     estimateResponse
		noStore bool
	}{
		"a complete answer may be cached": {out: estimateResponse{}, noStore: false},
		"a lower bound":                   {out: estimateResponse{LowerBound: true}, noStore: true},
		"a coverage gap":                  {out: estimateResponse{CoverageGap: true}, noStore: true},
		"the whole head counted as one":   {out: estimateResponse{HeadWhole: true}, noStore: true},
		"a table cut by limit":            {out: estimateResponse{HasMore: true}, noStore: false},
		"estimated groups":                {out: estimateResponse{EstimatedGroups: 2}, noStore: false},
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			setEstimateCacheControl(rec, &tc.out)
			if tc.noStore {
				assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
			} else {
				assert.Empty(t, rec.Header().Get("Cache-Control"))
			}
		})
	}
}
