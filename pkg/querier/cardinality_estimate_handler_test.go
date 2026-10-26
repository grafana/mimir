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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/compartments"
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
		// noLine is set for a request refused before any estimate, which
		// logs no line.
		noLine bool
		// limit is the limit that refused the request, if any.
		limit string
	}{
		"too many buckets answered one by one is a limit's refusal": {
			blocks: bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}},
			query:  "start=0&end=172800&step=30m", status: http.StatusUnprocessableEntity, body: "its 96 buckets would be answered one by one, more than 48 (the -querier.cardinality-estimate-max-buckets-answered-separately limit)",
			limit: refusedByMaxBucketsAnsweredSeparately,
		},
		"a store-gateway refusing on max_index_bytes is 422": {
			blocks: oneDay, clients: gatewayFailing(status.Error(codes.ResourceExhausted, "the request would read about 5000 index bytes, more than max_index_bytes 1000")),
			query: `start=0&end=86400&match[]={job="x"}&max_index_bytes=1000`, status: http.StatusUnprocessableEntity, body: "max_index_bytes",
			limit: refusedByMaxIndexBytes,
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
			blocks: oneDay, query: `start=0&end=86400&compare=true`, status: http.StatusBadRequest, body: "compare-enabled", noLine: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			q := seriesCountsQueryable(0, 0, tc.blocks, tc.clients)
			q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(tc.blocks, &bucketindex.Metadata{}, nil)
			var logs bytes.Buffer
			q.logger = log.NewLogfmtLogger(&logs)
			q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
			rec := estimateRequest(t, q, tc.cfg, false, context.Background(), tc.query)
			assert.Equal(t, tc.status, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.body)
			assert.NotContains(t, rec.Body.String(), "1.1.1.1", "an error names the kind of component, not its address")
			refusals := 0.0
			for _, limit := range []string{refusedByMaxHashes, refusedByMaxIndexBytes, refusedByMemory, refusedByMaxMetricNames, refusedByMaxBucketsAnsweredSeparately} {
				n := testutil.ToFloat64(q.metrics.estimateRefusals.WithLabelValues(limit))
				refusals += n
				if limit == tc.limit {
					assert.Equal(t, 1.0, n, "refused by %s", limit)
				}
			}
			if tc.limit == "" {
				assert.Zero(t, refusals, "only a limit's refusal is counted")
			} else {
				assert.Equal(t, 1.0, refusals)
				assert.Contains(t, logs.String(), "refused_by="+tc.limit)
			}
			if !tc.noLine {
				// A failed estimate's cost is not known, not zero.
				line := logs.String()
				assert.Contains(t, line, "cost=unknown")
				assert.NotContains(t, line, "index_bytes=")
				assert.NotContains(t, line, "series_counted=")
			}
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
	assert.Contains(t, logs.String(), "cost=unknown")

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

// A deduplicating estimate sends each store-gateway a share of the hashes
// the tenant's memory limit allows as max_hashes; a source over its share
// refuses with 422, and the log line, the refusals counter and the
// dedup-hashes histogram record what was asked.
func TestCardinalityEstimateHandler_MaxHashes(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	days := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}, {ID: b2, MinTime: dayMs, MaxTime: 2 * dayMs}}
	run := func(t *testing.T, clients map[BlocksStoreClient][][]ulid.ULID) (*BlocksStoreQueryable, *httptest.ResponseRecorder, string) {
		q := seriesCountsQueryable(0, 2*dayMs, days, clients)
		q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(days, &bucketindex.Metadata{}, nil)
		var logs bytes.Buffer
		q.logger = log.NewLogfmtLogger(&logs)
		q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
		// 100 hashes at 24 bytes each.
		limits := estimateMemoryLimits{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), estimate: 100 * estimateHashBytes}
		h := CardinalityEstimateHandler(NewCardinalityEstimator(q, nil, limits), CardinalityEstimateConfig{}, false)
		r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=172800&match[]={job="x"}&max_index_bytes=100000`, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r.WithContext(user.InjectOrgID(r.Context(), "user-1")))
		return q, rec, logs.String()
	}

	t.Run("each call gets a share, and the held hashes are recorded", func(t *testing.T) {
		var got1, got2 *storegatewaypb.SeriesCountsRequest
		q, rec, line := run(t, map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got1, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}, SeriesCounted: 3}): {{b1}},
			gatewayAnswering("2.2.2.2", &got2, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{3, 4}}}, SeriesCounted: 2}):    {{b2}},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NotNil(t, got1)
		require.NotNil(t, got2)
		assert.Equal(t, int64(50), got1.MaxHashes)
		assert.Equal(t, int64(50), got2.MaxHashes)
		assert.Contains(t, line, "hashes_needed=5")
		hist := &dto.Metric{}
		require.NoError(t, q.metrics.estimateDedupHashes.Write(hist))
		assert.Equal(t, uint64(1), hist.GetHistogram().GetSampleCount())
		assert.Equal(t, 4.0, hist.GetHistogram().GetSampleSum(), "four distinct hashes held")
	})

	t.Run("a store-gateway over its share is a max_hashes refusal", func(t *testing.T) {
		refusing := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, cardinality.MaxHashesRefusal(70, 50))
		}}
		q, rec, line := run(t, map[BlocksStoreClient][][]ulid.ULID{
			refusing: {{b1}},
			gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{}): {{b2}},
		})
		assert.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "the 100 hashes the tenant's cardinality estimate memory limit allows, split into 50 for each of 2 store-gateway calls")
		assert.NotContains(t, rec.Body.String(), "max_index_bytes 100000, split")
		assert.Contains(t, line, "hashes_needed=70")
		assert.Contains(t, line, "refused_by=max_hashes")
		assert.Equal(t, 1.0, testutil.ToFloat64(q.metrics.estimateRefusals.WithLabelValues(refusedByMaxHashes)))
	})
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

// Every request is counted by path, source and status, and timed whatever
// its outcome; an answer counts once per flag it carries, and its retries
// add up.
func TestObserveEstimate_RequestsFlagsAndRetries(t *testing.T) {
	q := &BlocksStoreQueryable{metrics: newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())}
	ctx := context.Background()
	req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs}
	answered := CardinalityEstimateResult{Read: readFullIndex, Source: sourceBlocks, Retries: 2}
	flagged := CardinalityEstimateResult{Read: readFullIndex, Source: sourceBlocks, Retries: 1, LowerBound: true, CoverageGap: true, HeadFallback: "refused", MissingBlocks: []ulid.ULID{ulid.MustNew(1, nil)}}

	q.observeEstimate(ctx, req, answered, &estimateResponse{}, time.Second, nil)
	q.observeEstimate(ctx, req, flagged, &estimateResponse{}, time.Second, nil)
	q.observeEstimate(ctx, req, CardinalityEstimateResult{}, nil, time.Second, refusedByLimit(refusedByMaxIndexBytes, "over max_index_bytes"))
	q.observeEstimate(ctx, req, CardinalityEstimateResult{}, nil, time.Second, badEstimateRequest("bad window"))
	q.observeEstimate(ctx, req, CardinalityEstimateResult{}, nil, time.Second, errors.New("store-gateway down"))
	q.observeRefusedRequest()

	requests := func(path, source, status string) float64 {
		return testutil.ToFloat64(q.metrics.estimateRequests.WithLabelValues(path, source, status))
	}
	assert.Equal(t, 2.0, requests(readFullIndex, sourceBlocks, "ok"))
	assert.Equal(t, 3.0, requests("unknown", "unknown", "refused"), "a limit, a bad request and one refused before any estimate")
	assert.Equal(t, 1.0, requests("unknown", "unknown", "failed"))
	for _, flag := range []string{"lower_bound", "coverage_gap", "head_fallback", "missing_blocks"} {
		assert.Equal(t, 1.0, testutil.ToFloat64(q.metrics.estimateFlagged.WithLabelValues(readFullIndex, flag)), flag)
	}
	assert.Equal(t, 3.0, testutil.ToFloat64(q.metrics.estimateRetries))

	durations := func(path, source, status string) uint64 {
		m := &dto.Metric{}
		require.NoError(t, q.metrics.estimateDuration.WithLabelValues(path, source, status).(prometheus.Metric).Write(m))
		return m.GetHistogram().GetSampleCount()
	}
	assert.Equal(t, uint64(2), durations(readFullIndex, sourceBlocks, "ok"))
	assert.Equal(t, uint64(2), durations("unknown", "unknown", "refused"), "refused before an estimate is not timed")
	assert.Equal(t, uint64(1), durations("unknown", "unknown", "failed"), "a failure is timed too")
	m := &dto.Metric{}
	require.NoError(t, q.metrics.estimateDuration.WithLabelValues(readFullIndex, sourceBlocks, "ok").(prometheus.Metric).Write(m))
	assert.Equal(t, 327.68, m.GetHistogram().GetBucket()[len(m.GetHistogram().GetBucket())-1].GetUpperBound(), "the buckets reach past -querier.timeout's 5m")
	assert.NotZero(t, m.GetHistogram().GetSchema()+int32(len(m.GetHistogram().GetPositiveSpan())), "a native histogram too")
}

// -querier.timeout bounds an estimate: one past it is answered as the
// Prometheus API answers a timed-out query.
func TestCardinalityEstimateHandler_Timeout(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	// The mock doesn't see the call's context; a gRPC client returns at its
	// deadline, as this one does once the timeout has passed.
	slow := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		time.Sleep(200 * time.Millisecond)
		return nil, status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error())
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{slow: {{b1}}})
	q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
	rec := estimateRequest(t, q, CardinalityEstimateConfig{Timeout: 50 * time.Millisecond}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "error", body["status"])
	assert.Equal(t, "timeout", body["errorType"])
	assert.Contains(t, body["error"], "cardinality estimate timed out after 50ms (-querier.timeout)")
	assert.Equal(t, 1.0, testutil.ToFloat64(q.metrics.estimateRequests.WithLabelValues("unknown", "unknown", "failed")))
}

func TestCardinalityEstimateHandler_NoTimeoutWhenUnset(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*storegatewaypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}}): {{b1}},
	})
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}
