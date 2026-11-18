// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// estimateRequest serves one request against q and returns the recorder.
func estimateRequest(t *testing.T, q *BlocksStoreQueryable, cfg CardinalityEstimateConfig, lbac bool, ctx context.Context, query string) *httptest.ResponseRecorder {
	t.Helper()
	return estimateRequestWithLimits(t, q, ingestersWithinLimit(13*time.Hour), cfg, lbac, ctx, query)
}

// estimateRequestWithLimits is estimateRequest under the given tenant limits.
func estimateRequestWithLimits(t *testing.T, q *BlocksStoreQueryable, limits estimateLimits, cfg CardinalityEstimateConfig, lbac bool, ctx context.Context, query string) *httptest.ResponseRecorder {
	t.Helper()
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, limits), cfg, lbac)
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
	gateway := gatewayAnswering("1.1.1.1", &got, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
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
		assert.Equal(t, ReadFullIndex, out.Read, "a policy forces the full-index read")
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

	t.Run("a policy with no selector is malformed and refused, not applied as no restriction", func(t *testing.T) {
		got = nil
		rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, true, policy([]*labels.Matcher{}), "start=0&end=86400")
		assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		assert.Contains(t, rec.Body.String(), "access policy has no selector")
		assert.Nil(t, got, "nothing is asked of the store-gateways")
	})
}

// group_by is a label name, echoed into the response, the log line and the
// spans, so anything else is refused before any estimate.
func TestCardinalityEstimateHandler_GroupByValidated(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	newQ := func() *BlocksStoreQueryable {
		return seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{}): {{b1}}})
	}
	for name, tc := range map[string]struct {
		groupBy string
		body    string
	}{
		"a label name is accepted":                              {groupBy: "pod"},
		"a UTF-8 name is accepted":                              {groupBy: "service.name"},
		"an invalid byte is refused":                            {groupBy: "pod\xff", body: "group_by must be a label name"},
		"a name over the tenant's label name length is refused": {groupBy: strings.Repeat("a", 1025), body: "group_by is 1025 bytes long, more than the 1024 a label name may be"},
		"a name at the length is accepted":                      {groupBy: strings.Repeat("a", 1024)},
		"a control character is refused":                        {groupBy: "pod\x01step=1h", body: "group_by must not hold control characters"},
	} {
		t.Run(name, func(t *testing.T) {
			rec := estimateRequest(t, newQ(), CardinalityEstimateConfig{}, false, context.Background(), "start=0&end=86400&group_by="+url.QueryEscape(tc.groupBy))
			if tc.body == "" {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				return
			}
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tc.body)
		})
	}

	// A tenant whose label name length is unset gets the flag's default.
	assert.Equal(t, 1024, NewCardinalityEstimator(nil, nil, zeroLabelNameLength{ingestersWithinLimit(0)}).maxGroupByLength("user-1"))
	assert.Equal(t, 1024, NewCardinalityEstimator(nil, nil, ingestersWithinLimit(0)).maxGroupByLength("user-1"))
}

// A caller that goes away before the answer gets 499, as a canceled query
// does, and the request is counted as canceled, not failed.
func TestCardinalityEstimateHandler_CallerCanceled(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	ctx, cancel := context.WithCancel(context.Background())
	gone := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		// The caller gives up while the store-gateway is asked.
		cancel()
		return nil, status.Error(codes.Canceled, "context canceled")
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gone: {{b1}}})
	q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, ctx, `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, 499, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "canceled by the caller")
	requests := func(status string) uint64 {
		m := &dto.Metric{}
		require.NoError(t, q.metrics.estimateDuration.WithLabelValues("unknown", SourceBlocks, status).(prometheus.Metric).Write(m))
		return m.GetHistogram().GetSampleCount()
	}
	assert.Equal(t, uint64(1), requests("canceled"))
	assert.Zero(t, requests("failed"))
	assert.Contains(t, logs.String(), "level=info", "not a failure of Mimir's")
	assert.Contains(t, logs.String(), "status=499")
}

// A body that fails while it is written is not followed by an error text
// written into it: the status was sent with the first bytes.
func TestCardinalityEstimateHandler_WriteFailureNotWrittenIntoBody(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	// A per-bucket table, written whole once counted, larger than the
	// response writer's buffer, so the body goes out in several writes.
	var groups []*cardinalitypb.SeriesCountGroup
	for i := range 500 {
		groups = append(groups, &cardinalitypb.SeriesCountGroup{Value: fmt.Sprintf("metric_name_%08d", i), Counts: []int64{2}})
	}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: groups}): {{b1}},
	})
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, ingestersWithinLimit(13*time.Hour)), CardinalityEstimateConfig{}, false)
	r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=86400&step=1d&match[]={job="x"}`, nil)
	r = r.WithContext(user.InjectOrgID(context.Background(), "user-1"))
	rec := httptest.NewRecorder()
	w := &failingAfterFirstWrite{ResponseWriter: rec}
	h.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, rec.Code, "the status went with the first write")
	assert.Equal(t, 2, w.writes, "the failing write is the last one; no error text follows it")
	assert.NotContains(t, rec.Body.String(), "connection reset")
	assert.Contains(t, logs.String(), "connection reset", "the failure goes to the log line")
	assert.Contains(t, logs.String(), "status=500")
}

// failingAfterFirstWrite is a ResponseWriter whose first Write succeeds and
// every later one fails, like a caller's connection dropping mid-body.
type failingAfterFirstWrite struct {
	http.ResponseWriter
	writes int
}

func (w *failingAfterFirstWrite) Write(b []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errors.New("write: connection reset by peer")
	}
	return w.ResponseWriter.Write(b)
}

func TestCardinalityEstimateHandler_Status(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
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
		"a store-gateway refusing on max_index_bytes is 422": {
			blocks: oneDay, clients: gatewayFailing(cardinality.Refusalf(cardinality.LimitMaxIndexBytes, "the request would read about 5000 index bytes, more than max_index_bytes 1000")),
			query: `start=0&end=86400&match[]={job="x"}&max_index_bytes=1000`, status: http.StatusUnprocessableEntity, body: "max_index_bytes",
			limit: refusedByMaxIndexBytes,
		},
		"a store-gateway refusing on max_groups is 422": {
			blocks: oneDay, clients: gatewayFailing(cardinality.Refusalf(cardinality.LimitMaxGroups, "the blocks hold 5 distinct values of pod, more than max_groups 2")),
			query: `start=0&end=86400&match[]={job="x"}`, status: http.StatusUnprocessableEntity, body: "max_groups 2",
			limit: refusedByMaxGroups,
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
	} {
		t.Run(name, func(t *testing.T) {
			q := seriesCountsQueryable(0, 0, tc.blocks, tc.clients)
			q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(tc.blocks, &bucketindex.Metadata{}, nil)
			var logs bytes.Buffer
			q.logger = log.NewLogfmtLogger(&logs)
			q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
			rec := estimateRequest(t, q, tc.cfg, false, context.Background(), tc.query)
			assert.Equal(t, tc.status, rec.Code, rec.Body.String())
			// Every error is a Prometheus API error body, whatever its status.
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			var errBody map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody), rec.Body.String())
			assert.Equal(t, "error", errBody["status"])
			assert.Equal(t, map[int]string{http.StatusBadRequest: "bad_data", http.StatusUnprocessableEntity: "execution", http.StatusInternalServerError: "internal"}[tc.status], errBody["errorType"])
			assert.Contains(t, errBody["error"], tc.body)
			assert.NotContains(t, rec.Body.String(), "1.1.1.1", "an error names the kind of component, not its address")
			refusals := 0.0
			for _, limit := range []string{refusedByMaxHashes, refusedByMaxIndexBytes, refusedByMemory, refusedByMaxGroups} {
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

// The ring's error for a store-gateway client lookup names the instance and
// its address, which stay in the log line and out of the caller's text.
func TestCardinalityEstimateHandler_ClientLookupErrorRedacted(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}, &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		errors.New("failed to get store-gateway client for store-gateway-zone-a-1 10.0.0.1:9095: connection refused"),
	}}}}}
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "the store-gateway client lookup failed")
	assert.NotContains(t, rec.Body.String(), "10.0.0.1")
	assert.NotContains(t, rec.Body.String(), "store-gateway-zone-a-1")
	assert.Contains(t, logs.String(), "10.0.0.1:9095")
}

// A block no store-gateway counted leaves the last store-gateway failure on
// the request's log line, with its address, while the caller's answer only
// lists the block.
func TestCardinalityEstimateHandler_MissingBlocksReasonLogged(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
	down := &storeGatewayClientMock{remoteAddr: "10.0.0.1:9095", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		return nil, status.Error(codes.Unavailable, "dial tcp 10.0.0.1:9095: connection refused")
	}}
	up := gatewayAnswering("2.2.2.2", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{down: {{b1}}, up: {{b2}}},
		errors.New("no store-gateway left"),
	}}}}}
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.True(t, out.LowerBound)
	assert.Equal(t, []string{b1.String()}, out.MissingBlocks)
	assert.NotContains(t, rec.Body.String(), "10.0.0.1")
	line := logs.String()
	assert.Contains(t, line, "missing_blocks=1")
	assert.Contains(t, line, "missing_reason=")
	assert.Contains(t, line, "connection refused")
	assert.Contains(t, line, "missing_store_gateway=10.0.0.1:9095")
}

// Blocks a store-gateway answered without, as for an index-header that didn't load, are logged by ID once no store-gateway is left to try.
func TestCardinalityEstimateHandler_UncountedBlocksLogged(t *testing.T) {
	b1, b2 := ulid.MustNew(1, nil), ulid.MustNew(2, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs, CompactorShardID: "1_of_2"}, {ID: b2, MinTime: 0, MaxTime: dayMs, CompactorShardID: "2_of_2"}}
	gateway := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{BlockIds: []string{b2.String()}, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(blocks, &bucketindex.Metadata{}, nil)
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
		map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1, b2}}},
	}}}}}
	var logs bytes.Buffer
	q.logger = log.NewLogfmtLogger(&logs)
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"lower_bound":true`)
	assert.Contains(t, logs.String(), `msg="cardinality estimate: blocks left uncounted after the last attempt" attempts=1 blocks=`+b1.String())
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
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{
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
		// 100 hashes at the 32 bytes each is budgeted.
		limits := estimateMemoryLimits{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), estimate: 100 * cardinality.HashBudgetBytes}
		h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, nil, limits), CardinalityEstimateConfig{}, false)
		r := httptest.NewRequest(http.MethodGet, `/prometheus/api/v1/cardinality/estimate?start=0&end=172800&match[]={job="x"}&max_index_bytes=100000`, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r.WithContext(user.InjectOrgID(r.Context(), "user-1")))
		return q, rec, logs.String()
	}

	t.Run("each call gets a share, and the held hashes are recorded", func(t *testing.T) {
		var got1, got2 *storegatewaypb.SeriesCountsRequest
		q, rec, line := run(t, map[BlocksStoreClient][][]ulid.ULID{
			gatewayAnswering("1.1.1.1", &got1, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2, 3}}}, SeriesCounted: 3}): {{b1}},
			gatewayAnswering("2.2.2.2", &got2, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{3, 4}}}, SeriesCounted: 2}):    {{b2}},
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
			return nil, cardinality.MaxHashesRefusal(70, 50)
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
	h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, ing, estimateDisabled{ingestersWithinLimit(13 * time.Hour)}), CardinalityEstimateConfig{}, false)
	r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?start=0&end=86400", nil)
	r = r.WithContext(user.InjectOrgID(context.Background(), "user-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.JSONEq(t, `{"status":"error","errorType":"bad_data","error":"cardinality estimate is disabled for the tenant: user-1"}`, rec.Body.String())
	finder.AssertNotCalled(t, "GetBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	assert.Nil(t, ing.gotSeriesCounts, "no ingester may be asked for a disabled tenant")
}

// Every request is counted by path, source and status, and timed whatever
// its outcome; an answer counts once per flag it carries, and its retries
// add up.
func TestObserveEstimate_RequestsFlagsAndRetries(t *testing.T) {
	q := &BlocksStoreQueryable{metrics: newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())}
	ctx := context.Background()
	req := CardinalityEstimateRequest{MinT: 0, MaxT: dayMs}
	answered := CardinalityEstimateResult{Read: ReadFullIndex, Source: SourceBlocks, Retries: 2}
	flagged := CardinalityEstimateResult{Read: ReadFullIndex, Source: SourceBlocks, Retries: 1, LowerBound: true, CoverageGap: true, MissingBlocks: []ulid.ULID{ulid.MustNew(1, nil)}, MissingShards: []missingShard{{Shard: "2_of_2", MaxTime: dayMs}}}

	q.observeEstimate(ctx, req, answered, &estimateResponse{}, time.Second, nil)
	q.observeEstimate(ctx, req, flagged, &estimateResponse{}, time.Second, nil)
	q.observeEstimate(ctx, req, CardinalityEstimateResult{Source: SourceBlocks}, nil, time.Second, refusedByLimit(refusedByMaxIndexBytes, "over max_index_bytes"))
	q.observeEstimate(ctx, req, CardinalityEstimateResult{Source: SourceHead}, nil, time.Second, badEstimateRequest("bad window"))
	q.observeEstimate(ctx, req, CardinalityEstimateResult{}, nil, time.Second, errors.New("store-gateway down"))
	q.observeRefusedRequest(time.Millisecond, badEstimateRequest("bad parameters"))

	for _, flag := range []string{"lower_bound", "coverage_gap", "missing_blocks", "missing_shards"} {
		assert.Equal(t, 1.0, testutil.ToFloat64(q.metrics.estimateFlagged.WithLabelValues(ReadFullIndex, flag)), flag)
	}
	assert.Equal(t, 3.0, testutil.ToFloat64(q.metrics.estimateRetries))

	durations := func(path, source, status string) uint64 {
		m := &dto.Metric{}
		require.NoError(t, q.metrics.estimateDuration.WithLabelValues(path, source, status).(prometheus.Metric).Write(m))
		return m.GetHistogram().GetSampleCount()
	}
	assert.Equal(t, uint64(2), durations(ReadFullIndex, SourceBlocks, "ok"))
	assert.Equal(t, uint64(1), durations("unknown", SourceBlocks, "refused"), "a failed estimate says which sources it asked")
	assert.Equal(t, uint64(1), durations("unknown", SourceHead, "refused"))
	assert.Equal(t, uint64(1), durations("none", "none", "refused"), "refused before any estimate")
	assert.Equal(t, uint64(1), durations("unknown", "unknown", "failed"), "a failure is timed too")
	m := &dto.Metric{}
	require.NoError(t, q.metrics.estimateDuration.WithLabelValues(ReadFullIndex, SourceBlocks, "ok").(prometheus.Metric).Write(m))
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
	failed := &dto.Metric{}
	require.NoError(t, q.metrics.estimateDuration.WithLabelValues("unknown", SourceBlocks, "failed").(prometheus.Metric).Write(failed))
	assert.Equal(t, uint64(1), failed.GetHistogram().GetSampleCount())
}

// When every source asked is at its limit of concurrent counts, the answer
// is a 503 as the Prometheus API answers an unavailable query, so the client
// retries.
func TestCardinalityEstimateHandler_BusySources(t *testing.T) {
	assertBusy := func(t *testing.T, rec *httptest.ResponseRecorder, msg string) {
		t.Helper()
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		var body map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "error", body["status"])
		assert.Equal(t, "unavailable", body["errorType"])
		assert.Contains(t, body["error"], msg)
		assert.NotContains(t, body["error"], "1.1.1.1")
	}

	t.Run("store-gateways", func(t *testing.T) {
		b1 := ulid.MustNew(1, nil)
		blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
		busy := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(*storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
			return nil, cardinality.Busyf("too many cardinality estimate count requests running: timed out waiting for a slot")
		}}
		q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{busy: {{b1}}})
		q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
		rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
		assertBusy(t, rec, "store-gateway: too many cardinality estimate count requests running")
	})

	t.Run("ingesters", func(t *testing.T) {
		q := seriesCountsQueryable(0, 0, nil, nil)
		q.compartments[0].finder.(*blocksFinderMock).On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
		q.metrics = newBlocksStoreQueryableMetrics(compartments.Config{}, prometheus.NewPedanticRegistry())
		// As the distributor passes on the refusal of the last ingester it asked.
		ing := &ingestersMock{seriesCountsErr: fmt.Errorf("partition 1: %w", cardinality.Busyf("too many cardinality estimate count walks running, more than -ingester.cardinality-estimate-max-concurrent 8"))}
		h := NewCardinalityEstimateHandler(NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour)), CardinalityEstimateConfig{}, false)
		now := time.Now().Unix()
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/prometheus/api/v1/cardinality/estimate?start=%d&end=%d", now-600, now), nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r.WithContext(user.InjectOrgID(context.Background(), "user-1")))
		assertBusy(t, rec, "ingesters: too many cardinality estimate count walks running, more than -ingester.cardinality-estimate-max-concurrent 8")
		require.NotNil(t, ing.gotSeriesCounts, "the ingesters were asked")
	})
}

func TestCardinalityEstimateHandler_NoTimeoutWhenUnset(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{
		gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{1}}}}): {{b1}},
	})
	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), `start=0&end=86400&match[]={job="x"}`)
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

// The route has no shards parameter: a request that sends one is answered
// with one call per store-gateway, with the matchers it asked for.
func TestCardinalityEstimateHandler_IgnoresShardsParameter(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	var (
		calls    atomic.Int32
		matchers []cardinalitypb.LabelMatcher
	)
	gateway := &storeGatewayClientMock{remoteAddr: "1.1.1.1", mockedSeriesCounts: func(req *storegatewaypb.SeriesCountsRequest) (*storegatewaypb.SeriesCountsResponse, error) {
		calls.Add(1)
		matchers = req.Matchers
		return &storegatewaypb.SeriesCountsResponse{BlockIds: req.BlockIds, Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}}, nil
	}}
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})

	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), fmt.Sprintf("start=0&end=%d&match[]={job=\"a\"}&shards=4", dayMs/1000))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, int32(1), calls.Load(), "one call per store-gateway")
	require.Len(t, matchers, 1)
	assert.Equal(t, "job", matchers[0].Name)
}

// The route has no compare parameter: a request that sends one is answered
// like any other, and the body carries no compare field.
func TestCardinalityEstimateHandler_IgnoresCompareParameter(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})

	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), fmt.Sprintf("start=0&end=%d&compare=true", dayMs/1000))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.NotContains(t, body, "compare")
}

// total sums every group's count, including the groups min_count left out.
func TestCardinalityEstimateHandler_Total(t *testing.T) {
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 0, MaxTime: dayMs}}
	gateway := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{
		{Value: "a", Counts: []int64{3}},
		{Value: "b", Counts: []int64{2}},
	}})
	q := seriesCountsQueryable(0, dayMs, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})

	rec := estimateRequest(t, q, CardinalityEstimateConfig{}, false, context.Background(), fmt.Sprintf("start=0&end=%d&min_count=3", dayMs/1000))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out estimateResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, int64(5), out.Total)
	assert.Equal(t, 2, out.GroupCount)
	assert.Equal(t, []estimateGroup{{Value: "a", Count: 3}}, out.Groups, "b is below min_count")
}

// zeroLabelNameLength is ingestersWithinLimit with no label name length set.
type zeroLabelNameLength struct{ ingestersWithinLimit }

func (zeroLabelNameLength) MaxLabelNameLength(string) int { return 0 }

// lookbackLimits is ingestersWithinLimit with a max query lookback and a blocks retention period.
type lookbackLimits struct {
	ingestersWithinLimit
	lookback, retention time.Duration
}

func (l lookbackLimits) MaxQueryLookback(string) time.Duration { return l.lookback }

func (l lookbackLimits) CompactorBlocksRetentionPeriod(string) time.Duration { return l.retention }

func TestCardinalityEstimator_ClampToLookback(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	for name, tc := range map[string]struct {
		limits   lookbackLimits
		req      CardinalityEstimateRequest
		wantMinT int64
		wantLeft bool
	}{
		"no limit keeps the window":                        {req: CardinalityEstimateRequest{MinT: 0, MaxT: 100 * hour}, wantMinT: 0, wantLeft: true},
		"a window inside the lookback is kept":             {limits: lookbackLimits{lookback: 10 * time.Hour}, req: CardinalityEstimateRequest{MinT: 95 * hour, MaxT: 100 * hour}, wantMinT: 95 * hour, wantLeft: true},
		"the start moves up to the lookback":               {limits: lookbackLimits{lookback: 10 * time.Hour}, req: CardinalityEstimateRequest{MinT: 0, MaxT: 100 * hour}, wantMinT: 90 * hour, wantLeft: true},
		"the start moves up to the retention":              {limits: lookbackLimits{retention: 20 * time.Hour}, req: CardinalityEstimateRequest{MinT: 0, MaxT: 100 * hour}, wantMinT: 80 * hour, wantLeft: true},
		"the shorter of the two applies":                   {limits: lookbackLimits{lookback: 10 * time.Hour, retention: 20 * time.Hour}, req: CardinalityEstimateRequest{MinT: 0, MaxT: 100 * hour}, wantMinT: 90 * hour, wantLeft: true},
		"a window that ends at the lookback is empty":      {limits: lookbackLimits{lookback: 10 * time.Hour}, req: CardinalityEstimateRequest{MinT: 0, MaxT: 90 * hour}, wantMinT: 0},
		"a window that ends before the retention is empty": {limits: lookbackLimits{retention: 10 * time.Hour}, req: CardinalityEstimateRequest{MinT: 0, MaxT: 50 * hour}, wantMinT: 0},
	} {
		t.Run(name, func(t *testing.T) {
			e := NewCardinalityEstimator(&BlocksStoreQueryable{}, nil, tc.limits)
			e.now = func() time.Time { return now }
			req := tc.req
			left := e.clampToLookback(spanlogger.FromContext(context.Background(), log.NewNopLogger()), "user-1", &req)
			assert.Equal(t, tc.wantLeft, left)
			assert.Equal(t, tc.wantMinT, req.MinT)
			assert.Equal(t, tc.req.MaxT, req.MaxT)
		})
	}
}

func TestCardinalityEstimateHandler_MaxQueryLookback(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	blocks := bucketindex.Blocks{{ID: b1, MinTime: 48 * hour, MaxTime: 72 * hour}}
	limits := lookbackLimits{ingestersWithinLimit: ingestersWithinLimit(13 * time.Hour), lookback: 50 * time.Hour}
	serve := func(q *BlocksStoreQueryable, query string) (estimateResponse, *httptest.ResponseRecorder) {
		q.logger = log.NewNopLogger()
		e := NewCardinalityEstimator(q, nil, limits)
		e.now = func() time.Time { return now }
		r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?"+query, nil)
		r = r.WithContext(user.InjectOrgID(r.Context(), "user-1"))
		rec := httptest.NewRecorder()
		NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, false).ServeHTTP(rec, r)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		var out estimateResponse
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return out, rec
	}

	// The blocks are asked for the window from the lookback on; the finder mock answers no other.
	gateway := gatewayAnswering("1.1.1.1", nil, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Counts: []int64{2}}}})
	q := seriesCountsQueryable(50*hour, 72*hour, blocks, map[BlocksStoreClient][][]ulid.ULID{gateway: {{b1}}})
	out, _ := serve(q, fmt.Sprintf(`start=0&end=%d&match[]={job="x"}`, 72*3600))
	assert.Equal(t, estimateWindow{Start: 50 * hour, End: 72 * hour}, out.Window)
	assert.Equal(t, []estimateGroup{{Value: "up", Count: 2}}, out.Groups)

	// A window that ends before the lookback reads no source.
	out, _ = serve(&BlocksStoreQueryable{}, fmt.Sprintf("start=0&end=%d", 48*3600))
	assert.Equal(t, estimateWindow{Start: 0, End: 48 * hour}, out.Window)
	assert.Empty(t, out.Groups)
	assert.Zero(t, out.Total)
	assert.False(t, out.LowerBound)
}

// The log line and span of an estimate carry the most the querier's memory
// tracker held for it, on an answer and on a memory refusal alike.
func TestCardinalityEstimateHandler_PeakMemory(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	hashes := make([]uint64, 100)
	for i := range hashes {
		hashes[i] = uint64(i + 1)
	}
	finder := &blocksFinderMock{}
	finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(bucketindex.Blocks(nil), &bucketindex.Metadata{}, nil)
	// The window is after query-store-after, so the ingesters alone answer it.
	q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder}}, queryStoreAfter: 12 * time.Hour}
	ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
		{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: hashes}}},
	}}}
	// The merger charges the group as a hash set, then its 100 distinct hashes.
	groupCharge := uint64(cardinality.GroupBytes + len("up"))
	allCharges := groupCharge + 100*estimateHashBytes

	for name, tc := range map[string]struct {
		limit    uint64
		wantCode int
		wantPeak uint64
	}{
		"answered":                        {limit: 1 << 20, wantCode: http.StatusOK, wantPeak: allCharges},
		"refused by the memory limit":     {limit: allCharges - 1, wantCode: http.StatusUnprocessableEntity, wantPeak: groupCharge},
		"answered without a memory limit": {limit: 0, wantCode: http.StatusOK, wantPeak: allCharges},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			q.logger = log.NewLogfmtLogger(&logs)
			spans := tracetest.NewSpanRecorder()
			ctx, span := tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(spans)).Tracer("test").Start(context.Background(), "estimate")

			e := NewCardinalityEstimator(q, ing, memoryLimit{ingestersWithinLimit(13 * time.Hour), tc.limit})
			e.now = func() time.Time { return now }
			r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?start=356400&end=360000", nil)
			rec := httptest.NewRecorder()
			NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, false).ServeHTTP(rec, r.WithContext(user.InjectOrgID(ctx, "user-1")))
			span.End()
			require.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			assert.Contains(t, logs.String(), fmt.Sprintf(" peak_memory_bytes=%d ", tc.wantPeak))
			require.Len(t, spans.Ended(), 1)
			var got []int64
			for _, a := range spans.Ended()[0].Attributes() {
				if a.Key == "peak_memory_bytes" {
					got = append(got, a.Value.AsInt64())
				}
			}
			assert.Equal(t, []int64{int64(tc.wantPeak)}, got)
		})
	}
}

// The access policy's selector reaches the ingesters, both in the matchers
// the distributor picks them with and in the request each one filters its
// series with, whether the ingesters answer alone or with the store-gateways.
func TestCardinalityEstimateHandler_LabelBasedAccessControlReachesTheIngesters(t *testing.T) {
	const hour = int64(time.Hour / time.Millisecond)
	now := time.UnixMilli(100 * hour)
	b1 := ulid.MustNew(1, nil)
	ns := labels.MustNewMatcher(labels.MatchEqual, "namespace", "a")
	ctx := labelaccess.InjectLabelMatchersContext(context.Background(), labelaccess.LabelPolicySet{"user-1": {{Selector: []*labels.Matcher{ns}}}})

	for name, tc := range map[string]struct {
		// start and end are the window in seconds.
		start, end int64
		blocks     bucketindex.Blocks
		wantSource string
	}{
		"ingesters alone": {start: 99 * 3600, end: 100 * 3600, wantSource: SourceHead},
		"store-gateways and ingesters": {
			start: 87 * 3600, end: 100 * 3600, wantSource: SourceBlocksAndHead,
			blocks: bucketindex.Blocks{{ID: b1, MinTime: 87 * hour, MaxTime: 89 * hour}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var gotGateway *storegatewaypb.SeriesCountsRequest
			gw := gatewayAnswering("1.1.1.1", &gotGateway, storegatewaypb.SeriesCountsResponse{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1}}}})
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "user-1", mock.Anything, mock.Anything).Return(tc.blocks, &bucketindex.Metadata{}, nil)
			q := &BlocksStoreQueryable{compartments: []blocksStoreCompartment{{finder: finder, stores: &blocksStoreSetMock{mockedResponses: []interface{}{
				map[BlocksStoreClient][][]ulid.ULID{gw: {{b1}}},
			}}}}, queryStoreAfter: 12 * time.Hour}
			ing := &ingestersMock{seriesCounts: distributor.SeriesCountsResult{Hashed: true, Responses: []*client.SeriesCountsResponse{
				{Groups: []*cardinalitypb.SeriesCountGroup{{Value: "up", Hashes: []uint64{1, 2}}}, MinTimeMs: 80 * hour},
			}}}
			e := NewCardinalityEstimator(q, ing, ingestersWithinLimit(13*time.Hour))
			e.now = func() time.Time { return now }
			query := fmt.Sprintf(`start=%d&end=%d&match[]={job="x"}`, tc.start, tc.end)
			r := httptest.NewRequest(http.MethodGet, "/prometheus/api/v1/cardinality/estimate?"+query, nil)
			rec := httptest.NewRecorder()
			NewCardinalityEstimateHandler(e, CardinalityEstimateConfig{}, true).ServeHTTP(rec, r.WithContext(user.InjectOrgID(ctx, "user-1")))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var out estimateResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantSource, out.Source)

			job := labels.MustNewMatcher(labels.MatchEqual, "job", "x")
			require.NotNil(t, ing.gotSeriesCounts)
			assert.Equal(t, []*labels.Matcher{job, ns}, ing.gotMatchers, "the distributor picks the ingesters with the selector")
			assert.Equal(t, cardinalitypb.ToLabelMatchers([]*labels.Matcher{job, ns}), ing.gotSeriesCounts.Matchers, "each ingester filters its series with the selector")
			if tc.wantSource == SourceBlocksAndHead {
				require.NotNil(t, gotGateway)
				assert.Equal(t, cardinalitypb.ToLabelMatchers([]*labels.Matcher{job, ns}), gotGateway.Matchers)
			}
		})
	}
}
