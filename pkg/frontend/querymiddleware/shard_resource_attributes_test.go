// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

func Test_shardResourceAttributesMiddleware_RoundTrip(t *testing.T) {
	const tenantID = "test"
	const tenantShardCount = 4
	const tenantMaxShardCount = 128

	validReq := func() *http.Request {
		r := httptest.NewRequest("GET", "/api/v1/resources?match[]={job=%22test%22}&start=1688515200000&end=1688540400000", nil)
		r.Header.Set("X-Scope-OrgID", tenantID)
		return r
	}

	seriesReq := func() *http.Request {
		r := httptest.NewRequest("GET", "/api/v1/resources/series?resource.attr=service.name:test&start=1688515200000&end=1688540400000", nil)
		r.Header.Set("X-Scope-OrgID", tenantID)
		return r
	}

	makeResponse := func(series ...string) string {
		items := make([]string, len(series))
		for i, s := range series {
			items[i] = fmt.Sprintf(`{"labels":{%s},"versions":[]}`, s)
		}
		return fmt.Sprintf(`{"status":"success","data":{"series":[%s]}}`, strings.Join(items, ","))
	}

	tests := []struct {
		name             string
		request          func() *http.Request
		shardCount       int
		maxShardCount    int
		upstreamFunc     func(req *http.Request) (*http.Response, error)
		expectedShards   int
		expectPassThru   bool
		expectErr        string
		expectedSeries   int
		expectMergedVers int // if >0, verify the first series has this many versions
	}{
		{
			name:           "passes through /api/v1/resources/series",
			request:        seriesReq,
			shardCount:     tenantShardCount,
			maxShardCount:  tenantMaxShardCount,
			expectPassThru: true,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
		},
		{
			name:           "passes through when shard count < 2",
			request:        validReq,
			shardCount:     1,
			maxShardCount:  tenantMaxShardCount,
			expectPassThru: true,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
		},
		{
			name:          "returns error when shard count exceeds max",
			request:       validReq,
			shardCount:    200,
			maxShardCount: 100,
			expectErr:     "shard count 200 exceeds allowed maximum (100)",
		},
		{
			name:          "shards with 2 shards and merges results",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				query := req.URL.Query()
				matchers := query["match[]"]

				// Verify the shard matcher is AND'd into the existing match[] entry,
				// not added as a separate OR'd entry. There should be exactly 1
				// match[] entry containing BOTH the user matcher and the shard matcher.
				if len(matchers) != 1 {
					return nil, fmt.Errorf("expected exactly 1 match[] entry per shard request, got %d: %v", len(matchers), matchers)
				}
				m := matchers[0]
				if !strings.Contains(m, "job") {
					return nil, fmt.Errorf("match[] entry should contain user matcher 'job', got: %s", m)
				}
				if !strings.Contains(m, "__query_shard__") {
					return nil, fmt.Errorf("match[] entry should contain shard matcher '__query_shard__', got: %s", m)
				}

				// Return different series per shard.
				if strings.Contains(m, "1_of_2") {
					return newJSONResponse(makeResponse(`"job":"shard1"`)), nil
				}
				if strings.Contains(m, "2_of_2") {
					return newJSONResponse(makeResponse(`"job":"shard2"`)), nil
				}
				return newJSONResponse(makeResponse()), nil
			},
			expectedSeries: 2,
		},
		{
			name:          "upstream error propagated",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return nil, fmt.Errorf("upstream error")
			},
			expectErr: "upstream error",
		},
		{
			name: "limit re-applied after merge",
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/resources?match[]={job=%22test%22}&start=1688515200000&end=1688540400000&limit=1", nil)
				r.Header.Set("X-Scope-OrgID", tenantID)
				return r
			},
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"a"`, `"job":"b"`)), nil
			},
			expectedSeries: 1,
		},
		{
			name:          "empty responses merge to empty",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse()), nil
			},
			expectedSeries: 0,
		},
		{
			name: "empty selector shards correctly",
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/resources?match[]={}&start=1688515200000&end=1688540400000", nil)
				r.Header.Set("X-Scope-OrgID", tenantID)
				return r
			},
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				query := req.URL.Query()
				matchers := query["match[]"]
				if len(matchers) != 1 {
					return nil, fmt.Errorf("expected exactly 1 match[] entry, got %d: %v", len(matchers), matchers)
				}
				if !strings.Contains(matchers[0], "__query_shard__") {
					return nil, fmt.Errorf("match[] entry should contain shard matcher, got: %s", matchers[0])
				}
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
			expectedSeries: 1,
		},
		{
			name:          "duplicate series across shards merges versions",
			request:       validReq,
			shardCount:    2,
			maxShardCount: tenantMaxShardCount,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				// Both shards return the same series with different versions.
				query := req.URL.Query()
				matchers := query["match[]"]
				m := matchers[0]
				if strings.Contains(m, "1_of_2") {
					return newJSONResponse(`{"status":"success","data":{"series":[{"labels":{"job":"test"},"versions":[{"minTimeMs":1000,"maxTimeMs":2000}]}]}}`), nil
				}
				return newJSONResponse(`{"status":"success","data":{"series":[{"labels":{"job":"test"},"versions":[{"minTimeMs":3000,"maxTimeMs":4000}]}]}}`), nil
			},
			expectedSeries:   1,
			expectMergedVers: 2,
		},
		{
			name: "no match[] params passes through",
			request: func() *http.Request {
				r := httptest.NewRequest("GET", "/api/v1/resources?start=1688515200000&end=1688540400000", nil)
				r.Header.Set("X-Scope-OrgID", tenantID)
				return r
			},
			shardCount:     2,
			maxShardCount:  tenantMaxShardCount,
			expectPassThru: true,
			upstreamFunc: func(req *http.Request) (*http.Response, error) {
				return newJSONResponse(makeResponse(`"job":"test"`)), nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := mockLimits{
				totalShards:       tc.shardCount,
				maxShardedQueries: tc.maxShardCount,
			}

			var upstreamCallCount atomic.Int32
			upstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				upstreamCallCount.Add(1)
				if tc.upstreamFunc != nil {
					return tc.upstreamFunc(req)
				}
				return newJSONResponse(`{"status":"success","data":{"series":[]}}`), nil
			})

			mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

			req := tc.request()
			ctx := user.InjectOrgID(req.Context(), tenantID)
			req = req.WithContext(ctx)

			resp, err := mw.RoundTrip(req)

			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)

			if tc.expectPassThru {
				require.Equal(t, 1, int(upstreamCallCount.Load()), "expected single upstream call for pass-through")
				return
			}

			// Verify sharded requests were made.
			require.Equal(t, tc.shardCount, int(upstreamCallCount.Load()), "expected %d sharded requests", tc.shardCount)

			// Parse response and check series count.
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			_ = resp.Body.Close()

			var parsed struct {
				Status string `json:"status"`
				Data   struct {
					Series []stdjson.RawMessage `json:"series"`
				} `json:"data"`
			}
			require.NoError(t, stdjson.Unmarshal(body, &parsed))
			assert.Equal(t, "success", parsed.Status)
			assert.Equal(t, tc.expectedSeries, len(parsed.Data.Series))

			if tc.expectMergedVers > 0 && len(parsed.Data.Series) > 0 {
				var firstSeries struct {
					Versions []stdjson.RawMessage `json:"versions"`
				}
				require.NoError(t, stdjson.Unmarshal(parsed.Data.Series[0], &firstSeries))
				assert.Equal(t, tc.expectMergedVers, len(firstSeries.Versions), "expected merged versions count")
			}
		})
	}
}

func Test_shardResourceAttributesMiddleware_MultipleMatchEntries(t *testing.T) {
	// Regression test: when the original request has multiple match[] entries,
	// the shard matcher must be AND'd into each one independently without
	// corrupting the shared backing array (slice mutation bug).
	const tenantID = "test"

	req := httptest.NewRequest("GET",
		"/api/v1/resources?match[]={job=%22a%22}&match[]={job=%22b%22}&start=1000&end=2000", nil)
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(req.Context(), tenantID)
	req = req.WithContext(ctx)

	var (
		requestsMu sync.Mutex
		requests   []*http.Request
	)
	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		requestsMu.Lock()
		requests = append(requests, r)
		requestsMu.Unlock()
		return newJSONResponse(`{"status":"success","data":{"series":[]}}`), nil
	})

	limits := mockLimits{totalShards: 2, maxShardedQueries: 128}
	mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

	resp, err := mw.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// We should get 2 shard requests, each with 2 match[] entries.
	require.Equal(t, 2, len(requests))

	for i, r := range requests {
		matchers := r.URL.Query()["match[]"]
		require.Equal(t, 2, len(matchers), "shard %d should have 2 match[] entries", i)

		// Each match[] entry must contain the shard matcher.
		for j, m := range matchers {
			assert.Contains(t, m, "__query_shard__",
				"shard %d, match[%d] should contain shard matcher: %s", i, j, m)
			assert.Contains(t, m, "job",
				"shard %d, match[%d] should contain original job matcher: %s", i, j, m)
		}

		// Verify the two match[] entries reference different jobs.
		assert.NotEqual(t, matchers[0], matchers[1],
			"shard %d match[] entries should differ (different original matchers)", i)
	}
}

func Test_mergeSeriesVersionsJSON_Dedup(t *testing.T) {
	// Regression test: when the same version appears in both shards,
	// it should be deduplicated in the merge.
	existing := stdjson.RawMessage(`{"labels":{"job":"test"},"versions":[{"minTimeMs":1000,"maxTimeMs":2000,"identifying":{"service.name":"svc"}}]}`)
	incoming := stdjson.RawMessage(`{"labels":{"job":"test"},"versions":[{"minTimeMs":1000,"maxTimeMs":2000,"identifying":{"service.name":"svc"}},{"minTimeMs":3000,"maxTimeMs":4000,"identifying":{"service.name":"svc"}}]}`)

	merged, err := mergeSeriesVersionsJSON(existing, incoming)
	require.NoError(t, err)

	var result struct {
		Versions []stdjson.RawMessage `json:"versions"`
	}
	require.NoError(t, stdjson.Unmarshal(merged, &result))
	assert.Equal(t, 2, len(result.Versions), "duplicate version should be deduped, unique version kept")
}

func Test_mergeSeriesVersionsJSON_SortOrder(t *testing.T) {
	// Regression test: versions must be sorted by minTimeMs after merge.
	existing := stdjson.RawMessage(`{"labels":{"job":"test"},"versions":[{"minTimeMs":5000,"maxTimeMs":6000}]}`)
	incoming := stdjson.RawMessage(`{"labels":{"job":"test"},"versions":[{"minTimeMs":1000,"maxTimeMs":2000}]}`)

	merged, err := mergeSeriesVersionsJSON(existing, incoming)
	require.NoError(t, err)

	var result struct {
		Versions []parsedVersion `json:"versions"`
	}
	require.NoError(t, stdjson.Unmarshal(merged, &result))
	require.Equal(t, 2, len(result.Versions))
	assert.True(t, result.Versions[0].MinTimeMs < result.Versions[1].MinTimeMs,
		"versions should be sorted by minTimeMs: got %d, %d", result.Versions[0].MinTimeMs, result.Versions[1].MinTimeMs)
}

func Test_matchersToSelector(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple matcher",
			input:    `{job="test"}`,
			expected: `{job="test"}`,
		},
		{
			name:     "multiple matchers",
			input:    `{job="test",instance="localhost"}`,
			expected: `{job="test",instance="localhost"}`,
		},
		{
			name:     "regex matcher",
			input:    `{job=~"test.*"}`,
			expected: `{job=~"test.*"}`,
		},
	}

	parser := promqlext.NewPromQLParser()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the input to get matchers.
			ms, err := parser.ParseMetricSelector(tc.input)
			require.NoError(t, err)

			// Serialize back and verify round-trip.
			result := matchersToSelector(ms)

			// Re-parse to verify it's valid.
			roundTripped, err := parser.ParseMetricSelector(result)
			require.NoError(t, err, "matchersToSelector output should be parseable: %s", result)
			assert.Equal(t, len(ms), len(roundTripped), "round-trip should preserve matcher count")
		})
	}
}

func Test_shardResourceAttributesMiddleware_LimitNotForwardedPerShard(t *testing.T) {
	// Regression test: the limit param must be stripped from per-shard requests
	// to avoid premature truncation, and only re-applied post-merge.
	const tenantID = "test"

	req := httptest.NewRequest("GET",
		"/api/v1/resources?match[]={job=%22test%22}&start=1000&end=2000&limit=1", nil)
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(req.Context(), tenantID)
	req = req.WithContext(ctx)

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		// Verify the limit param is NOT present in shard requests.
		assert.Empty(t, r.URL.Query().Get("limit"),
			"limit should not be forwarded to shard requests")
		return newJSONResponse(`{"status":"success","data":{"series":[{"labels":{"job":"a"},"versions":[]},{"labels":{"job":"b"},"versions":[]}]}}`), nil
	})

	limits := mockLimits{totalShards: 2, maxShardedQueries: 128}
	mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

	resp, err := mw.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()

	var parsed struct {
		Data struct {
			Series []stdjson.RawMessage `json:"series"`
		} `json:"data"`
	}
	require.NoError(t, stdjson.Unmarshal(body, &parsed))
	// The limit=1 should be applied post-merge. Each shard returns 2 series,
	// but after dedup (4 unique across 2 shards) and limit=1, we get 1.
	assert.Equal(t, 1, len(parsed.Data.Series), "limit should be applied post-merge")
}

func Test_shardResourceAttributesMiddleware_InvalidLimit(t *testing.T) {
	const tenantID = "test"

	req := httptest.NewRequest("GET",
		"/api/v1/resources?match[]={job=%22test%22}&start=1000&end=2000&limit=abc", nil)
	req.Header.Set("X-Scope-OrgID", tenantID)
	ctx := user.InjectOrgID(req.Context(), tenantID)
	req = req.WithContext(ctx)

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		return newJSONResponse(`{"status":"success","data":{"series":[{"labels":{"job":"a"},"versions":[]},{"labels":{"job":"b"},"versions":[]}]}}`), nil
	})

	limits := mockLimits{totalShards: 2, maxShardedQueries: 128}
	mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

	_, err := mw.RoundTrip(req)
	require.Error(t, err, "invalid limit should return an error")
	require.Contains(t, err.Error(), "invalid limit parameter")
}

func Test_mergeSeriesVersionsJSON_UnparseableExistingVersion(t *testing.T) {
	// Bug: When an existing version is unparseable (e.g., wrong field types),
	// it is not added to the `seen` set. If the incoming side has a parseable
	// version with the same dedup key, it would be added, creating a
	// potential duplicate.
	//
	// This test verifies the behavior with an unparseable existing version.
	// The unparseable version should be kept (not lost), and parseable
	// incoming versions should still be added correctly.

	// Existing series has one unparseable version (minTimeMs as string instead of number)
	// and one parseable version.
	existing := stdjson.RawMessage(`{
		"labels":{"job":"test"},
		"versions":[
			{"minTimeMs":"not_a_number","maxTimeMs":2000,"identifying":{"k":"v"}},
			{"minTimeMs":3000,"maxTimeMs":4000,"identifying":{"k":"v2"}}
		]
	}`)

	// Incoming has a parseable version that matches the parseable existing one (should be deduped)
	// and a new unique version.
	incoming := stdjson.RawMessage(`{
		"labels":{"job":"test"},
		"versions":[
			{"minTimeMs":3000,"maxTimeMs":4000,"identifying":{"k":"v2"}},
			{"minTimeMs":5000,"maxTimeMs":6000,"identifying":{"k":"v3"}}
		]
	}`)

	merged, err := mergeSeriesVersionsJSON(existing, incoming)
	require.NoError(t, err)

	var result struct {
		Versions []stdjson.RawMessage `json:"versions"`
	}
	require.NoError(t, stdjson.Unmarshal(merged, &result))

	// The unparseable version is kept.
	// The duplicate parseable version (3000-4000) should be deduped.
	// The new version (5000-6000) should be added.
	// Total: 3 versions (unparseable + parseable existing + new).
	assert.Equal(t, 3, len(result.Versions),
		"unparseable existing version should be kept, duplicate deduped, new version added")
}

func Test_shardResourceAttributesMiddleware_AcceptEncodingNotForwarded(t *testing.T) {
	// Regression test: the original request's Accept-Encoding header must not
	// be forwarded to shard sub-requests, otherwise the querier's gzip handler
	// compresses the response and mergeResourceAttributesResponses fails to
	// JSON-unmarshal the raw gzip bytes.
	const tenantID = "test"

	req := httptest.NewRequest("GET",
		"/api/v1/resources?match[]={job=%22test%22}&start=1000&end=2000", nil)
	req.Header.Set("X-Scope-OrgID", tenantID)
	req.Header.Set("Accept-Encoding", "gzip")
	ctx := user.InjectOrgID(req.Context(), tenantID)
	req = req.WithContext(ctx)

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		assert.Empty(t, r.Header.Get("Accept-Encoding"),
			"Accept-Encoding must not be forwarded to shard sub-requests")
		return newJSONResponse(`{"status":"success","data":{"series":[{"labels":{"job":"test"},"versions":[]}]}}`), nil
	})

	limits := mockLimits{totalShards: 2, maxShardedQueries: 128}
	mw := newShardResourceAttributesMiddleware(upstream, limits, log.NewNopLogger())

	resp, err := mw.RoundTrip(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func newJSONResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
