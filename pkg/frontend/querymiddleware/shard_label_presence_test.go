// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
)

func Test_shardLabelPresenceMiddleware_RoundTrip(t *testing.T) {
	const tenantShardCount = 2
	const tenantMaxShardCount = 128

	req := func(shardHeader int) *http.Request {
		r := httptest.NewRequest("POST", "/api/v1/cardinality/label_presence",
			strings.NewReader(`selector={__name__="metric"}&label[]=cluster&label[]=namespace&limit=10`))
		r.Header.Add("X-Scope-OrgID", "test")
		r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		if shardHeader > 0 {
			r.Header.Add(requestoptions.TotalShardsControlHeader, strconv.Itoa(shardHeader))
		}
		return r
	}

	// Per-shard responses keyed by shard index.
	shardResponses := []*cardinality.LabelPresenceResponse{
		{
			TotalSeries:     3,
			CompliantSeries: 2,
			Labels: []cardinality.LabelPresenceItem{
				{LabelName: "cluster", MissingCount: 0},
				{LabelName: "namespace", MissingCount: 1},
			},
			Examples: []labels.Labels{labels.FromStrings("__name__", "metric", "cluster", "a")},
		},
		{
			TotalSeries:     2,
			CompliantSeries: 0,
			Labels: []cardinality.LabelPresenceItem{
				{LabelName: "cluster", MissingCount: 2},
				{LabelName: "namespace", MissingCount: 1},
			},
			Examples: []labels.Labels{labels.FromStrings("__name__", "metric", "namespace", "x")},
		},
	}

	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		require.NoError(t, r.ParseForm())
		parsed, err := cardinality.DecodeLabelPresenceRequestFromValues(r.Form)
		require.NoError(t, err)
		// label[] and limit must be preserved across shards.
		require.Equal(t, []string{"cluster", "namespace"}, parsed.Labels)
		require.Equal(t, 10, parsed.Limit)

		shard, _, err := sharding.ShardFromMatchers(parsed.Matchers)
		require.NoError(t, err)
		require.NotNil(t, shard, "each sharded request must carry a shard matcher")

		body, err := json.Marshal(shardResponses[shard.ShardIndex])
		require.NoError(t, err)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})

	s := newShardLabelPresenceMiddleware(upstream, mockLimits{maxShardedQueries: tenantMaxShardCount, totalShards: tenantShardCount}, log.NewNopLogger())

	resp, err := s.RoundTrip(req(0).WithContext(user.InjectOrgID(req(0).Context(), "test")))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	bodyContent, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	var merged cardinality.LabelPresenceResponse
	require.NoError(t, json.Unmarshal(bodyContent, &merged))

	assert.Equal(t, uint64(5), merged.TotalSeries)
	assert.Equal(t, uint64(2), merged.CompliantSeries)
	assert.ElementsMatch(t, []cardinality.LabelPresenceItem{
		{LabelName: "cluster", MissingCount: 2},
		{LabelName: "namespace", MissingCount: 2},
	}, merged.Labels)
	assert.Len(t, merged.Examples, 2)
}

func Test_shardLabelPresenceMiddleware_shardingDisabled(t *testing.T) {
	var upstreamCalls int
	upstream := RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		upstreamCalls++
		require.NoError(t, r.ParseForm())
		// Selector must be unmodified (no shard matcher injected).
		assert.Equal(t, `{__name__="metric"}`, r.Form.Get("selector"))
		body, err := json.Marshal(&cardinality.LabelPresenceResponse{TotalSeries: 1})
		require.NoError(t, err)
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}, nil
	})

	// totalShards = 1 disables sharding.
	s := newShardLabelPresenceMiddleware(upstream, mockLimits{maxShardedQueries: 128, totalShards: 1}, log.NewNopLogger())

	r := httptest.NewRequest("POST", "/api/v1/cardinality/label_presence",
		strings.NewReader(`selector={__name__="metric"}&label[]=cluster`))
	r.Header.Add("X-Scope-OrgID", "test")
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := s.RoundTrip(r.WithContext(user.InjectOrgID(r.Context(), "test")))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, upstreamCalls)
}

func Test_shardLabelPresenceMiddleware_shardTooLarge(t *testing.T) {
	upstream := RoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusRequestEntityTooLarge, Body: io.NopCloser(strings.NewReader("too large"))}, nil
	})

	s := newShardLabelPresenceMiddleware(upstream, mockLimits{maxShardedQueries: 128, totalShards: 4}, log.NewNopLogger())

	r := httptest.NewRequest("POST", "/api/v1/cardinality/label_presence",
		strings.NewReader(`selector={__name__="metric"}&label[]=cluster`))
	r.Header.Add("X-Scope-OrgID", "test")
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	_, err := s.RoundTrip(r.WithContext(user.InjectOrgID(r.Context(), "test")))
	require.Error(t, err)
	apiErr := &apierror.APIError{}
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierror.TypeTooLargeEntry, apiErr.Type)
}
