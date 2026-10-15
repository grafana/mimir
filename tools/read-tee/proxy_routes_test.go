// SPDX-License-Identifier: AGPL-3.0-only

package readtee

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	prom_remote "github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/require"
)

// recordedRequest is a request received by the test backend.
type recordedRequest struct {
	query url.Values
	body  []byte
}

// runAmplifiedRequest serves req through an endpoint for route with the given amplification factor
// and replica label, and returns every request the backend received, original included.
func runAmplifiedRequest(t *testing.T, route Route, factor float64, ampReplicaLabel string, req *http.Request) []recordedRequest {
	t.Helper()
	logger := log.NewNopLogger()
	metrics := NewProxyMetrics(prometheus.NewRegistry())

	var mu sync.Mutex
	var got []recordedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		got = append(got, recordedRequest{query: r.URL.Query(), body: body})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	backend := NewHTTPProxyBackend("backend1", mustParseURL(t, server.URL), 5*time.Second, false, 1000)
	asyncDispatcher := NewAsyncBackendDispatcher(1000, metrics, logger)
	opts := rewriteOptions{ampReplicaLabel: ampReplicaLabel}
	endpoint := NewProxyEndpoint(backend, route, metrics, logger, factor, 0, opts, ampReplicaLabel != "", amplifyAllReplicasConfig{}, 0.0, asyncDispatcher)

	rec := httptest.NewRecorder()
	endpoint.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	asyncDispatcher.Stop()
	asyncDispatcher.Await()

	mu.Lock()
	defer mu.Unlock()
	return append([]recordedRequest(nil), got...)
}

func TestProxyEndpoint_AmplifiesSelectorParam(t *testing.T) {
	route := Route{Path: "/api/v1/cardinality/active_series", RouteName: "api_v1_cardinality_active_series", Methods: []string{"GET"}}
	req := httptest.NewRequest("GET", "/api/v1/cardinality/active_series?selector="+url.QueryEscape(`up{job="api"}`)+"&limit=10", nil)

	got := runAmplifiedRequest(t, route, 3, "", req)

	var selectors []string
	for _, r := range got {
		require.Equal(t, "10", r.query.Get("limit"))
		selectors = append(selectors, r.query.Get("selector"))
	}
	sort.Strings(selectors)
	require.Equal(t, []string{`up{job="api"}`, `up{job="api_amp1"}`, `up{job="api_amp2"}`}, selectors)
}

func TestProxyEndpoint_AmplifiesRemoteRead(t *testing.T) {
	route := Route{Path: "/api/v1/read", RouteName: "api_v1_read", Methods: []string{"POST"}, RemoteRead: true}
	body := encodeRemoteReadRequest(t, [][]*labels.Matcher{
		{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchEqual, "job", "api")},
	})
	req := httptest.NewRequest("POST", "/api/v1/read", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	got := runAmplifiedRequest(t, route, 3, "__amp__", req)

	var selectors []string
	for _, r := range got {
		queries := decodeRemoteReadRequest(t, r.body)
		require.Len(t, queries, 1)
		selectors = append(selectors, labelsMatchersString(queries[0]))
	}
	sort.Strings(selectors)
	require.Equal(t, []string{
		// The original, scoped to the base series.
		`{__name__="up", job="api", __amp__=""}`,
		`{__name__="up", job="api_amp1", __amp__="1"}`,
		`{__name__="up", job="api_amp2", __amp__="2"}`,
	}, selectors)
}

func TestRewriteRemoteReadBody(t *testing.T) {
	body := encodeRemoteReadRequest(t, [][]*labels.Matcher{
		{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up"), labels.MustNewMatcher(labels.MatchRegexp, "job", "api|db")},
		{labels.MustNewMatcher(labels.MatchEqual, "__n000__", "eadp"), labels.MustNewMatcher(labels.MatchNotEqual, "env", "dev")},
	})

	rewritten, err := rewriteRemoteReadBody(body, 2, rewriteOptions{excludeAmplifiedNegative: true})
	require.NoError(t, err)

	queries := decodeRemoteReadRequest(t, rewritten)
	require.Len(t, queries, 2)
	require.Equal(t, `{__name__="up", job=~"(?:api|db)_amp2"}`, labelsMatchersString(queries[0]))
	require.Equal(t, `{__n000__="eadp_amp2", env!~"dev(?:_amp[0-9]+)?"}`, labelsMatchersString(queries[1]))

	_, err = rewriteRemoteReadBody([]byte("not snappy"), 1, rewriteOptions{})
	require.ErrorContains(t, err, "snappy")
}

func encodeRemoteReadRequest(t *testing.T, matcherSets [][]*labels.Matcher) []byte {
	t.Helper()
	req := &prompb.ReadRequest{}
	for _, ms := range matcherSets {
		pms, err := prom_remote.ToLabelMatchers(ms)
		require.NoError(t, err)
		req.Queries = append(req.Queries, &prompb.Query{StartTimestampMs: 1, EndTimestampMs: 2, Matchers: pms})
	}
	data, err := proto.Marshal(req)
	require.NoError(t, err)
	return snappy.Encode(nil, data)
}

func decodeRemoteReadRequest(t *testing.T, body []byte) [][]*labels.Matcher {
	t.Helper()
	data, err := snappy.Decode(nil, body)
	require.NoError(t, err)
	var req prompb.ReadRequest
	require.NoError(t, proto.Unmarshal(data, &req))

	var out [][]*labels.Matcher
	for _, q := range req.Queries {
		require.Equal(t, int64(1), q.StartTimestampMs)
		require.Equal(t, int64(2), q.EndTimestampMs)
		ms, err := prom_remote.FromLabelMatchers(q.Matchers)
		require.NoError(t, err)
		out = append(out, ms)
	}
	return out
}

func labelsMatchersString(ms []*labels.Matcher) string {
	var buf bytes.Buffer
	buf.WriteString("{")
	for i, m := range ms {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(m.String())
	}
	buf.WriteString("}")
	return buf.String()
}
