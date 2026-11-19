// SPDX-License-Identifier: AGPL-3.0-only

package nativemetadataprober

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

// serve answers every request with status and body, and the Server-Timing header if any, and records the request.
func serve(t *testing.T, status int, body, serverTiming string) (*HTTPClient, *http.Request) {
	var got http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = *r.Clone(r.Context())
		if serverTiming != "" {
			w.Header().Set("Server-Timing", serverTiming)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	client, err := NewHTTPClient(server.URL+"/prometheus/", time.Second)
	require.NoError(t, err)
	return client, &got
}

func TestHTTPClient_Query(t *testing.T) {
	ts := time.UnixMilli(1_700_000_000_500)

	t.Run("an instant vector, with the query's native stats", func(t *testing.T) {
		const body = `{"status":"success","data":{"resultType":"vector","result":[
			{"metric":{"job":"api","__name__":"up","instance":"a"},"value":[1700000000.5,"1"]},
			{"metric":{"__name__":"latency","job":"api"},"histogram":[1700000000.5,{"count":"2","sum":"3","buckets":[[0,"0","1","2"]]}]}
		]}}`
		// The stats the query-frontend returns, of which the native metadata prototype's are the counts named native_.
		const serverTiming = "querier_wall_time;dur=1.5, native_lookup_response_bytes;val=1234, native_lookup_series_visited;val=5, native_target_info_fallbacks;val=1, fetched_series_count;val=9"
		client, req := serve(t, http.StatusOK, body, serverTiming)

		res, err := client.Query(t.Context(), "tenant", "info(up)", ts, ModeNative)
		require.NoError(t, err)
		require.Equal(t, "/prometheus/api/v1/query", req.URL.Path)
		require.Equal(t, url.Values{"query": {"info(up)"}, "time": {"1700000000.5"}}, req.URL.Query())
		require.Equal(t, "tenant", req.Header.Get("X-Scope-OrgID"))
		require.Equal(t, "native", req.Header.Get("X-Mimir-Native-Target-Info"))
		require.Equal(t, "true", req.Header.Get("X-Mimir-Response-Query-Stats"))
		require.Equal(t, "no-store", req.Header.Get("Cache-Control"), "a probe mustn't be answered from the results cache")

		require.Equal(t, []Sample{
			{Labels: labels.FromStrings("__name__", "up", "instance", "a", "job", "api"), Value: "1"},
			{Labels: labels.FromStrings("__name__", "latency", "job", "api"), Value: `Count: 2.000000, Sum: 3.000000, Buckets: [(0,1]:2]`},
		}, res.Samples)
		require.Equal(t, uint64(1234), res.LookupResponseBytes)
		require.Equal(t, uint64(1), res.TargetInfoFallbacks)
	})

	// The query-frontend reports the native stats only for a query that made lookups or fell back.
	t.Run("an answer without native stats", func(t *testing.T) {
		client, _ := serve(t, http.StatusOK, `{"status":"success","data":{"resultType":"vector","result":[]}}`, "querier_wall_time;dur=1.5")
		res, err := client.Query(t.Context(), "tenant", "info(up)", ts, ModeJoin)
		require.NoError(t, err)
		require.Equal(t, QueryResult{Samples: []Sample{}}, res)
	})

	t.Run("an error status", func(t *testing.T) {
		client, _ := serve(t, http.StatusUnprocessableEntity, `{"status":"error","error":"bad query"}`, "")
		_, err := client.Query(t.Context(), "tenant", "info(up)", ts, ModeNative)
		require.ErrorContains(t, err, "status 422")
		require.ErrorContains(t, err, "bad query")
	})

	// info() answers instant vectors: a probe query that doesn't is misconfigured.
	t.Run("an answer that isn't an instant vector", func(t *testing.T) {
		client, _ := serve(t, http.StatusOK, `{"status":"success","data":{"resultType":"scalar","result":[1700000000.5,"1"]}}`, "")
		_, err := client.Query(t.Context(), "tenant", "1", ts, ModeNative)
		require.ErrorContains(t, err, `"scalar"`)
	})
}

func TestHTTPClient_Lookup(t *testing.T) {
	start, end := time.Unix(1_700_000_000, 0), time.Unix(1_700_003_600, 0)
	const body = `{"results":[{"labels":{"job":"api"}},{"labels":{"job":"db"}}],"limited":true,"coverage_start_ms":0}`

	for lookup, expectedParams := range map[Lookup]url.Values{
		LookupResources: {"match[]": {`{__name__="up"}`}, "start": {"1700000000"}, "end": {"1700003600"}},
		// Native metric metadata has no time range.
		LookupMetricMetadata: {"match[]": {`{__name__="up"}`}},
	} {
		t.Run(string(lookup), func(t *testing.T) {
			client, req := serve(t, http.StatusOK, body, "")
			res, err := client.Lookup(t.Context(), "tenant", lookup, []string{`{__name__="up"}`}, start, end)
			require.NoError(t, err)
			require.Equal(t, "/prometheus/api/v1/"+string(lookup), req.URL.Path)
			require.Equal(t, expectedParams, req.URL.Query())
			require.Equal(t, "tenant", req.Header.Get("X-Scope-OrgID"))
			require.Equal(t, LookupResult{Series: 2, Limited: true}, res)
		})
	}
}
