// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
)

func TestOTLPDeltaQueryHeaders(t *testing.T) {
	requests := make(chan *http.Request, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Clone(r.Context())
		if strings.HasSuffix(r.URL.Path, "/metadata") {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"status":"success","data":{"metric":[{"type":"counter","help":"test","unit":""}]}}`)
			return
		}
		resultType := "vector"
		if strings.HasSuffix(r.URL.Path, "/query_range") {
			resultType = "matrix"
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":%q,"result":[]}}`, resultType)
	}))
	defer server.Close()

	var cfg ClientConfig
	flagext.DefaultValues(&cfg)
	cfg.TenantID = "delta-tenant"
	cfg.BasicAuthUser = "user"
	cfg.BasicAuthPassword = "password"
	require.NoError(t, cfg.ReadBaseEndpoint.Set(server.URL))
	require.NoError(t, cfg.WriteBaseEndpoint.Set(server.URL))

	client, err := NewClient(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)

	var seriesCfg WriteReadSeriesTestConfig
	flagext.DefaultValues(&seriesCfg)
	seriesCfg.WithHistograms = true
	delta := NewOTLPDeltaTest(OTLPDeltaTestConfig{Enabled: true}, seriesCfg, client, log.NewNopLogger(), prometheus.NewRegistry())
	now := time.Now().Truncate(time.Minute)

	checkRequest := func(deltaQuery bool) {
		t.Helper()
		select {
		case req := <-requests:
			wantHeader, wantCache := "", ""
			if deltaQuery {
				wantHeader, wantCache = "true", "no-store"
			}
			require.Equal(t, wantHeader, req.Header.Get(requestoptions.UseStartTimestampsHeader))
			require.Equal(t, wantHeader, req.Header.Get(compat.ForceFallbackHeaderName))
			require.Equal(t, wantCache, req.Header.Get("Cache-Control"))
			require.Equal(t, cfg.TenantID, req.Header.Get("X-Scope-OrgID"))
			user, password, ok := req.BasicAuth()
			require.True(t, ok)
			require.Equal(t, cfg.BasicAuthUser, user)
			require.Equal(t, cfg.BasicAuthPassword, password)
			if deltaQuery && strings.HasSuffix(req.URL.Path, "/query") {
				require.Equal(t, "preserved", req.Header.Get("X-Test"))
			}
		default:
			t.Fatal("expected a query request")
		}
	}

	// Conflicting caller options must not change the delta query semantics.
	headers := map[string]string{
		strings.ToLower(requestoptions.UseStartTimestampsHeader): "false",
		strings.ToLower(compat.ForceFallbackHeaderName):          "false",
		"cache-control": "public",
		"X-Test":        "preserved",
	}
	originalHeaders := maps.Clone(headers)
	_, err = delta.client.Query(t.Context(), "metric", now, WithAdditionalHeaders(headers), WithResultsCacheEnabled(true))
	require.NoError(t, err)
	checkRequest(true)
	require.Equal(t, originalHeaders, headers)

	_, err = delta.client.QueryRange(t.Context(), "metric", now.Add(-time.Minute), now, time.Second)
	require.NoError(t, err)
	checkRequest(true)

	// Recovery uses the same query settings for both sample types.
	require.NoError(t, delta.Init(t.Context(), now))
	checkRequest(true)
	checkRequest(true)
	require.Empty(t, requests)

	_, err = delta.client.Metadata(t.Context(), "metric")
	require.NoError(t, err)
	checkRequest(false)

	_, err = client.Query(t.Context(), "metric", now)
	require.NoError(t, err)
	checkRequest(false)
	_, err = client.QueryRange(t.Context(), "metric", now.Add(-time.Minute), now, time.Second)
	require.NoError(t, err)
	checkRequest(false)
}
