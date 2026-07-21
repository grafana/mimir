// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor"
)

type fakeExplainer struct {
	plan        distributor.ReadcacheQueryPlan
	calls       int
	gotFrom     model.Time
	gotTo       model.Time
	gotMatchers []*labels.Matcher
}

func (f *fakeExplainer) ExplainReadcacheQuery(_ context.Context, _ string, from, to model.Time, matchers []*labels.Matcher) distributor.ReadcacheQueryPlan {
	f.calls++
	f.gotFrom, f.gotTo, f.gotMatchers = from, to, matchers
	return f.plan
}

func doExplainRequest(t *testing.T, h *ReadcacheExplainHandler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, ReadcacheExplainPathPrefix+"?"+form.Encode(), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReadcacheExplainHandler_RendersPlan(t *testing.T) {
	fake := &fakeExplainer{plan: distributor.ReadcacheQueryPlan{
		RoutingEnabled: true,
		Named:          true,
		MetricName:     "http_requests_total",
		Partitions: []distributor.ReadcachePartitionPlan{
			{PartitionID: 7, Calls: []distributor.ReadcacheQueryStreamCall{
				{PartitionID: 7, Owner: "readcache-3", InstanceID: "readcache-3/p7"},
			}},
		},
		TotalCalls: 1,
	}}
	h := NewReadcacheExplainHandler(fake, 5*time.Minute, log.NewNopLogger())

	form := url.Values{}
	form.Set("query", "sum(rate(http_requests_total[5m]))")
	form.Set("tenant", "12345")
	rec := doExplainRequest(t, h, form)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Positive(t, fake.calls)
	assert.Contains(t, body, "readcache-3")
	assert.Contains(t, body, "p7")
	assert.Contains(t, body, "http_requests_total")
	// The exact __name__ matcher must have been forwarded to the explainer.
	require.NotEmpty(t, fake.gotMatchers)
}

func TestReadcacheExplainHandler_RequiresTenant(t *testing.T) {
	h := NewReadcacheExplainHandler(&fakeExplainer{}, time.Minute, log.NewNopLogger())

	form := url.Values{}
	form.Set("query", "up")
	rec := doExplainRequest(t, h, form)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "tenant")
}

func TestReadcacheExplainHandler_EmptyQueryRendersForm(t *testing.T) {
	fake := &fakeExplainer{}
	h := NewReadcacheExplainHandler(fake, time.Minute, log.NewNopLogger())

	rec := doExplainRequest(t, h, url.Values{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Zero(t, fake.calls, "no explain call should be made without a query")
	assert.Contains(t, rec.Body.String(), "PromQL query")
}

func TestReadcacheExplainHandler_ExtractSelectors(t *testing.T) {
	h := NewReadcacheExplainHandler(&fakeExplainer{}, 5*time.Minute, log.NewNopLogger())
	start := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	sels, err := h.extractSelectors(`rate(http_requests_total[10m]) + on() up`, start, end)
	require.NoError(t, err)
	require.Len(t, sels, 2)

	var matrix, instant selectorInfo
	for _, s := range sels {
		if s.rng > 0 {
			matrix = s
		} else {
			instant = s
		}
	}

	// Matrix (range) selector reaches back by its range.
	assert.Equal(t, 10*time.Minute, matrix.rng)
	assert.Equal(t, start.Add(-10*time.Minute).UnixMilli(), matrix.from.Time().UnixMilli())
	assert.Equal(t, end.UnixMilli(), matrix.to.Time().UnixMilli())

	// Instant vector selector reaches back by the lookback delta.
	assert.Zero(t, instant.rng)
	assert.Equal(t, start.Add(-5*time.Minute).UnixMilli(), instant.from.Time().UnixMilli())
	assert.Equal(t, end.UnixMilli(), instant.to.Time().UnixMilli())
}

func TestReadcacheExplainHandler_ExtractSelectorsWithOffset(t *testing.T) {
	h := NewReadcacheExplainHandler(&fakeExplainer{}, 5*time.Minute, log.NewNopLogger())
	start := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)

	sels, err := h.extractSelectors(`up offset 30m`, start, end)
	require.NoError(t, err)
	require.Len(t, sels, 1)

	sel := sels[0]
	assert.Equal(t, 30*time.Minute, sel.offset)
	// Offset shifts both bounds back, and the instant selector still
	// reaches back one lookback before the (shifted) start.
	assert.Equal(t, start.Add(-30*time.Minute-5*time.Minute).UnixMilli(), sel.from.Time().UnixMilli())
	assert.Equal(t, end.Add(-30*time.Minute).UnixMilli(), sel.to.Time().UnixMilli())
}
