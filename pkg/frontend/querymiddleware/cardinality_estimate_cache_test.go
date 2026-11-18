// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier"
	mimirtest "github.com/grafana/mimir/pkg/util/test"
)

func TestCardinalityEstimateCache(t *testing.T) {
	now := time.Unix(100*3600, 0)
	limits := mockLimits{maxCacheFreshness: 10 * time.Minute, outOfOrderTimeWindow: time.Hour, resultsCacheTTL: 7 * 24 * time.Hour, resultsCacheOutOfOrderWindowTTL: 10 * time.Minute}

	type downstream struct {
		calls  int
		body   string
		header http.Header
		status int
	}
	newRT := func(d *downstream, backend cache.Cache) http.RoundTripper {
		next := RoundTripFunc(func(*http.Request) (*http.Response, error) {
			d.calls++
			h := http.Header{"Content-Type": []string{"application/json"}}
			for k, v := range d.header {
				h[k] = v
			}
			status := d.status
			if status == 0 {
				status = http.StatusOK
			}
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(d.body)), Header: h}, nil
		})
		rt := newCardinalityEstimateCacheRoundTripper(backend, DefaultCacheKeyGenerator{}, limits, next, mimirtest.NewTestingLogger(t), nil)
		rt.(*cardinalityEstimateCache).now = func() time.Time { return now }
		return rt
	}
	request := func(query string) *http.Request {
		u, err := url.Parse("/prometheus/api/v1/cardinality/estimate?" + query)
		require.NoError(t, err)
		return (&http.Request{Method: http.MethodGet, URL: u, Header: http.Header{}}).WithContext(user.InjectOrgID(context.Background(), "user-1"))
	}
	roundTrip := func(rt http.RoundTripper, query string) string {
		res, err := rt.RoundTrip(request(query))
		require.NoError(t, err)
		b, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		return string(b)
	}
	at := func(hoursAgo float64) string {
		return fmt.Sprintf("%.3f", float64(now.Unix())-hoursAgo*3600)
	}

	t.Run("a past window is answered from the cache the second time, for results-cache-ttl", func(t *testing.T) {
		d := &downstream{body: `{"counts":[]}`}
		backend := cache.NewInstrumentedMockCache()
		rt := newRT(d, backend)
		q := "start=" + at(26) + "&end=" + at(2) + "&group_by=pod"
		assert.Equal(t, d.body, roundTrip(rt, q))
		assert.Equal(t, d.body, roundTrip(rt, q))
		assert.Equal(t, 1, d.calls)
		require.Equal(t, 1, backend.CountStoreCalls())
		for _, item := range backend.GetItems() {
			assert.WithinDuration(t, time.Now().Add(7*24*time.Hour), item.ExpiresAt, 5*time.Second)
		}
	})

	t.Run("a tenant the endpoint is disabled for gets the querier's refusal, not a cached answer", func(t *testing.T) {
		d := &downstream{body: `{"counts":[]}`}
		backend := cache.NewInstrumentedMockCache()
		q := "start=" + at(26) + "&end=" + at(2)
		assert.Equal(t, d.body, roundTrip(newRT(d, backend), q))
		require.Equal(t, 1, backend.CountStoreCalls())

		enabled := limits
		limits.cardinalityEstimateDisabled = true
		defer func() { limits = enabled }()
		cacheDisabled := request(q)
		cacheDisabled.Header.Set("Cache-Control", "no-store")
		for _, req := range []*http.Request{request(q), cacheDisabled} {
			res, err := newRT(d, backend).RoundTrip(req)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, res.StatusCode)
			assert.Equal(t, "application/json", res.Header.Get("Content-Type"))
			b, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			assert.JSONEq(t, `{"status":"error","errorType":"bad_data","error":"cardinality estimate is disabled for the tenant: user-1"}`, string(b))
		}
		assert.Equal(t, 1, d.calls, "neither the cache nor the querier answers")

		// The querier refuses a federated request on its own.
		federated := request(q).WithContext(user.InjectOrgID(context.Background(), "user-1|user-2"))
		_, err := newRT(d, backend).RoundTrip(federated)
		require.NoError(t, err)
		assert.Equal(t, 2, d.calls)
	})

	t.Run("a window with RFC 3339 timestamps is cached like one with seconds", func(t *testing.T) {
		d := &downstream{body: `{"counts":[]}`}
		backend := cache.NewInstrumentedMockCache()
		rt := newRT(d, backend)
		rfc := func(hoursAgo int) string {
			return now.Add(-time.Duration(hoursAgo) * time.Hour).UTC().Format(time.RFC3339)
		}
		q := "start=" + rfc(26) + "&end=" + rfc(2) + "&group_by=pod"
		assert.Equal(t, d.body, roundTrip(rt, q))
		assert.Equal(t, d.body, roundTrip(rt, q))
		assert.Equal(t, 1, d.calls)
		assert.Equal(t, 1, backend.CountStoreCalls())

		// An end within max-cache-freshness is still not cached.
		d.calls = 0
		q = "start=" + rfc(1) + "&end=" + now.UTC().Format(time.RFC3339)
		roundTrip(rt, q)
		roundTrip(rt, q)
		assert.Equal(t, 2, d.calls)
	})

	t.Run("a window ending inside the out-of-order window is kept for the shorter TTL", func(t *testing.T) {
		d := &downstream{body: `{}`}
		backend := cache.NewInstrumentedMockCache()
		roundTrip(newRT(d, backend), "start="+at(3)+"&end="+at(0.5))
		require.Equal(t, 1, backend.CountStoreCalls())
		for _, item := range backend.GetItems() {
			assert.WithinDuration(t, time.Now().Add(10*time.Minute), item.ExpiresAt, 5*time.Second)
		}
	})

	t.Run("a window ending within max-cache-freshness is not cached", func(t *testing.T) {
		d := &downstream{body: `{}`}
		backend := cache.NewInstrumentedMockCache()
		rt := newRT(d, backend)
		roundTrip(rt, "start="+at(1)+"&end="+at(0))
		roundTrip(rt, "start="+at(1)+"&end="+at(0))
		assert.Equal(t, 2, d.calls)
		assert.Zero(t, backend.CountStoreCalls())
	})

	t.Run("a response marked no-store or an error is not cached", func(t *testing.T) {
		for name, tc := range map[string]struct {
			d     *downstream
			query string
		}{
			"lower bound": {&downstream{body: `{"lower_bound":true}`, header: http.Header{"Cache-Control": []string{"no-store"}}}, ""},
			"refused":     {&downstream{body: "over max_index_bytes", status: http.StatusUnprocessableEntity}, ""},
		} {
			backend := cache.NewInstrumentedMockCache()
			roundTrip(newRT(tc.d, backend), "start="+at(26)+"&end="+at(2)+tc.query)
			assert.Zero(t, backend.CountStoreCalls(), name)
		}
	})

	t.Run("an answer is kept only until its window's start falls behind the max query lookback or the blocks retention", func(t *testing.T) {
		unlimited := limits
		defer func() { limits = unlimited }()
		windowFrom := func(hoursAgo int) string {
			return fmt.Sprintf(`{"window":{"start":%d,"end":%d}}`, now.Add(-time.Duration(hoursAgo)*time.Hour).UnixMilli(), now.Add(-2*time.Hour).UnixMilli())
		}
		for name, tc := range map[string]struct {
			lookback, retention time.Duration
			body                string
			ttl                 time.Duration
		}{
			"the start is 4h inside the lookback":                 {lookback: 30 * time.Hour, body: windowFrom(26), ttl: 4 * time.Hour},
			"the start is 4h inside the retention":                {retention: 30 * time.Hour, body: windowFrom(26), ttl: 4 * time.Hour},
			"the shorter of the two counts":                       {lookback: 30 * time.Hour, retention: 28 * time.Hour, body: windowFrom(26), ttl: 2 * time.Hour},
			"the limit leaves more than results-cache-ttl":        {lookback: 30 * 24 * time.Hour, body: windowFrom(26), ttl: 7 * 24 * time.Hour},
			"the querier moved the start up to the lookback":      {lookback: 24 * time.Hour, body: windowFrom(24)},
			"a window wholly before the lookback, answered empty": {lookback: 24 * time.Hour, body: windowFrom(48)},
			"an answer that doesn't say its window":               {lookback: 30 * time.Hour, body: `{}`},
		} {
			limits.maxQueryLookback, limits.compactorBlocksRetentionPeriod = tc.lookback, tc.retention
			backend := cache.NewInstrumentedMockCache()
			d := &downstream{body: tc.body}
			// The key holds the start asked for, so the answer's own window decides.
			assert.Equal(t, tc.body, roundTrip(newRT(d, backend), "start="+at(48)+"&end="+at(2)), name)
			if tc.ttl == 0 {
				assert.Zero(t, backend.CountStoreCalls(), name)
				continue
			}
			require.Equal(t, 1, backend.CountStoreCalls(), name)
			for _, item := range backend.GetItems() {
				assert.WithinDuration(t, time.Now().Add(tc.ttl), item.ExpiresAt, 5*time.Second, name)
			}
		}
	})

	t.Run("a response over 1 MiB is passed through whole and not cached", func(t *testing.T) {
		big := strings.Repeat("x", maxCardinalityEstimateCachedBytes+10)
		d := &downstream{body: big}
		backend := cache.NewInstrumentedMockCache()
		assert.Equal(t, big, roundTrip(newRT(d, backend), "start="+at(26)+"&end="+at(2)))
		assert.Zero(t, backend.CountStoreCalls())
	})

	t.Run("the key covers every parameter that changes the answer, with match[] in any order", func(t *testing.T) {
		key := func(q string) string {
			k, err := DefaultCacheKeyGenerator{}.CardinalityEstimate(request(q))
			require.NoError(t, err)
			return k.CacheKey
		}
		base := "start=1&end=2&match[]=a&match[]=b"
		assert.Equal(t, key(base), key("end=2&match[]=b&start=1&match[]=a"))
		for _, extra := range []string{"&group_by=pod", "&limit=5", "&min_count=2", "&max_index_bytes=9"} {
			assert.NotEqual(t, key(base), key(base+extra), extra)
		}
		assert.NotEqual(t, key(base), key("start=1&end=3&match[]=a&match[]=b"))
	})

	t.Run("the key reads the first value of a repeated parameter, as the handler does", func(t *testing.T) {
		key := func(q string) string {
			k, err := DefaultCacheKeyGenerator{}.CardinalityEstimate(request(q))
			require.NoError(t, err)
			return k.CacheKey
		}
		base := "start=1&end=2"
		assert.NotEqual(t, key(base+"&limit=1&limit=100"), key(base+"&limit=100&limit=1"))
		assert.Equal(t, key(base+"&limit=1"), key(base+"&limit=1&limit=100"))
	})

	t.Run("the key covers every parameter the querier's handler reads", func(t *testing.T) {
		key := func(q string) string {
			k, err := DefaultCacheKeyGenerator{}.CardinalityEstimate(request(q))
			require.NoError(t, err)
			return k.CacheKey
		}
		for _, name := range querier.CardinalityEstimateRequestParams {
			assert.NotEqual(t, key(""), key(name+"=x"), name)
		}
	})
}

func TestCardinalityEstimateCacheKey_NoCollision(t *testing.T) {
	// The tail a parameter value would need to carry to spell out the parameters after group_by.
	tail := "\x01step=\x01window=\x01limit=\x01min_count=\x01max_index_bytes="
	for name, pair := range map[string][2]url.Values{
		"group_by spelling out max_index_bytes=5 against max_index_bytes spelling it out": {
			{"start": {"1"}, "end": {"2"}, "group_by": {"a" + tail + "x"}, "max_index_bytes": {"5"}},
			{"start": {"1"}, "end": {"2"}, "group_by": {"a"}, "max_index_bytes": {"x" + tail + "5"}},
		},
		"match[] values against one value holding the separator": {
			{"start": {"1"}, "end": {"2"}, "match[]": {"a", "b"}},
			{"start": {"1"}, "end": {"2"}, "match[]": {"a\x00b"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			assert.NotEqual(t, cardinalityEstimateCacheKey(pair[0]), cardinalityEstimateCacheKey(pair[1]))
		})
	}

	t.Run("match[] order does not change the key", func(t *testing.T) {
		assert.Equal(t,
			cardinalityEstimateCacheKey(url.Values{"start": {"1"}, "end": {"2"}, "match[]": {"a", "b"}}),
			cardinalityEstimateCacheKey(url.Values{"start": {"1"}, "end": {"2"}, "match[]": {"b", "a"}}))
	})
}

type failingReadBody struct{ closed bool }

func (*failingReadBody) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (b *failingReadBody) Close() error           { b.closed = true; return nil }

func TestCardinalityEstimateCache_ClosesBodyOnReadError(t *testing.T) {
	now := time.Unix(100*3600, 0)
	limits := mockLimits{maxCacheFreshness: 10 * time.Minute, resultsCacheTTL: 7 * 24 * time.Hour}
	body := &failingReadBody{}
	next := RoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body, Header: http.Header{"Content-Type": []string{"application/json"}}}, nil
	})
	rt := newCardinalityEstimateCacheRoundTripper(cache.NewInstrumentedMockCache(), DefaultCacheKeyGenerator{}, limits, next, mimirtest.NewTestingLogger(t), nil)
	rt.(*cardinalityEstimateCache).now = func() time.Time { return now }

	u, err := url.Parse(fmt.Sprintf("/prometheus/api/v1/cardinality/estimate?start=%d&end=%d", now.Unix()-26*3600, now.Unix()-2*3600))
	require.NoError(t, err)
	req := (&http.Request{Method: http.MethodGet, URL: u, Header: http.Header{}}).WithContext(user.InjectOrgID(context.Background(), "user-1"))
	_, err = rt.RoundTrip(req)
	require.Error(t, err)
	assert.True(t, body.closed)
}
