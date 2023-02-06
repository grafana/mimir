// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
)

func Test_cardinalityEstimateBucket_GenerateCacheKey_keyFormat(t *testing.T) {
	requestTime := parseTimeRFC3339(t, "2023-01-09T03:24:12Z")
	hoursSinceEpoch := util.TimeToMillis(requestTime) / time.Hour.Milliseconds()
	daysSinceEpoch := hoursSinceEpoch / 24

	type args struct {
		userID string
		r      Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "instant query",
			args: args{
				userID: "tenant-a",
				r: &PrometheusInstantQueryRequest{
					Time:  requestTime.UnixMilli(),
					Query: "up",
				},
			},
			want: fmt.Sprintf("tenant-a:up:%d", daysSinceEpoch),
		},
		{
			name: "range query",
			args: args{
				userID: "tenant-b",
				r: &PrometheusRangeQueryRequest{
					Start: requestTime.UnixMilli(),
					End:   requestTime.Add(2 * time.Hour).UnixMilli(),
					Query: "up",
				},
			},
			want: fmt.Sprintf("tenant-b:up:%d:%d", daysSinceEpoch, 0),
		},
		{
			name: "range query with large range",
			args: args{
				userID: "tenant-b",
				r: &PrometheusRangeQueryRequest{
					Start: requestTime.UnixMilli(),
					// Over 24 hours, range part should be 1
					End:   requestTime.Add(25 * time.Hour).UnixMilli(),
					Query: "up",
				},
			},
			want: fmt.Sprintf("tenant-b:up:%d:%d", daysSinceEpoch, 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, generateCardinalityEstimationCacheKey(tt.args.userID, tt.args.r, 24*time.Hour))
		})
	}
}

func Test_cardinalityEstimation_lookupCardinalityForKey(t *testing.T) {
	ctx := context.Background()
	c := cache.NewInstrumentedMockCache()

	presentKey := "tenant-a:up:1234:4321"
	presentValue := uint64(25)
	marshaled, err := proto.Marshal(&QueryCardinality{presentValue})
	require.NoError(t, err)
	c.Store(ctx, map[string][]byte{cacheHashKey(presentKey): marshaled}, 1*time.Hour)

	expectedFetchCount := 0
	tests := []struct {
		cache           cache.Cache
		name            string
		key             string
		wantCardinality uint64
		wantPresent     bool
	}{
		{
			cache:           nil,
			name:            "nil cache",
			key:             presentKey,
			wantCardinality: 0,
			wantPresent:     false,
		},
		{
			cache:           c,
			name:            "cache hit",
			key:             presentKey,
			wantCardinality: presentValue,
			wantPresent:     true,
		},
		{
			cache:           c,
			name:            "cache miss",
			key:             "not present",
			wantCardinality: 0,
			wantPresent:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			estimate, ok := (&cardinalityEstimation{cache: tt.cache}).lookupCardinalityForKey(ctx, tt.key)
			if tt.cache != nil {
				expectedFetchCount++
			}
			assert.Equal(t, expectedFetchCount, c.CountFetchCalls())
			assert.Equal(t, tt.wantCardinality, estimate)
			assert.Equal(t, tt.wantPresent, ok)
		})
	}
}

func Test_cardinalityEstimation_Do(t *testing.T) {
	const numSeries = uint64(25)
	request := &PrometheusRangeQueryRequest{
		Start: parseTimeRFC3339(t, "2023-01-31T09:00:00Z").Unix() * 1000,
		End:   parseTimeRFC3339(t, "2023-01-31T10:00:00Z").Unix() * 1000,
		Query: "up",
	}
	addSeriesHandler := func(estimate, actual uint64) HandlerFunc {
		return func(ctx context.Context, request Request) (Response, error) {
			require.NotNil(t, request.GetHints())
			require.Equal(t, request.GetHints().EstimatedCardinality, estimate)

			queryStats := stats.FromContext(ctx)
			queryStats.AddFetchedSeries(actual)
			return &PrometheusResponse{}, nil
		}
	}
	marshaledEstimate, err := proto.Marshal(&QueryCardinality{numSeries})
	require.NoError(t, err)

	tests := []struct {
		name              string
		orgID             string
		downstreamHandler HandlerFunc
		cacheContent      map[string][]byte
		wantLoads         int
		wantStores        int
		wantErr           assert.ErrorAssertionFunc
	}{
		{
			name:  "no orgID",
			orgID: "",
			downstreamHandler: func(_ context.Context, _ Request) (Response, error) {
				return &PrometheusResponse{}, nil
			},
			wantLoads:  0,
			wantStores: 0,
			wantErr:    assert.NoError,
		},
		{
			name:  "downstream error",
			orgID: "1",
			downstreamHandler: func(_ context.Context, _ Request) (Response, error) {
				return nil, errors.New("test error")
			},
			wantLoads:  1,
			wantStores: 0,
			wantErr:    assert.Error,
		},
		{
			name:              "with populated cache and unchanged cardinality",
			orgID:             "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries),
			cacheContent:      map[string][]byte{cacheHashKey(generateCardinalityEstimationCacheKey("1", request, 24*time.Hour)): marshaledEstimate},
			wantLoads:         1,
			wantStores:        0,
			wantErr:           assert.NoError,
		},
		{
			name:              "with populated cache and marginally changed cardinality",
			orgID:             "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries+1),
			cacheContent:      map[string][]byte{cacheHashKey(generateCardinalityEstimationCacheKey("1", request, 24*time.Hour)): marshaledEstimate},
			wantLoads:         1,
			wantStores:        0,
			wantErr:           assert.NoError,
		},
		{
			name:              "with populated cache and significantly changed cardinality",
			orgID:             "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries*2),
			cacheContent:      map[string][]byte{cacheHashKey(generateCardinalityEstimationCacheKey("1", request, 24*time.Hour)): marshaledEstimate},
			wantLoads:         1,
			wantStores:        1,
			wantErr:           assert.NoError,
		},
		{
			name:  "with empty cache",
			orgID: "1",
			downstreamHandler: func(ctx context.Context, request Request) (Response, error) {
				queryStats := stats.FromContext(ctx)
				queryStats.AddFetchedSeries(numSeries)
				return &PrometheusResponse{}, nil
			},
			wantLoads:  1,
			wantStores: 1,
			wantErr:    assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := cache.NewInstrumentedMockCache()
			mw := newCardinalityEstimationMiddleware(c, log.NewNopLogger(), nil)
			handler := mw.Wrap(tt.downstreamHandler)
			_, ctx := stats.ContextWithEmptyStats(context.Background())
			if tt.orgID != "" {
				ctx = user.InjectOrgID(ctx, tt.orgID)
			}
			numSetupStoreCalls := 0
			if len(tt.cacheContent) > 0 {
				c.Store(ctx, tt.cacheContent, time.Minute)
				numSetupStoreCalls++
			}

			_, err := handler.Do(ctx, request)

			tt.wantErr(t, err)
			assert.Equal(t, tt.wantLoads, c.CountFetchCalls())
			assert.Equal(t, numSetupStoreCalls+tt.wantStores, c.CountStoreCalls())
		})
	}

}

func Test_cardinalityEstimateBucket_GenerateCacheKey_requestEquality(t *testing.T) {
	rangeQuery := &PrometheusRangeQueryRequest{
		Start: util.TimeToMillis(parseTimeRFC3339(t, "2023-01-31T09:00:00Z")),
		End:   util.TimeToMillis(parseTimeRFC3339(t, "2023-01-31T10:00:00Z")),
		Query: "up",
	}
	tests := []struct {
		name      string
		tenantA   string
		tenantB   string
		requestA  Request
		requestB  Request
		wantEqual bool
	}{
		{
			name:      "same tenant, same request",
			tenantA:   "1",
			requestA:  rangeQuery,
			tenantB:   "1",
			requestB:  rangeQuery,
			wantEqual: true,
		},
		{
			name:      "different tenant, same request",
			tenantA:   "1",
			tenantB:   "2",
			requestA:  rangeQuery,
			requestB:  rangeQuery,
			wantEqual: false,
		},
		{
			name:      "same tenant, same query with start time in same bucket",
			tenantA:   "1",
			tenantB:   "1",
			requestA:  rangeQuery,
			requestB:  rangeQuery.WithStartEnd(rangeQuery.GetStart()+5*time.Minute.Milliseconds(), rangeQuery.GetEnd()+5*time.Minute.Milliseconds()),
			wantEqual: true,
		},
		{
			name:      "same tenant, same query with start time in different bucket",
			tenantA:   "1",
			tenantB:   "1",
			requestA:  rangeQuery,
			requestB:  rangeQuery.WithStartEnd(rangeQuery.GetStart()+24*time.Hour.Milliseconds(), rangeQuery.GetEnd()+24*time.Hour.Milliseconds()),
			wantEqual: false,
		},
		{
			name:      "same tenant, same query with start time in same bucket and range size in same bucket",
			tenantA:   "1",
			tenantB:   "1",
			requestA:  rangeQuery,
			requestB:  rangeQuery.WithStartEnd(rangeQuery.GetStart(), rangeQuery.GetEnd()+time.Second.Milliseconds()),
			wantEqual: true,
		},
		{
			name:      "same tenant, same query with start time in same bucket and range size in different bucket",
			tenantA:   "1",
			tenantB:   "1",
			requestA:  rangeQuery,
			requestB:  rangeQuery.WithStartEnd(rangeQuery.GetStart()+5*time.Minute.Milliseconds(), rangeQuery.GetEnd()+25*time.Hour.Milliseconds()),
			wantEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			keyA := generateCardinalityEstimationCacheKey(tt.tenantA, tt.requestA, 24*time.Hour)
			keyB := generateCardinalityEstimationCacheKey(tt.tenantB, tt.requestB, 24*time.Hour)
			if tt.wantEqual {
				assert.Equal(t, keyA, keyB)
			} else {
				assert.NotEqual(t, keyA, keyB)
			}
		})
	}
}

func Test_newCardinalityEstimationMiddleware_canWrapMoreThanOnce(t *testing.T) {
	req := &PrometheusRangeQueryRequest{}

	mw := newCardinalityEstimationMiddleware(nil, log.NewNopLogger(), prometheus.NewRegistry())

	require.NotPanics(t, func() {
		_, err := mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
		_, err = mw.Wrap(mockHandlerWith(nil, nil)).Do(user.InjectOrgID(context.Background(), "test"), req)
		require.Nil(t, err)
	})
}
