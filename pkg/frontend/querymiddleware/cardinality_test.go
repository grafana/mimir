// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/querier/stats"
)

func Test_cardinalityEstimateBucket_GenerateCacheKey_keyFormat(t *testing.T) {
	requestTime := time.Date(2023, time.January, 9, 3, 24, 12, 0, time.UTC)
	hoursSinceEpoch := requestTime.UnixMilli() / time.Hour.Milliseconds()
	daysSinceEpoch := hoursSinceEpoch / 24

	type args struct {
		userID string
		r      Request
	}
	tests := []struct {
		name string
		s    cardinalityEstimateBucket
		args args
		want string
	}{
		{
			name: "instant query",
			s:    cardinalityEstimateBucket(24 * time.Hour),
			args: args{
				userID: "tenant-a",
				r: &PrometheusInstantQueryRequest{
					Time:  requestTime.UnixMilli(),
					Query: "sum(up)",
				},
			},
			want: fmt.Sprintf("tenant-a:sum(up):%d", daysSinceEpoch),
		},
		{
			name: "range query",
			s:    cardinalityEstimateBucket(time.Hour),
			args: args{
				userID: "tenant-b",
				r: &PrometheusRangeQueryRequest{
					Start: requestTime.UnixMilli(),
					End:   requestTime.Add(5 * time.Hour).UnixMilli(),
					Query: "sum(up)",
				},
			},
			want: fmt.Sprintf("tenant-b:sum(up):%d:%d", hoursSinceEpoch, int(5*time.Hour.Seconds())),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.GenerateCacheKey(tt.args.userID, tt.args.r), "GenerateCacheKey(%v, %v)", tt.args.userID, tt.args.r)
		})
	}
}

func Test_cardinalityEstimation_lookupCardinalityForKey(t *testing.T) {
	ctx := context.Background()
	c := cache.NewInstrumentedMockCache()

	presentKey := "tenant-a:sum(up):1234:4321"
	presentValue := uint64(25)
	c.Store(ctx, map[string][]byte{cacheHashKey(presentKey): marshalBinary(presentValue)}, 1*time.Hour)

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
				expectedFetchCount += 1
			}
			assert.Equalf(t, expectedFetchCount, c.CountFetchCalls(), "lookupCardinalityForKey(%v)", tt.key)
			assert.Equalf(t, tt.wantCardinality, estimate, "lookupCardinalityForKey(%v)", tt.key)
			assert.Equalf(t, tt.wantPresent, ok, "lookupCardinalityForKey(%v)", tt.key)
		})
	}
}

func Test_injectCardinalityEstimate(t *testing.T) {
	type args struct {
		request  Request
		estimate uint64
	}
	tests := []struct {
		name string
		args args
		want Request
	}{
		{
			name: "hints present",
			args: args{
				request:  &PrometheusInstantQueryRequest{Hints: &Hints{TotalQueries: 10}},
				estimate: 1,
			},
			want: &PrometheusInstantQueryRequest{Hints: &Hints{
				TotalQueries:         10,
				EstimatedCardinality: 1,
			}},
		},
		{
			name: "without hints",
			args: args{
				request:  &PrometheusRangeQueryRequest{Hints: nil},
				estimate: 2,
			},
			want: &PrometheusRangeQueryRequest{Hints: &Hints{
				EstimatedCardinality: 2,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, injectCardinalityEstimate(tt.args.request, tt.args.estimate), "injectCardinalityEstimate(%v, %v)", tt.args.request, tt.args.estimate)
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
			cacheContent:      map[string][]byte{cacheHashKey(cardinalityEstimateBucket(24*time.Hour).GenerateCacheKey("1", request)): marshalBinary(numSeries)},
			wantLoads:         1,
			wantStores:        0,
			wantErr:           assert.NoError,
		},
		{
			name:              "with populated cache and changed cardinality",
			orgID:             "1",
			downstreamHandler: addSeriesHandler(numSeries, numSeries+1),
			cacheContent:      map[string][]byte{cacheHashKey(cardinalityEstimateBucket(24*time.Hour).GenerateCacheKey("1", request)): marshalBinary(numSeries)},
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
			c := cache.NewInstrumentedMockCache()
			mw := newCardinalityEstimationMiddleware(c)
			handler := mw.Wrap(tt.downstreamHandler)
			_, ctx := stats.ContextWithEmptyStats(context.Background())
			if tt.orgID != "" {
				ctx = user.InjectOrgID(ctx, tt.orgID)
			}
			numSetupStoreCalls := 0
			if len(tt.cacheContent) > 0 {
				c.Store(ctx, tt.cacheContent, time.Minute)
				numSetupStoreCalls += 1
			}

			_, err := handler.Do(ctx, request)

			tt.wantErr(t, err)
			assert.Equal(t, tt.wantLoads, c.CountFetchCalls())
			assert.Equal(t, numSetupStoreCalls+tt.wantStores, c.CountStoreCalls())
		})
	}

}

func Test_cardinalityEstimateBucket_GenerateCacheKey_requestEquality(t *testing.T) {
	splitter := cardinalityEstimateBucket(24 * time.Hour)
	rangeQuery := &PrometheusRangeQueryRequest{
		Start: parseTimeRFC3339(t, "2023-01-31T09:00:00Z").Unix() * 1000,
		End:   parseTimeRFC3339(t, "2023-01-31T10:00:00Z").Unix() * 1000,
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
			name:      "same tenant, same query with start time in same bucket but different range size",
			tenantA:   "1",
			tenantB:   "1",
			requestA:  rangeQuery,
			requestB:  rangeQuery.WithStartEnd(rangeQuery.GetStart(), rangeQuery.GetEnd()+time.Second.Milliseconds()),
			wantEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyA := splitter.GenerateCacheKey(tt.tenantA, tt.requestA)
			keyB := splitter.GenerateCacheKey(tt.tenantB, tt.requestB)
			if tt.wantEqual {
				assert.Equal(t, keyA, keyB)
			} else {
				assert.NotEqual(t, keyA, keyB)
			}
		})
	}
}
