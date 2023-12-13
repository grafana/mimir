// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
)

const rangeURLFormat = "/api/v1/query_range?end=%d&query=&start=%d&step=%d"

func makeRangeHTTPRequest(ctx context.Context, start, end time.Time, step int64) *http.Request {
	rangeURL := fmt.Sprintf(rangeURLFormat, end.Unix(), start.Unix(), step)
	rangeHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", rangeURL, bytes.NewReader([]byte{}))
	rangeHTTPReq.RequestURI = rangeHTTPReq.URL.RequestURI()
	return rangeHTTPReq
}

const instantURLFormat = "/api/v1/query?query=&time=%d"

func makeInstantHTTPRequest(ctx context.Context, time time.Time) *http.Request {
	instantURL := fmt.Sprintf(instantURLFormat, time.Unix())
	instantHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", instantURL, bytes.NewReader([]byte{}))
	instantHTTPReq.RequestURI = instantHTTPReq.URL.RequestURI()
	return instantHTTPReq
}

const labelValuesURLFormat = "/prometheus/api/v1/label/__name__/values?end=%d&start=%d"

func makeLabelValuesHTTPRequest(ctx context.Context, start, end time.Time) *http.Request {
	labelValuesURL := fmt.Sprintf(labelValuesURLFormat, end.Unix(), start.Unix())
	labelValuesHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", labelValuesURL, bytes.NewReader([]byte{}))
	labelValuesHTTPReq.RequestURI = labelValuesHTTPReq.URL.RequestURI()
	return labelValuesHTTPReq
}

func TestExtractAdditionalQueueDimensions(t *testing.T) {
	adapter := &frontendToSchedulerAdapter{
		cfg:    Config{QueryStoreAfter: 12 * time.Hour},
		limits: limits{queryIngestersWithin: 13 * time.Hour},
	}

	now := time.Now()

	// range and label queries have `start` and `end` params,
	// requiring different cases than instant queries with only a `time` param
	rangeAndLabelQueryTests := map[string]struct {
		start                       time.Time
		end                         time.Time
		expectedAddlQueueDimensions []string
	}{
		"query with start after query store after is ingesters only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                            |----|
			start:                       now.Add(-adapter.cfg.QueryStoreAfter).Add(1 * time.Minute),
			end:                         now,
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersQueueDimension},
		},
		"query with end before query ingesters within is store-gateways only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:        |----|
			start:                       now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Hour),
			end:                         now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryStoreGatewayQueueDimension},
		},
		"query with start before query ingesters and end after query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:            |--------------|
			start:                       now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-adapter.cfg.QueryStoreAfter).Add(1 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersAndStoreGatewayQueueDimension},
		},
		"query with start before query ingesters and end before query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:             |---------|
			start:                       now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-adapter.cfg.QueryStoreAfter).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersAndStoreGatewayQueueDimension},
		},
		"query with start after query ingesters and end after query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                  |---------|
			start:                       now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-adapter.cfg.QueryStoreAfter).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersAndStoreGatewayQueueDimension},
		},
		"query with start and end between query ingesters and query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                 |-----|
			start:                       now.Add(-adapter.limits.QueryIngestersWithin("")).Add(30 * time.Minute),
			end:                         now,
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersAndStoreGatewayQueueDimension},
		},
	}

	for testName, testData := range rangeAndLabelQueryTests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "tenant-0")

			rangeHTTPReq := makeRangeHTTPRequest(ctx, testData.start, testData.end, 60)
			labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, testData.start, testData.end)

			reqs := []*http.Request{rangeHTTPReq, labelValuesHTTPReq}

			for _, req := range reqs {
				httpgrpcReq, err := httpgrpc.FromHTTPRequest(req)
				require.NoError(t, err)

				additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
					ctx, httpgrpcReq, now,
				)
				require.NoError(t, err)
				require.Equal(t, testData.expectedAddlQueueDimensions, additionalQueueDimensions)
			}
		})
	}

	instantQueryTests := map[string]struct {
		time                        time.Time
		expectedAddlQueueDimensions []string
	}{
		"query with time after query store after is ingesters only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                                 |
			time:                        now.Add(-adapter.cfg.QueryStoreAfter).Add(1 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersQueueDimension},
		},
		"query with end before query ingesters within is store-gateways only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                   |
			time:                        now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Hour),
			expectedAddlQueueDimensions: []string{ShouldQueryStoreGatewayQueueDimension},
		},
		"query with start and end between query ingesters and query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                          |
			time:                        now.Add(-adapter.limits.QueryIngestersWithin("")).Add(30 * time.Minute),
			expectedAddlQueueDimensions: []string{ShouldQueryIngestersAndStoreGatewayQueueDimension},
		},
	}
	for testName, testData := range instantQueryTests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "tenant-0")

			instantHTTPReq := makeInstantHTTPRequest(ctx, testData.time)
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(instantHTTPReq)
			require.NoError(t, err)

			additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Equal(t, testData.expectedAddlQueueDimensions, additionalQueueDimensions)
		})
	}

	t.Run("malformed httpgrpc requests fail decoding", func(t *testing.T) {
		reqFailsHTTPDecode := &httpgrpc.HTTPRequest{Method: ";"}

		_, errHTTPDecode := adapter.extractAdditionalQueueDimensions(context.Background(), reqFailsHTTPDecode, time.Now())
		require.Error(t, errHTTPDecode)
		require.Contains(t, errHTTPDecode.Error(), "net/http")
	})

}
