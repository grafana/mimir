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

	t.Run("query with start after query store after is ingesters only", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		startAfterQueryStoreAfter := now.Add(-adapter.cfg.QueryStoreAfter).Add(1 * time.Minute)
		end := now

		rangeHTTPReq := makeRangeHTTPRequest(ctx, startAfterQueryStoreAfter, end, 60)
		instantHTTPReq := makeInstantHTTPRequest(ctx, startAfterQueryStoreAfter)
		labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, startAfterQueryStoreAfter, end)

		reqs := []*http.Request{rangeHTTPReq, instantHTTPReq, labelValuesHTTPReq}

		for _, req := range reqs {
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(req)
			require.NoError(t, err)

			additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Equal(t, []string{ShouldQueryIngestersQueueDimension}, additionalQueueDimensions)
		}
	})

	t.Run("query with end before query ingesters within is store-gateways only", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		startBeforeQueryIngestersWithin := now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Hour)
		endBeforeQueryIngestersWithin := now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute)

		rangeHTTPReq := makeRangeHTTPRequest(ctx, startBeforeQueryIngestersWithin, endBeforeQueryIngestersWithin, 60)
		instantHTTPReq := makeInstantHTTPRequest(ctx, startBeforeQueryIngestersWithin)
		labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, startBeforeQueryIngestersWithin, endBeforeQueryIngestersWithin)

		reqs := []*http.Request{rangeHTTPReq, instantHTTPReq, labelValuesHTTPReq}

		for _, req := range reqs {
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(req)
			require.NoError(t, err)

			additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Equal(t, []string{ShouldQueryStoreGatewayQueueDimension}, additionalQueueDimensions)
		}
	})

	t.Run("query with time between query ingesters within and query store after is ingesters and store-gateways", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		startAfterQueryIngestersWithinAndBeforeQueryStoreAfter := now.Add(-adapter.limits.QueryIngestersWithin("")).Add(30 * time.Minute)
		end := now

		rangeHTTPReq := makeRangeHTTPRequest(ctx, startAfterQueryIngestersWithinAndBeforeQueryStoreAfter, end, 60)
		instantHTTPReq := makeInstantHTTPRequest(ctx, startAfterQueryIngestersWithinAndBeforeQueryStoreAfter)
		labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, startAfterQueryIngestersWithinAndBeforeQueryStoreAfter, end)

		reqs := []*http.Request{rangeHTTPReq, instantHTTPReq, labelValuesHTTPReq}

		for _, req := range reqs {
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(req)
			require.NoError(t, err)

			additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Equal(t, []string{ShouldQueryIngestersAndStoreGatewayQueueDimension}, additionalQueueDimensions)
		}
	})

	t.Run("malformed httpgrpc requests fail decoding", func(t *testing.T) {
		reqFailsHTTPDecode := &httpgrpc.HTTPRequest{Method: ";"}

		_, errHTTPDecode := adapter.extractAdditionalQueueDimensions(context.Background(), reqFailsHTTPDecode, time.Now())
		require.Error(t, errHTTPDecode)
		require.Contains(t, errHTTPDecode.Error(), "net/http")
	})

}
