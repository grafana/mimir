// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

func TestExtractAdditionalQueueDimensions(t *testing.T) {
	prometheusCodec := querymiddleware.NewPrometheusCodec(prometheus.NewPedanticRegistry(), "json")
	adapter := &frontendToSchedulerAdapter{
		cfg:             Config{QueryStoreAfter: 12 * time.Hour},
		limits:          limits{queryIngestersWithin: 13 * time.Hour},
		prometheusCodec: prometheusCodec,
	}

	now := time.Now()

	t.Run("query with start after query store after is ingesters only", func(t *testing.T) {
		promReq := &querymiddleware.PrometheusRangeQueryRequest{
			Path:  "/api/v1/query_range",
			Start: now.Add(-adapter.cfg.QueryStoreAfter).Add(1 * time.Minute).Unix(),
			End:   now.Unix(),
			Step:  60,
		}
		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		httpReq, err := adapter.prometheusCodec.EncodeRequest(ctx, promReq)
		require.NoError(t, err)

		httpgrpcReq, err := httpgrpc.FromHTTPRequest(httpReq)
		require.NoError(t, err)

		additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
			ctx, httpgrpcReq, now,
		)
		require.NoError(t, err)
		require.Equal(t, additionalQueueDimensions, []string{ShouldQueryIngestersQueueDimension})
	})

	t.Run("query with end before query ingesters within is store-gateways only", func(t *testing.T) {
		promReq := &querymiddleware.PrometheusRangeQueryRequest{
			Path:  "/api/v1/query_range",
			Start: now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Hour).Unix(),
			End:   now.Add(-adapter.limits.QueryIngestersWithin("")).Add(-1 * time.Minute).Unix(),
			Step:  60,
		}

		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		httpReq, err := adapter.prometheusCodec.EncodeRequest(ctx, promReq)
		require.NoError(t, err)

		httpgrpcReq, err := httpgrpc.FromHTTPRequest(httpReq)
		require.NoError(t, err)

		additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
			ctx, httpgrpcReq, now,
		)
		require.NoError(t, err)
		require.Equal(t, additionalQueueDimensions, []string{ShouldQueryStoreGatewayQueueDimension})
	})

	t.Run("query with time between query ingesters within and query store after is ingesters and store-gateways", func(t *testing.T) {

		promReq := &querymiddleware.PrometheusRangeQueryRequest{
			Path: "/api/v1/query_range",
			// any times which start early enough to query store,
			// end late enough to query ingesters,
			// and end >= start
			Start: now.Add(-adapter.limits.QueryIngestersWithin("")).Add(1 * time.Minute).Unix(),
			End:   now.Add(-adapter.cfg.QueryStoreAfter).Add(-1 * time.Minute).Unix(),
			Step:  60,
		}

		ctx := user.InjectOrgID(context.Background(), "tenant-0")
		httpReq, err := adapter.prometheusCodec.EncodeRequest(ctx, promReq)
		require.NoError(t, err)

		httpgrpcReq, err := httpgrpc.FromHTTPRequest(httpReq)
		require.NoError(t, err)

		additionalQueueDimensions, err := adapter.extractAdditionalQueueDimensions(
			ctx, httpgrpcReq, now,
		)
		require.NoError(t, err)
		require.Equal(t, []string{ShouldQueryIngestersAndStoreGatewayQueueDimension}, additionalQueueDimensions)

	})

	t.Run("malformed httpgrpc requests fail decoding", func(t *testing.T) {
		reqFailsHTTPDecode := &httpgrpc.HTTPRequest{Method: ";"}
		//reqFailsPromRangeDecode := &httpgrpc.HTTPRequest{
		//	Url:
		//}

		_, errHTTPDecode := adapter.extractAdditionalQueueDimensions(context.Background(), reqFailsHTTPDecode, time.Now())
		require.Error(t, errHTTPDecode)
		require.Contains(t, errHTTPDecode.Error(), "net/http")

		//_, errPromDecode := adapter.extractAdditionalQueueDimensions(context.Background(), reqFailsPromDecode, time.Now())
		//require.Error(t, errPromDecode)
		//require.Contains(t, errPromDecode.Error(), "prometheus")
	})

}
