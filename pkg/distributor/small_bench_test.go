// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/util/validation"
)

func BenchmarkOTLPHandlerSmallMessage(b *testing.B) {
	// ~10,000 datapoints: 10 resources * 100 metrics * 10 datapoints
	const numberOfMetrics = 100
	const numberOfResourceMetrics = 10
	const numberOfDatapoints = 10
	const stepDuration = 10 * time.Second

	startTime := time.Date(2020, time.October, 30, 23, 0, 0, 0, time.UTC)

	createMetrics := func(mts pmetric.MetricSlice) {
		mts.EnsureCapacity(numberOfMetrics)
		for idx := range numberOfMetrics {
			mt := mts.AppendEmpty()
			mt.SetName(fmt.Sprintf("metric-%d", idx))
			datapoints := mt.SetEmptyGauge().DataPoints()
			datapoints.EnsureCapacity(numberOfDatapoints)

			sampleTime := startTime
			for j := range numberOfDatapoints {
				datapoint := datapoints.AppendEmpty()
				datapoint.SetTimestamp(pcommon.NewTimestampFromTime(sampleTime))
				datapoint.SetIntValue(int64(j))
				attrs := datapoint.Attributes()
				attrs.PutStr("route", "/hello")
				attrs.PutStr("status", "200")
				sampleTime = sampleTime.Add(stepDuration)
			}
		}
	}

	createScopedMetrics := func(rm pmetric.ResourceMetrics) {
		sms := rm.ScopeMetrics()
		sm := sms.AppendEmpty()
		scope := sm.Scope()
		scope.SetName("scope")
		metrics := sm.Metrics()
		createMetrics(metrics)
	}

	createResourceMetrics := func(md pmetric.Metrics) {
		rms := md.ResourceMetrics()
		rms.EnsureCapacity(numberOfResourceMetrics)
		for idx := range numberOfResourceMetrics {
			rm := rms.AppendEmpty()
			attrs := rm.Resource().Attributes()
			attrs.PutStr("env", "dev")
			attrs.PutStr("region", "us-east-1")
			attrs.PutStr("pod", fmt.Sprintf("pod-%d", idx))
			createScopedMetrics(rm)
		}
	}

	pushFunc := func(_ context.Context, pushReq *Request) error {
		if _, err := pushReq.WriteRequest(); err != nil {
			return err
		}
		pushReq.CleanUp()
		return nil
	}
	limits := validation.MockDefaultOverrides()
	handler := OTLPHandler(
		200000000, nil, nil, limits, nil, nil,
		RetryConfig{}, nil, pushFunc, nil, nil, log.NewNopLogger(),
	)

	b.Run("protobuf", func(b *testing.B) {
		md := pmetric.NewMetrics()
		createResourceMetrics(md)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		req := createOTLPProtoRequest(b, exportReq, "")

		b.ResetTimer()
		for range b.N {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, 200, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})

	b.Run("protobuf_gzip", func(b *testing.B) {
		md := pmetric.NewMetrics()
		createResourceMetrics(md)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		req := createOTLPProtoRequest(b, exportReq, "gzip")

		b.ResetTimer()
		for range b.N {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, 200, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})

	b.Run("protobuf_zstd", func(b *testing.B) {
		md := pmetric.NewMetrics()
		createResourceMetrics(md)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		req := createOTLPProtoRequest(b, exportReq, "zstd")

		b.ResetTimer()
		for range b.N {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			require.Equal(b, 200, resp.Code)
			req.Body.(*reusableReader).Reset()
		}
	})
}
