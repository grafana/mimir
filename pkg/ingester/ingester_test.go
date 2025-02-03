// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/kv"
	dslog "github.com/grafana/dskit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/costattribution"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/usagestats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func mustNewActiveSeriesCustomTrackersConfigFromMap(t *testing.T, source map[string]string) asmodel.CustomTrackersConfig {
	m, err := asmodel.NewCustomTrackersConfig(source)
	require.NoError(t, err)
	return m
}

func TestIngester_StartPushRequest(t *testing.T) {
	const reqSize = 10
	instanceLimits := &InstanceLimits{}
	pushReqState := &pushRequestState{
		requestSize: 10,
	}
	ctx := context.Background()
	ctxWithPushReqState := context.WithValue(ctx, pushReqCtxKey, pushReqState)

	type testCase struct {
		ctx                               context.Context
		failingIngester                   bool
		cbOpen                            bool
		cbInitialDelay                    time.Duration
		instanceLimitReached              bool
		expectedStatus                    bool
		expectedInFlightPushRequests      int64
		expectedInflightPushRequestsBytes int64
		verifyCtxFn                       func(context.Context, context.Context)
		verifyErr                         func(error)
	}

	setupIngester := func(tc testCase) *failingIngester {
		cfg := defaultIngesterTestConfig(t)
		cfg.PushCircuitBreaker = CircuitBreakerConfig{
			Enabled:        true,
			InitialDelay:   tc.cbInitialDelay,
			CooldownPeriod: 10 * time.Second,
		}
		cfg.InstanceLimitsFn = func() *InstanceLimits {
			return instanceLimits
		}
		var (
			failingCause  error
			expectedState services.State
		)
		if tc.failingIngester {
			expectedState = services.Terminated
			failingCause = newUnavailableError(expectedState)
		} else {
			expectedState = services.Running
		}
		failingIng := setupFailingIngester(t, cfg, failingCause)
		failingIng.startWaitAndCheck(ctx, t)
		require.Equal(t, expectedState, failingIng.lifecycler.State())

		if tc.cbOpen {
			failingIng.circuitBreaker.push.cb.Open()
		} else {
			failingIng.circuitBreaker.push.cb.Close()
		}

		if tc.instanceLimitReached {
			instanceLimits.MaxInflightPushRequestsBytes = 1
		} else {
			instanceLimits.MaxInflightPushRequestsBytes = 0
		}

		return failingIng
	}

	testCases := map[string]testCase{
		"fail if ingester is not available for push": {
			failingIngester:                   true,
			ctx:                               ctx,
			expectedStatus:                    false,
			expectedInFlightPushRequests:      0,
			expectedInflightPushRequestsBytes: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &unavailableError{})
			},
		},
		"fail if circuit breaker is open": {
			ctx:                               ctx,
			cbOpen:                            true,
			expectedStatus:                    false,
			expectedInFlightPushRequests:      0,
			expectedInflightPushRequestsBytes: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
			},
		},
		"fail if instance limit is reached": {
			ctx:                               ctx,
			instanceLimitReached:              true,
			expectedStatus:                    false,
			expectedInFlightPushRequests:      0,
			expectedInflightPushRequestsBytes: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &instanceLimitReachedError{})
			},
		},
		"do not fail if circuit breaker is not active": {
			ctx:                               ctx,
			cbInitialDelay:                    1 * time.Minute,
			expectedStatus:                    true,
			expectedInFlightPushRequests:      1,
			expectedInflightPushRequestsBytes: reqSize,
			verifyCtxFn: func(inCtx, outCtx context.Context) {
				require.NotEqual(t, inCtx, outCtx)
			},
		},
		"do not fail and return the same context if it already contains a pushRequestState": {
			ctx:                               ctxWithPushReqState,
			expectedStatus:                    false,
			expectedInFlightPushRequests:      0,
			expectedInflightPushRequestsBytes: 0,
			verifyCtxFn: func(inCtx, outCtx context.Context) {
				require.Equal(t, inCtx, outCtx)
			},
		},
		"do not fail and add pushRequestState to the context if everything is ok": {
			ctx:                               ctx,
			expectedStatus:                    true,
			expectedInFlightPushRequests:      1,
			expectedInflightPushRequestsBytes: reqSize,
			verifyCtxFn: func(inCtx, outCtx context.Context) {
				require.NotEqual(t, inCtx, outCtx)
			},
		},
	}
	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			failingIng := setupIngester(tc)
			defer services.StopAndAwaitTerminated(context.Background(), failingIng) //nolint:errcheck

			ctx, shouldFinish, err := failingIng.startPushRequest(tc.ctx, reqSize)
			require.Equal(t, tc.expectedStatus, shouldFinish)
			require.Equal(t, tc.expectedInFlightPushRequests, failingIng.inflightPushRequests.Load())
			require.Equal(t, tc.expectedInflightPushRequestsBytes, failingIng.inflightPushRequestsBytes.Load())

			if err == nil {
				pushReqState := getPushRequestState(ctx)
				require.NotNil(t, pushReqState)

				if tc.verifyCtxFn != nil {
					tc.verifyCtxFn(tc.ctx, ctx)
				}
			} else {
				require.Nil(t, ctx)
				require.NotNil(t, tc.verifyErr)
				tc.verifyErr(err)
			}
		})
	}
}

func TestIngester_StartReadRequest(t *testing.T) {
	type testCase struct {
		setup                       func(*failingIngester)
		verifyErr                   func(error)
		expectedAcquiredPermitCount int
	}

	var (
		acquiredPermitCount  *atomic.Int64
		recordedSuccessCount *atomic.Int64
		recordedFailureCount *atomic.Int64
		utilizationLimiter   = &fakeUtilizationBasedLimiter{limitingReason: "cpu"}
	)

	setupIngester := func(testCase) *failingIngester {
		cfg := defaultIngesterTestConfig(t)
		cfg.ReadCircuitBreaker = CircuitBreakerConfig{
			Enabled:        true,
			CooldownPeriod: 10 * time.Second,
			RequestTimeout: 30 * time.Second,
		}
		cfg.PushCircuitBreaker = CircuitBreakerConfig{
			Enabled:        true,
			CooldownPeriod: 10 * time.Second,
			RequestTimeout: 2 * time.Second,
		}
		failingIng := newFailingIngester(t, cfg, nil, nil)
		failingIng.startWaitAndCheck(context.Background(), t)
		require.Equal(t, services.Running, failingIng.lifecycler.State())

		acquiredPermitCount = atomic.NewInt64(0)
		recordedSuccessCount = atomic.NewInt64(0)
		recordedFailureCount = atomic.NewInt64(0)

		failingIng.circuitBreaker.read.cb = &mockedCircuitBreaker{
			acquiredPermitCount: acquiredPermitCount,
			recordSuccessCount:  recordedSuccessCount,
			recordFailureCount:  recordedFailureCount,
			CircuitBreaker:      failingIng.circuitBreaker.read.cb,
		}
		failingIng.circuitBreaker.read.cb.Close()

		return failingIng
	}

	testCases := map[string]testCase{
		"fail if ingester is not available for read, and do not acquire a permit": {
			setup: func(failingIng *failingIngester) {
				services.StopAndAwaitTerminated(context.Background(), failingIng) //nolint:errcheck
			},
			expectedAcquiredPermitCount: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &unavailableError{})
			},
		},
		"fail if ingester is overloaded, and do not acquire a permit": {
			setup: func(failingIng *failingIngester) {
				failingIng.utilizationBasedLimiter = utilizationLimiter
			},
			expectedAcquiredPermitCount: 0,
			verifyErr: func(err error) {
				require.ErrorIs(t, err, errTooBusy)
			},
		},
		"fail if read circuit breaker is open, and do not acquire a permit": {
			setup: func(failingIng *failingIngester) {
				failingIng.circuitBreaker.read.cb.Open()
			},
			expectedAcquiredPermitCount: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
			},
		},
		"fail if push circuit breaker is open, and do not acquire a permit": {
			setup: func(failingIng *failingIngester) {
				failingIng.circuitBreaker.push.cb.Open()
			},
			expectedAcquiredPermitCount: 0,
			verifyErr: func(err error) {
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
			},
		},
		"do not fail if read circuit breaker is not active, and do not acquire a permit": {
			setup: func(failingIng *failingIngester) {
				failingIng.circuitBreaker.read.deactivate()
			},
			expectedAcquiredPermitCount: 0,
		},
		"do not fail if everything is ok, and acquire a permit": {
			expectedAcquiredPermitCount: 1,
		},
	}
	for testName, tc := range testCases {
		t.Run(testName, func(t *testing.T) {
			failingIng := setupIngester(tc)
			if tc.setup != nil {
				tc.setup(failingIng)
			}
			defer services.StopAndAwaitTerminated(context.Background(), failingIng) //nolint:errcheck

			ctx, err := failingIng.StartReadRequest(context.Background())
			require.Equal(t, int64(tc.expectedAcquiredPermitCount), acquiredPermitCount.Load())

			if err == nil {
				require.Nil(t, tc.verifyErr)
				require.NotNil(t, ctx)

				// Calling finish must release a potentially acquired permit
				// and in that case record a success, and no failures.
				expectedSuccessCount := acquiredPermitCount.Load()
				failingIng.FinishReadRequest(ctx)
				require.Equal(t, int64(0), acquiredPermitCount.Load())
				require.Equal(t, expectedSuccessCount, recordedSuccessCount.Load())
				require.Equal(t, int64(0), recordedFailureCount.Load())
			} else {
				require.NotNil(t, tc.verifyErr)
				tc.verifyErr(err)
				require.Nil(t, ctx)
			}
		})
	}
}

func TestIngester_Push(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabelSet := mimirpb.FromLabelAdaptersToMetric(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_discarded_samples_total",
		"cortex_ingester_active_series",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_buckets",
		"cortex_ingester_tsdb_head_min_timestamp_seconds",
		"cortex_ingester_tsdb_head_max_timestamp_seconds",
	}
	userID := "test"
	now := time.Now()

	histogramWithBucketCountMismatch := util_test.GenerateTestHistogram(1)
	histogramWithBucketCountMismatch.Count++

	histogramWithCountNotBigEnough := util_test.GenerateTestHistogram(1)
	histogramWithCountNotBigEnough.Sum = math.NaN()
	histogramWithCountNotBigEnough.Count--

	histogramWithNegativeBucketCount := util_test.GenerateTestHistogram(1)
	histogramWithNegativeBucketCount.NegativeBuckets[1] = -100

	histogramWithSpanNegativeOffset := util_test.GenerateTestHistogram(1)
	histogramWithSpanNegativeOffset.PositiveSpans[1].Offset = -2 // The first span can start at negative offset, hence the 1.

	histogramWithSpansBucketsMismatch := util_test.GenerateTestHistogram(1)
	histogramWithSpansBucketsMismatch.PositiveSpans[1].Length++

	tests := map[string]struct {
		reqs                       []*mimirpb.WriteRequest
		expectedErr                error
		expectedIngested           model.Matrix
		expectedMetadataIngested   []*mimirpb.MetricMetadata
		expectedExemplarsIngested  []mimirpb.TimeSeries
		expectedExemplarsDropped   []mimirpb.TimeSeries
		expectedMetrics            string
		additionalMetrics          []string
		disableActiveSeries        bool
		maxExemplars               int
		maxMetadataPerUser         int
		maxMetadataPerMetric       int
		nativeHistograms           bool
		disableOOONativeHistograms bool
		ignoreOOOExemplars         bool
	}{
		"should succeed on valid series and metadata": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					[]*mimirpb.MetricMetadata{
						{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
					},
					mimirpb.API),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					[]*mimirpb.MetricMetadata{
						{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGE},
					},
					mimirpb.API),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGE},
				{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should succeed on valid series with histograms and metadata": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest([]*mimirpb.MetricMetadata{
					{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.HISTOGRAM},
				}, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1, util_test.GenerateTestHistogram(1))}, nil),
				mimirpb.NewWriteRequest([]*mimirpb.MetricMetadata{
					{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGEHISTOGRAM},
				}, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(2, util_test.GenerateTestGaugeHistogram(2))}, nil),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Timestamp: model.Time(1), Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1))},
					{Timestamp: model.Time(2), Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestGaugeHistogram(2))},
				}},
			},
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{MetricFamilyName: "metric_name_2", Help: "a help for metric_name_2", Unit: "", Type: mimirpb.GAUGEHISTOGRAM},
				{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.HISTOGRAM},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.001
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.002
			`,
			nativeHistograms: true,
		},
		"should succeed on new float series with an exemplar": {
			maxExemplars: 2,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
					nil,
					mimirpb.API,
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should succeed on new float series and an exemplar": {
			maxExemplars: 2,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							},
						},
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009
			`,
		},
		"should succeed on existing float series with an exemplar": {
			maxExemplars: 2,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
					nil,
					mimirpb.API,
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should soft fail on new float series and an exemplar and an out of order exemplar": {
			maxExemplars: 2,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							},
						},
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}},
										TimestampMs: 500,
										Value:       2000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newTSDBIngestExemplarErr(storage.ErrOutOfOrderExemplar, model.Time(500), []mimirpb.LabelAdapter{metricLabelAdapters[0]}, []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}}), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
			`,
		},
		"should succeed on new float series and an exemplar and an out of order exemplar but OOO exemplars are ignored": {
			maxExemplars:       2,
			ignoreOOOExemplars: true,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							},
						},
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}},
										TimestampMs: 500,
										Value:       2000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: nil, // Explicit to show that we expect no error due to ignoreOOOExemplars.
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
			`,
		},
		"should succeed on new histogram series with an exemplar": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should succeed on new histogram series and an exemplar": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Histograms: []mimirpb.Histogram{
									mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1)),
								},
							},
						},
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9}}},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should succeed on existing histogram series with an exemplar": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					nil,
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(2))},
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9},
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(2)), Timestamp: 10}},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 1

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should succeed on existing histogram series with multiple exemplars": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					nil,
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(2))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9},
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(2)), Timestamp: 10}},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 2

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 0
			`,
		},
		"should succeed on existing histogram series with partial updated exemplars": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(2))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							// This will never be ingested/appeneded as it is out of order.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}},
							TimestampMs: 1500,
							Value:       1500,
						},
						{
							// This is appended as duplicate.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
						{
							// This is appended as new exemplar and pushes out the oldest exemplar.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "789"}},
							TimestampMs: 3000,
							Value:       3000,
						},
					},
				),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9},
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(2)), Timestamp: 10}},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "789"}},
							TimestampMs: 3000,
							Value:       3000,
						},
					},
				},
			},
			expectedExemplarsDropped: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							// This examplar is dropped due to the max exemplar limit.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							// This example is ignored as equal to the previous one.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 3

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 2

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 4

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
			`,
		},
		"should soft fail if histogram has a bucket count vs overall count mismatch": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, histogramWithBucketCountMismatch)},
					nil,
				),
			},
			// Expect the error string instead of constructing the error to catch if Prometheus changes the error message.
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramCountMismatch, fmt.Errorf("21 observations found in buckets, but the Count field is 22: histogram's observation count should equal the number of observations found in the buckets (in absence of NaN)"), model.Time(10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail if histogram has a bucket count higher than overall count and sum NaN": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, histogramWithCountNotBigEnough)},
					nil,
				),
			},
			// Expect the error string instead of constructing the error to catch if Prometheus changes the error message.
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramCountNotBigEnough, fmt.Errorf("21 observations found in buckets, but the Count field is 20: histogram's observation count should be at least the number of observations found in the buckets"), model.Time(10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail if histogram has a negative span offset": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, histogramWithSpanNegativeOffset)},
					nil,
				),
			},
			// Expect the error string instead of constructing the error to catch if Prometheus changes the error message.
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramSpanNegativeOffset, fmt.Errorf("positive side: span number 2 with offset -2: histogram has a span whose offset is negative"), model.Time(10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail if histogram has different number of buckets than encoded in spans": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, histogramWithSpansBucketsMismatch)},
					nil,
				),
			},
			// Expect the error string instead of constructing the error to catch if Prometheus changes the error message.
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramSpansBucketsMismatch, fmt.Errorf("positive side: spans need 5 buckets, have 4 buckets: histogram spans specify different number of buckets than provided"), model.Time(10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail if OOO native histograms are disabled": {
			nativeHistograms:           true,
			disableOOONativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(1))},
					nil,
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(-10, util_test.GenerateTestHistogram(1))},
					nil,
				),
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramOOODisabled, fmt.Errorf("out-of-order native histogram ingestion is disabled"), model.Time(-10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 10}},
				},
			},
		},
		"should soft fail if histogram has a negative bucket count": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, histogramWithNegativeBucketCount)},
					nil,
				),
			},
			// Expect the error string instead of constructing the error to catch if Prometheus changes the error message.
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newNativeHistogramValidationError(globalerror.NativeHistogramNegativeBucketCount, fmt.Errorf("negative side: bucket number 2 has observation count of -98: histogram has a bucket whose observation count is negative"), model.Time(10), []mimirpb.LabelAdapter{metricLabelAdapters[0]}), userID), codes.InvalidArgument),
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="invalid-native-histogram",user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail on existing histogram series if all exemplars are out of order": {
			maxExemplars:     2,
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(2))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							// This will never be ingested/appeneded as it is out of order.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							// This will never be ingested/appeneded as it is out of order.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}},
							TimestampMs: 1500,
							Value:       1500,
						},
					},
				),
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newTSDBIngestExemplarErr(storage.ErrOutOfOrderExemplar, model.Time(1500), []mimirpb.LabelAdapter{metricLabelAdapters[0]}, []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}}), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9},
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(2)), Timestamp: 10}},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 2

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 2
			`,
		},
		"should succeed on existing histogram series if all exemplars are out of order but OOO exemplars are ignored": {
			maxExemplars:       2,
			nativeHistograms:   true,
			ignoreOOOExemplars: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(9, util_test.GenerateTestHistogram(1))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				),
				mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(10, util_test.GenerateTestHistogram(2))},
					nil,
				).AddExemplarsAt(0, // Add exemplars to the first series.
					[]*mimirpb.Exemplar{
						{
							// This will never be ingested/appeneded as it is out of order.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							// This will never be ingested/appeneded as it is out of order.
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "333"}},
							TimestampMs: 1500,
							Value:       1500,
						},
					},
				),
			},
			expectedErr: nil, // Explicit to show that we expect no error due to ignoreOOOExemplars.
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(1)), Timestamp: 9},
					{Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestHistogram(2)), Timestamp: 10}},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 2000,
							Value:       2000,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 2

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 2

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 2
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					mimirpb.API),
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}, {Value: 2, Timestamp: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail on sample out-of-order": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleOutOfOrderError(model.Time(9), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.01
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.01
			`,
		},
		"should soft fail on all samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  metricLabelAdapters,
								Samples: []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)}, {Value: 1, TimestampMs: 1575043969 - (86000 * 1000)}},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
			`,
		},
		"should soft fail on all samples with histograms out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 3 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:     metricLabelAdapters,
								Samples:    []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)}, {Value: 1, TimestampMs: 1575043969 - (86000 * 1000)}},
								Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1575043969-(86800*1000), util_test.GenerateTestHistogram(0))},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86800*1000)), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 3
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 3
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
			`,
			nativeHistograms: true,
		},
		"should succeed if histograms are out of bound but samples are not and histograms are not accepted": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:     metricLabelAdapters,
								Samples:    []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 + 1000}},
								Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1575043969-(86800*1000), util_test.GenerateTestHistogram(0))},
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}, {Value: 0, Timestamp: 1575043969 + 1000}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 1575044.969
			`,
			nativeHistograms: false,
		},
		"should soft fail on some samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)},
									{Value: 1, TimestampMs: 1575043969 - (86000 * 1000)},
									{Value: 3, TimestampMs: 1575043969 + 1}},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}, {Value: 3, Timestamp: 1575043969 + 1}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.970
			`,
		},
		"should soft fail on some samples with timestamp too far in future in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: now.UnixMilli()}},
					nil,
					nil,
					mimirpb.API,
				),
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 2, TimestampMs: now.UnixMilli() + (86400 * 1000)},
									{Value: 3, TimestampMs: now.UnixMilli() + 1}},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{
					{Value: 1, Timestamp: model.Time(now.UnixMilli())},
					{Value: 3, Timestamp: model.Time(now.UnixMilli() + 1)},
				}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli())/1000) + `
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli()+1)/1000) + `
			`,
		},
		"should soft fail on some histograms with timestamp too far in future in a write request": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Histograms: []mimirpb.Histogram{
									mimirpb.FromHistogramToHistogramProto(now.UnixMilli(), util_test.GenerateTestHistogram(0)),
									mimirpb.FromHistogramToHistogramProto(now.UnixMilli()+(86400*1000), util_test.GenerateTestHistogram(1))},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Histograms: []model.SampleHistogramPair{
					{Timestamp: model.Time(now.UnixMilli()), Histogram: mimirpb.FromHistogramToPromHistogram(util_test.GenerateTestGaugeHistogram(0))},
				}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
				# TYPE cortex_ingester_active_native_histogram_buckets gauge
				cortex_ingester_active_native_histogram_buckets{user="test"} 8
				# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
				# TYPE cortex_ingester_active_native_histogram_series gauge
				cortex_ingester_active_native_histogram_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli())/1000) + `
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli())/1000) + `
			`,
		},
		"should soft fail on some exemplars with timestamp too far in future in a write request": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 1, TimestampMs: now.UnixMilli()}},
								Exemplars: []mimirpb.Exemplar{
									{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "111"}}, Value: 1, TimestampMs: now.UnixMilli()},
									{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "222"}}, Value: 2, TimestampMs: now.UnixMilli() + (86400 * 1000)},
								},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "222"}}), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{
					Metric: metricLabelSet,
					Values: []model.SamplePair{
						{Value: 1, Timestamp: model.Time(now.UnixMilli())},
					},
				},
			},
			expectedExemplarsIngested: []mimirpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "111"}}, TimestampMs: now.UnixMilli(), Value: 1},
					},
				},
			},
			additionalMetrics: []string{
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 1
				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli())/1000) + `
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds ` + fmt.Sprintf("%g", float64(now.UnixMilli())/1000) + `
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newSampleDuplicateTimestampError(model.Time(1575043969), metricLabelAdapters), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 2, Timestamp: 1575043969}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 1575043.969
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 1575043.969
			`,
		},
		"should soft fail on exemplar with unknown series": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				// This is not done here.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr:              newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarMissingSeriesError(model.Time(1000), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}), userID), codes.InvalidArgument),
			expectedIngested:         nil,
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 0
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 0

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 0

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
			`,
		},
		"should soft fail on exemplar with series later in the same write request": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				// This is done too late here.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							},
						},
					},
				},
			},
			expectedErr: newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarMissingSeriesError(model.Time(1000), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}), userID), codes.InvalidArgument),
			expectedIngested: model.Matrix{
				&model.SampleStream{Metric: metricLabelSet, Values: []model.SamplePair{{Value: 1, Timestamp: 9}}},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_ingested_exemplars_total",
				"cortex_ingester_ingested_exemplars_failures_total",
			},
			expectedMetrics: `
			    # HELP cortex_ingester_active_series Number of currently active series per user.
			    # TYPE cortex_ingester_active_series gauge
			    cortex_ingester_active_series{user="test"} 1
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total{user="test"} 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total{user="test"} 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 0

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out-of-order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0

				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0.009

				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0.009

				# HELP cortex_ingester_ingested_exemplars_total The total number of exemplars ingested.
				# TYPE cortex_ingester_ingested_exemplars_total counter
				cortex_ingester_ingested_exemplars_total 0

				# HELP cortex_ingester_ingested_exemplars_failures_total The total number of exemplars that errored on ingestion.
				# TYPE cortex_ingester_ingested_exemplars_failures_total counter
				cortex_ingester_ingested_exemplars_failures_total 1
			`,
		},
		"should succeed with a request containing only metadata": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				{
					Metadata: []*mimirpb.MetricMetadata{
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
					},
				},
			},
			expectedErr:      nil,
			expectedIngested: nil,
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric", Help: "This is a test metric."},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_active_appenders",
			},
			// NOTE cortex_ingester_memory_users is 0 here - the metric really counts tsdbs not users.
			// we may want to change that one day but for now make the test match the code.
			expectedMetrics: `
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
                cortex_ingester_memory_users 0
				# HELP cortex_ingester_tsdb_head_active_appenders Number of currently active TSDB appender transactions.
				# TYPE cortex_ingester_tsdb_head_active_appenders gauge
				cortex_ingester_tsdb_head_active_appenders 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0
			`,
		},
		"should discard metadata when max metadata per user exceeded": {
			maxMetadataPerUser:   1,
			maxMetadataPerMetric: 0,
			reqs: []*mimirpb.WriteRequest{
				{
					Metadata: []*mimirpb.MetricMetadata{
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "This is a test metric."},
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_2", Help: "This is a test metric."},
					},
				},
			},
			expectedErr:      nil,
			expectedIngested: nil,
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "This is a test metric."},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
				"cortex_discarded_metadata_total",
			},
			expectedMetrics: `
				# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
				# TYPE cortex_discarded_metadata_total counter
				cortex_discarded_metadata_total{reason="per_user_metadata_limit",user="test"} 1
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 1
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 1
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 1
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0
			`,
		},
		"should discard metadata when max metadata per metric exceeded": {
			maxMetadataPerUser:   0,
			maxMetadataPerMetric: 1,
			reqs: []*mimirpb.WriteRequest{
				{
					Metadata: []*mimirpb.MetricMetadata{
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "This is a test metric."},
						{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "A test metric this is."},
					},
				},
			},
			expectedErr:      nil,
			expectedIngested: nil,
			expectedMetadataIngested: []*mimirpb.MetricMetadata{
				{Type: mimirpb.COUNTER, MetricFamilyName: "test_metric_1", Help: "This is a test metric."},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
				"cortex_discarded_metadata_total",
			},
			expectedMetrics: `
				# HELP cortex_discarded_metadata_total The total number of metadata that were discarded.
				# TYPE cortex_discarded_metadata_total counter
				cortex_discarded_metadata_total{reason="per_metric_metadata_limit",user="test"} 1
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 1
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 1
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 1
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 0
				# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
				cortex_ingester_tsdb_head_min_timestamp_seconds 0
				# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
				# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
				cortex_ingester_tsdb_head_max_timestamp_seconds 0
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			memorySeriesStats.Set(0)
			memoryTenantsStats.Set(0)
			tenantsWithOutOfOrderEnabledStat.Set(0)
			minOutOfOrderTimeWindowSecondsStat.Set(0)
			maxOutOfOrderTimeWindowSecondsStat.Set(0)
			appendedSamplesStatsBefore := appendedSamplesStats.Total()
			appendedExemplarsStatsBefore := appendedExemplarsStats.Total()

			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.IngesterRing.ReplicationFactor = 1
			cfg.ActiveSeriesMetrics.Enabled = !testData.disableActiveSeries
			limits := defaultLimitsTestConfig()
			limits.MaxGlobalExemplarsPerUser = testData.maxExemplars
			limits.MaxGlobalMetricsWithMetadataPerUser = testData.maxMetadataPerUser
			limits.MaxGlobalMetadataPerMetric = testData.maxMetadataPerMetric
			limits.NativeHistogramsIngestionEnabled = testData.nativeHistograms
			limits.IgnoreOOOExemplars = testData.ignoreOOOExemplars
			var oooTimeWindow int64
			if testData.disableOOONativeHistograms {
				oooTimeWindow = int64(1 * time.Hour.Seconds())
				limits.OutOfOrderTimeWindow = model.Duration(1 * time.Hour)
				limits.OOONativeHistogramsIngestionEnabled = false
			}

			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is healthy and owns tokens. Note that the timeout here is set
			// such that it is longer than the MinReadyDuration configuration for the ingester ring.
			test.Poll(t, time.Second, nil, func() interface{} {
				return i.lifecycler.CheckReady(context.Background())
			})

			// Push timeseries
			for idx, req := range testData.reqs {
				// Push metrics to the ingester. Override the default cleanup method of mimirpb.ReuseSlice with a no-op one.
				err := i.PushWithCleanup(ctx, req, func() {})

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					assert.NoError(t, err)
				} else {
					if testData.expectedErr == nil {
						assert.NoError(t, err)
					} else {
						require.Error(t, err)
						handledErr := mapPushErrorToErrorWithStatus(err)
						errWithStatus, ok := handledErr.(globalerror.ErrorWithStatus)
						require.True(t, ok)
						require.Truef(t, errWithStatus.Equals(testData.expectedErr), "errors don't match \nactual:   '%v'\nexpected: '%v'", errWithStatus, testData.expectedErr)
					}
				}
			}

			// Read back samples to see what has been really ingested
			s := &stream{ctx: ctx}
			err = i.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
			}, s)
			require.NoError(t, err)

			res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)
			if len(res) == 0 {
				res = nil
			}
			require.Equal(t, testData.expectedIngested, res)

			// Read back samples to see what has been really ingested
			exemplarRes, err := i.QueryExemplars(ctx, &client.ExemplarQueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers: []*client.LabelMatchers{
					{Matchers: []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}}},
				},
			})

			require.NoError(t, err)
			require.NotNil(t, exemplarRes)
			assert.Equal(t, testData.expectedExemplarsIngested, exemplarRes.Timeseries)

			// Read back metadata to see what has been really ingested.
			mres, err := i.MetricsMetadata(ctx, client.DefaultMetricsMetadataRequest())

			require.NoError(t, err)
			require.NotNil(t, mres)

			// Order is never guaranteed.
			assert.ElementsMatch(t, testData.expectedMetadataIngested, mres.Metadata)

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.updateActiveSeries(time.Now())
			}

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)

			// Check anonymous usage stats.
			expectedTenantsCount := 0
			expectedSamplesCount := 0
			expectedHistogramsCount := 0
			expectedExemplarsCount := 0
			if len(testData.expectedIngested) > 0 {
				expectedTenantsCount = 1
			}
			for _, stream := range testData.expectedIngested {
				expectedSamplesCount += len(stream.Values) + len(stream.Histograms)
				expectedHistogramsCount += len(stream.Histograms)
			}
			for _, series := range testData.expectedExemplarsIngested {
				expectedExemplarsCount += len(series.Exemplars)
			}
			for _, series := range testData.expectedExemplarsDropped {
				expectedExemplarsCount += len(series.Exemplars)
			}

			i.updateUsageStats()

			if !testData.disableActiveSeries {
				assert.Equal(t, int64(len(testData.expectedIngested)), usagestats.GetInt(activeSeriesStatsName).Value())
			}
			assert.Equal(t, int64(len(testData.expectedIngested)), usagestats.GetInt(memorySeriesStatsName).Value())
			assert.Equal(t, int64(expectedTenantsCount), usagestats.GetInt(memoryTenantsStatsName).Value())
			assert.Equal(t, int64(expectedSamplesCount)+appendedSamplesStatsBefore, usagestats.GetCounter(appendedSamplesStatsName).Total())
			assert.Equal(t, int64(expectedExemplarsCount)+appendedExemplarsStatsBefore, usagestats.GetCounter(appendedExemplarsStatsName).Total())

			assert.Equal(t, testData.disableOOONativeHistograms, usagestats.GetInt(tenantsWithOutOfOrderEnabledStatName).Value() == int64(1))
			assert.Equal(t, oooTimeWindow, usagestats.GetInt(minOutOfOrderTimeWindowSecondsStatName).Value())
			assert.Equal(t, oooTimeWindow, usagestats.GetInt(maxOutOfOrderTimeWindowSecondsStatName).Value())
		})
	}
}

func TestIngester_Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
	metricLabelAdaptersHist := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test_histogram"}}}
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_buckets",
		"cortex_ingester_tsdb_head_min_timestamp_seconds",
		"cortex_ingester_tsdb_head_max_timestamp_seconds",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push timeseries for each user
	for ix, userID := range []string{"test-1", "test-2"} {
		reqs := []*mimirpb.WriteRequest{
			mimirpb.ToWriteRequest(
				metricLabelAdapters,
				[]mimirpb.Sample{{Value: 1, TimestampMs: int64(ix)*1000 + 9}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.ToWriteRequest(
				metricLabelAdapters,
				[]mimirpb.Sample{{Value: 2, TimestampMs: int64(ix)*1000 + 10}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(metricLabelAdaptersHist,
				[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(7, util_test.GenerateTestHistogram(1))}, nil),
			mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(metricLabelAdaptersHist,
				[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(8, util_test.GenerateTestGaugeHistogram(2))}, nil),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series for metrics check.
	i.updateActiveSeries(time.Now())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="test-1"} 4
		cortex_ingester_ingested_samples_total{user="test-2"} 4
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total{user="test-1"} 0
		cortex_ingester_ingested_samples_failures_total{user="test-2"} 0
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 2
		# HELP cortex_ingester_memory_series The current number of series in memory.
		# TYPE cortex_ingester_memory_series gauge
		cortex_ingester_memory_series 4
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 2
		cortex_ingester_memory_series_created_total{user="test-2"} 2
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 2
		cortex_ingester_active_series{user="test-2"} 2
		# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
		# TYPE cortex_ingester_active_native_histogram_series gauge
		cortex_ingester_active_native_histogram_series{user="test-1"} 1
		cortex_ingester_active_native_histogram_series{user="test-2"} 1
		# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
		# TYPE cortex_ingester_active_native_histogram_buckets gauge
		cortex_ingester_active_native_histogram_buckets{user="test-1"} 8
		cortex_ingester_active_native_histogram_buckets{user="test-2"} 8
		# HELP cortex_ingester_tsdb_head_min_timestamp_seconds Minimum timestamp of the head block across all tenants.
		# TYPE cortex_ingester_tsdb_head_min_timestamp_seconds gauge
		cortex_ingester_tsdb_head_min_timestamp_seconds 0.007
		# HELP cortex_ingester_tsdb_head_max_timestamp_seconds Maximum timestamp of the head block across all tenants.
		# TYPE cortex_ingester_tsdb_head_max_timestamp_seconds gauge
		cortex_ingester_tsdb_head_max_timestamp_seconds 1.01
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_Push_DecreaseInactiveSeries(t *testing.T) {
	metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
	metricLabelAdaptersHist := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test_histogram"}}}
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_buckets",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	currentTime := time.Now()
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*mimirpb.WriteRequest{
			mimirpb.ToWriteRequest(
				metricLabelAdapters,
				[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.ToWriteRequest(
				metricLabelAdapters,
				[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				mimirpb.API,
			),
			mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(metricLabelAdaptersHist,
				[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(7, util_test.GenerateTestHistogram(1))}, nil),
			mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(metricLabelAdaptersHist,
				[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(8, util_test.GenerateTestGaugeHistogram(2))}, nil),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series the after the idle timeout (in the future).
	// This will remove inactive series.
	currentTime = currentTime.Add(cfg.ActiveSeriesMetrics.IdleTimeout + 1*time.Second)
	i.updateActiveSeries(currentTime)

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 2
		cortex_ingester_memory_series_created_total{user="test-2"} 2
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func BenchmarkIngesterPush(b *testing.B) {
	costAttributionCases := []struct {
		state          string
		limitsCfg      func(*validation.Limits)
		customRegistry *prometheus.Registry
	}{
		{
			state:          "enabled",
			limitsCfg:      func(*validation.Limits) {},
			customRegistry: nil,
		},
		{
			state: "disabled",
			limitsCfg: func(limits *validation.Limits) {
				if limits == nil {
					return
				}
				limits.CostAttributionLabels = []string{"cpu"}
				limits.MaxCostAttributionCardinalityPerUser = 100
			},
			customRegistry: prometheus.NewRegistry(),
		},
	}

	tests := []struct {
		name      string
		limitsCfg func() validation.Limits
	}{
		{
			name: "ingester push succeeded",
			limitsCfg: func() validation.Limits {
				limitsCfg := defaultLimitsTestConfig()
				limitsCfg.NativeHistogramsIngestionEnabled = true
				return limitsCfg
			},
		},
	}

	for _, caCase := range costAttributionCases {
		b.Run(fmt.Sprintf("cost_attribution=%s", caCase.state), func(b *testing.B) {
			for _, t := range tests {
				b.Run(fmt.Sprintf("scenario=%s", t.name), func(b *testing.B) {
					registry := prometheus.NewRegistry()
					ctx := user.InjectOrgID(context.Background(), userID)

					// Create a mocked ingester
					cfg := defaultIngesterTestConfig(b)

					limitCfg := t.limitsCfg()
					caCase.limitsCfg(&limitCfg)

					overrides, err := validation.NewOverrides(limitCfg, nil)
					require.NoError(b, err)

					var cam *costattribution.Manager
					if caCase.customRegistry != nil {
						cam, err = costattribution.NewManager(5*time.Second, 10*time.Second, nil, overrides, caCase.customRegistry)
						require.NoError(b, err)
					}

					ingester, err := prepareIngesterWithBlockStorageOverridesAndCostAttribution(b, cfg, overrides, nil, "", "", registry, cam)
					require.NoError(b, err)
					require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))

					b.Cleanup(func() {
						require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ingester))
					})

					// Wait until the ingester is healthy
					test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
						return ingester.lifecycler.HealthyInstancesCount()
					})

					// Push a single time series to set the TSDB min time.
					metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
					startTime := util.TimeToMillis(time.Now())

					currTimeReq := mimirpb.ToWriteRequest(
						metricLabelAdapters,
						[]mimirpb.Sample{{Value: 1, TimestampMs: startTime}},
						nil,
						nil,
						mimirpb.API,
					)
					_, err = ingester.Push(ctx, currTimeReq)
					require.NoError(b, err)

					// so we are benchmark 5000 series with 10 sample each
					const (
						series  = 5000
						samples = 10
					)

					allLabels, allSamples := benchmarkData(series)

					b.ResetTimer()
					for iter := 0; iter < b.N; iter++ {
						// Bump the timestamp on each of our test samples each time round the loop
						for j := 0; j < samples; j++ {
							for i := range allSamples {
								allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
							}
							_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(allLabels, allSamples, nil, nil, mimirpb.API))
							require.NoError(b, err)
						}
					}
				})
			}
		})
	}
}

func verifyErrorString(tb testing.TB, err error, expectedErr string) {
	if err == nil || !strings.Contains(err.Error(), expectedErr) {
		tb.Helper()
		tb.Fatalf("unexpected error. expected: %s actual: %v", expectedErr, err)
	}
}

func Benchmark_Ingester_PushOnError(b *testing.B) {
	var (
		ctx             = user.InjectOrgID(context.Background(), userID)
		sampleTimestamp = int64(100)
		metricName      = "test"
	)

	scenarios := map[string]struct {
		numSeriesPerRequest  int
		numConcurrentClients int
	}{
		"no walReplayConcurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 1,
		},
		"low walReplayConcurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 100,
		},
		"high walReplayConcurrency": {
			numSeriesPerRequest:  500,
			numConcurrentClients: 1000,
		},
		"low number of series per request and very high walReplayConcurrency": {
			numSeriesPerRequest:  100,
			numConcurrentClients: 2500,
		},
	}

	instanceLimits := map[string]*InstanceLimits{
		"no limits":  nil,
		"limits set": {MaxIngestionRate: 1e12, MaxInMemoryTenants: 1, MaxInMemorySeries: 500, MaxInflightPushRequests: 2500}, // these match max values from scenarios
	}

	tests := map[string]struct {
		// If this returns false, test is skipped.
		prepareConfig   func(limits *validation.Limits, instanceLimits *InstanceLimits) bool
		beforeBenchmark func(b *testing.B, ingester *Ingester, numSeriesPerRequest int)
		runBenchmark    func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample)
	}{
		"out of bound samples": {
			prepareConfig: func(*validation.Limits, *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				// Push a single time series to set the TSDB min time.
				currTimeReq := mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: metricName}}},
					[]mimirpb.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				expectedErr := storage.ErrOutOfBounds.Error()

				// Push out of bound samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"out-of-order samples": {
			prepareConfig: func(*validation.Limits, *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// For each series, push a single sample with a timestamp greater than next pushes.
				for i := 0; i < numSeriesPerRequest; i++ {
					currTimeReq := mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
						nil,
						nil,
						mimirpb.API)

					_, err := ingester.Push(ctx, currTimeReq)
					require.NoError(b, err)
				}
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				expectedErr := storage.ErrOutOfOrderSample.Error()

				// Push out-of-order samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"per-user series limit reached": {
			prepareConfig: func(limits *validation.Limits, _ *InstanceLimits) bool {
				limits.MaxGlobalSeriesPerUser = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				// Push a series with a metric name different than the one used during the benchmark.
				currTimeReq := mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "another"}}},
					[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				// Push series with a different name than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-user series limit")
				}
			},
		},
		"per-metric series limit reached": {
			prepareConfig: func(limits *validation.Limits, _ *InstanceLimits) bool {
				limits.MaxGlobalSeriesPerMetric = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				// Push a series with the same metric name but different labels than the one used during the benchmark.
				currTimeReq := mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: "another"}}},
					[]mimirpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					mimirpb.API,
				)
				_, err := ingester.Push(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-metric series limit")
				}
			},
		},
		"very low ingestion rate limit": {
			prepareConfig: func(_ *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxIngestionRate = 0.00001 // very low
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				// Send a lot of samples
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)

				ingester.ingestionRate.Tick()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "push rate limit reached")
				}
			},
		},
		"max number of tenants reached": {
			prepareConfig: func(_ *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemoryTenants = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				// Send some samples for one tenant (not the same that is used during the test)
				ctx := user.InjectOrgID(context.Background(), "different_tenant")
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "max tenants limit reached")
				}
			},
		},
		"max number of series reached": {
			prepareConfig: func(_ *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemorySeries = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, _ int) {
				_, err := ingester.Push(ctx, generateSamplesForLabel(labels.FromStrings(labels.MetricName, "test"), 1, 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "max series limit reached")
				}
			},
		},
		"max inflight requests reached": {
			prepareConfig: func(_ *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInflightPushRequests = 1
				return true
			},
			beforeBenchmark: func(_ *testing.B, ingester *Ingester, _ int) {
				ingester.inflightPushRequests.Inc()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics [][]mimirpb.LabelAdapter, samples []mimirpb.Sample) {
				for n := 0; n < b.N; n++ {
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))
					verifyErrorString(b, err, "too many inflight push requests")
				}
			},
		},
	}

	for testName, testData := range tests {
		for scenarioName, scenario := range scenarios {
			for limitsName, limits := range instanceLimits {
				b.Run(fmt.Sprintf("failure: %s, scenario: %s, limits: %s", testName, scenarioName, limitsName), func(b *testing.B) {
					registry := prometheus.NewRegistry()

					instanceLimits := limits
					if instanceLimits != nil {
						// make a copy, to avoid changing value in the instanceLimits map.
						newLimits := &InstanceLimits{}
						*newLimits = *instanceLimits
						instanceLimits = newLimits
					}

					// Create a mocked ingester
					cfg := defaultIngesterTestConfig(b)

					limits := defaultLimitsTestConfig()
					if !testData.prepareConfig(&limits, instanceLimits) {
						b.SkipNow()
					}

					cfg.InstanceLimitsFn = func() *InstanceLimits {
						return instanceLimits
					}

					ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", registry)
					require.NoError(b, err)
					require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
					defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

					// Wait until the ingester is healthy
					test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
						return ingester.lifecycler.HealthyInstancesCount()
					})

					testData.beforeBenchmark(b, ingester, scenario.numSeriesPerRequest)

					// Prepare the request.
					metrics := make([][]mimirpb.LabelAdapter, 0, scenario.numSeriesPerRequest)
					samples := make([]mimirpb.Sample, 0, scenario.numSeriesPerRequest)
					for i := 0; i < scenario.numSeriesPerRequest; i++ {
						metrics = append(metrics, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}})
						samples = append(samples, mimirpb.Sample{Value: float64(i), TimestampMs: sampleTimestamp})
					}

					// Run the benchmark.
					wg := sync.WaitGroup{}
					wg.Add(scenario.numConcurrentClients)
					start := make(chan struct{})

					b.ReportAllocs()
					b.ResetTimer()

					for c := 0; c < scenario.numConcurrentClients; c++ {
						go func() {
							defer wg.Done()
							<-start

							testData.runBenchmark(b, ingester, metrics, samples)
						}()
					}

					b.ResetTimer()
					close(start)
					wg.Wait()
				})
			}
		}
	}
}

func Test_Ingester_LabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
		{labels.FromStrings(labels.MetricName, "test_3", "status", "500"), 2, 200000},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	t.Run("without matchers", func(t *testing.T) {
		expected := []string{"__name__", "status", "route"}

		// Get label names
		res, err := i.LabelNames(ctx, &client.LabelNamesRequest{EndTimestampMs: math.MaxInt64})
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("with matchers", func(t *testing.T) {
		// test_2 and test_3 are selected in this test, they don't have the "route" label
		expected := []string{"__name__", "status"}

		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchNotEqual, "route", "get_user"),
		}

		req, err := client.ToLabelNamesRequest(0, model.Latest, nil, matchers)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("without matchers, with limit", func(t *testing.T) {
		expected := []string{"__name__", "route"}

		hints := &storage.LabelHints{Limit: 2}

		req, err := client.ToLabelNamesRequest(0, model.Latest, hints, nil)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("without matchers, with limit set to 0", func(t *testing.T) {
		expected := []string{"__name__", "status", "route"}

		hints := &storage.LabelHints{Limit: 0}

		req, err := client.ToLabelNamesRequest(0, model.Latest, hints, nil)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("without matchers, with limit set to same number of items returned", func(t *testing.T) {
		expected := []string{"__name__", "status", "route"}

		hints := &storage.LabelHints{Limit: 3}

		req, err := client.ToLabelNamesRequest(0, model.Latest, hints, nil)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})

	t.Run("without matchers, with limit set to a higher number than the items returned", func(t *testing.T) {
		expected := []string{"__name__", "status", "route"}

		hints := &storage.LabelHints{Limit: 10}

		req, err := client.ToLabelNamesRequest(0, model.Latest, hints, nil)
		require.NoError(t, err)

		// Get label names
		res, err := i.LabelNames(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expected, res.LabelNames)
	})
}

func Test_Ingester_LabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	expected := map[string][]string{
		"__name__": {"test_1", "test_2"},
		"status":   {"200", "500"},
		"route":    {"get_user"},
		"unknown":  {},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Get label values
	for labelName, expectedValues := range expected {
		req := &client.LabelValuesRequest{LabelName: labelName, EndTimestampMs: math.MaxInt64}
		res, err := i.LabelValues(ctx, req)
		require.NoError(t, err)
		assert.ElementsMatch(t, expectedValues, res.LabelValues)
	}

	expectedLimit := map[int][]string{
		0: {"test_1", "test_2"}, // no limit
		1: {"test_1"},
		2: {"test_1", "test_2"}, // limit equals to the number of results
		4: {"test_1", "test_2"}, // limit greater than the number of results
	}
	t.Run("with limit", func(t *testing.T) {
		for limit, expectedValues := range expectedLimit {
			hints := &storage.LabelHints{Limit: limit}

			req, err := client.ToLabelValuesRequest("__name__", 0, model.Latest, hints, nil)
			require.NoError(t, err)

			// Get label names
			res, err := i.LabelValues(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, expectedValues, res.LabelValues)
		}
	})

	t.Run("limited due to resource utilization", func(t *testing.T) {
		origLimiter := i.utilizationBasedLimiter
		t.Cleanup(func() {
			i.utilizationBasedLimiter = origLimiter
		})
		i.utilizationBasedLimiter = &fakeUtilizationBasedLimiter{limitingReason: "cpu"}

		_, err := i.LabelValues(ctx, &client.LabelValuesRequest{})
		stat, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, stat.Code())
		require.Equal(t, ingesterTooBusyMsg, stat.Message())
		verifyUtilizationLimitedRequestsMetric(t, registry)
	})
}

func l2m(lbls labels.Labels) model.Metric {
	m := make(model.Metric, 16)
	lbls.Range(func(l labels.Label) {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	})
	return m
}

func Test_Ingester_Query(t *testing.T) {
	series := []series{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatcher
		expected model.Matrix
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: model.Matrix{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
				&model.SampleStream{Metric: l2m(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[2].lbls), Values: []model.SamplePair{{Value: 2, Timestamp: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
				&model.SampleStream{Metric: l2m(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[2].lbls), Values: []model.SamplePair{{Value: 2, Timestamp: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[1].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: model.Matrix{
				&model.SampleStream{Metric: l2m(series[0].lbls), Values: []model.SamplePair{{Value: 1, Timestamp: 100000}}},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, streamingEnabled := range []bool{true, false} {
				t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
					req := &client.QueryRequest{
						StartTimestampMs: testData.from,
						EndTimestampMs:   testData.to,
						Matchers:         testData.matchers,
					}

					if streamingEnabled {
						req.StreamingChunksBatchSize = 64
					}

					s := stream{ctx: ctx}
					err = i.QueryStream(req, &s)
					require.NoError(t, err)

					res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
					require.NoError(t, err)
					assert.ElementsMatch(t, testData.expected, res)
				})
			}
		})
	}
}

func TestIngester_LabelNamesAndValues(t *testing.T) {
	series := []series{
		{labels.FromStrings(labels.MetricName, "metric_0", "status", "500"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "metric_0", "status", "200"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "metric_1", "env", "prod"), 2, 200000},
		{labels.FromStrings(labels.MetricName, "metric_1", "env", "prod", "status", "300"), 3, 200000},
	}

	tests := []struct {
		testName string
		matchers []*client.LabelMatcher
		expected []*client.LabelValues
	}{
		{testName: "expected all label with values",
			matchers: []*client.LabelMatcher{},
			expected: []*client.LabelValues{
				{LabelName: labels.MetricName, Values: []string{"metric_0", "metric_1"}},
				{LabelName: "status", Values: []string{"200", "300", "500"}},
				{LabelName: "env", Values: []string{"prod"}}},
		},
		{testName: "expected label values only from `metric_0`",
			matchers: []*client.LabelMatcher{{Type: client.EQUAL, Name: "__name__", Value: "metric_0"}},
			expected: []*client.LabelValues{
				{LabelName: labels.MetricName, Values: []string{"metric_0"}},
				{LabelName: "status", Values: []string{"200", "500"}},
			},
		},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)

	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	// Run tests
	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			req := &client.LabelNamesAndValuesRequest{
				Matchers: tc.matchers,
			}

			s := mockLabelNamesAndValuesServer{context: ctx}
			require.NoError(t, i.LabelNamesAndValues(req, &s))

			assert.ElementsMatch(t, extractItemsWithSortedValues(s.SentResponses), tc.expected)
		})
	}
}

func TestIngester_LabelValuesCardinality(t *testing.T) {
	series := []series{
		{
			lbls:      labels.FromStrings(labels.MetricName, "metric_0", "status", "500"),
			value:     1.5,
			timestamp: 100000,
		},
		{
			lbls:      labels.FromStrings(labels.MetricName, "metric_0", "status", "200"),
			value:     1.5,
			timestamp: 110030,
		},
		{
			lbls:      labels.FromStrings(labels.MetricName, "metric_1", "env", "prod"),
			value:     1.5,
			timestamp: 100060,
		},
		{
			lbls:      labels.FromStrings(labels.MetricName, "metric_1", "env", "prod", "status", "300"),
			value:     1.5,
			timestamp: 100090,
		},
	}
	tests := map[string]struct {
		labelNames    []string
		matchers      []*client.LabelMatcher
		expectedItems []*client.LabelValueSeriesCount
	}{
		"expected all label values cardinality": {
			labelNames: []string{labels.MetricName, "env", "status"},
			matchers:   []*client.LabelMatcher{},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName: "status",
					LabelValueSeries: map[string]uint64{
						"200": 1,
						"300": 1,
						"500": 1,
					},
				},
				{
					LabelName: labels.MetricName,
					LabelValueSeries: map[string]uint64{
						"metric_0": 2,
						"metric_1": 2,
					},
				},
				{
					LabelName: "env",
					LabelValueSeries: map[string]uint64{
						"prod": 2,
					},
				},
			},
		},
		"expected status values cardinality applying matchers": {
			labelNames: []string{"status"},
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: labels.MetricName, Value: "metric_1"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName:        "status",
					LabelValueSeries: map[string]uint64{"300": 1},
				},
			},
		},
		"empty response is returned when no matchers match the requested labels": {
			labelNames: []string{"status"},
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: "job", Value: "store-gateway"},
			},
			expectedItems: nil,
		},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)

	ctx := user.InjectOrgID(context.Background(), "test")
	require.NoError(t, pushSeriesToIngester(ctx, t, i, series))

	// Run tests
	for tName, tc := range tests {
		t.Run(tName, func(t *testing.T) {
			req := &client.LabelValuesCardinalityRequest{
				LabelNames: tc.labelNames,
				Matchers:   tc.matchers,
			}

			s := &mockLabelValuesCardinalityServer{context: ctx}
			require.NoError(t, i.LabelValuesCardinality(req, s))

			if len(tc.expectedItems) == 0 {
				require.Len(t, s.SentResponses, 0)
				return
			}
			require.Len(t, s.SentResponses, 1)
			require.ElementsMatch(t, s.SentResponses[0].Items, tc.expectedItems)
		})
	}
}

type series struct {
	lbls      labels.Labels
	value     float64
	timestamp int64
}

func pushSeriesToIngester(ctx context.Context, t testing.TB, i *Ingester, series []series) error {
	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		if err != nil {
			return err
		}
	}
	return nil
}

func extractItemsWithSortedValues(responses []client.LabelNamesAndValuesResponse) []*client.LabelValues {
	var items []*client.LabelValues
	for _, res := range responses {
		items = append(items, res.Items...)
	}
	for _, it := range items {
		slices.Sort(it.Values)
	}
	return items
}

func TestIngester_QueryStream_QuerySharding(t *testing.T) {
	const (
		numSeries = 1000
		numShards = 16
	)

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	// Push all series. We push half of the series, then we compact the TSDB head into a block (flush)
	// and finally we push the remaining series. This way we can both test querying back series both
	// from compacted blocks and head.
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := labels.FromStrings(labels.MetricName, "foo", "series_id", strconv.Itoa(seriesID))
		req, _, _, _ := mockWriteRequest(t, lbls, float64(seriesID), int64(seriesID))
		_, err = i.Push(ctx, req)
		require.NoError(t, err)

		// Compact the TSDB head once half of the series have been pushed.
		if seriesID == numSeries/2 {
			i.Flush()
		}
	}

	for _, streamingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
			// Query all series.
			var actualTimeseries model.Matrix

			for shardIndex := 0; shardIndex < numShards; shardIndex++ {
				req := &client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"},
						{Type: client.EQUAL, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
							ShardIndex: uint64(shardIndex),
							ShardCount: uint64(numShards),
						}.LabelValue()},
					},
				}

				if streamingEnabled {
					req.StreamingChunksBatchSize = 128
				}

				s := stream{ctx: ctx}
				err = i.QueryStream(req, &s)
				require.NoError(t, err)

				res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
				require.NoError(t, err)
				actualTimeseries = append(actualTimeseries, res...)
			}

			// We expect that all series have been returned.
			require.Len(t, actualTimeseries, numSeries)

			actualSeriesIDs := []int{}

			for _, series := range actualTimeseries {
				seriesID, err := strconv.Atoi(string(series.Metric[model.LabelName("series_id")]))
				require.NoError(t, err)

				// We expect no duplicated series in the result.
				assert.NotContains(t, actualSeriesIDs, seriesID, "series was returned multiple times")
				actualSeriesIDs = append(actualSeriesIDs, seriesID)

				// We expect 1 sample with the same timestamp and value we've written.
				require.Len(t, series.Values, 1)
				require.Equal(t, int64(seriesID), int64(series.Values[0].Timestamp))
				require.Equal(t, float64(seriesID), float64(series.Values[0].Value))
			}

			expectedSeriesIDs := []int{}

			for seriesID := 0; seriesID < numSeries; seriesID++ {
				expectedSeriesIDs = append(expectedSeriesIDs, seriesID)
			}

			require.ElementsMatch(t, expectedSeriesIDs, actualSeriesIDs)
		})
	}
}

func TestIngester_QueryStream_QueryShardingShouldGuaranteeSeriesShardingConsistencyOverTime(t *testing.T) {
	const (
		numSeries = 100
		numShards = 2
	)

	// You should NEVER CHANGE the expected series here, otherwise it means you're introducing
	// a backward incompatible change.
	expectedSeriesIDByShard := map[string][]int{
		"1_of_2": {0, 1, 10, 12, 16, 18, 2, 22, 23, 24, 26, 28, 29, 3, 30, 33, 34, 35, 36, 39, 40, 41, 42, 43, 44, 47, 53, 54, 57, 58, 60, 61, 63, 66, 67, 68, 69, 7, 71, 75, 77, 80, 81, 83, 84, 86, 87, 89, 9, 90, 91, 92, 94, 96, 98, 99},
		"2_of_2": {11, 13, 14, 15, 17, 19, 20, 21, 25, 27, 31, 32, 37, 38, 4, 45, 46, 48, 49, 5, 50, 51, 52, 55, 56, 59, 6, 62, 64, 65, 70, 72, 73, 74, 76, 78, 79, 8, 82, 85, 88, 93, 95, 97},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	// Push all series.
	for seriesID := 0; seriesID < numSeries; seriesID++ {
		lbls := labels.FromStrings(labels.MetricName, "test", "series_id", strconv.Itoa(seriesID))
		req, _, _, _ := mockWriteRequest(t, lbls, float64(seriesID), int64(seriesID))
		_, err = i.Push(ctx, req)
		require.NoError(t, err)
	}

	for _, streamingEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
			// Query all series, 1 shard at a time.
			for shardID := 0; shardID < numShards; shardID++ {
				shardLabel := sharding.FormatShardIDLabelValue(uint64(shardID), numShards)
				expectedSeriesIDs := expectedSeriesIDByShard[shardLabel]

				req := &client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: "series_id", Value: ".+"},
						{Type: client.EQUAL, Name: sharding.ShardLabel, Value: shardLabel},
					},
				}

				if streamingEnabled {
					req.StreamingChunksBatchSize = numSeries
				}

				s := stream{ctx: ctx}
				err = i.QueryStream(req, &s)
				require.NoError(t, err)
				require.Greater(t, len(s.responses), 0)
				actualSeriesIDs := []int{}

				for _, res := range s.responses {
					if streamingEnabled {
						for _, series := range res.StreamingSeries {
							seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)
							seriesID, err := strconv.Atoi(seriesLabels.Get("series_id"))
							require.NoError(t, err)

							actualSeriesIDs = append(actualSeriesIDs, seriesID)
						}
					} else {
						for _, series := range res.Chunkseries {
							seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)
							seriesID, err := strconv.Atoi(seriesLabels.Get("series_id"))
							require.NoError(t, err)

							actualSeriesIDs = append(actualSeriesIDs, seriesID)
						}
					}
				}

				require.ElementsMatch(t, expectedSeriesIDs, actualSeriesIDs)
			}
		})
	}
}

func TestIngester_QueryStream_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	for _, streamingEnabled := range []bool{true, false} {
		i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
		defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

		// Mock request
		userID := "test"
		ctx := user.InjectOrgID(context.Background(), userID)
		req := &client.QueryRequest{}

		if streamingEnabled {
			req.StreamingChunksBatchSize = 64
		} else {
			req.StreamingChunksBatchSize = 0
		}

		s := stream{ctx: ctx}
		err = i.QueryStream(req, &s)
		require.NoError(t, err)

		assert.Empty(t, s.responses)

		// Check if the TSDB has been created
		_, tsdbCreated := i.tsdbs[userID]
		assert.False(t, tsdbCreated)
	}
}

func TestIngester_LabelValues_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelValuesRequest{}

	res, err := i.LabelValues(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelValuesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_LabelNames_ShouldNotCreateTSDBIfDoesNotExists(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &client.LabelNamesRequest{EndTimestampMs: math.MaxInt64}

	res, err := i.LabelNames(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, &client.LabelNamesResponse{}, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.tsdbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngester_Push_ShouldNotCreateTSDBIngesterServiceIsNotInRunningState(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)

	// Configure the lifecycler to not immediately leave the ring, to make sure
	// the ingester service will stay in Stopping state for longer.
	cfg.IngesterRing.FinalSleep = 5 * time.Second

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	// Start the ingester and then stop it.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	i.StopAsync()

	// Wait until the ingester service switches to Stopping state.
	require.Eventually(t, func() bool {
		return i.State() == services.Stopping
	}, time.Second, 10*time.Millisecond)

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, 0)

	res, err := pushWithSimulatedGRPCHandler(ctx, i, req)

	assert.EqualError(t, err, newUnavailableError(services.Stopping).Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	assert.Nil(t, i.getTSDB(userID))

	// Wait until terminated.
	require.NoError(t, i.AwaitTerminated(context.Background()))
}

func Test_Ingester_MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.FromStrings(labels.MetricName, "collision", "app", "l", "uniq0", "0", "uniq1", "1"), 1, 300000},
		{labels.FromStrings(labels.MetricName, "collision", "app", "m", "uniq0", "1", "uniq1", "1"), 1, 300000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatchers
		expected []*mimirpb.Metric
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*mimirpb.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should filter metrics by time range to return nothing when queried for older time ranges": {
			from: 100,
			to:   1000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*mimirpb.Metric{},
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*mimirpb.Metric{
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: mimirpb.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
			}

			res, err := i.MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)
		})
	}
}

func Test_Ingester_MetricsForLabelMatchers_Deduplication(t *testing.T) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeries(t, userID, numSeries, 1, now, 1)
	ctx := user.InjectOrgID(context.Background(), "test")

	req := &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: now,
		EndTimestampMs:   now,
		// Overlapping matchers to make sure series are correctly deduplicated.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}},
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*0"},
			}},
		},
	}

	res, err := i.MetricsForLabelMatchers(ctx, req)
	require.NoError(t, err)
	require.Len(t, res.GetMetric(), numSeries)
}

func Benchmark_Ingester_MetricsForLabelMatchers(b *testing.B) {
	var (
		userID              = "test"
		numSeries           = 10000
		numSamplesPerSeries = 60 * 6 // 6h on 1 sample per minute
		startTimestamp      = util.TimeToMillis(time.Now())
		step                = int64(60000) // 1 sample per minute
	)

	i := createIngesterWithSeries(b, userID, numSeries, numSamplesPerSeries, startTimestamp, step)
	ctx := user.InjectOrgID(context.Background(), "test")

	// Flush the ingester to ensure blocks have been compacted, so we'll test
	// fetching labels from blocks.
	i.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		req := &client.MetricsForLabelMatchersRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}}},
		}

		res, err := i.MetricsForLabelMatchers(ctx, req)
		require.NoError(b, err)
		require.Len(b, res.GetMetric(), numSeries)
	}
}

// createIngesterWithSeries creates an ingester and push numSeries with numSamplesPerSeries each.
func createIngesterWithSeries(t testing.TB, userID string, numSeries, numSamplesPerSeries int, startTimestamp, step int64) *Ingester {
	const maxBatchSize = 1000

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push fixtures.
	ctx := user.InjectOrgID(context.Background(), userID)

	for ts := startTimestamp; ts < startTimestamp+(step*int64(numSamplesPerSeries)); ts += step {
		for o := 0; o < numSeries; o += maxBatchSize {
			batchSize := min(maxBatchSize, numSeries-o)

			// Generate metrics and samples (1 for each series).
			metrics := make([][]mimirpb.LabelAdapter, 0, batchSize)
			samples := make([]mimirpb.Sample, 0, batchSize)

			for s := 0; s < batchSize; s++ {
				metrics = append(metrics, []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: fmt.Sprintf("test_%d", o+s)}})
				samples = append(samples, mimirpb.Sample{
					TimestampMs: ts,
					Value:       1,
				})
			}

			// Send metrics to the ingester.
			req := mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API)
			_, err := i.Push(ctx, req)
			require.NoError(t, err)
		}
	}

	return i
}

func TestIngester_QueryStream(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	// Change stream type in runtime.
	var streamType QueryStreamType
	cfg.StreamTypeFn = func() QueryStreamType {
		return streamType
	}

	registry := prometheus.NewRegistry()

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	// Push all series. We push half of the series, then we compact the TSDB head into a block (flush)
	// and finally we push the remaining series. This way we can both test querying back series both
	// from compacted blocks and head.
	const numSeries = 1000

	for seriesID := 0; seriesID < numSeries; seriesID++ {
		floatLbls := labels.FromStrings(labels.MetricName, "foo", "series_id", strconv.Itoa(seriesID), "type", "float")
		floatReq, _, _, _ := mockWriteRequest(t, floatLbls, float64(seriesID), int64(seriesID))
		_, err = i.Push(ctx, floatReq)
		require.NoError(t, err)

		histLbls := labels.FromStrings(labels.MetricName, "foo", "series_id", strconv.Itoa(seriesID), "type", "histogram")
		histReq := mockHistogramWriteRequest(histLbls, int64(seriesID), seriesID, false)
		_, err = i.Push(ctx, histReq)
		require.NoError(t, err)

		fhistLbls := labels.FromStrings(labels.MetricName, "foo", "series_id", strconv.Itoa(seriesID), "type", "floathistogram")
		fhistReq := mockHistogramWriteRequest(fhistLbls, int64(seriesID), seriesID, true)
		_, err = i.Push(ctx, fhistReq)
		require.NoError(t, err)

		// Compact the TSDB head once half of the series have been pushed.
		if seriesID == numSeries/2 {
			i.Flush()
		}
	}

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	c, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	defer c.Close()

	tests := map[string]struct {
		streamType         QueryStreamType
		numShards          int
		expectedStreamType QueryStreamType
	}{
		"should query chunks by default": {
			streamType:         QueryStreamDefault,
			expectedStreamType: QueryStreamChunks,
		},
		"should query samples when configured with QueryStreamSamples": {
			streamType:         QueryStreamSamples,
			expectedStreamType: QueryStreamSamples,
		},
		"should query chunks when configured with QueryStreamChunks": {
			streamType:         QueryStreamChunks,
			expectedStreamType: QueryStreamChunks,
		},
		"should support sharding when returning samples": {
			streamType:         QueryStreamSamples,
			numShards:          16,
			expectedStreamType: QueryStreamSamples,
		},
		"should support sharding when returning chunks": {
			streamType:         QueryStreamChunks,
			numShards:          16,
			expectedStreamType: QueryStreamChunks,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Configure the stream type.
			streamType = testData.streamType

			// Query all series.
			var actualTimeseries []mimirpb.TimeSeries
			var actualChunkseries []client.TimeSeriesChunk

			runQueryAndSaveResponse := func(req *client.QueryRequest) (receivedSeries int, err error) {
				s, err := c.QueryStream(ctx, req)
				if err != nil {
					return 0, err
				}

				for {
					resp, err := s.Recv()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return receivedSeries, err
					}

					actualTimeseries = append(actualTimeseries, resp.Timeseries...)
					actualChunkseries = append(actualChunkseries, resp.Chunkseries...)

					receivedSeries += len(resp.Timeseries)
					receivedSeries += len(resp.Chunkseries)
				}

				return receivedSeries, nil
			}

			expectedNumSeries := numSeries * 3

			if testData.numShards > 0 {
				for shardIndex := 0; shardIndex < testData.numShards; shardIndex++ {
					receivedSeries, err := runQueryAndSaveResponse(&client.QueryRequest{
						StartTimestampMs: math.MinInt64,
						EndTimestampMs:   math.MaxInt64,
						Matchers: []*client.LabelMatcher{
							{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"},
							{Type: client.EQUAL, Name: sharding.ShardLabel, Value: sharding.ShardSelector{
								ShardIndex: uint64(shardIndex),
								ShardCount: uint64(testData.numShards),
							}.LabelValue()},
						},
					})

					require.NoError(t, err)
					assert.Greater(t, receivedSeries, 0)
				}

			} else {
				receivedSeries, err := runQueryAndSaveResponse(&client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "foo"}},
				})

				require.NoError(t, err)
				assert.Greater(t, receivedSeries, 0)
			}

			// Ensure we received the expected data types in response.
			if testData.expectedStreamType == QueryStreamSamples {
				assert.Len(t, actualTimeseries, expectedNumSeries)
				assert.Empty(t, actualChunkseries)
			} else {
				assert.Len(t, actualChunkseries, expectedNumSeries)
				assert.Empty(t, actualTimeseries)
			}

			// We expect that all series have been returned once per type.
			actualSeriesIDs := map[string]map[int]struct{}{}
			for _, typeLabel := range []string{"float", "histogram", "floathistogram"} {
				actualSeriesIDs[typeLabel] = make(map[int]struct{})
			}

			for _, series := range actualTimeseries {
				lbls := mimirpb.FromLabelAdaptersToLabels(series.Labels)
				typeLabel := lbls.Get("type")

				seriesID, err := strconv.Atoi(lbls.Get("series_id"))
				require.NoError(t, err)

				// We expect no duplicated series in the result.
				_, exists := actualSeriesIDs[typeLabel][seriesID]
				assert.False(t, exists)
				actualSeriesIDs[typeLabel][seriesID] = struct{}{}

				switch typeLabel {
				case "float":
					// We expect 1 sample with the same timestamp and value we've written.
					require.Len(t, series.Samples, 1)
					assert.Equal(t, int64(seriesID), series.Samples[0].TimestampMs)
					assert.Equal(t, float64(seriesID), series.Samples[0].Value)
				case "histogram":
					require.Len(t, series.Histograms, 1)
					require.Equal(t, mimirpb.FromHistogramToHistogramProto(int64(seriesID), util_test.GenerateTestHistogram(seriesID)), series.Histograms[0])
				case "floathistogram":
					require.Len(t, series.Histograms, 1)
					require.Equal(t, mimirpb.FromFloatHistogramToHistogramProto(int64(seriesID), util_test.GenerateTestFloatHistogram(seriesID)), series.Histograms[0])
				default:
					require.Fail(t, "unexpected metric name")
				}
			}

			for _, series := range actualChunkseries {
				lbls := mimirpb.FromLabelAdaptersToLabels(series.Labels)
				typeLabel := lbls.Get("type")

				seriesID, err := strconv.Atoi(lbls.Get("series_id"))
				require.NoError(t, err)

				// We expect no duplicated series in the result.
				_, exists := actualSeriesIDs[typeLabel][seriesID]
				assert.False(t, exists)
				actualSeriesIDs[typeLabel][seriesID] = struct{}{}

				// We expect 1 chunk.
				require.Len(t, series.Chunks, 1)

				enc := series.Chunks[0].Encoding
				switch enc {
				case int32(chunk.PrometheusXorChunk):
					chk, err := chunkenc.FromData(chunkenc.EncXOR, series.Chunks[0].Data)
					require.NoError(t, err)

					// We expect 1 sample with the same timestamp and value we've written.
					it := chk.Iterator(nil)

					require.Equal(t, chunkenc.ValFloat, it.Next())
					actualTs, actualValue := it.At()
					assert.Equal(t, int64(seriesID), actualTs)
					assert.Equal(t, float64(seriesID), actualValue)

					assert.Equal(t, chunkenc.ValNone, it.Next())
					assert.NoError(t, it.Err())
				case int32(chunk.PrometheusHistogramChunk):
					chk, err := chunkenc.FromData(chunkenc.EncHistogram, series.Chunks[0].Data)
					require.NoError(t, err)

					// We expect 1 sample with the same timestamp and value we've written.
					it := chk.Iterator(nil)

					require.Equal(t, chunkenc.ValHistogram, it.Next())
					actualTs, actualHist := it.AtHistogram(nil)
					require.Equal(t, int64(seriesID), actualTs)
					require.Equal(t, util_test.GenerateTestHistogram(seriesID), actualHist)

					assert.Equal(t, chunkenc.ValNone, it.Next())
					assert.NoError(t, it.Err())
				case int32(chunk.PrometheusFloatHistogramChunk):
					chk, err := chunkenc.FromData(chunkenc.EncFloatHistogram, series.Chunks[0].Data)
					require.NoError(t, err)

					// We expect 1 sample with the same timestamp and value we've written.
					it := chk.Iterator(nil)

					require.Equal(t, chunkenc.ValFloatHistogram, it.Next())
					actualTs, actualHist := it.AtFloatHistogram(nil)
					require.Equal(t, int64(seriesID), actualTs)
					require.Equal(t, util_test.GenerateTestFloatHistogram(seriesID), actualHist)

					assert.Equal(t, chunkenc.ValNone, it.Next())
					assert.NoError(t, it.Err())
				default:
					require.Fail(t, "unexpected encoding")
				}
			}
		})
	}

	t.Run("limited due to resource utilization", func(t *testing.T) {
		origLimiter := i.utilizationBasedLimiter
		t.Cleanup(func() {
			i.utilizationBasedLimiter = origLimiter
		})
		i.utilizationBasedLimiter = &fakeUtilizationBasedLimiter{limitingReason: "cpu"}

		_, err := i.StartReadRequest(context.Background())
		stat, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Equal(t, codes.ResourceExhausted, stat.Code())
		require.Equal(t, ingesterTooBusyMsg, stat.Message())
		verifyUtilizationLimitedRequestsMetric(t, registry)
	})
}

func TestIngester_QueryStream_TimeseriesWithManySamples(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.StreamChunksWhenUsingBlocks = false

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 100000
	samples := generateSamples(samplesCount)

	// 10k samples encode to around 140 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "1"), samples[0:10000]))
	require.NoError(t, err)

	// 100k samples encode to around 1.4 MiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "2"), samples))
	require.NoError(t, err)

	// 50k samples encode to around 716 KiB,
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "3"), samples[0:50000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	c, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Timeseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Timeseries)

		for _, ts := range resp.Timeseries {
			totalSamples += len(ts.Samples)
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (10k + 50k in first, 100k in second)
	// or 3 messages (small series first, 100k second, small series last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 10000+50000+samplesCount, totalSamples)
}

func setupQueryingManySamplesAsChunksTest(ctx context.Context, t *testing.T, cfg Config) client.HealthAndIngesterClient {
	const sampleCount = 1_000_000

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	})

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series.
	samples := generateSamples(sampleCount)

	// 100k samples in chunks use about 154 KiB
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "1"), samples[0:100000]))
	require.NoError(t, err)

	// 1M samples in chunks use about 1.51 MiB
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "2"), samples))
	require.NoError(t, err)

	// 500k samples in chunks need 775 KiB
	_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", "3"), samples[0:500000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	t.Cleanup(serv.GracefulStop)
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	c, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() }) //nolint:errcheck

	return c
}

func generateSamples(sampleCount int) []mimirpb.Sample {
	samples := make([]mimirpb.Sample, 0, sampleCount)

	for i := 0; i < sampleCount; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	return samples
}

func TestIngester_QueryStream_ChunkseriesWithManySamples(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.StreamChunksWhenUsingBlocks = true
	ctx := user.InjectOrgID(context.Background(), userID)

	c := setupQueryingManySamplesAsChunksTest(ctx, t, cfg)

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   1000001,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Chunkseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Chunkseries)

		for _, ts := range resp.Chunkseries {
			for _, c := range ts.Chunks {
				ch, err := chunk.NewForEncoding(chunk.Encoding(c.Encoding))
				require.NoError(t, err)
				require.NoError(t, ch.UnmarshalFromBuf(c.Data))

				totalSamples += ch.Len()
			}
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (100k + 500k in first, 1M in second)
	// or 3 messages (100k or 500k first, 1M second, and 500k or 100k last).
	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+1000000, totalSamples)
}

func TestIngester_QueryStream_StreamingWithManySamples(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	ctx := user.InjectOrgID(context.Background(), userID)

	c := setupQueryingManySamplesAsChunksTest(ctx, t, cfg)

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs:         0,
		EndTimestampMs:           1000001,
		StreamingChunksBatchSize: 64,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	resp, err := s.Recv()
	require.NoError(t, err)

	seriesLabelsMsg := client.QueryStreamResponse{
		StreamingSeries: []client.QueryStreamSeries{
			{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "foo", "l", "1")), ChunkCount: 834},
			{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "foo", "l", "2")), ChunkCount: 8334},
			{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(labels.MetricName, "foo", "l", "3")), ChunkCount: 4167},
		},
		IsEndOfSeriesStream: true,
	}

	require.Equal(t, seriesLabelsMsg, *resp)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.StreamingSeriesChunks) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.StreamingSeriesChunks)

		for _, ts := range resp.StreamingSeriesChunks {
			for _, c := range ts.Chunks {
				ch, err := chunk.NewForEncoding(chunk.Encoding(c.Encoding))
				require.NoError(t, err)
				require.NoError(t, ch.UnmarshalFromBuf(c.Data))

				totalSamples += ch.Len()
			}
		}
	}

	require.Equal(t, 3, recvMsgs) // 1 for each series: second series must be sent in a message of its own as it will be over 1 MiB
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+1000000, totalSamples)
}

func TestIngester_QueryStream_StreamingWithManySeries(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	ctx := user.InjectOrgID(context.Background(), userID)

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	})

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series, alternating between different series that have chunks that encode to different sizes.
	smallSampleSet := generateSamples(10000)   // 10k samples in chunks use about 15.4 KiB, so 64 series will take ~1 MiB
	mediumSampleSet := generateSamples(600000) // 600k samples in chunks use about 924 KiB
	largeSampleSet := generateSamples(1000000) // 1M samples in chunks use about 1.51 MiB
	expectedSeries := []labels.Labels{}

	for idx := 0; idx < 201; idx++ {
		samples := smallSampleSet

		if idx%80 == 0 {
			samples = mediumSampleSet
		} else if idx == 105 {
			samples = largeSampleSet
		}

		l := labels.FromStrings(labels.MetricName, "foo", "l", fmt.Sprintf("%3v", idx))
		expectedSeries = append(expectedSeries, l.Copy())

		_, err = i.Push(ctx, writeRequestSingleSeries(l, samples))
		require.NoError(t, err)
	}

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	t.Cleanup(serv.GracefulStop)
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	c, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() }) //nolint:errcheck

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs:         0,
		EndTimestampMs:           1000001,
		StreamingChunksBatchSize: 64,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	actualSeries := []labels.Labels{}
	seriesBatchCount := 0

	for {
		resp, err := s.Recv()
		require.NoError(t, err)

		seriesBatchCount++
		require.LessOrEqual(t, seriesBatchCount, 2, "expecting no more than two batches")

		switch seriesBatchCount {
		case 1:
			require.Len(t, resp.StreamingSeries, 128, "first batch should be maximum batch size")
		case 2:
			require.Len(t, resp.StreamingSeries, 201-128, "second batch should contain remaining series")
		}

		for _, series := range resp.StreamingSeries {
			actualSeries = append(actualSeries, mimirpb.FromLabelAdaptersToLabels(series.Labels))
		}

		if resp.IsEndOfSeriesStream {
			break
		}
	}

	require.Equal(t, expectedSeries, actualSeries)

	seriesReceivedCount := 0
	actualSeriesPerChunksMessage := []int{}

	// There are two limits when creating messages. As soon as either limit is reached for the batch, the batch is sent:
	// 1. no message can be more than 1 MiB (unless it contains a single series whose chunks total more than 1 MiB)
	// 2. no message can contain more series than the requested batch size
	expectedSeriesPerChunksMessage := []int{
		7,  // First series is ~924KiB, so with 6 small series takes us to 1 MiB
		64, // Reaches maximum number of series per batch
		9,  // 80th series needs to go in the next message as it would push us over a 1 MiB message
		7,  // 80th series + 6 small series to get to 1 MiB message
		18, // 105th series needs a message all of its own
		1,  // 105th series
		54, // 160th series needs to go in the next message as it would push us over a 1 MiB message
		7,  // 160th series + 6 small series to get to 1 MiB message
		34, // Remaining series
	}

	for {
		resp, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		actualSeriesPerChunksMessage = append(actualSeriesPerChunksMessage, len(resp.StreamingSeriesChunks))

		for _, s := range resp.StreamingSeriesChunks {
			require.Equal(t, seriesReceivedCount, int(s.SeriesIndex))

			expectedSampleCount := len(smallSampleSet)

			if seriesReceivedCount%80 == 0 {
				expectedSampleCount = len(mediumSampleSet)
			} else if seriesReceivedCount == 105 {
				expectedSampleCount = len(largeSampleSet)
			}

			actualSampleCount := 0

			for _, c := range s.Chunks {
				ch, err := chunk.NewForEncoding(chunk.Encoding(c.Encoding))
				require.NoError(t, err)
				require.NoError(t, ch.UnmarshalFromBuf(c.Data))

				actualSampleCount += ch.Len()
			}

			require.Equal(t, expectedSampleCount, actualSampleCount)

			seriesReceivedCount++
		}
	}

	require.Equal(t, len(expectedSeries), seriesReceivedCount, "expected to receive chunks for all series")
	require.Equal(t, expectedSeriesPerChunksMessage, actualSeriesPerChunksMessage)
}

func TestIngester_QueryExemplars(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	user.InjectOrgID(context.Background(), userID)
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	})

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
}

// This test shows a single ingester returns compacted OOO and in-order chunks separately after compaction, even if they overlap.
func TestIngester_QueryStream_CounterResets(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false
	cfg.TSDBConfigUpdatePeriod = 1 * time.Second

	// Set the OOO window to 30 minutes and enable native histograms.
	limits := map[string]*validation.Limits{
		userID: {
			OutOfOrderTimeWindow:                model.Duration(30 * time.Minute),
			OOONativeHistogramsIngestionEnabled: true,
			NativeHistogramsIngestionEnabled:    true,
		},
	}
	override, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))
	require.NoError(t, err)

	i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	histLbls := labels.FromStrings(labels.MetricName, "foo", "series_id", strconv.Itoa(0), "type", "histogram")
	histReq := mockHistogramWriteRequest(histLbls, int64(0), 4, false)
	_, err = i.Push(ctx, histReq)
	require.NoError(t, err)

	histReq = mockHistogramWriteRequest(histLbls, int64(2), 6, false)
	_, err = i.Push(ctx, histReq)
	require.NoError(t, err)

	histReq = mockHistogramWriteRequest(histLbls, int64(4), 8, false)
	_, err = i.Push(ctx, histReq)
	require.NoError(t, err)

	histReq = mockHistogramWriteRequest(histLbls, int64(1), 2, false)
	_, err = i.Push(ctx, histReq)
	require.NoError(t, err)

	histReq = mockHistogramWriteRequest(histLbls, int64(3), 3, false)
	_, err = i.Push(ctx, histReq)
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	c, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	defer c.Close()

	runQuery := func() ([]chunkenc.CounterResetHeader, [][]sample) {
		s, err := c.QueryStream(ctx, &client.QueryRequest{
			StartTimestampMs: 0,
			EndTimestampMs:   5,

			Matchers: []*client.LabelMatcher{{
				Type:  client.EQUAL,
				Name:  model.MetricNameLabel,
				Value: "foo",
			}},
		})
		require.NoError(t, err)

		recvMsgs := 0

		chunks := []client.Chunk{}
		for {
			resp, err := s.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)

			for _, c := range resp.Chunkseries {
				chunks = append(chunks, c.Chunks...)
			}
			recvMsgs++
		}

		require.Equal(t, recvMsgs, 1)
		// Sort chunks by time
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].StartTimestampMs < chunks[j].StartTimestampMs
		})

		headers := []chunkenc.CounterResetHeader{}
		var samples [][]sample
		for _, c := range chunks {
			require.Equal(t, c.Encoding, int32(chunk.PrometheusHistogramChunk))
			chk, err := chunkenc.FromData(chunkenc.EncHistogram, c.Data)
			require.NoError(t, err)

			s := []sample{}
			it := chk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, h := it.AtHistogram(nil)
				s = append(s, sample{t: ts, h: h})
			}
			samples = append(samples, s)
			headers = append(headers, chk.(*chunkenc.HistogramChunk).GetCounterResetHeader())
		}
		return headers, samples
	}

	// Check samples before compaction (OOO and in-order samples are merged when both are in the head).
	actHeaders, actSamples := runQuery()
	require.Equal(t, []chunkenc.CounterResetHeader{chunkenc.UnknownCounterReset, chunkenc.CounterReset, chunkenc.CounterReset}, actHeaders)
	require.Equal(t, [][]sample{
		{
			{t: 0, h: histogramWithHint(4, histogram.UnknownCounterReset)},
		},
		{
			{t: 1, h: histogramWithHint(2, histogram.UnknownCounterReset)},
			{t: 2, h: histogramWithHint(6, histogram.NotCounterReset)},
		},
		{
			{t: 3, h: histogramWithHint(3, histogram.UnknownCounterReset)},
			{t: 4, h: histogramWithHint(8, histogram.NotCounterReset)},
		},
	}, actSamples)

	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	// Compaction
	i.compactBlocks(context.Background(), false, 0, nil) // Should be compacted because the TSDB is idle.
	verifyCompactedHead(t, i, true)

	defer c.Close()

	actHeaders, actSamples = runQuery()
	require.Equal(t, []chunkenc.CounterResetHeader{chunkenc.UnknownCounterReset, chunkenc.UnknownCounterReset}, actHeaders)
	require.Equal(t, [][]sample{
		{
			{t: 0, h: histogramWithHint(4, histogram.UnknownCounterReset)},
			{t: 2, h: histogramWithHint(6, histogram.NotCounterReset)},
			{t: 4, h: histogramWithHint(8, histogram.NotCounterReset)},
		},
		{
			{t: 1, h: histogramWithHint(2, histogram.UnknownCounterReset)},
			{t: 3, h: histogramWithHint(3, histogram.NotCounterReset)},
		},
	}, actSamples)
}

func histogramWithHint(idx int, hint histogram.CounterResetHint) *histogram.Histogram {
	h := util_test.GenerateTestHistogram(idx)
	h.CounterResetHint = hint
	return h
}

type sample struct {
	t int64
	h *histogram.Histogram
}

func writeRequestSingleSeries(lbls labels.Labels, samples []mimirpb.Sample) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{
		Source: mimirpb.API,
	}

	ts := mimirpb.TimeSeries{}
	ts.Labels = mimirpb.FromLabelsToLabelAdapters(lbls)
	ts.Samples = samples
	req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &ts})

	return req
}

type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockQueryStreamServer) Send(*client.QueryStreamResponse) error {
	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

func BenchmarkIngester_QueryStream(b *testing.B) {
	const (
		numSeries       = 25000 // Number of series to push.
		numHeadSamples  = 240   // Number of samples in the TSDB Head (2h of samples at 30s scrape interval).
		numBlockSamples = 240   // Number of samples in compacted blocks (2h of samples at 30s scrape interval).
		numShards       = 16    // Number of shards to query when query sharding is enabled.
	)

	cfg := defaultIngesterTestConfig(b)
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0

	// Change stream type in runtime.
	var streamType QueryStreamType
	cfg.StreamTypeFn = func() QueryStreamType {
		return streamType
	}

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	b.Cleanup(func() {
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(b, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series to a compacted block.
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := make([]mimirpb.Sample, 0, numBlockSamples)

	for i := 0; i < numBlockSamples; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	for s := 0; s < numSeries; s++ {
		_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", strconv.Itoa(s)), samples))
		require.NoError(b, err)
	}

	i.Flush()

	// Push series to TSDB head.
	samples = samples[:0]
	for i := numBlockSamples; i < numBlockSamples+numHeadSamples; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	for s := 0; s < numSeries; s++ {
		_, err = i.Push(ctx, writeRequestSingleSeries(labels.FromStrings(labels.MetricName, "foo", "l", strconv.Itoa(s)), samples))
		require.NoError(b, err)
	}

	// Benchmark different ranges.
	ranges := []struct {
		name       string
		start, end int64
	}{
		{
			name:  "full data",
			start: math.MinInt64,
			end:   math.MaxInt64,
		},
		{
			name:  "partial block",
			start: 1,
			end:   numBlockSamples / 3,
		},
		{
			name:  "partial head",
			start: numBlockSamples + 5,
			end:   numBlockSamples + numHeadSamples - 5,
		},
		{
			name:  "head + partial block",
			start: numBlockSamples / 3,
			end:   numBlockSamples + numHeadSamples,
		},
	}

	b.Run("query samples", func(b *testing.B) {
		streamType = QueryStreamSamples

		for _, timeRange := range ranges {
			for _, queryShardingEnabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("time range=%v, query sharding=%v", timeRange.name, queryShardingEnabled), func(b *testing.B) {
					benchmarkIngesterQueryStream(ctx, b, i, timeRange.start, timeRange.end, queryShardingEnabled, numShards)
				})
			}
		}
	})

	b.Run("query chunks", func(b *testing.B) {
		streamType = QueryStreamChunks

		for _, timeRange := range ranges {
			for _, queryShardingEnabled := range []bool{false, true} {
				b.Run(fmt.Sprintf("time range=%v, query sharding=%v", timeRange.name, queryShardingEnabled), func(b *testing.B) {
					benchmarkIngesterQueryStream(ctx, b, i, timeRange.start, timeRange.end, queryShardingEnabled, numShards)
				})
			}
		}
	})
}

func requireActiveIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) *Ingester {
	ingester := getStartedIngesterWithBlocksStorage(t, ingesterCfg, registerer)
	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})
	return ingester
}

func getStartedIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) *Ingester {
	ingester, err := prepareIngesterWithBlocksStorage(t, ingesterCfg, nil, registerer)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})
	return ingester
}

func benchmarkIngesterQueryStream(ctx context.Context, b *testing.B, i *Ingester, start, end int64, queryShardingEnabled bool, numShards int) {
	mockStream := &mockQueryStreamServer{ctx: ctx}

	metricMatcher := &client.LabelMatcher{
		Type:  client.EQUAL,
		Name:  model.MetricNameLabel,
		Value: "foo",
	}

	for ix := 0; ix < b.N; ix++ {
		if queryShardingEnabled {
			// Query each shard.
			for idx := 0; idx < numShards; idx++ {
				req := &client.QueryRequest{
					StartTimestampMs: start,
					EndTimestampMs:   end,
					Matchers: []*client.LabelMatcher{metricMatcher, {
						Type:  client.EQUAL,
						Name:  sharding.ShardLabel,
						Value: sharding.ShardSelector{ShardIndex: uint64(idx), ShardCount: uint64(numShards)}.LabelValue(),
					}},
				}

				err := i.QueryStream(req, mockStream)
				require.NoError(b, err)
			}
		} else {
			req := &client.QueryRequest{
				StartTimestampMs: start,
				EndTimestampMs:   end,
				Matchers:         []*client.LabelMatcher{metricMatcher},
			}

			err := i.QueryStream(req, mockStream)
			require.NoError(b, err)
		}
	}
}

func mockHistogramWriteRequest(lbls labels.Labels, timestampMs int64, histIdx int, genFloatHist bool) *mimirpb.WriteRequest {
	var histograms []mimirpb.Histogram
	if genFloatHist {
		h := util_test.GenerateTestFloatHistogram(histIdx)
		histograms = []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(timestampMs, h)}
	} else {
		h := util_test.GenerateTestHistogram(histIdx)
		histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(timestampMs, h)}
	}
	return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)}, histograms, nil)
}

func mockWriteRequest(t testing.TB, lbls labels.Labels, value float64, timestampMs int64) (*mimirpb.WriteRequest, *client.QueryResponse, *client.QueryStreamResponse, *client.QueryStreamResponse) {
	samples := []mimirpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)}, samples, nil, nil, mimirpb.API)

	// Generate the expected response
	expectedQueryRes := &client.QueryResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	expectedQueryStreamResSamples := &client.QueryStreamResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	require.NoError(t, err)
	app.Append(timestampMs, value)
	chk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: mimirpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         int32(chunk.PrometheusXorChunk),
						Data:             chk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryRes, expectedQueryStreamResSamples, expectedQueryStreamResChunks
}

func prepareHealthyIngester(b testing.TB, mutateLimits func(*validation.Limits)) *Ingester {
	cfg := defaultIngesterTestConfig(b)
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0
	if mutateLimits != nil {
		mutateLimits(&limits)
	}

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	b.Cleanup(func() {
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(b, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
	return i
}

func prepareIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, ingestersRing ring.ReadRing, registerer prometheus.Registerer) (*Ingester, error) {
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.NativeHistogramsIngestionEnabled = true
	return prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limitsCfg, ingestersRing, "", registerer)
}

func prepareIngesterWithBlocksStorageAndLimits(t testing.TB, ingesterCfg Config, limits validation.Limits, ingestersRing ring.ReadRing, dataDir string, registerer prometheus.Registerer) (*Ingester, error) {
	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, err
	}
	return prepareIngesterWithBlockStorageAndOverrides(t, ingesterCfg, overrides, ingestersRing, dataDir, "", registerer)
}

func prepareIngesterWithBlockStorageAndOverrides(t testing.TB, ingesterCfg Config, overrides *validation.Overrides, ingestersRing ring.ReadRing, dataDir string, bucketDir string, registerer prometheus.Registerer) (*Ingester, error) {
	return prepareIngesterWithBlockStorageOverridesAndCostAttribution(t, ingesterCfg, overrides, ingestersRing, dataDir, bucketDir, registerer, nil)
}

func prepareIngesterWithBlockStorageOverridesAndCostAttribution(t testing.TB, ingesterCfg Config, overrides *validation.Overrides, ingestersRing ring.ReadRing, dataDir string, bucketDir string, registerer prometheus.Registerer, cam *costattribution.Manager) (*Ingester, error) {
	return prepareIngesterWithBlockStorageAndOverridesAndPartitionRing(t, ingesterCfg, overrides, ingestersRing, nil, dataDir, bucketDir, registerer, cam)
}

func prepareIngesterWithBlockStorageAndOverridesAndPartitionRing(t testing.TB, ingesterCfg Config, overrides *validation.Overrides, ingestersRing ring.ReadRing, partitionsRing *ring.PartitionRingWatcher, dataDir string, bucketDir string, registerer prometheus.Registerer, cam *costattribution.Manager) (*Ingester, error) {
	// Create a data dir if none has been provided.
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	if bucketDir == "" {
		bucketDir = t.TempDir()
	}

	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dataDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = bucketDir

	// Disable TSDB head compaction jitter to have predictable tests.
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	if ingestersRing == nil {
		ingestersRing = createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig())
	}

	ingester, err := New(ingesterCfg, overrides, ingestersRing, partitionsRing, nil, cam, registerer, noDebugNoopLogger{}) // LOGGING: log.NewLogfmtLogger(os.Stderr)
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func createAndStartRing(t testing.TB, ringConfig ring.Config) *ring.Ring {
	// Start the ingester ring
	rng, err := ring.New(ringConfig, "ingester", IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), rng))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), rng))
	})

	return rng
}

// Logger which does nothing and implements the DebugEnabled interface used by SpanLogger.
type noDebugNoopLogger struct{}

func (noDebugNoopLogger) Log(...interface{}) error { return nil }
func (noDebugNoopLogger) DebugEnabled() bool       { return false }

func TestIngester_OpenExistingTSDBOnStartup(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		walReplayConcurrency                         int
		deprecatedMaxTSDBOpeningConcurrencyOnStartup int
		setup                                        func(*testing.T, string)
		check                                        func(*testing.T, *Ingester)
		expectedErr                                  string
	}{
		"should not load TSDB if the user directory is empty": {
			walReplayConcurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user0"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Nil(t, i.getTSDB("user0"))
			},
		},
		"should not load any TSDB if the root directory is empty": {
			walReplayConcurrency: 10,
			setup:                func(*testing.T, string) {},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.tsdbs))
			},
		},
		"should not load any TSDB is the root directory is missing": {
			walReplayConcurrency: 10,
			setup: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(dir))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Zero(t, len(i.tsdbs))
			},
		},
		"should load TSDB for any non-empty user directory": {
			walReplayConcurrency: 10,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}
				require.NoError(t, os.Mkdir(filepath.Join(dir, "user2"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 2, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1"} {
					require.NotNil(t, i.getTSDB(userID))
				}
				require.Nil(t, i.getTSDB("user2"))
			},
		},
		"should load all TSDBs on walReplayConcurrency < number of TSDBs": {
			walReplayConcurrency: 2,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NotNil(t, i.getTSDB(userID))
					walReplayConcurrency := getWALReplayConcurrencyFromTSDBHeadOptions(i.getTSDB(userID))
					require.Equal(t, 2, walReplayConcurrency)
				}
			},
		},
		"should load all TSDBs on walReplayConcurrency > number of TSDBs": {
			walReplayConcurrency: 10,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NotNil(t, i.getTSDB(userID))
					walReplayConcurrency := getWALReplayConcurrencyFromTSDBHeadOptions(i.getTSDB(userID))
					require.Equal(t, 10, walReplayConcurrency)
				}
			},
		},
		"should load all TSDBs on number of TSDBs > maxTSDBOpenWithoutConcurrency": {
			walReplayConcurrency: 2,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 11, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"} {
					require.NotNil(t, i.getTSDB(userID))
					walReplayConcurrency := getWALReplayConcurrencyFromTSDBHeadOptions(i.getTSDB(userID))
					require.Equal(t, 1, walReplayConcurrency)
				}
			},
		},
		"should fail and rollback if an error occur while loading a TSDB on walReplayConcurrency > number of TSDBs": {
			walReplayConcurrency: 10,
			setup: func(t *testing.T, dir string) {
				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user0", "chunks_head", ""), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user0", "chunks_head", "00000002"), nil, 0700))

				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user1", "dummy"), 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 0, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1"} {
					require.Nil(t, i.getTSDB(userID))
				}
			},
			expectedErr: "unable to open TSDB for user user0",
		},
		"should fail and rollback if an error occur while loading a TSDB on walReplayConcurrency < number of TSDBs": {
			walReplayConcurrency: 2,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1", "user3", "user4"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}

				// Create a fake TSDB on disk with an empty chunks head segment file (it's invalid unless
				// it's the last one and opening TSDB should fail).
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "wal", ""), 0700))
				require.NoError(t, os.MkdirAll(filepath.Join(dir, "user2", "chunks_head", ""), 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000001"), nil, 0700))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "user2", "chunks_head", "00000002"), nil, 0700))
			},
			check: func(t *testing.T, i *Ingester) {
				for _, userID := range []string{"user0", "user1"} {
					require.Nil(t, i.getTSDB(userID))
				}
			},
			expectedErr: "unable to open TSDB for user user2",
		},
		"should load all TSDBs and honor DeprecatedMaxTSDBOpeningConcurrencyOnStartup when walReplayConcurrency = 0": {
			walReplayConcurrency:                         0,
			deprecatedMaxTSDBOpeningConcurrencyOnStartup: 2,
			setup: func(t *testing.T, dir string) {
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NoError(t, os.MkdirAll(filepath.Join(dir, userID, "dummy"), 0700))
				}
			},
			check: func(t *testing.T, i *Ingester) {
				require.Equal(t, 5, len(i.tsdbs))
				for _, userID := range []string{"user0", "user1", "user2", "user3", "user4"} {
					require.NotNil(t, i.getTSDB(userID))
					walReplayConcurrency := getWALReplayConcurrencyFromTSDBHeadOptions(i.getTSDB(userID))
					require.NotEqual(t, 0, walReplayConcurrency)
				}
			},
		},
	}

	for name, test := range tests {
		testName := name
		testData := test
		t.Run(testName, func(t *testing.T) {
			limits := defaultLimitsTestConfig()

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(t, err)

			// Create a temporary directory for TSDB
			tempDir := t.TempDir()

			ingesterCfg := defaultIngesterTestConfig(t)
			ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
			ingesterCfg.BlocksStorageConfig.TSDB.WALReplayConcurrency = testData.walReplayConcurrency
			ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
			ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"

			// setup the tsdbs dir
			testData.setup(t, tempDir)

			ingester, err := New(ingesterCfg, overrides, createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig()), nil, nil, nil, nil, log.NewNopLogger())
			require.NoError(t, err)

			startErr := services.StartAndAwaitRunning(context.Background(), ingester)
			if testData.expectedErr == "" {
				require.NoError(t, startErr)
			} else {
				require.Error(t, startErr)
				assert.Contains(t, startErr.Error(), testData.expectedErr)
			}

			defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck
			testData.check(t, ingester)
		})
	}
}

func getWALReplayConcurrencyFromTSDBHeadOptions(userTSDB *userTSDB) int {
	head := reflect.ValueOf(userTSDB.db.Head()).Elem()
	opts := head.FieldByName("opts").Elem()
	walReplayConcurrency := opts.FieldByName("WALReplayConcurrency")
	return int(walReplayConcurrency.Int())
}

func TestIngester_shipBlocks(t *testing.T) {
	ctx := context.Background()

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Create the TSDB for 3 users and then replace the shipper with the mocked one
	mocks := []*uploaderMock{}
	for _, userID := range []string{"user-1", "user-2", "user-3"} {
		userDB, err := i.getOrCreateTSDB(userID)
		require.NoError(t, err)
		require.NotNil(t, userDB)

		m := &uploaderMock{}
		m.On("Sync", mock.Anything).Return(0, nil)
		mocks = append(mocks, m)

		userDB.shipper = m
	}

	// Ship blocks and assert on the mocked shipper
	i.shipBlocks(ctx, nil)

	for _, m := range mocks {
		m.AssertNumberOfCalls(t, "Sync", 1)
	}
}

func TestIngester_dontShipBlocksWhenTenantDeletionMarkerIsPresent(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	i.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())
	i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	require.Equal(t, int64(0), i.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjects := len(bucket.Objects())
	require.NotZero(t, numObjects)

	require.NoError(t, mimir_tsdb.WriteTenantDeletionMark(context.Background(), bucket, userID, nil, mimir_tsdb.NewTenantDeletionMark(time.Now())))
	numObjects++ // For deletion marker

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	db.lastDeletionMarkCheck.Store(0)

	// After writing tenant deletion mark,
	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())
	i.compactBlocks(context.Background(), true, math.MaxInt64, nil)
	require.Equal(t, int64(0), i.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjectsAfterMarkingTenantForDeletion := len(bucket.Objects())
	require.Equal(t, numObjects, numObjectsAfterMarkingTenantForDeletion)
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_seriesCountIsCorrectAfterClosingTSDBForDeletedTenant(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	// Write tenant deletion mark.
	require.NoError(t, mimir_tsdb.WriteTenantDeletionMark(context.Background(), bucket, userID, nil, mimir_tsdb.NewTenantDeletionMark(time.Now())))

	i.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	require.Equal(t, int64(1), i.seriesCount.Load())

	// We call shipBlocks to check for deletion marker (it happens inside this method).
	i.shipBlocks(context.Background(), nil)

	// Verify that tenant deletion mark was found.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.deletionMarkFound.Load())

	// If we try to close TSDB now, it should succeed, even though TSDB is not idle and empty.
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Closing should decrease series count.
	require.Equal(t, int64(0), i.seriesCount.Load())
}

func TestIngester_closeAndDeleteUserTSDBIfIdle_shouldNotCloseTSDBIfShippingIsInProgress(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// We want it to be idle immediately (setting to 1ns because 0 means disabled).
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = time.Nanosecond

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Mock the shipper to slow down Sync() execution.
	s := mockUserShipper(t, i)
	s.On("Sync", mock.Anything).Run(func(mock.Arguments) {
		time.Sleep(3 * time.Second)
	}).Return(0, nil)

	// Mock the shipper meta (no blocks).
	db := i.getTSDB(userID)
	require.NoError(t, writeShipperMetaFile(log.NewNopLogger(), db.db.Dir(), shipperMeta{
		Version: shipperMetaVersion1,
	}))

	// Run blocks shipping in a separate go routine.
	go i.shipBlocks(ctx, nil)

	// Wait until shipping starts.
	test.Poll(t, 1*time.Second, activeShipping, func() interface{} {
		db.stateMtx.RLock()
		defer db.stateMtx.RUnlock()
		return db.state
	})

	assert.Equal(t, tsdbNotActive, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngester_closingAndOpeningTsdbConcurrently(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	_, err = i.getOrCreateTSDB(userID)
	require.NoError(t, err)

	iterations := 5000
	chanErr := make(chan error, 1)
	quit := make(chan bool)

	go func() {
		for {
			select {
			case <-quit:
				return
			default:
				_, err = i.getOrCreateTSDB(userID)
				if err != nil {
					chanErr <- err
				}
			}
		}
	}()

	for k := 0; k < iterations; k++ {
		i.closeAndDeleteUserTSDBIfIdle(userID)
	}

	select {
	case err := <-chanErr:
		assert.Fail(t, err.Error())
		quit <- true
	default:
		quit <- true
	}
}

func TestIngester_idleCloseEmptyTSDB(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	defer services.StopAndAwaitTerminated(ctx, i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	db, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, db)

	// Run compaction and shipping.
	i.compactBlocks(ctx, true, math.MaxInt64, nil)
	i.shipBlocks(ctx, nil)

	// Make sure we can close completely empty TSDB without problems.
	require.Equal(t, tsdbIdleClosed, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Verify that it was closed.
	db = i.getTSDB(userID)
	require.Nil(t, db)

	// And we can recreate it again, if needed.
	db, err = i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, db)
}

type uploaderMock struct {
	mock.Mock
}

// Sync mocks BlocksUploader.Sync()
func (m *uploaderMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func TestIngester_invalidSamplesDontChangeLastUpdateTime(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sampleTimestamp := int64(model.Now())

	{
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	lastUpdate := db.getLastUpdate()

	// Wait until 1 second passes.
	test.Poll(t, 1*time.Second, time.Now().Unix()+1, func() interface{} {
		return time.Now().Unix()
	})

	// Push another sample to the same metric and timestamp, with different value. We expect to get error.
	{
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 1, sampleTimestamp)
		_, err = i.Push(ctx, req)
		require.Error(t, err)
	}

	// Make sure last update hasn't changed.
	require.Equal(t, lastUpdate, db.getLastUpdate())
}

func TestIngester_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, reg *prometheus.Registry)
	}{
		"should flush blocks on shutdown when enabled through the configuration": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())
			},
		},

		"should flush blocks on shutdown when enabled through the prepare shutdown API endpoint": {
			setupIngester: func(cfg *Config) {
				cfg.IngesterRing.UnregisterOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				response1 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response1, httptest.NewRequest("GET", "/ingester/prepare-shutdown", nil))
				require.Equal(t, "unset\n", response1.Body.String())
				require.Equal(t, 200, response1.Code)

				response2 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response2, httptest.NewRequest("POST", "/ingester/prepare-shutdown", nil))
				require.Equal(t, 204, response2.Code)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_prepare_shutdown_requested If the ingester has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_ingester_prepare_shutdown_requested gauge
					cortex_ingester_prepare_shutdown_requested 1
				`), "cortex_ingester_prepare_shutdown_requested"))

				response3 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response3, httptest.NewRequest("GET", "/ingester/prepare-shutdown", nil))
				require.Equal(t, "set\n", response3.Body.String())
				require.Equal(t, 200, response3.Code)

				response4 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response4, httptest.NewRequest("DELETE", "/ingester/prepare-shutdown", nil))
				require.Equal(t, 204, response4.Code)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_prepare_shutdown_requested If the ingester has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_ingester_prepare_shutdown_requested gauge
					cortex_ingester_prepare_shutdown_requested 0
				`), "cortex_ingester_prepare_shutdown_requested"))

				response5 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response5, httptest.NewRequest("POST", "/ingester/prepare-shutdown", nil))
				require.Equal(t, 204, response5.Code)

				// Shutdown ingester. This triggers compaction and flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())

				// If the ingester isn't "running", requests to the prepare-shutdown endpoint should fail
				response6 := httptest.NewRecorder()
				i.PrepareShutdownHandler(response6, httptest.NewRequest("POST", "/ingester/prepare-shutdown", nil))
				require.Equal(t, 503, response6.Code)
			},
		},

		"should flush blocks when the shutdown API endpoint is called": {
			setupIngester: func(cfg *Config) {
				cfg.IngesterRing.UnregisterOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/shutdown", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())
			},
		},

		"should flush blocks for all tenants when the flush API endpoint is called without tenants list": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())
			},
		},

		"should flush blocks for requested tenants when the flush API endpoint is called with tenants list": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadata(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				users := url.Values{}
				users.Add(tenantParam, "unknown-user")
				users.Add(tenantParam, "another-unknown-user")

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true&"+users.Encode(), nil))

				// Still nothing shipped or compacted.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))
				verifyCompactedHead(t, i, false)

				users = url.Values{}
				users.Add(tenantParam, "different-user")
				users.Add(tenantParam, userID) // Our user
				users.Add(tenantParam, "yet-another-user")

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true&"+users.Encode(), nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())
			},
		},

		"should flush blocks spanning multiple days with flush API endpoint is called": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				// Pushing 5 samples, spanning over 3 days.
				// First block
				pushSingleSampleAtTime(t, i, 23*time.Hour.Milliseconds())
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()-1)

				// Second block
				pushSingleSampleAtTime(t, i, 24*time.Hour.Milliseconds()+1)
				pushSingleSampleAtTime(t, i, 25*time.Hour.Milliseconds())

				// Third block, far in the future.
				pushSingleSampleAtTime(t, i, 50*time.Hour.Milliseconds())

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0

					# HELP cortex_ingester_shipper_last_successful_upload_timestamp_seconds Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.
					# TYPE cortex_ingester_shipper_last_successful_upload_timestamp_seconds gauge
					cortex_ingester_shipper_last_successful_upload_timestamp_seconds 0
				`), "cortex_ingester_shipper_uploads_total", "cortex_ingester_shipper_last_successful_upload_timestamp_seconds"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 3
				`), "cortex_ingester_shipper_uploads_total"))

				verifyShipperLastSuccessfulUploadTimeMetric(t, reg, time.Now().Unix())

				userDB := i.getTSDB(userID)
				require.NotNil(t, userDB)

				blocks := userDB.Blocks()
				require.Equal(t, 3, len(blocks))
				require.Equal(t, 23*time.Hour.Milliseconds(), blocks[0].Meta().MinTime)
				require.Equal(t, 24*time.Hour.Milliseconds(), blocks[0].Meta().MaxTime) // Block maxt is exclusive.

				require.Equal(t, 24*time.Hour.Milliseconds()+1, blocks[1].Meta().MinTime)
				require.Equal(t, 26*time.Hour.Milliseconds(), blocks[1].Meta().MaxTime)

				require.Equal(t, 50*time.Hour.Milliseconds()+1, blocks[2].Meta().MaxTime) // Block maxt is exclusive.
			},
		},

		"should not allow to flush blocks with flush API endpoint if ingester is not in Running state": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, _ *prometheus.Registry) {
				// Stop the ingester.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				rec := httptest.NewRecorder()
				i.FlushHandler(rec, httptest.NewRequest("POST", "/ingester/flush?wait=true", nil))

				assert.Equal(t, http.StatusServiceUnavailable, rec.Result().StatusCode)

				body, err := io.ReadAll(rec.Result().Body)
				require.NoError(t, err)
				assert.Equal(t, newUnavailableError(services.Terminated).Error(), string(body))
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute // Long enough to not be reached during the test.

			if tc.setupIngester != nil {
				tc.setupIngester(&cfg)
			}

			// Create ingester
			reg := prometheus.NewPedanticRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's healthy
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// mock user's shipper
			tc.action(t, i, reg)
		})
	}
}

func TestIngester_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	reg = prometheus.NewPedanticRegistry()
	i, err = NewForFlusher(i.cfg, i.limits, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
}

func mockUserShipper(t *testing.T, i *Ingester) *uploaderMock {
	m := &uploaderMock{}
	userDB, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, userDB)

	userDB.shipper = m
	return m
}

func Test_Ingester_UserStats(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200", "route", "get_user"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "route", "get_user"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	registry := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.tsdbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	assert.Equal(t, uint64(3), res.NumSeries)

	res, err = i.UserStats(ctx, &client.UserStatsRequest{CountMethod: client.IN_MEMORY})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	// Active series are considered according to the wall time during the push, not the sample timestamp.
	// Therefore all three series are still active at this point.
	assert.Equal(t, uint64(3), res.NumSeries)
}

func Test_Ingester_AllUserStats(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.FromStrings(labels.MetricName, "test_1_1", "status", "200", "route", "get_user"), 1, 100000},
		{"user-1", labels.FromStrings(labels.MetricName, "test_1_1", "status", "500", "route", "get_user"), 1, 110000},
		{"user-1", labels.FromStrings(labels.MetricName, "test_1_2"), 2, 200000},
		{"user-2", labels.FromStrings(labels.MetricName, "test_2_1"), 2, 200000},
		{"user-2", labels.FromStrings(labels.MetricName, "test_2_2"), 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.tsdbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.AllUserStats(context.Background(), &client.UserStatsRequest{})
	require.NoError(t, err)

	expect := []*client.UserIDStatsResponse{
		{
			UserId: "user-1",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.2,
				NumSeries:         3,
				ApiIngestionRate:  0.2,
				RuleIngestionRate: 0,
			},
		},
		{
			UserId: "user-2",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.13333333333333333,
				NumSeries:         2,
				ApiIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
			},
		},
	}
	assert.ElementsMatch(t, expect, res.Stats)
}

func TestIngesterCompactIdleBlock(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)

	i.compactBlocks(context.Background(), false, 0, nil)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	i.compactBlocks(context.Background(), false, 0, nil)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))

	// Pushing another sample still works.
	pushSingleSampleWithMetadata(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), "cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users"))
}

func TestIngesterCompactAndCloseIdleTSDB(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Second // Required to enable shipping.
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 100 * time.Millisecond
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = 100 * time.Millisecond

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(time.Now())

	require.Equal(t, int64(1), i.seriesCount.Load())

	metricsToCheck := []string{"cortex_ingester_memory_series_created_total", "cortex_ingester_memory_series_removed_total", "cortex_ingester_memory_users", "cortex_ingester_active_series",
		"cortex_ingester_memory_metadata", "cortex_ingester_memory_metadata_created_total", "cortex_ingester_memory_metadata_removed_total"}

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))

	// Wait until TSDB has been closed and removed.
	test.Poll(t, 20*time.Second, 0, func() interface{} {
		i.tsdbsMtx.Lock()
		defer i.tsdbsMtx.Unlock()
		return len(i.tsdbs)
	})

	require.Greater(t, testutil.ToFloat64(i.metrics.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))), float64(0))
	i.updateActiveSeries(time.Now())
	require.Equal(t, int64(0), i.seriesCount.Load()) // Flushing removed all series from memory.

	// Verify that user has disappeared from metrics.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 0

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
    `), metricsToCheck...))

	// Pushing another sample will recreate TSDB.
	pushSingleSampleWithMetadata(t, i)
	i.updateActiveSeries(time.Now())

	// User is back.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))
}

func verifyCompactedHead(t *testing.T, i *Ingester, expected bool) {
	db := i.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()
	require.Equal(t, expected, h.NumSeries() == 0)
}

func pushSingleSampleWithMetadata(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, util.TimeToMillis(time.Now()))
	req.Metadata = append(req.Metadata, &mimirpb.MetricMetadata{MetricFamilyName: "test", Help: "a help for metric", Unit: "", Type: mimirpb.COUNTER})
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func pushSingleSampleAtTime(t *testing.T, i *Ingester, ts int64) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, ts)
	_, err := i.Push(ctx, req)
	require.NoError(t, err)
}

func TestHeadCompactionOnStartup(t *testing.T) {
	// Create a temporary directory for TSDB
	tempDir := t.TempDir()

	// Build TSDB for user, with data covering 24 hours.
	{
		// Number of full chunks, 12 chunks for 24hrs.
		numFullChunks := 12
		chunkRange := 2 * time.Hour.Milliseconds()

		userDir := filepath.Join(tempDir, userID)
		require.NoError(t, os.Mkdir(userDir, 0700))

		db, err := tsdb.Open(userDir, nil, nil, &tsdb.Options{
			RetentionDuration: int64(time.Hour * 25 / time.Millisecond),
			NoLockfile:        true,
			MinBlockDuration:  chunkRange,
			MaxBlockDuration:  chunkRange,
		}, nil)
		require.NoError(t, err)

		db.DisableCompactions()
		head := db.Head()

		l := labels.FromStrings("n", "v")
		for i := 0; i < numFullChunks; i++ {
			// Not using db.Appender() as it checks for compaction.
			app := head.Appender(context.Background())
			_, err := app.Append(0, l, int64(i)*chunkRange+1, 9.99)
			require.NoError(t, err)
			_, err = app.Append(0, l, int64(i+1)*chunkRange, 9.99)
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}

		dur := time.Duration(head.MaxTime()-head.MinTime()) * time.Millisecond
		require.True(t, dur > 23*time.Hour)
		require.Equal(t, 0, len(db.Blocks()))
		require.NoError(t, db.Close())
	}

	limits := defaultLimitsTestConfig()

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.BlocksStorageConfig.TSDB.Dir = tempDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "s3"
	ingesterCfg.BlocksStorageConfig.Bucket.S3.Endpoint = "localhost"
	ingesterCfg.BlocksStorageConfig.TSDB.Retention = 2 * 24 * time.Hour // Make sure that no newly created blocks are deleted.

	ingester, err := New(ingesterCfg, overrides, createAndStartRing(t, ingesterCfg.IngesterRing.ToRingConfig()), nil, nil, nil, nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))

	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)

	h := db.Head()

	dur := time.Duration(h.MaxTime()-h.MinTime()) * time.Millisecond
	require.True(t, dur <= 2*time.Hour)
	require.Equal(t, 11, len(db.Blocks()))
}

func TestIngester_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push some data.
	pushSingleSampleWithMetadata(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}

func TestIngesterNotDeleteUnshippedBlocks(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = time.Millisecond // Which means delete all but first block.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.NoError(t, db.Compact())

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Saying that we have shipped the second block, so only that should get deleted.
	require.Nil(t, writeShipperMetaFile(nil, db.db.Dir(), shipperMeta{
		Version: shipperMetaVersion1,
		Shipped: map[ulid.ULID]model.Time{oldBlocks[1].Meta().ULID: model.TimeFromUnixNano(time.Now().UnixNano())},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.NoError(t, db.Compact())

	// Only the second block should be gone along with a new block.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[0].Meta().ULID, newBlocks[0].Meta().ULID)    // First block remains same.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID)    // 3rd block becomes 2nd now.
	require.NotEqual(t, oldBlocks[1].Meta().ULID, newBlocks[2].Meta().ULID) // The new block won't match previous 2nd block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Shipping 2 more blocks, hence all the blocks from first round.
	require.Nil(t, writeShipperMetaFile(nil, db.db.Dir(), shipperMeta{
		Version: shipperMetaVersion1,
		Shipped: map[ulid.ULID]model.Time{
			oldBlocks[1].Meta().ULID: model.TimeFromUnixNano(time.Now().UnixNano()),
			newBlocks[0].Meta().ULID: model.TimeFromUnixNano(time.Now().UnixNano()),
			newBlocks[1].Meta().ULID: model.TimeFromUnixNano(time.Now().UnixNano()),
		},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(6); j < 7; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.NoError(t, db.Compact())

	// All blocks from the old blocks should be gone now.
	newBlocks2 := db.Blocks()
	require.Equal(t, 2, len(newBlocks2))

	require.Equal(t, newBlocks[2].Meta().ULID, newBlocks2[0].Meta().ULID) // Block created in last round.
	for _, b := range oldBlocks {
		// Second block is not one among old blocks.
		require.NotEqual(t, b.Meta().ULID, newBlocks2[1].Meta().ULID)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks2[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))
}

func TestIngesterNotDeleteShippedBlocksUntilRetentionExpires(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = 1 * time.Hour // This means only blocks that are shipped for more than an hour can be deleted

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.NoError(t, db.Compact())

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Lets say that the first block was shipped 2 hours ago and the second block only 30 minutes ago.
	require.Nil(t, writeShipperMetaFile(nil, db.db.Dir(), shipperMeta{
		Version: shipperMetaVersion1,
		Shipped: map[ulid.ULID]model.Time{
			oldBlocks[0].Meta().ULID: model.TimeFromUnixNano(time.Now().Add(-2 * time.Hour).UnixNano()),
			oldBlocks[1].Meta().ULID: model.TimeFromUnixNano(time.Now().Add(-30 * time.Minute).UnixNano()),
		},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}
	require.NoError(t, db.Compact())

	// Only the last two old blocks plus the one containing the newly added samples should remain.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[1].Meta().ULID, newBlocks[0].Meta().ULID) // Second block becomes first block.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID) // Third block becomes second block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[1].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds")) // Note it has to be newBlocks[1] because newBlocks[0] was already shipped, it is just kept due to the retention.
}

func TestIngesterWithShippingDisabledDeletesBlocksOnlyAfterRetentionExpires(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0            // Disabled shipping
	cfg.BlocksStorageConfig.TSDB.Retention = 5 * time.Second // With shipping disabled this means will only expire 1 hour after the block creation time.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, j*chunkRangeMilliSec)
		_, err := i.Push(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.NoError(t, db.Compact())

	oldBlocks := db.Blocks()
	require.Len(t, oldBlocks, 3)

	// Yes, we're sleeping in this test to let the retention of the newly compacted blocks expire
	time.Sleep(cfg.BlocksStorageConfig.TSDB.Retention)

	// Add more samples that could trigger another compaction and hence reload of blocks.
	req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, 5*chunkRangeMilliSec)
	_, err = i.Push(ctx, req)
	require.NoError(t, err)
	require.NoError(t, db.Compact())

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 4
	`), "cortex_ingester_tsdb_compactions_total"))

	// Only last compacted block should remain.
	newBlocks := db.Blocks()
	require.Equal(t, 1, len(newBlocks))
	require.NotContains(t, []ulid.ULID{oldBlocks[0].Meta().ULID, oldBlocks[1].Meta().ULID, oldBlocks[2].Meta().ULID}, newBlocks[0].Meta().ULID)
}

func TestIngesterPushErrorDuringForcedCompaction(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push a sample, it should succeed.
	pushSingleSampleWithMetadata(t, i)

	// We mock a flushing by setting the boolean.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	ok, _ := db.changeStateToForcedCompaction(active, math.MaxInt64)
	require.True(t, ok)

	// Ingestion should fail with a 503.
	req, _, _, _ := mockWriteRequest(t, labels.FromStrings(labels.MetricName, "test"), 0, util.TimeToMillis(time.Now()))
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = i.Push(ctx, req)
	expectedErr := newErrorWithStatus(wrapOrAnnotateWithUser(errTSDBForcedCompaction, userID), codes.Internal)
	checkErrorWithStatus(t, err, expectedErr)

	// Ingestion is successful after a flush.
	ok, _ = db.changeState(forceCompacting, active)
	require.True(t, ok)
	pushSingleSampleWithMetadata(t, i)
}

func TestIngesterNoFlushWithInFlightRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, registry)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	// Push few samples.
	for j := 0; j < 5; j++ {
		pushSingleSampleWithMetadata(t, i)
	}

	// Verifying that compaction won't happen when a request is in flight.

	// This mocks a request in flight.
	db := i.getTSDB(userID)
	lockState, err := db.acquireAppendLock(0)
	require.NoError(t, err)

	// Flush handler only triggers compactions, but doesn't wait for them to finish. We cannot use ?wait=true here,
	// because it would deadlock -- flush will wait for appendLock to be released.
	i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/ingester/flush", nil))

	// Flushing should not have succeeded even after 5 seconds.
	time.Sleep(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// No requests in flight after this.
	db.releaseAppendLock(lockState)

	// Let's wait until all head series have been flushed.
	test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
		db := i.getTSDB(userID)
		if db == nil {
			return false
		}
		return db.Head().NumSeries()
	})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 1
	`), "cortex_ingester_tsdb_compactions_total"))
}

func TestIngester_PushInstanceLimits(t *testing.T) {
	tests := map[string]struct {
		limits                     InstanceLimits
		reqs                       map[string][]*mimirpb.WriteRequest
		expectedErr                error
		expectedOptionalLoggingErr bool
		expectedGRPCErr            bool
	}{
		"should succeed creating one user and series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*mimirpb.WriteRequest{
				"test": {
					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						[]*mimirpb.MetricMetadata{
							{MetricFamilyName: "metric_name_1", Help: "a help for metric_name_1", Unit: "", Type: mimirpb.COUNTER},
						},
						mimirpb.API,
					),
				},
			},
			expectedErr: nil,
		},

		"should fail creating two series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*mimirpb.WriteRequest{
				"test": {
					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test1"}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),

					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test2"}}}, // another series
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},
			expectedErr:                errMaxInMemorySeriesReached,
			expectedGRPCErr:            true,
			expectedOptionalLoggingErr: true,
		},

		"should fail creating two users": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},

			reqs: map[string][]*mimirpb.WriteRequest{
				"user1": {
					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test1"}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),
				},

				"user2": {
					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test2"}}}, // another series
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},
			expectedErr:                errMaxTenantsReached,
			expectedGRPCErr:            true,
			expectedOptionalLoggingErr: true,
		},

		"should fail pushing samples in two requests due to rate limit": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1, MaxIngestionRate: 0.001},

			reqs: map[string][]*mimirpb.WriteRequest{
				"user1": {
					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test1"}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),

					mimirpb.ToWriteRequest(
						[][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test1"}}},
						[]mimirpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				},
			},
			expectedErr:                errMaxIngestionRateReached,
			expectedGRPCErr:            false,
			expectedOptionalLoggingErr: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.InstanceLimitsFn = func() *InstanceLimits {
				return &testData.limits
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// Iterate through users in sorted order (by username).
			uids := []string{}
			totalPushes := 0
			for uid, requests := range testData.reqs {
				uids = append(uids, uid)
				totalPushes += len(requests)
			}
			slices.Sort(uids)

			pushIdx := 0
			for _, uid := range uids {
				ctx := user.InjectOrgID(context.Background(), uid)

				for _, origReq := range testData.reqs[uid] {
					pushIdx++

					// Clone the request so that it's safe to be sent multiple times.
					reqData, marshalErr := origReq.Marshal()
					require.NoError(t, marshalErr)
					req := &mimirpb.WriteRequest{}
					require.NoError(t, req.Unmarshal(reqData))

					// We simulate the sequence of calls done by the gRPC handler.
					_, err := pushWithSimulatedGRPCHandler(ctx, i, req)

					if pushIdx < totalPushes {
						require.NoError(t, err)
					} else {
						// Last push may expect error.
						if testData.expectedErr != nil {
							assert.ErrorIs(t, err, testData.expectedErr)

							if testData.expectedOptionalLoggingErr {
								var optional middleware.OptionalLogging
								assert.ErrorAs(t, err, &optional)
							}

							if testData.expectedGRPCErr {
								s, ok := grpcutil.ErrorToStatus(err)
								require.True(t, ok, "expected to be able to convert to gRPC status")
								assert.Equal(t, codes.Unavailable, s.Code())
							}
						} else {
							assert.NoError(t, err)
						}
					}

					// imitate time ticking between each push
					i.ingestionRate.Tick()

					rate := testutil.ToFloat64(i.metrics.ingestionRate)
					require.NotZero(t, rate)
				}
			}
		})
	}
}

func TestIngester_PushGrpcMethod_Disabled(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.PushGrpcMethodEnabled = false

	registry := prometheus.NewRegistry()

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() any {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	req := writeRequestSingleSeries(
		labels.FromStrings(labels.MetricName, "foo", "l", "1"),
		[]mimirpb.Sample{{TimestampMs: 1_000, Value: 1}},
	)
	_, err = i.Push(ctx, req)
	require.ErrorIs(t, err, errPushGrpcDisabled)
}

func TestIngester_instanceLimitsMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	l := InstanceLimits{
		MaxIngestionRate:             10,
		MaxInMemoryTenants:           20,
		MaxInMemorySeries:            30,
		MaxInflightPushRequests:      40,
		MaxInflightPushRequestsBytes: 50,
	}

	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits {
		return &l
	}

	_, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 30
		cortex_ingester_instance_limits{limit="max_tenants"} 20
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 40
		cortex_ingester_instance_limits{limit="max_inflight_push_requests_bytes"} 50
	`), "cortex_ingester_instance_limits"))

	l.MaxInMemoryTenants = 1000
	l.MaxInMemorySeries = 2000

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_limits Instance limits used by this ingester.
		# TYPE cortex_ingester_instance_limits gauge
		cortex_ingester_instance_limits{limit="max_ingestion_rate"} 10
		cortex_ingester_instance_limits{limit="max_series"} 2000
		cortex_ingester_instance_limits{limit="max_tenants"} 1000
		cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 40
		cortex_ingester_instance_limits{limit="max_inflight_push_requests_bytes"} 50
	`), "cortex_ingester_instance_limits"))
}

func TestIngester_inflightPushRequests(t *testing.T) {
	t.Run("with classic ingester", func(t *testing.T) {
		limits := InstanceLimits{MaxInflightPushRequests: 1}

		cfg := defaultIngesterTestConfig(t)
		cfg.InstanceLimitsFn = func() *InstanceLimits { return &limits }

		// Create a mocked ingester
		reg := prometheus.NewPedanticRegistry()
		i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
		require.NoError(t, err)

		testIngesterInflightPushRequests(t, i, reg)
	})

	t.Run("with ingest storage enabled", func(t *testing.T) {
		limits := InstanceLimits{MaxInflightPushRequests: 1}

		overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		require.NoError(t, err)

		cfg := defaultIngesterTestConfig(t)
		cfg.InstanceLimitsFn = func() *InstanceLimits { return &limits }

		reg := prometheus.NewPedanticRegistry()
		i, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, reg)

		// Re-enable push gRPC method to simulate migration period, when ingester can receive requests from gRPC
		i.cfg.PushGrpcMethodEnabled = true

		testIngesterInflightPushRequests(t, i, reg)
	})
}

func testIngesterInflightPushRequests(t *testing.T, i *Ingester, reg prometheus.Gatherer) {
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	})

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	startCh := make(chan struct{})

	const targetRequestDuration = time.Second

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		req := prepareRequestForTargetRequestDuration(ctx, t, i, targetRequestDuration)

		// Signal that we're going to do the real push now.
		close(startCh)

		_, err := pushWithSimulatedGRPCHandler(ctx, i, req)
		return err
	})

	g.Go(func() error {
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, "testcase"), 1, 1024)

		select {
		case <-ctx.Done():
		// failed to setup
		case <-startCh:
			// we can start the test.
		}

		test.Poll(t, targetRequestDuration/3, int64(1), func() interface{} {
			return i.inflightPushRequests.Load()
		})

		_, err := pushWithSimulatedGRPCHandler(ctx, i, req)
		require.ErrorIs(t, err, errMaxInflightRequestsReached)

		return nil
	})

	require.NoError(t, g.Wait())

	// Ensure the rejected request has been tracked in a metric.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_inflight_push_requests Current number of inflight push requests in ingester.
		# TYPE cortex_ingester_inflight_push_requests gauge
		cortex_ingester_inflight_push_requests 0
		# HELP cortex_ingester_instance_rejected_requests_total Requests rejected for hitting per-instance limits
		# TYPE cortex_ingester_instance_rejected_requests_total counter
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests"} 1
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests_bytes"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_ingestion_rate"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_series"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_tenants"} 0
	`), "cortex_ingester_instance_rejected_requests_total", "cortex_ingester_inflight_push_requests"))
}

func TestIngester_inflightPushRequestsBytes(t *testing.T) {
	var limitsMx sync.Mutex
	limits := InstanceLimits{MaxInflightPushRequestsBytes: 0}

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits {
		limitsMx.Lock()
		defer limitsMx.Unlock()

		// Make a copy
		il := limits
		return &il
	}

	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	startCh := make(chan int)

	const targetRequestDuration = time.Second

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		req := prepareRequestForTargetRequestDuration(ctx, t, i, targetRequestDuration)

		// Update instance limits. Set limit to EXACTLY the request size.
		limitsMx.Lock()
		limits.MaxInflightPushRequestsBytes = int64(req.Size())
		limitsMx.Unlock()

		// Signal that we're going to do the real push now.
		startCh <- req.Size()
		close(startCh)

		_, err := pushWithSimulatedGRPCHandler(ctx, i, req)
		return err
	})

	g.Go(func() error {
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, "testcase1"), 1, 1024)

		var requestSize int
		select {
		case <-ctx.Done():
		// failed to setup
		case requestSize = <-startCh:
			// we can start the test.
		}

		test.Poll(t, targetRequestDuration/3, int64(1), func() interface{} {
			return i.inflightPushRequests.Load()
		})

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_ingester_inflight_push_requests_bytes Total sum of inflight push request sizes in ingester in bytes.
			# TYPE cortex_ingester_inflight_push_requests_bytes gauge
			cortex_ingester_inflight_push_requests_bytes %d
		`, requestSize)), "cortex_ingester_inflight_push_requests_bytes"))

		// Starting push request fails
		_, err = i.StartPushRequest(ctx, 100)
		require.ErrorIs(t, err, errMaxInflightRequestsBytesReached)

		// Starting push request with unknown size fails
		_, err = i.StartPushRequest(ctx, 0)
		require.ErrorIs(t, err, errMaxInflightRequestsBytesReached)

		// Sending push request fails
		_, err := pushWithSimulatedGRPCHandler(ctx, i, req)
		require.ErrorIs(t, err, errMaxInflightRequestsBytesReached)

		return nil
	})

	require.NoError(t, g.Wait())

	// Ensure the rejected request has been tracked in a metric.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_instance_rejected_requests_total Requests rejected for hitting per-instance limits
		# TYPE cortex_ingester_instance_rejected_requests_total counter
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_inflight_push_requests_bytes"} 3
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_ingestion_rate"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_series"} 0
		cortex_ingester_instance_rejected_requests_total{reason="ingester_max_tenants"} 0
	`), "cortex_ingester_instance_rejected_requests_total"))
}

func prepareRequestForTargetRequestDuration(ctx context.Context, t *testing.T, i *Ingester, targetRequestDuration time.Duration) *mimirpb.WriteRequest {
	samples := 100000
	ser := 1

	// Find right series&samples count to make sure that push takes given target duration.
	for {
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("test-%d-%d", ser, samples)), ser, samples)

		start := time.Now()
		_, err := i.Push(ctx, req)
		require.NoError(t, err)

		elapsed := time.Since(start)
		t.Log(ser, samples, elapsed)
		if elapsed > targetRequestDuration {
			break
		}

		samples = int(float64(samples) * float64(targetRequestDuration/elapsed) * 1.5) // Adjust number of series to hit our targetRequestDuration push duration.
		for samples >= int(time.Hour.Milliseconds()) {
			// We generate one sample per millisecond, if we have more than an hour of samples TSDB will fail with "out of bounds".
			// So we trade samples for series here.
			samples /= 10
			ser *= 10
		}
	}

	// Now repeat push with number of samples calibrated to our target request duration.
	req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("real-%d-%d", ser, samples)), ser, samples)
	return req
}

func generateSamplesForLabel(baseLabels labels.Labels, series, samples int) *mimirpb.WriteRequest {
	lbls := make([][]mimirpb.LabelAdapter, 0, series*samples)
	ss := make([]mimirpb.Sample, 0, series*samples)

	for s := 0; s < series; s++ {
		l := labels.NewBuilder(baseLabels).Set("series", strconv.Itoa(s)).Labels()
		for i := 0; i < samples; i++ {
			ss = append(ss, mimirpb.Sample{
				Value:       float64(i),
				TimestampMs: int64(i),
			})
			lbls = append(lbls, mimirpb.FromLabelsToLabelAdapters(l))
		}
	}

	return mimirpb.ToWriteRequest(lbls, ss, nil, nil, mimirpb.API)
}

func buildTestMatrix(numSeries int, samplesPerSeries int, offset int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        model.LabelValue(fmt.Sprintf("testjob%d", i%2)),
			},
			Values: make([]model.SamplePair, 0, samplesPerSeries),
		}
		for j := 0; j < samplesPerSeries; j++ {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(i + j + offset),
				Value:     model.SampleValue(i + j + offset),
			})
		}
		m = append(m, &ss)
	}
	sort.Sort(m)
	return m
}

func matrixToSamples(m model.Matrix) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, mimirpb.Sample{
				TimestampMs: int64(sp.Timestamp),
				Value:       float64(sp.Value),
			})
		}
	}
	return samples
}

// Return one copy of the labels per sample
func matrixToLables(m model.Matrix) [][]mimirpb.LabelAdapter {
	var labels [][]mimirpb.LabelAdapter
	for _, ss := range m {
		for range ss.Values {
			labels = append(labels, mimirpb.FromMetricsToLabelAdapters(ss.Metric))
		}
	}
	return labels
}

func runTestQuery(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string) (model.Matrix, *client.QueryRequest, error) {
	return runTestQueryTimes(ctx, t, ing, ty, n, v, model.Earliest, model.Latest)
}

func runTestQueryTimes(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string, start, end model.Time) (model.Matrix, *client.QueryRequest, error) {
	matcher, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		return nil, nil, err
	}
	req, err := client.ToQueryRequest(start, end, []*labels.Matcher{matcher})
	if err != nil {
		return nil, nil, err
	}
	s := stream{ctx: ctx}
	err = ing.QueryStream(req, &s)
	require.NoError(t, err)

	res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
	require.NoError(t, err)
	sort.Sort(res)
	return res, req, nil
}

func pushTestMetadata(t *testing.T, ing *Ingester, numMetadata, metadataPerMetric int) ([]string, map[string][]*mimirpb.MetricMetadata) {
	userIDs := []string{"1", "2", "3"}

	// Create test metadata.
	// Map of userIDs, to map of metric => metadataSet
	testData := map[string][]*mimirpb.MetricMetadata{}
	for _, userID := range userIDs {
		metadata := make([]*mimirpb.MetricMetadata, 0, metadataPerMetric)
		for i := 0; i < numMetadata; i++ {
			metricName := fmt.Sprintf("testmetric_%d", i)
			for j := 0; j < metadataPerMetric; j++ {
				m := &mimirpb.MetricMetadata{MetricFamilyName: metricName, Help: fmt.Sprintf("a help for %d", j), Unit: "", Type: mimirpb.COUNTER}
				metadata = append(metadata, m)
			}
		}
		testData[userID] = metadata
	}

	// Append metadata.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, testData[userID], mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func pushTestSamples(t testing.TB, ing *Ingester, numSeries, samplesPerSeries, offset int) ([]string, map[string]model.Matrix) {
	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, samplesPerSeries, i+offset)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), nil, nil, mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func TestIngesterPurgeMetadata(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestMetadata(t, ing, 10, 3)

	time.Sleep(40 * time.Millisecond)
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		ing.purgeUserMetricsMetadata()

		resp, err := ing.MetricsMetadata(ctx, client.DefaultMetricsMetadataRequest())
		require.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetMetadata()))
	}
}

func TestIngesterMetadataMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), nil, "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	_, _ = pushTestMetadata(t, ing, 10, 3)

	pushTestMetadata(t, ing, 10, 3)
	pushTestMetadata(t, ing, 10, 3) // We push the _exact_ same metrics again to ensure idempotency. Metadata is kept as a set so there shouldn't be a change of metrics.

	metricNames := []string{
		"cortex_ingester_memory_metadata_created_total",
		"cortex_ingester_memory_metadata_removed_total",
		"cortex_ingester_memory_metadata",
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 90
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
	`), metricNames...))

	time.Sleep(40 * time.Millisecond)
	ing.purgeUserMetricsMetadata()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
		# HELP cortex_ingester_memory_metadata_removed_total The total number of metadata that were removed per user.
		# TYPE cortex_ingester_memory_metadata_removed_total counter
		cortex_ingester_memory_metadata_removed_total{user="1"} 30
		cortex_ingester_memory_metadata_removed_total{user="2"} 30
		cortex_ingester_memory_metadata_removed_total{user="3"} 30
	`), metricNames...))

}

func TestIngesterSendsOnlySeriesWithData(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), nil, "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestSamples(t, ing, 10, 1000, 0)

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, req, err := runTestQueryTimes(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+", model.Latest.Add(-15*time.Second), model.Latest)
		require.NoError(t, err)

		s := stream{
			ctx: ctx,
		}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err)

		// Nothing should be selected.
		require.Equal(t, 0, len(s.responses))
	}

	// Read samples back via chunk store.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

type stream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*client.QueryStreamResponse
}

func (s *stream) Context() context.Context {
	return s.ctx
}

func (s *stream) Send(response *client.QueryStreamResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

// Test that blank labels are removed by the ingester
func TestIngester_Push_SeriesWithBlankLabel(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), nil, "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	lbls := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: ""}, {Name: "bar", Value: ""}}}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing.Push(ctx, mimirpb.ToWriteRequest(
		lbls,
		[]mimirpb.Sample{{TimestampMs: 1, Value: 0}},
		nil,
		nil,
		mimirpb.API,
	))
	require.NoError(t, err)

	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, labels.MetricName, "testmetric")

	require.NoError(t, err)
	expected := model.Matrix{
		{
			Metric: model.Metric{labels.MetricName: "testmetric"},
			Values: []model.SamplePair{
				{Timestamp: 1, Value: 0},
			},
		},
	}

	assert.Equal(t, expected, res)
}

func TestIngesterUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerUser = 1
	limits.MaxGlobalMetricsWithMetadataPerUser = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		// Global Ingester limits are computed based on replication factor
		// Set RF=1 here to ensure the series and metadata limits
		// are actually set to 1 instead of 3.
		cfg.IngesterRing.ReplicationFactor = 1
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	// Series
	labels1 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}
	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric2", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{labels1}, []mimirpb.Sample{sample1}, nil, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append to two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		expectedErr := newErrorWithStatus(wrapOrAnnotateWithUser(newPerUserSeriesLimitReachedError(ing.limiter.limits.MaxGlobalSeriesPerUser(userID)), userID), codes.FailedPrecondition)
		checkErrorWithStatus(t, err, expectedErr)

		// Append two metadata, expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(labels1),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		require.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, client.DefaultMetricsMetadataRequest())
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()

}

func TestIngesterMetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 1
	limits.MaxGlobalMetadataPerMetric = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		// Global Ingester limits are computed based on replication factor
		// Set RF=1 here to ensure the series and metadata limits
		// are actually set to 1 instead of 3.
		cfg.IngesterRing.ReplicationFactor = 1
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	labels1 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{labels1}, []mimirpb.Sample{sample1}, nil, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		expectedErr := newErrorWithStatus(wrapOrAnnotateWithUser(newPerMetricSeriesLimitReachedError(ing.limiter.limits.MaxGlobalSeriesPerMetric(userID), labels3), userID), codes.FailedPrecondition)
		checkErrorWithStatus(t, err, expectedErr)

		// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		// Verify Series
		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(labels1),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		assert.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, client.DefaultMetricsMetadataRequest())
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()
}

// Construct a set of realistic-looking samples, all with slightly different label sets
func benchmarkData(nSeries int) (allLabels [][]mimirpb.LabelAdapter, allSamples []mimirpb.Sample) {
	// Real example from Kubernetes' embedded cAdvisor metrics, lightly obfuscated.
	var benchmarkLabels = labels.FromStrings(
		model.MetricNameLabel, "container_cpu_usage_seconds_total",
		"beta_kubernetes_io_arch", "amd64",
		"beta_kubernetes_io_instance_type", "c3.somesize",
		"beta_kubernetes_io_os", "linux",
		"container_name", "some-name",
		"cpu", "cpu01",
		"failure_domain_beta_kubernetes_io_region", "somewhere-1",
		"failure_domain_beta_kubernetes_io_zone", "somewhere-1b",
		"id", "/kubepods/burstable/pod6e91c467-e4c5-11e7-ace3-0a97ed59c75e/a3c8498918bd6866349fed5a6f8c643b77c91836427fb6327913276ebc6bde28",
		"image", "registry/organisation/name@sha256:dca3d877a80008b45d71d7edc4fd2e44c0c8c8e7102ba5cbabec63a374d1d506",
		"instance", "ip-111-11-1-11.ec2.internal",
		"job", "kubernetes-cadvisor",
		"kubernetes_io_hostname", "ip-111-11-1-11",
		"monitor", "prod",
		"name", "k8s_some-name_some-other-name-5j8s8_kube-system_6e91c467-e4c5-11e7-ace3-0a97ed59c75e_0",
		"namespace", "kube-system",
		"pod_name", "some-other-name-5j8s8",
	)

	for j := 0; j < nSeries; j++ {
		labels := mimirpb.FromLabelsToLabelAdapters(benchmarkLabels.Copy())
		for i := range labels {
			if labels[i].Name == "cpu" {
				labels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}
		allLabels = append(allLabels, labels)
		allSamples = append(allSamples, mimirpb.Sample{TimestampMs: 0, Value: float64(j)})
	}
	return
}

type TenantLimitsMock struct {
	mock.Mock
	validation.TenantLimits
}

func (t *TenantLimitsMock) ByUserID(userID string) *validation.Limits {
	returnArgs := t.Called(userID)
	if returnArgs.Get(0) == nil {
		return nil
	}
	return returnArgs.Get(0).(*validation.Limits)
}

func TestIngesterActiveSeries(t *testing.T) {
	labelsToPush := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}
	labelsToPushHist := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}

	req := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			nil,
			mimirpb.API,
		)
	}
	reqHist := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(t.UnixMilli(), util_test.GenerateTestGaugeHistogram(1))}, nil)
	}

	metricNames := []string{
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_series_custom_tracker",
		"cortex_ingester_active_native_histogram_buckets",
		"cortex_ingester_active_native_histogram_buckets_custom_tracker",
	}
	userID := "test_user"
	userID2 := "other_test_user"

	activeSeriesDefaultConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"bool_is_true_flagbased":  `{bool="true"}`,
		"bool_is_false_flagbased": `{bool="false"}`,
	})

	activeSeriesTenantConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"team_a": `{team="a"}`,
		"team_b": `{team="b"}`,
	})

	activeSeriesTenantOverride := new(TenantLimitsMock)
	activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesBaseCustomTrackersConfig: activeSeriesTenantConfig, NativeHistogramsIngestionEnabled: true})
	activeSeriesTenantOverride.On("ByUserID", userID2).Return(nil)

	tests := map[string]struct {
		test                func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer)
		reqs                []*mimirpb.WriteRequest
		expectedMetrics     string
		disableActiveSeries bool
	}{
		"successful push, should count active series": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should cleanup metrics when tsdb closed": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
				// close tsdbs and check for cleanup
				ingester.closeAllTSDB()
				expectedMetrics = ""
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should track custom matchers, removing when zero": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Pushing second time to have entires which are not going to be purged
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)

				// Adding time to make the first batch of pushes idle.
				// We update them in the exact moment in time where append time of the first push is already considered idle,
				// while the second append happens after the purge timestamp.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout)
				ingester.updateActiveSeries(currentTime)

				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Update active series again in a further future where no series are active anymore.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout)
				ingester.updateActiveSeries(currentTime)
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(""), metricNames...))
			},
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				expectedMetrics := ``

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Check that no active series are returned
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "team", "a")}
				series, err := listActiveSeries(context.Background(), ingester.getTSDB(userID), matchers)
				require.NoError(t, err)
				ts := buildSeriesSet(t, series)
				assert.Empty(t, ts)
			},
		},
		"active series for cardinality API": {
			test: func(t *testing.T, ingester *Ingester, _ prometheus.Gatherer) {
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(time.Now())

				// Get a subset of series for team A.
				matchers := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "team", "a"),
					labels.MustNewMatcher(labels.MatchEqual, "bool", "true"),
				}
				series, err := listActiveSeries(context.Background(), ingester.getTSDB(userID), matchers)
				require.NoError(t, err)

				var labelSet []labels.Labels
				labelSet = buildSeriesSet(t, series)
				// Expect 2 series for team="a"
				assert.Len(t, labelSet, 2)
				for _, lbls := range labelSet {
					assert.Equal(t, "a", lbls.Get("team"))
				}

				// Query first shard of test_metric, expect one series.
				shard1 := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_metric"),
					sharding.ShardSelector{ShardIndex: 0, ShardCount: 2}.Matcher(),
				}
				series, err = listActiveSeries(context.Background(), ingester.getTSDB(userID), shard1)
				require.NoError(t, err)
				labelSet = buildSeriesSet(t, series)
				assert.Len(t, labelSet, 1)

				// Query second shard of test_metric, expect remaining three series.
				shard2 := []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "test_metric"),
					sharding.ShardSelector{ShardIndex: 1, ShardCount: 2}.Matcher(),
				}
				series, err = listActiveSeries(context.Background(), ingester.getTSDB(userID), shard2)
				require.NoError(t, err)
				labelSet = buildSeriesSet(t, series)
				assert.Len(t, labelSet, 3)

				// Fast-forward to make series stale.
				ingester.updateActiveSeries(time.Now().Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout))

				series, err = listActiveSeries(context.Background(), ingester.getTSDB(userID), matchers)
				require.NoError(t, err)
				labelSet = buildSeriesSet(t, series)

				// No series should be active anymore.
				assert.Empty(t, labelSet)
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.Enabled = !testData.disableActiveSeries

			limits := defaultLimitsTestConfig()
			limits.ActiveSeriesBaseCustomTrackersConfig = activeSeriesDefaultConfig
			limits.NativeHistogramsIngestionEnabled = true
			overrides, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
			require.NoError(t, err)

			ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, nil, "", "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return ing.lifecycler.HealthyInstancesCount()
			})

			testData.test(t, ing, registry)
		})
	}
}

func TestIngesterActiveSeriesConfigChanges(t *testing.T) {
	labelsToPush := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}
	labelsToPushHist := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}

	req := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			nil,
			mimirpb.API,
		)
	}
	reqHist := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(t.UnixMilli(), util_test.GenerateTestGaugeHistogram(1))}, nil)
	}

	metricNames := []string{
		"cortex_ingester_active_series_loading",
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
		"cortex_ingester_active_native_histogram_series",
		"cortex_ingester_active_native_histogram_series_custom_tracker",
		"cortex_ingester_active_native_histogram_buckets",
		"cortex_ingester_active_native_histogram_buckets_custom_tracker",
	}
	userID := "test_user"
	userID2 := "other_test_user"

	activeSeriesDefaultConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"bool_is_true_flagbased":  `{bool="true"}`,
		"bool_is_false_flagbased": `{bool="false"}`,
	})

	activeSeriesTenantConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"team_a": `{team="a"}`,
		"team_b": `{team="b"}`,
	})

	defaultActiveSeriesTenantOverride := new(TenantLimitsMock)
	defaultActiveSeriesTenantOverride.On("ByUserID", userID2).Return(nil)
	defaultActiveSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesBaseCustomTrackersConfig: activeSeriesTenantConfig, NativeHistogramsIngestionEnabled: true})

	tests := map[string]struct {
		test               func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer)
		reqs               []*mimirpb.WriteRequest
		expectedMetrics    string
		activeSeriesConfig asmodel.CustomTrackersConfig
		tenantLimits       *TenantLimitsMock
	}{
		"override flag based config with runtime overwrite": {
			tenantLimits:       nil,
			activeSeriesConfig: activeSeriesDefaultConfig,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 16
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Add new runtime configs
				activeSeriesTenantOverride := new(TenantLimitsMock)
				activeSeriesTenantOverride.On("ByUserID", userID2).Return(nil)
				activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesBaseCustomTrackersConfig: activeSeriesTenantConfig, NativeHistogramsIngestionEnabled: true})
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesBaseCustomTrackersConfig = activeSeriesDefaultConfig
				limits.NativeHistogramsIngestionEnabled = true
				override, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
				require.NoError(t, err)
				ingester.limits = override
				currentTime = time.Now()
				// First update reloads the config
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
            	    cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
            	    cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
            	    cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
            	    cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
            	    cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"remove runtime overwrite and revert to flag based config": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			tenantLimits:       defaultActiveSeriesTenantOverride,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
            	    cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
            	    cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
            	    cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
            	    cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
            	    cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Remove runtime configs
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesBaseCustomTrackersConfig = activeSeriesDefaultConfig
				limits.NativeHistogramsIngestionEnabled = true
				override, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
            	    cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 16
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"changing runtime override should result in new metrics": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="test_user"} 16
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Change runtime configs
				activeSeriesTenantOverride := new(TenantLimitsMock)
				activeSeriesTenantOverride.On("ByUserID", userID).Return(&validation.Limits{ActiveSeriesBaseCustomTrackersConfig: mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
					"team_a": `{team="a"}`,
					"team_b": `{team="b"}`,
					"team_c": `{team="b"}`,
					"team_d": `{team="b"}`,
				}), NativeHistogramsIngestionEnabled: true})
				limits := defaultLimitsTestConfig()
				limits.ActiveSeriesBaseCustomTrackersConfig = activeSeriesDefaultConfig
				limits.NativeHistogramsIngestionEnabled = true
				override, err := validation.NewOverrides(limits, activeSeriesTenantOverride)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Saving time before second push to avoid purging it before exposing.
				currentTime = time.Now()
				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				// Adding idleTimeout to expose the metrics but not purge the pushes.
				currentTime = currentTime.Add(ingester.cfg.ActiveSeriesMetrics.IdleTimeout)
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_c",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_d",user="test_user"} 4

					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_c",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_d",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_c",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_d",user="test_user"} 16
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should cleanup loading metric at close": {
			activeSeriesConfig: activeSeriesDefaultConfig,
			tenantLimits:       defaultActiveSeriesTenantOverride,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				currentTime := time.Now()

				pushWithUser(t, ingester, labelsToPush, userID, req)
				pushWithUser(t, ingester, labelsToPush, userID2, req)
				pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)
				pushWithUser(t, ingester, labelsToPushHist, userID2, reqHist)

				// Update active series for metrics check.
				ingester.updateActiveSeries(currentTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="other_test_user"} 8
					cortex_ingester_active_series{user="test_user"} 8
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_a",user="test_user"} 4
					cortex_ingester_active_series_custom_tracker{name="team_b",user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series Number of currently active native histogram series per user.
					# TYPE cortex_ingester_active_native_histogram_series gauge
					cortex_ingester_active_native_histogram_series{user="other_test_user"} 4
					cortex_ingester_active_native_histogram_series{user="test_user"} 4
					# HELP cortex_ingester_active_native_histogram_series_custom_tracker Number of currently active native histogram series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_series_custom_tracker gauge
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_a",user="test_user"} 2
					cortex_ingester_active_native_histogram_series_custom_tracker{name="team_b",user="test_user"} 2
					# HELP cortex_ingester_active_native_histogram_buckets Number of currently active native histogram buckets per user.
					# TYPE cortex_ingester_active_native_histogram_buckets gauge
					cortex_ingester_active_native_histogram_buckets{user="other_test_user"} 32
					cortex_ingester_active_native_histogram_buckets{user="test_user"} 32
					# HELP cortex_ingester_active_native_histogram_buckets_custom_tracker Number of currently active native histogram buckets matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_native_histogram_buckets_custom_tracker gauge
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_true_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="bool_is_false_flagbased",user="other_test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_a",user="test_user"} 16
					cortex_ingester_active_native_histogram_buckets_custom_tracker{name="team_b",user="test_user"} 16
				`
				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Remove all configs
				limits := defaultLimitsTestConfig()
				override, err := validation.NewOverrides(limits, nil)
				require.NoError(t, err)
				ingester.limits = override
				ingester.updateActiveSeries(currentTime)
				expectedMetrics = `
					# HELP cortex_ingester_active_series_loading Indicates that active series configuration is being reloaded, and waiting to become stable. While this metric is non zero, values from active series metrics shouldn't be considered.
					# TYPE cortex_ingester_active_series_loading gauge
					cortex_ingester_active_series_loading{user="test_user"} 1
					cortex_ingester_active_series_loading{user="other_test_user"} 1
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
				ingester.closeAllTSDB()
				expectedMetrics = `
				`
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.Enabled = true

			limits := defaultLimitsTestConfig()
			limits.ActiveSeriesBaseCustomTrackersConfig = testData.activeSeriesConfig
			limits.NativeHistogramsIngestionEnabled = true
			var overrides *validation.Overrides
			var err error
			// Without this, TenantLimitsMock(nil) != nil when using getOverridesForUser in limits.go
			if testData.tenantLimits != nil {
				overrides, err = validation.NewOverrides(limits, testData.tenantLimits)
				require.NoError(t, err)
			} else {
				overrides, err = validation.NewOverrides(limits, nil)
				require.NoError(t, err)
			}

			ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, nil, "", "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return ing.lifecycler.HealthyInstancesCount()
			})

			testData.test(t, ing, registry)
		})
	}
}

func pushWithUser(t *testing.T, ingester *Ingester, labelsToPush [][]mimirpb.LabelAdapter, userID string, req func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest) {
	for _, label := range labelsToPush {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ingester.Push(ctx, req(label, time.Now()))
		require.NoError(t, err)
	}
}

func TestGetIgnoreSeriesLimitForMetricNamesMap(t *testing.T) {
	cfg := Config{}

	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = ", ,,,"
	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = "foo, bar, ,"
	require.Equal(t, map[string]struct{}{"foo": {}, "bar": {}}, cfg.getIgnoreSeriesLimitForMetricNamesMap())
}

// Test_Ingester_OutOfOrder tests basic ingestion and query of out-of-order samples.
// It also tests if the OutOfOrderTimeWindow gets changed during runtime.
// The correctness of changed runtime is already tested in Prometheus, so we only check if the
// change is being applied here.
func Test_Ingester_OutOfOrder(t *testing.T) {
	for name, tc := range ingesterSampleTypeScenarios {
		t.Run(name, func(t *testing.T) {
			testIngesterOutOfOrder(t, tc.makeWriteRequest, tc.makeExpectedSamples)
		})
	}
}

func testIngesterOutOfOrder(t *testing.T,
	makeWriteRequest func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest,
	makeExpectedSamples func(start, end int64, m model.Metric) model.Matrix) {
	cfg := defaultIngesterTestConfig(t)
	cfg.TSDBConfigUpdatePeriod = 1 * time.Second

	l := defaultLimitsTestConfig()
	l.NativeHistogramsIngestionEnabled = true
	tenantOverride := new(TenantLimitsMock)
	tenantOverride.On("ByUserID", "test").Return(nil)
	override, err := validation.NewOverrides(l, tenantOverride)
	require.NoError(t, err)

	setOOOTimeWindow := func(oooTW model.Duration) {
		tenantOverride.ExpectedCalls = nil
		tenantOverride.On("ByUserID", "test").Return(&validation.Limits{
			OutOfOrderTimeWindow:                oooTW,
			OOONativeHistogramsIngestionEnabled: true,

			// Need to set this in the tenant limits even though it's already set in the global config as once the
			// tenant limits is not nil, all configs are read from the tenant limits rather than the global one.
			NativeHistogramsIngestionEnabled: true,
		})
		// TSDB config is updated every second.
		<-time.After(1500 * time.Millisecond)
	}

	registry := prometheus.NewRegistry()

	i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	pushSamples := func(start, end int64, expErr bool, errorContains string) {
		start = start * time.Minute.Milliseconds()
		end = end * time.Minute.Milliseconds()

		s := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}
		wReq := makeWriteRequest(start, end, s)
		_, err = i.Push(ctx, wReq)
		if expErr {
			require.Error(t, err, "should have failed on push")
			require.ErrorContains(t, err, errorContains)
		} else {
			require.NoError(t, err)
		}
	}

	verifySamples := func(start, end int64) {
		start = start * time.Minute.Milliseconds()
		end = end * time.Minute.Milliseconds()

		expMatrix := makeExpectedSamples(start, end, model.Metric{"__name__": "test_1", "status": "200"})

		req := &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
		}

		s := stream{ctx: ctx}
		err = i.QueryStream(req, &s)
		require.NoError(t, err)

		res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		assert.ElementsMatch(t, expMatrix, res)
	}

	// Push first in-order sample at minute 100.
	pushSamples(100, 100, false, "")
	verifySamples(100, 100)

	// OOO is not enabled. So it errors out. No sample ingested.
	pushSamples(90, 99, true, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed")
	verifySamples(100, 100)

	i.updateUsageStats()
	assert.Equal(t, int64(0), usagestats.GetInt(tenantsWithOutOfOrderEnabledStatName).Value())
	assert.Equal(t, int64(0), usagestats.GetInt(minOutOfOrderTimeWindowSecondsStatName).Value())
	assert.Equal(t, int64(0), usagestats.GetInt(maxOutOfOrderTimeWindowSecondsStatName).Value())

	// no ooo samples appended, but the ooo delta is still calculated
	expectedMetrics := `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 0
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 10
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 3300
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 10
		`
	metricNames := []string{"cortex_ingester_tsdb_out_of_order_samples_appended_total", "cortex_ingester_tsdb_sample_out_of_order_delta_seconds"}
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	// Increasing the OOO time window.
	setOOOTimeWindow(model.Duration(30 * time.Minute))

	// Now it works.
	pushSamples(90, 99, false, "")
	verifySamples(90, 100)

	// 10 ooo samples appended and more observations for the ooo delta
	expectedMetrics = `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 10
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 20
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 6600
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 20
		`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	// Gives an error for sample 69 since it's outside time window, but rest is ingested.
	pushSamples(69, 99, true, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window")
	verifySamples(70, 100)

	// 20 more ooo samples appended (between 70-89, the other 10 between 90-99 are discarded as dupes of previously ingested samples)
	expectedMetrics = `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 30
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 30
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 50
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 51
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 36360
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 51
		`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	// All beyond the ooo time window. None ingested.
	pushSamples(50, 69, true, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window")
	verifySamples(70, 100)

	expectedMetrics = `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 30
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 30
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 50
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 71
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 84960
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 71
		`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	i.updateUsageStats()
	assert.Equal(t, int64(1), usagestats.GetInt(tenantsWithOutOfOrderEnabledStatName).Value())
	assert.Equal(t, int64(30*60), usagestats.GetInt(minOutOfOrderTimeWindowSecondsStatName).Value())
	assert.Equal(t, int64(30*60), usagestats.GetInt(maxOutOfOrderTimeWindowSecondsStatName).Value())

	// Increase the time window again. It works.
	setOOOTimeWindow(model.Duration(60 * time.Minute))
	pushSamples(50, 69, false, "")
	verifySamples(50, 100)

	expectedMetrics = `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 50
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 30
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 50
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 91
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 133560
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 91
		`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	i.updateUsageStats()
	assert.Equal(t, int64(1), usagestats.GetInt(tenantsWithOutOfOrderEnabledStatName).Value())
	assert.Equal(t, int64(60*60), usagestats.GetInt(minOutOfOrderTimeWindowSecondsStatName).Value())
	assert.Equal(t, int64(60*60), usagestats.GetInt(maxOutOfOrderTimeWindowSecondsStatName).Value())

	// Decrease the time window again. Same push should fail.
	setOOOTimeWindow(model.Duration(30 * time.Minute))
	pushSamples(50, 69, true, "the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window")
	verifySamples(50, 100)

	expectedMetrics = `
		# HELP cortex_ingester_tsdb_out_of_order_samples_appended_total Total number of out-of-order samples appended.
		# TYPE cortex_ingester_tsdb_out_of_order_samples_appended_total counter
		cortex_ingester_tsdb_out_of_order_samples_appended_total{user="test"} 50
		# HELP cortex_ingester_tsdb_sample_out_of_order_delta_seconds Delta in seconds by which a sample is considered out-of-order.
		# TYPE cortex_ingester_tsdb_sample_out_of_order_delta_seconds histogram
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="600"} 30
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="1800"} 50
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="3600"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="7200"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="10800"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="21600"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="43200"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_bucket{le="+Inf"} 111
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_sum 182160
        cortex_ingester_tsdb_sample_out_of_order_delta_seconds_count 111
		`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))

	i.updateUsageStats()
	assert.Equal(t, int64(1), usagestats.GetInt(tenantsWithOutOfOrderEnabledStatName).Value())
	assert.Equal(t, int64(30*60), usagestats.GetInt(minOutOfOrderTimeWindowSecondsStatName).Value())
	assert.Equal(t, int64(30*60), usagestats.GetInt(maxOutOfOrderTimeWindowSecondsStatName).Value())
}

// Test_Ingester_OutOfOrder_CompactHead tests that the OOO head is compacted
// when the compaction is forced or when the TSDB is idle.
func Test_Ingester_OutOfOrder_CompactHead(t *testing.T) {
	for name, tc := range ingesterSampleTypeScenarios {
		t.Run(name, func(t *testing.T) {
			testIngesterOutOfOrderCompactHead(t, tc.makeWriteRequest, tc.makeExpectedSamples)
		})
	}
}

func testIngesterOutOfOrderCompactHead(t *testing.T,
	makeWriteRequest func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest,
	makeExpectedSamples func(start, end int64, m model.Metric) model.Matrix) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.
	cfg.TSDBConfigUpdatePeriod = 1 * time.Second

	// Set the OOO window to 30 minutes and enable native histograms.
	limits := map[string]*validation.Limits{
		userID: {
			OutOfOrderTimeWindow:                model.Duration(30 * time.Minute),
			OOONativeHistogramsIngestionEnabled: true,
			NativeHistogramsIngestionEnabled:    true,
		},
	}
	override, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))
	require.NoError(t, err)

	i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), userID)

	pushSamples := func(start, end int64) {
		start = start * time.Minute.Milliseconds()
		end = end * time.Minute.Milliseconds()

		s := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}
		wReq := makeWriteRequest(start, end, s)
		_, err = i.Push(ctx, wReq)
		require.NoError(t, err)
	}

	verifySamples := func(start, end int64) {
		start = start * time.Minute.Milliseconds()
		end = end * time.Minute.Milliseconds()

		expMatrix := makeExpectedSamples(start, end, model.Metric{"__name__": "test_1", "status": "200"})

		req := &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
		}

		s := stream{ctx: ctx}
		err = i.QueryStream(req, &s)
		require.NoError(t, err)

		res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		assert.ElementsMatch(t, expMatrix, res)
	}

	pushSamples(100, 100)
	pushSamples(90, 99)
	verifySamples(90, 100)

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))
	i.compactBlocks(context.Background(), false, 0, nil) // Should be compacted because the TSDB is idle.
	verifyCompactedHead(t, i, true)

	pushSamples(110, 110)
	pushSamples(101, 109)
	verifySamples(90, 110)
	i.compactBlocks(context.Background(), true, math.MaxInt64, nil) // Should be compacted because it's forced.
	verifyCompactedHead(t, i, true)
}

// Test_Ingester_OutOfOrder_CompactHead_StillActive tests that active series correctly tracks OOO series after compaction.
func Test_Ingester_OutOfOrder_CompactHead_StillActive(t *testing.T) {
	for name, tc := range ingesterSampleTypeScenarios {
		t.Run(name, func(t *testing.T) {
			testIngesterOutOfOrderCompactHeadStillActive(t,
				func(ts int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest {
					return tc.makeWriteRequest(ts, ts, s)
				})
		})
	}
}

func testIngesterOutOfOrderCompactHeadStillActive(t *testing.T,
	makeWriteRequest func(ts int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest) {
	cfg := defaultIngesterTestConfig(t)
	cfg.TSDBConfigUpdatePeriod = 1 * time.Second

	// Set the OOO window to 10h and enable native histograms.
	limits := map[string]*validation.Limits{
		userID: {
			OutOfOrderTimeWindow:                model.Duration(10 * time.Hour),
			NativeHistogramsIngestionEnabled:    true,
			OOONativeHistogramsIngestionEnabled: true,
		},
	}
	override, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))
	require.NoError(t, err)

	i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() any { return i.lifecycler.HealthyInstancesCount() })

	ctx := user.InjectOrgID(context.Background(), userID)

	pushSamples := func(ts int64, series string) {
		wReq := makeWriteRequest(ts*time.Minute.Milliseconds(), []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test_1"}, {Name: "series", Value: series}})
		_, err = i.Push(ctx, wReq)
		require.NoError(t, err)
	}

	// Samples for regular series.
	pushSamples(1, "series")
	pushSamples(240, "series")

	// OOO-only series.
	pushSamples(0, "ooo_series")
	pushSamples(0, "ooo_series_not_pushed_again")

	// Head should have 3 series, all 3 active
	db := i.getTSDB(userID)
	require.Equal(t, uint64(3), db.Head().NumSeries())
	active, _, _ := db.activeSeries.Active()
	require.Equal(t, 3, active)

	// Run a regular compaction.
	i.compactBlocks(context.Background(), false, 0, nil)

	// Check that there's only one series in the head. This is not something we _want_,
	// but this is something we know is happening because of very aggressive OOO series garbage collection.
	// If this isn't 1 but 2, then it doesn't make sense to check active series.
	require.Equal(t, uint64(1), db.Head().NumSeries())

	// There should be still 3 active series.
	active, _, _ = db.activeSeries.Active()
	require.Equal(t, 3, active)

	// Send more samples to both series.
	pushSamples(480, "series")
	pushSamples(240, "ooo_series")

	// Run a regular compaction.
	i.compactBlocks(context.Background(), false, math.MaxInt64, nil)

	// OOO series were GC-ed.
	require.Equal(t, uint64(1), db.Head().NumSeries())

	// There should be still 3 active series.
	active, _, _ = db.activeSeries.Active()
	require.Equal(t, 3, active)
}

// Test_Ingester_ShipperLabelsOutOfOrderBlocksOnUpload tests whether out-of-order
// data is compacted and uploaded into a block that is labeled as being out-of-order.
func Test_Ingester_ShipperLabelsOutOfOrderBlocksOnUpload(t *testing.T) {
	for _, addOOOLabel := range []bool{true, false} {
		t.Run(fmt.Sprintf("AddOutOfOrderExternalLabel=%t", addOOOLabel), func(t *testing.T) {
			const tenant = "test"

			cfg := defaultIngesterTestConfig(t)
			cfg.TSDBConfigUpdatePeriod = 1 * time.Second

			tenantLimits := map[string]*validation.Limits{
				tenant: {
					OutOfOrderTimeWindow:                 model.Duration(30 * time.Minute),
					OutOfOrderBlocksExternalLabelEnabled: addOOOLabel,
				},
			}

			override, err := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(tenantLimits))
			require.NoError(t, err)

			tmpDir := t.TempDir()
			bucketDir := t.TempDir()

			i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, tmpDir, bucketDir, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's healthy
			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			ctx := user.InjectOrgID(context.Background(), tenant)

			pushSamples := func(start, end int64) {
				start = start * time.Minute.Milliseconds()
				end = end * time.Minute.Milliseconds()

				s := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}
				var samples []mimirpb.Sample
				var lbls [][]mimirpb.LabelAdapter
				for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
					samples = append(samples, mimirpb.Sample{
						TimestampMs: ts,
						Value:       float64(ts),
					})
					lbls = append(lbls, s)
				}

				wReq := mimirpb.ToWriteRequest(lbls, samples, nil, nil, mimirpb.API)
				_, err = i.Push(ctx, wReq)
				require.NoError(t, err)
			}

			// Push first in-order sample at minute 100.
			pushSamples(100, 100)
			// Push older, out-of-order samples
			pushSamples(90, 99)

			// Compact and upload the blocks
			i.compactBlocks(ctx, true, math.MaxInt64, nil)
			i.shipBlocks(ctx, nil)

			// Now check that an OOO block was uploaded and labeled correctly

			bucket, err := filesystem.NewBucket(filepath.Join(bucketDir, tenant)) // need to add the tenant to the directory
			require.NoError(t, err)

			userTSDB := i.getTSDB(tenant)
			require.Equal(t, 2, len(userTSDB.shippedBlocks), "there should be two uploaded blocks")

			var oooMeta []block.Meta
			var inOrderMeta []block.Meta
			for ulid := range userTSDB.shippedBlocks {
				meta, err := block.DownloadMeta(ctx, log.NewNopLogger(), bucket, ulid)
				require.NoError(t, err)
				if meta.Compaction.FromOutOfOrder() {
					oooMeta = append(oooMeta, meta)
				} else {
					inOrderMeta = append(inOrderMeta, meta)
				}
			}

			require.Len(t, oooMeta, 1, "only one of the blocks should have an ooo compactor hint")
			require.Empty(t, inOrderMeta[0].Thanos.Labels, "in-order block should not have the ooo label")
			if addOOOLabel {
				require.Equal(t, map[string]string{mimir_tsdb.OutOfOrderExternalLabel: mimir_tsdb.OutOfOrderExternalLabelValue}, oooMeta[0].Thanos.Labels)
			} else {
				require.Empty(t, oooMeta[0].Thanos.Labels)
			}
		})
	}
}

func TestIngesterCanEnableIngestAndQueryNativeHistograms(t *testing.T) {
	expectedSampleHistogram := mimirpb.FromMimirSampleToPromHistogram(mimirpb.FromFloatHistogramToSampleHistogram(util_test.GenerateTestFloatHistogram(0)))

	tests := map[string]struct {
		sampleHistograms []mimirpb.Histogram
		expectHistogram  *model.SampleHistogram
	}{
		"integer histogram": {
			sampleHistograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1, util_test.GenerateTestHistogram(0))},
			expectHistogram:  expectedSampleHistogram,
		},
		"float histogram": {
			sampleHistograms: []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(1, util_test.GenerateTestFloatHistogram(0))},
			expectHistogram:  expectedSampleHistogram,
		},
	}
	for testName, testCfg := range tests {
		for _, streamChunks := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/streamChunks=%v", testName, streamChunks), func(t *testing.T) {
				testIngesterCanEnableIngestAndQueryNativeHistograms(t, testCfg.sampleHistograms, testCfg.expectHistogram, streamChunks)
			})
		}
	}
}

func testIngesterCanEnableIngestAndQueryNativeHistograms(t *testing.T, sampleHistograms []mimirpb.Histogram, expectHistogram *model.SampleHistogram, streamChunks bool) {
	limits := defaultLimitsTestConfig()
	limits.NativeHistogramsIngestionEnabled = false

	userID := "1"
	tenantOverride := new(TenantLimitsMock)
	tenantOverride.On("ByUserID", userID).Return(nil)
	override, err := validation.NewOverrides(limits, tenantOverride)
	require.NoError(t, err)

	setNativeHistogramsIngestionEnabled := func(enabled bool) {
		tenantOverride.ExpectedCalls = nil
		tenantOverride.On("ByUserID", userID).Return(&validation.Limits{
			NativeHistogramsIngestionEnabled: enabled,
		})
		// TSDB config is updated every second.
		<-time.After(1500 * time.Millisecond)
	}

	registry := prometheus.NewRegistry()

	newIngester := func() *Ingester {
		cfg := defaultIngesterTestConfig(t)
		cfg.TSDBConfigUpdatePeriod = 1 * time.Second
		// Global Ingester limits are computed based on replication factor
		// Set RF=1 here to ensure the series and metadata limits
		// are actually set to 1 instead of 3.
		cfg.IngesterRing.ReplicationFactor = 1
		cfg.StreamChunksWhenUsingBlocks = streamChunks
		ing, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", registry)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	labels1 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}

	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing.Push(ctx, mimirpb.NewWriteRequest([]*mimirpb.MetricMetadata{metadata1}, mimirpb.API).
		AddFloatSeries([][]mimirpb.LabelAdapter{labels1}, []mimirpb.Sample{sample1}, nil).
		AddHistogramSeries([][]mimirpb.LabelAdapter{labels1}, sampleHistograms, nil))
	require.NoError(t, err)

	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="1"} 1
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total{user="1"} 0
		`
	metricNames := []string{"cortex_ingester_ingested_samples_total", "cortex_ingester_ingested_samples_failures_total"}
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...), "Except histogram writes to fail and floats to succeed")

	testResult := func(expected model.Matrix, msg string) {
		// Query back the histogram
		req := &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "testmetric"},
			},
		}

		s := stream{ctx: ctx}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err, msg)

		res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err, msg)
		assert.ElementsMatch(t, expected, res, msg)
	}

	testResult(model.Matrix{{
		Metric: model.Metric{"__name__": "testmetric", "foo": "bar"},
		Values: []model.SamplePair{{
			Timestamp: 0,
			Value:     1,
		}},
		Histograms: nil,
	}}, "Should have no histogram in result")

	setNativeHistogramsIngestionEnabled(true)

	// resend the histogram at time 2
	sampleHistograms[0].Timestamp = 2

	// Metadata
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.HISTOGRAM}

	// Append only one series and one metadata first, expect no error.
	_, err = ing.Push(ctx, mimirpb.NewWriteRequest([]*mimirpb.MetricMetadata{metadata2}, mimirpb.API).
		AddHistogramSeries([][]mimirpb.LabelAdapter{labels1}, sampleHistograms, nil))
	require.NoError(t, err)

	expectedMetrics = `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="1"} 2
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion per user.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total{user="1"} 0
	`
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...), "Except histogram writes to succeed")

	expectedMatrix := model.Matrix{{
		Metric: model.Metric{"__name__": "testmetric", "foo": "bar"},
		Values: []model.SamplePair{{
			Timestamp: 0,
			Value:     1,
		}},
		Histograms: []model.SampleHistogramPair{{
			Timestamp: 2,
			Histogram: expectHistogram,
		}},
	}}

	testResult(expectedMatrix, "Result should contain the histogram when accepting histograms")

	setNativeHistogramsIngestionEnabled(false)

	testResult(expectedMatrix, "Result should contain the histogram even when not accepting histograms")
}

func TestIngester_GetOpenTSDBsConcurrencyConfig(t *testing.T) {
	tests := map[string]struct {
		walReplayConcurrency             int
		tenantCount                      int
		expectedTSDBOpenConcurrency      int
		expectedTSDBWALReplayConcurrency int
	}{
		"if -blocks-storage.tsdb.wal-replay-concurrency is 0, mimir_tsdb.DefaultMaxTSDBOpeningConcurrencyOnStartup is used": {
			walReplayConcurrency:             0,
			tenantCount:                      5,
			expectedTSDBOpenConcurrency:      mimir_tsdb.DefaultMaxTSDBOpeningConcurrencyOnStartup,
			expectedTSDBWALReplayConcurrency: 0,
		},
		"if -blocks-storage.tsdb.wal-replay-concurrency > 0 and there are <= 10 tenants, parallelize WAL replay on sequential openings": {
			walReplayConcurrency:             3,
			tenantCount:                      5,
			expectedTSDBOpenConcurrency:      1,
			expectedTSDBWALReplayConcurrency: 3,
		},
		"if -blocks-storage.tsdb.wal-replay-concurrency > 0 and there are > 10 tenants, parallelize openings with single WAL replay": {
			walReplayConcurrency:             3,
			tenantCount:                      15,
			expectedTSDBOpenConcurrency:      3,
			expectedTSDBWALReplayConcurrency: 1,
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tsdbConfig := mimir_tsdb.TSDBConfig{
				WALReplayConcurrency: testData.walReplayConcurrency,
			}
			tsdbOpenConcurrency, tsdbWALReplayConcurrency := getOpenTSDBsConcurrencyConfig(tsdbConfig, testData.tenantCount)
			require.Equal(t, testData.expectedTSDBOpenConcurrency, tsdbOpenConcurrency)
			require.Equal(t, testData.expectedTSDBWALReplayConcurrency, tsdbWALReplayConcurrency)
		})
	}
}

func TestIngester_PushWithSampledErrors(t *testing.T) {
	metricLabelAdapters := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricNames := []string{
		"cortex_discarded_samples_total",
	}
	errorSampleRate := 5
	now := time.Now()

	users := []string{"user-1", "user-2"}

	tests := map[string]struct {
		reqs             []*mimirpb.WriteRequest
		expectedErrs     []globalerror.ErrorWithStatus
		expectedMetrics  string
		expectedSampling bool
		maxExemplars     int
		nativeHistograms bool
	}{
		"should soft fail on sample out-of-order": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleOutOfOrderError(model.Time(9), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleOutOfOrderError(model.Time(9), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="user-1"} 4
				cortex_discarded_samples_total{group="",reason="sample-out-of-order",user="user-2"} 1
			`,
		},
		"should soft fail on all samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:  metricLabelAdapters,
								Samples: []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)}, {Value: 1, TimestampMs: 1575043969 - (86000 * 1000)}},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-1"} 8
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-2"} 2
			`,
		},
		"should soft fail on all histograms out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 1 "too old" histogram.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:     metricLabelAdapters,
								Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1575043969-(86800*1000), util_test.GenerateTestHistogram(0))},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86800*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86800*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-1"} 4
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-2"} 1
			`,
			nativeHistograms: true,
		},
		"should soft fail on all samples and histograms out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 3 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels:     metricLabelAdapters,
								Samples:    []mimirpb.Sample{{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)}, {Value: 1, TimestampMs: 1575043969 - (86000 * 1000)}},
								Histograms: []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1575043969-(86800*1000), util_test.GenerateTestHistogram(0))},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86800*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86800*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-1"} 12
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-2"} 3
			`,
			nativeHistograms: true,
		},
		"should soft fail on some samples out of bound in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				// Write request with 1 series and 2 samples.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 0, TimestampMs: 1575043969 - (86400 * 1000)},
									{Value: 1, TimestampMs: 1575043969 - (86000 * 1000)},
									{Value: 3, TimestampMs: 1575043969 + 1}},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooOldError(model.Time(1575043969-(86400*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-1"} 8
				cortex_discarded_samples_total{group="",reason="sample-timestamp-too-old",user="user-2"} 2
			`,
		},
		"should soft fail on some samples with timestamp too far in future in a write request": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: now.UnixMilli()}},
					nil,
					nil,
					mimirpb.API,
				),
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 2, TimestampMs: now.UnixMilli() + (86400 * 1000)},
									{Value: 3, TimestampMs: now.UnixMilli() + 1}},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="user-1"} 4
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="user-2"} 1
			`,
		},
		"should soft fail on some histograms with timestamp too far in future in a write request": {
			nativeHistograms: true,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Histograms: []mimirpb.Histogram{
									mimirpb.FromHistogramToHistogramProto(now.UnixMilli(), util_test.GenerateTestHistogram(0)),
									mimirpb.FromHistogramToHistogramProto(now.UnixMilli()+(86400*1000), util_test.GenerateTestHistogram(1))},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="user-1"} 4
				cortex_discarded_samples_total{group="",reason="sample-too-far-in-future",user="user-2"} 1
			`,
		},
		"should soft fail on some exemplars with timestamp too far in future in a write request": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: metricLabelAdapters,
								Samples: []mimirpb.Sample{
									{Value: 1, TimestampMs: now.UnixMilli()}},
								Exemplars: []mimirpb.Exemplar{
									{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "111"}}, Value: 1, TimestampMs: now.UnixMilli()},
									{Labels: []mimirpb.LabelAdapter{{Name: "traceID", Value: "222"}}, Value: 2, TimestampMs: now.UnixMilli() + (86400 * 1000)},
								},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "222"}}), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarTimestampTooFarInFutureError(model.Time(now.UnixMilli()+(86400*1000)), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "222"}}), users[1]), codes.InvalidArgument),
			},
			expectedSampling: false,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*mimirpb.WriteRequest{
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
				mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{metricLabelAdapters},
					[]mimirpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					nil,
					mimirpb.API,
				),
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleDuplicateTimestampError(model.Time(1575043969), metricLabelAdapters), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newSampleDuplicateTimestampError(model.Time(1575043969), metricLabelAdapters), users[1]), codes.InvalidArgument),
			},
			expectedSampling: true,
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="user-1"} 4
				cortex_discarded_samples_total{group="",reason="new-value-for-timestamp",user="user-2"} 1
			`,
		},
		"should soft fail on exemplar with unknown series": {
			maxExemplars: 1,
			reqs: []*mimirpb.WriteRequest{
				// Ingesting an exemplar requires a sample to create the series first
				// This is not done here.
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						{
							TimeSeries: &mimirpb.TimeSeries{
								Labels: []mimirpb.LabelAdapter{metricLabelAdapters[0]}, // Cannot reuse test slice var because it is cleared and returned to the pool
								Exemplars: []mimirpb.Exemplar{
									{
										Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}},
										TimestampMs: 1000,
										Value:       1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErrs: []globalerror.ErrorWithStatus{
				newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarMissingSeriesError(model.Time(1000), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}), users[0]), codes.InvalidArgument),
				newErrorWithStatus(wrapOrAnnotateWithUser(newExemplarMissingSeriesError(model.Time(1000), metricLabelAdapters, []mimirpb.LabelAdapter{{Name: "traceID", Value: "123"}}), users[1]), codes.InvalidArgument),
			},
			expectedSampling: false,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			ingesterCfg := defaultIngesterTestConfig(t)
			ingesterCfg.IngesterRing.ReplicationFactor = 1
			ingesterCfg.ErrorSampleRate = int64(errorSampleRate)
			limits := defaultLimitsTestConfig()
			limits.MaxGlobalExemplarsPerUser = testData.maxExemplars
			limits.NativeHistogramsIngestionEnabled = testData.nativeHistograms

			i, err := prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limits, nil, "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			logger := newLoggerWithCounter(t, bytes.NewBuffer(nil))
			serverLog := middleware.GRPCServerLog{
				Log: logger,
			}
			grpcOptions := []grpc.ServerOption{
				grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
				grpc.ChainUnaryInterceptor(serverLog.UnaryServerInterceptor),
			}

			server := grpc.NewServer(grpcOptions...)
			defer server.GracefulStop()
			client.RegisterIngesterServer(server, i)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			go func() {
				require.NoError(t, server.Serve(listener))
			}()

			inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
			client, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
			require.NoError(t, err)
			defer client.Close()

			ctxs := make([]context.Context, 0, len(users))
			for _, userID := range users {
				ctxs = append(ctxs, user.InjectOrgID(context.Background(), userID))
			}

			// Push timeseries

			for idx, req := range testData.reqs {
				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					for i := 0; i < len(users); i++ {
						_, err := client.Push(ctxs[i], req)
						assert.NoError(t, err)
					}
				} else {
					// We push additional series causing the expected error type in the following way:
					// - we push additional (errorSampleRate - 1) series related to users[0].
					// - we push 1 additional series related to users[1].
					// The expected result is that only 1 sampled error related to users[0] will be logged,
					// but the cortex_discarded_samples_total counter will contain:
					// - (errorSampleRate - 1) samples for users[0] and
					// - 1 sample for users[1].
					for i := 0; i < errorSampleRate-1; i++ {
						_, err = client.Push(ctxs[0], req)
						require.Error(t, err)
						status, ok := grpcutil.ErrorToStatus(err)
						require.True(t, ok)
						require.ErrorContains(t, status.Err(), testData.expectedErrs[0].UnderlyingErr.Error())
					}
					_, err = client.Push(ctxs[1], req)
					require.Error(t, err)
					status, ok := grpcutil.ErrorToStatus(err)
					require.True(t, ok)
					require.ErrorContains(t, status.Err(), testData.expectedErrs[1].UnderlyingErr.Error())
				}
			}

			if testData.expectedSampling {
				// If sampling is expected, we expect to see:
				// - only 1 log entry related to the expected error for users[0].
				// - no log entry related to the expected error fro users[1].
				require.Equal(t, 1, logger.count(testData.expectedErrs[0].Error()))
				require.Equal(t, 0, logger.count(testData.expectedErrs[1].Error()))
			} else {
				// Otherwise we expect to see all log entries.
				require.Equal(t, errorSampleRate-1, logger.count(testData.expectedErrs[0].Error()))
				require.Equal(t, 1, logger.count(testData.expectedErrs[1].Error()))
			}

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), metricNames...)
			assert.NoError(t, err)
		})
	}
}

func TestIngester_SampledUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerUser = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()
	registry := prometheus.NewRegistry()

	errorSampleRate := 5
	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.IngesterRing.ReplicationFactor = 1
	ingesterCfg.ErrorSampleRate = int64(errorSampleRate)
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limits, nil, dataDir, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, time.Second, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	logger := newLoggerWithCounter(t, bytes.NewBuffer(nil))
	serverLog := middleware.GRPCServerLog{
		Log: logger,
	}
	grpcOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.ChainUnaryInterceptor(serverLog.UnaryServerInterceptor),
	}

	server := grpc.NewServer(grpcOptions...)
	defer server.GracefulStop()
	client.RegisterIngesterServer(server, ing)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.Serve(listener))
	}()

	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	client, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	defer client.Close()

	userID := "1"
	timestamp := int64(1575043969)
	metricLabelAdapters1 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}, {Name: "foo", Value: "bar"}}
	metricLabelAdapters2 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}, {Name: "foo", Value: "biz"}}
	sample1 := mimirpb.Sample{
		TimestampMs: timestamp + 1,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: timestamp + 2,
		Value:       2,
	}
	sample3 := mimirpb.Sample{
		TimestampMs: timestamp + 3,
		Value:       3,
	}

	ctx := user.InjectOrgID(context.Background(), userID)

	// Append only one series first, expect no error.
	res, err := client.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{metricLabelAdapters1}, []mimirpb.Sample{sample1}, nil, nil, mimirpb.API))
	require.NoError(t, err)
	require.NotNil(t, res)

	expectedError := wrapOrAnnotateWithUser(ing.errorSamplers.maxSeriesPerUserLimitExceeded.WrapError(newPerUserSeriesLimitReachedError(ing.limiter.limits.MaxGlobalSeriesPerUser(userID))), userID)
	require.Error(t, expectedError)

	// We push 2 times more than errorSampleRate series hitting the max-series-per-user limit, i.e., 10 series in total.
	// As a result we expect to see:
	// - only 2 related log entries,
	// - all 10 samples as discarded.
	for i := 0; i < 2*errorSampleRate; i++ {
		// Append 2 series first, expect max-series-per-user error.
		_, err = client.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{metricLabelAdapters1, metricLabelAdapters2}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		require.Error(t, err)
		status, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Errorf(t, expectedError, status.Message())
	}

	// We expect to see 2 log entries related to max-series-per-user error.
	require.Equal(t, 2, logger.count(expectedError.Error()))

	// We expect to see all 10 samples causing max-series-per-user error as discarded.
	metricNames := []string{
		"cortex_discarded_samples_total",
	}
	expectedMetrics := `
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="per_user_series_limit",user="1"} 10
	`
	err = testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...)
	assert.NoError(t, err)
}

func TestIngester_SampledMetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 1
	limits.MaxGlobalMetadataPerMetric = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()
	registry := prometheus.NewRegistry()

	errorSampleRate := 5
	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.IngesterRing.ReplicationFactor = 1
	ingesterCfg.ErrorSampleRate = int64(errorSampleRate)
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limits, nil, dataDir, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until it's healthy
	test.Poll(t, time.Second, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	logger := newLoggerWithCounter(t, bytes.NewBuffer(nil))
	serverLog := middleware.GRPCServerLog{
		Log: logger,
	}
	grpcOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.ChainUnaryInterceptor(serverLog.UnaryServerInterceptor),
	}

	server := grpc.NewServer(grpcOptions...)
	defer server.GracefulStop()
	client.RegisterIngesterServer(server, ing)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.Serve(listener))
	}()

	inst := ring.InstanceDesc{Id: "test", Addr: listener.Addr().String()}
	client, err := client.MakeIngesterClient(inst, defaultClientTestConfig(), client.NewMetrics(nil))
	require.NoError(t, err)
	defer client.Close()

	userID := "1"
	timestamp := int64(1575043969)
	metricLabelAdapters1 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}, {Name: "foo", Value: "bar"}}
	metricLabelAdapters2 := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}, {Name: "foo", Value: "biz"}}
	sample1 := mimirpb.Sample{
		TimestampMs: timestamp + 1,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: timestamp + 2,
		Value:       2,
	}
	sample3 := mimirpb.Sample{
		TimestampMs: timestamp + 3,
		Value:       3,
	}

	ctx := user.InjectOrgID(context.Background(), userID)

	// Append only one series first, expect no error.
	res, err := client.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{metricLabelAdapters1}, []mimirpb.Sample{sample1}, nil, nil, mimirpb.API))
	require.NoError(t, err)
	require.NotNil(t, res)

	expectedError := wrapOrAnnotateWithUser(ing.errorSamplers.maxSeriesPerUserLimitExceeded.WrapError(newPerMetricSeriesLimitReachedError(ing.limiter.limits.MaxGlobalSeriesPerMetric(userID), metricLabelAdapters2)), userID)
	require.Error(t, expectedError)

	// We push 2 times more than errorSampleRate series hitting the max-series-per-metric, i.e., 10 series in total.
	// As a result we expect to see:
	// - only 2 related log entries,
	// - all 10 samples as discarded.
	for i := 0; i < 2*errorSampleRate; i++ {
		// Append 2 series first, expect max-series-per-user error.
		_, err = client.Push(ctx, mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{metricLabelAdapters1, metricLabelAdapters2}, []mimirpb.Sample{sample2, sample3}, nil, nil, mimirpb.API))
		require.Error(t, err)
		status, ok := grpcutil.ErrorToStatus(err)
		require.True(t, ok)
		require.Errorf(t, expectedError, status.Message())
	}

	// We expect to see 2 log entries related to max-series-per-metric error.
	require.Equal(t, 2, logger.count(expectedError.Error()))

	// We expect to see all 10 samples causing max-series-per-metric error as discarded.
	metricNames := []string{
		"cortex_discarded_samples_total",
	}
	expectedMetrics := `
		# HELP cortex_discarded_samples_total The total number of samples that were discarded.
		# TYPE cortex_discarded_samples_total counter
		cortex_discarded_samples_total{group="",reason="per_metric_series_limit",user="1"} 10
	`
	err = testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...)
	assert.NoError(t, err)
}

type loggerWithBuffer struct {
	logger log.Logger
	buf    *bytes.Buffer
}

func newLoggerWithCounter(t *testing.T, buf *bytes.Buffer) *loggerWithBuffer {
	var lvl dslog.Level
	require.NoError(t, lvl.Set("info"))
	logger := dslog.NewGoKitWithWriter(dslog.LogfmtFormat, buf)
	level.NewFilter(logger, lvl.Option)
	return &loggerWithBuffer{
		logger: logger,
		buf:    buf,
	}
}

func (l *loggerWithBuffer) Log(keyvals ...interface{}) error {
	return l.logger.Log(keyvals...)
}

func (l *loggerWithBuffer) count(msg string) int {
	msg = strings.ReplaceAll(msg, "\"", "\\\"")
	return bytes.Count(l.buf.Bytes(), []byte(msg))
}

type fakeUtilizationBasedLimiter struct {
	services.BasicService

	limitingReason string
}

func (l *fakeUtilizationBasedLimiter) LimitingReason() string {
	return l.limitingReason
}

func verifyUtilizationLimitedRequestsMetric(t *testing.T, reg *prometheus.Registry) {
	t.Helper()

	const expMetrics = `
				# HELP cortex_ingester_utilization_limited_read_requests_total Total number of times read requests have been rejected due to utilization based limiting.
				# TYPE cortex_ingester_utilization_limited_read_requests_total counter
				cortex_ingester_utilization_limited_read_requests_total{reason="cpu"} 1
				`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expMetrics),
		"cortex_ingester_utilization_limited_read_requests_total"))
}

func verifyShipperLastSuccessfulUploadTimeMetric(t *testing.T, reg *prometheus.Registry, expected int64) {
	t.Helper()

	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(reg)
	require.NoError(t, err)
	assert.InDelta(t, float64(expected), metrics.MaxGauges("cortex_ingester_shipper_last_successful_upload_timestamp_seconds"), 5)
}

func TestIngester_lastUpdatedTimeIsNotInTheFuture(t *testing.T) {
	ctx := context.Background()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 0
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Minute
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 0 // Will not run the loop, but will allow us to close any TSDB fast.

	l := defaultLimitsTestConfig()
	l.CreationGracePeriod = model.Duration(time.Hour) * 24 * 365 * 15 // 15 years in the future
	override, err := validation.NewOverrides(l, nil)
	require.NoError(t, err)

	// Create ingester
	i, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, override, nil, "", "", nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, i))
	})

	// Wait until it's healthy
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	db, err := i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.InDelta(t, time.Now().Unix(), db.getLastUpdate().Unix(), 5) // within 5 seconds of "now"

	// push sample 10 years, 10 months and 10 days in the future.
	futureTS := time.Now().AddDate(10, 10, 10).UnixMilli()
	pushSingleSampleAtTime(t, i, futureTS)

	// Close TSDB
	i.closeAllTSDB()

	// and open it again (it must exist)
	db, err = i.getOrCreateTSDB(userID)
	require.NoError(t, err)
	require.NotNil(t, db)

	// "last update" time should still be "now", not in the future.
	require.InDelta(t, time.Now().Unix(), db.getLastUpdate().Unix(), 5) // within 5 seconds of "now"

	// Verify that maxTime of TSDB is actually our future sample.
	require.Equal(t, futureTS, db.db.Head().MaxTime())
}

func checkErrorWithStatus(t *testing.T, err error, expectedErr error) {
	require.Error(t, err)
	errWithStatus, ok := err.(globalerror.ErrorWithStatus)
	require.True(t, ok)
	require.True(t, errWithStatus.Equals(expectedErr))
}

func buildSeriesSet(t *testing.T, series *Series) []labels.Labels {
	var labelSets []labels.Labels
	for series.Next() {
		l := series.At()
		require.NoError(t, series.Err())
		labelSets = append(labelSets, l)
	}
	return labelSets
}

func TestIngester_Starting(t *testing.T) {
	tests := map[string]struct {
		failingCause                         error
		isIngesterInTheRing                  bool
		expectedLifecyclerStateAfterStarting services.State
		expectedRingStateAfterStarting       ring.InstanceState
	}{
		"if starting() of an ingester which is not in the ring runs into context.Canceled, its lifecycler terminates, and it is not in the ring": {
			failingCause:                         context.Canceled,
			isIngesterInTheRing:                  false,
			expectedLifecyclerStateAfterStarting: services.Terminated,
		},
		"if starting() of an ingester which is not in the ring runs into an error, its lifecycler terminates, and it is not in the ring": {
			failingCause:                         errors.New("this is an error"),
			isIngesterInTheRing:                  false,
			expectedLifecyclerStateAfterStarting: services.Terminated,
		},
		"if starting() of an ingester with ring state LEAVING runs into context.Canceled, its lifecycler terminates, and its ring state is LEAVING": {
			failingCause:                         context.Canceled,
			expectedLifecyclerStateAfterStarting: services.Terminated,
			expectedRingStateAfterStarting:       ring.LEAVING,
		},
		"if starting() of an ingester with ring state LEAVING runs into an error, its lifecycler terminates, and its ring state is LEAVING": {
			failingCause:                         errors.New("this is an error"),
			expectedLifecyclerStateAfterStarting: services.Terminated,
			expectedRingStateAfterStarting:       ring.LEAVING,
		},
	}

	for testName, testCase := range tests {
		cfg := defaultIngesterTestConfig(t)
		t.Run(testName, func(t *testing.T) {
			var checkInitalRingState func(context.Context, *failingIngester)
			var checkFinalRingState func(context.Context, *failingIngester)
			if testCase.isIngesterInTheRing {
				cfg.IngesterRing.UnregisterOnShutdown = false
				// Ensure that ingester fI is already in the ring, and its state is LEAVING.
				checkInitalRingState = func(ctx context.Context, fI *failingIngester) { fI.checkRingState(ctx, t, ring.LEAVING) }
				// Ensure that ingester fI is in the expected state after failing starting().
				checkFinalRingState = func(ctx context.Context, fI *failingIngester) {
					fI.checkRingState(ctx, t, testCase.expectedRingStateAfterStarting)
				}
			} else {
				// Ensure that ingester fI is not in the ring either before or after failing starting().
				checkInitalRingState = func(ctx context.Context, fI *failingIngester) { require.Nil(t, fI.getInstance(ctx)) }
				checkFinalRingState = func(ctx context.Context, fI *failingIngester) { require.Nil(t, fI.getInstance(ctx)) }
			}
			fI := setupFailingIngester(t, cfg, testCase.failingCause)
			ctx := context.Background()
			checkInitalRingState(ctx, fI)

			fI.startWaitAndCheck(ctx, t)
			require.Equal(t, testCase.expectedLifecyclerStateAfterStarting, fI.lifecycler.State())
			checkFinalRingState(ctx, fI)
		})
	}
}

func TestIngester_PrepareUnregisterHandler(t *testing.T) {
	ctx := context.Background()

	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)

	type testCase struct {
		name                     string
		startIngester            bool
		httpMethod               string
		requestBody              io.Reader
		prepare                  func(i *Ingester)
		expectedStatusCode       int
		expectedResponseBody     string
		expectedUnregisterStatus bool
	}

	tests := []testCase{
		{
			name:                     "returns HTTP 503 if ingester is not running",
			startIngester:            false,
			httpMethod:               http.MethodGet,
			requestBody:              nil,
			prepare:                  nil,
			expectedStatusCode:       http.StatusServiceUnavailable,
			expectedResponseBody:     "",
			expectedUnregisterStatus: true,
		},
		{
			name:                     "returns HTTP 400 on PUT with request body that is not valid JSON",
			startIngester:            true,
			httpMethod:               http.MethodPut,
			requestBody:              strings.NewReader("invalid json"),
			prepare:                  nil,
			expectedStatusCode:       http.StatusBadRequest,
			expectedResponseBody:     "",
			expectedUnregisterStatus: true,
		},
		{
			name:                     "returns HTTP 400 on PUT with request body that is valid JSON but has incorrect structure",
			startIngester:            true,
			httpMethod:               http.MethodPut,
			requestBody:              strings.NewReader(`{"ping": "pong"}`),
			prepare:                  nil,
			expectedStatusCode:       http.StatusBadRequest,
			expectedResponseBody:     "",
			expectedUnregisterStatus: true,
		},
		{
			name:                     "returns HTTP 200 and unregister status on PUT with valid request body",
			startIngester:            true,
			httpMethod:               http.MethodPut,
			requestBody:              strings.NewReader(`{"unregister": false}`),
			prepare:                  nil,
			expectedStatusCode:       http.StatusOK,
			expectedResponseBody:     `{"unregister":false}`,
			expectedUnregisterStatus: false,
		},
		{
			name:                     "returns HTTP 200 with unregister status on GET request",
			startIngester:            true,
			httpMethod:               http.MethodGet,
			requestBody:              nil,
			prepare:                  nil,
			expectedStatusCode:       http.StatusOK,
			expectedResponseBody:     `{"unregister":true}`,
			expectedUnregisterStatus: true,
		},
		{
			name:          "returns HTTP 200 with unregister status on DELETE request",
			startIngester: true,
			httpMethod:    http.MethodDelete,
			requestBody:   nil,
			prepare: func(i *Ingester) {
				i.lifecycler.SetUnregisterOnShutdown(false)
			},
			expectedStatusCode:       http.StatusOK,
			expectedResponseBody:     `{"unregister":true}`,
			expectedUnregisterStatus: true,
		},
	}

	setup := func(t *testing.T, start bool, cfg Config) *Ingester {
		ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, prometheus.NewPedanticRegistry())

		if start {
			require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
			})

			test.Poll(t, 1*time.Second, 1, func() interface{} {
				return ingester.lifecycler.HealthyInstancesCount()
			})
		}

		return ingester
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ingester := setup(t, tc.startIngester, defaultIngesterTestConfig(t))
			if tc.prepare != nil {
				tc.prepare(ingester)
			}
			res := httptest.NewRecorder()
			ingester.PrepareUnregisterHandler(res, httptest.NewRequest(tc.httpMethod, "/ingester/unregister-on-shutdown", tc.requestBody))
			require.Equal(t, tc.expectedStatusCode, res.Code)
			require.Equal(t, tc.expectedResponseBody, res.Body.String())
			require.Equal(t, tc.expectedUnregisterStatus, ingester.lifecycler.ShouldUnregisterOnShutdown())
		})
	}
}

func setupFailingIngester(t *testing.T, cfg Config, failingCause error) *failingIngester {
	// Start the first ingester. This ensures the ring will be created.
	fI := newFailingIngester(t, cfg, nil, nil)
	kvStore := fI.kvStore
	ctx := context.Background()
	fI.startWaitAndCheck(ctx, t)
	fI.shutDownWaitAndCheck(ctx, t)

	// Start the same ingester with an error.
	return newFailingIngester(t, cfg, kvStore, failingCause)
}

type failingIngester struct {
	*Ingester
	kvStore      kv.Client
	failingCause error
}

func newFailingIngester(t *testing.T, cfg Config, kvStore kv.Client, failingCause error) *failingIngester {
	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, prometheus.NewRegistry())
	require.NoError(t, err)
	fI := &failingIngester{Ingester: i, failingCause: failingCause}
	if kvStore != nil {
		fI.kvStore = kvStore
	}
	fI.BasicService = services.NewBasicService(fI.starting, fI.ingesterRunning, fI.stopping)
	return fI
}

func (i *failingIngester) startWaitAndCheck(ctx context.Context, t *testing.T) {
	err := services.StartAndAwaitRunning(ctx, i)
	var expectedHealthyIngesters int
	if i.failingCause == nil {
		require.NoError(t, err)
		expectedHealthyIngesters = 1
	} else {
		require.Error(t, err)
		expectedHealthyIngesters = 0
	}
	test.Poll(t, 100*time.Millisecond, expectedHealthyIngesters, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
}

func (i *failingIngester) shutDownWaitAndCheck(ctx context.Context, t *testing.T) {
	// We properly shut down ingester, and ensure that it lifecycler is terminated.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, i))
	require.Equal(t, services.Terminated, i.lifecycler.BasicService.State())
}

func (i *failingIngester) starting(parentCtx context.Context) error {
	if i.failingCause == nil {
		return i.Ingester.starting(parentCtx)
	}
	if errors.Is(i.failingCause, context.Canceled) {
		ctx, cancel := context.WithCancel(parentCtx)
		cancel()
		return i.Ingester.starting(ctx)
	}
	return i.Ingester.starting(&mockContext{ctx: parentCtx, err: i.failingCause})
}

func (i *failingIngester) getInstance(ctx context.Context) *ring.InstanceDesc {
	d, err := i.lifecycler.KVStore.Get(ctx, IngesterRingKey)
	if err != nil {
		return nil
	}
	instanceDesc, ok := ring.GetOrCreateRingDesc(d).Ingesters[i.lifecycler.ID]
	if !ok {
		return nil
	}
	return &instanceDesc
}

func (i *failingIngester) checkRingState(ctx context.Context, t *testing.T, expectedState ring.InstanceState) {
	instance := i.getInstance(ctx)
	require.NotNil(t, instance)
	require.Equal(t, expectedState, instance.GetState())
}

type mockContext struct {
	ctx context.Context
	err error
}

func (c *mockContext) Deadline() (time.Time, bool) {
	return c.ctx.Deadline()
}

func (c *mockContext) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (c *mockContext) Err() error {
	return c.err
}

func (c *mockContext) Value(key any) interface{} {
	return c.ctx.Value(key)
}

func pushWithSimulatedGRPCHandler(ctx context.Context, i *Ingester, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	ctx, err := i.StartPushRequest(ctx, int64(req.Size()))
	if err != nil {
		return nil, err
	}
	defer i.FinishPushRequest(ctx)

	return i.Push(ctx, req)
}

var ingesterSampleTypeScenarios = map[string]struct {
	makeWriteRequest    func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest
	makeExpectedSamples func(start, end int64, m model.Metric) model.Matrix
}{
	"float": {
		makeWriteRequest: func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest {
			var samples []mimirpb.Sample
			var lbls [][]mimirpb.LabelAdapter
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				samples = append(samples, mimirpb.Sample{
					TimestampMs: ts,
					Value:       float64(ts),
				})
				lbls = append(lbls, s)
			}
			return mimirpb.ToWriteRequest(lbls, samples, nil, nil, mimirpb.API)
		},
		makeExpectedSamples: func(start, end int64, m model.Metric) model.Matrix {
			var expSamples []model.SamplePair
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				expSamples = append(expSamples, model.SamplePair{
					Timestamp: model.Time(ts),
					Value:     model.SampleValue(ts),
				})
			}
			return model.Matrix{{
				Metric: m,
				Values: expSamples,
			}}
		},
	},
	"int histogram": {
		makeWriteRequest: func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest {
			var histograms []mimirpb.Histogram
			var lbls [][]mimirpb.LabelAdapter
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				h := util_test.GenerateTestHistogram(int(ts))
				histograms = append(histograms, mimirpb.FromHistogramToHistogramProto(ts, h))
				lbls = append(lbls, s)
			}
			return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(lbls, histograms, nil)
		},
		makeExpectedSamples: func(start, end int64, m model.Metric) model.Matrix {
			var expSamples []model.SampleHistogramPair
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				expSamples = append(expSamples, model.SampleHistogramPair{
					Timestamp: model.Time(ts),
					Histogram: mimirpb.FromMimirSampleToPromHistogram(mimirpb.FromFloatHistogramToSampleHistogram(util_test.GenerateTestFloatHistogram(int(ts)))),
				})
			}
			return model.Matrix{{
				Metric:     m,
				Histograms: expSamples,
			}}
		},
	},
	"float histogram": {
		makeWriteRequest: func(start, end int64, s []mimirpb.LabelAdapter) *mimirpb.WriteRequest {
			var histograms []mimirpb.Histogram
			var lbls [][]mimirpb.LabelAdapter
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				h := util_test.GenerateTestFloatHistogram(int(ts))
				histograms = append(histograms, mimirpb.FromFloatHistogramToHistogramProto(ts, h))
				lbls = append(lbls, s)
			}
			return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries(lbls, histograms, nil)
		},
		makeExpectedSamples: func(start, end int64, m model.Metric) model.Matrix {
			var expSamples []model.SampleHistogramPair
			for ts := start; ts <= end; ts += time.Minute.Milliseconds() {
				expSamples = append(expSamples, model.SampleHistogramPair{
					Timestamp: model.Time(ts),
					Histogram: mimirpb.FromMimirSampleToPromHistogram(mimirpb.FromFloatHistogramToSampleHistogram(util_test.GenerateTestFloatHistogram(int(ts)))),
				})
			}
			return model.Matrix{{
				Metric:     m,
				Histograms: expSamples,
			}}
		},
	},
}
