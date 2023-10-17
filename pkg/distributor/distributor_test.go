// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/mtime"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/scrape"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/globalerror"
	util_math "github.com/grafana/mimir/pkg/util/math"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errFail                    = httpgrpc.Errorf(http.StatusInternalServerError, "Fail")
	emptyResponse              = &mimirpb.WriteResponse{}
	generateTestHistogram      = util_test.GenerateTestHistogram
	generateTestFloatHistogram = util_test.GenerateTestFloatHistogram
)

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		initLimits func(*validation.Limits)
		expected   error
	}{
		"default config should pass": {
			initLimits: func(_ *validation.Limits) {},
			expected:   nil,
		},
		"should fail if the default shard size is negative": {
			initLimits: func(limits *validation.Limits) {
				limits.IngestionTenantShardSize = -5
			},
			expected: errInvalidTenantShardSize,
		},
		"should pass if the default shard size >= 0": {
			initLimits: func(limits *validation.Limits) {
				limits.IngestionTenantShardSize = 3
			},
			expected: nil,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			limits := validation.Limits{}
			flagext.DefaultValues(&cfg, &limits)

			testData.initLimits(&limits)

			assert.Equal(t, testData.expected, cfg.Validate(limits))
		})
	}
}

func TestDistributor_Push(t *testing.T) {
	// Metrics to assert on.
	lastSeenTimestamp := "cortex_distributor_latest_seen_sample_timestamp_seconds"
	distributorSampleDelay := "cortex_distributor_sample_delay_seconds"
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(func() {
		mtime.NowReset()
	})

	expErrFail := httpgrpc.Errorf(http.StatusInternalServerError, "failed pushing to ingester: Fail")

	type samplesIn struct {
		num              int
		startTimestampMs int64
	}
	for name, tc := range map[string]struct {
		metricNames          []string
		numIngesters         int
		happyIngesters       int
		samples              samplesIn
		metadata             int
		expectedError        error
		expectedGRPCError    *status.Status
		expectedErrorDetails *mimirpb.WriteErrorDetails
		expectedMetrics      string
		timeOut              bool
	}{
		"A push of no samples shouldn't block or return error, even if ingesters are sad": {
			numIngesters:   3,
			happyIngesters: 0,
		},
		"A push to 3 happy ingesters should succeed": {
			numIngesters:   3,
			happyIngesters: 3,
			samples:        samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:       5,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.004
			`,
		},
		"A push to 2 happy ingesters should succeed": {
			numIngesters:   3,
			happyIngesters: 2,
			samples:        samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:       5,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.004
			`,
		},
		"A push to 1 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 1,
			samples:        samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedError:  expErrFail,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
		"A push to 0 happy ingesters should fail": {
			numIngesters:   3,
			happyIngesters: 0,
			samples:        samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedError:  expErrFail,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
		"A push exceeding burst size should fail": {
			numIngesters:         3,
			happyIngesters:       3,
			samples:              samplesIn{num: 25, startTimestampMs: 123456789000},
			metadata:             5,
			expectedGRPCError:    status.New(codes.ResourceExhausted, newIngestionRateLimitedError(20, 20).Error()),
			expectedErrorDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
			metricNames:          []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.024
			`,
		},
		"A push to ingesters with an old sample should report the correct metrics with no metadata": {
			numIngesters:   3,
			happyIngesters: 2,
			samples:        samplesIn{num: 1, startTimestampMs: now.UnixMilli() - 80000*1000}, // 80k seconds old
			metadata:       0,
			metricNames:    []string{distributorSampleDelay},
			expectedMetrics: `
				# HELP cortex_distributor_sample_delay_seconds Number of seconds by which a sample came in late wrt wallclock.
				# TYPE cortex_distributor_sample_delay_seconds histogram
				cortex_distributor_sample_delay_seconds_bucket{le="30"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="60"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="120"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="240"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="480"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="1800"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="3600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="7200"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="10800"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="21600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="86400"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="+Inf"} 2
				cortex_distributor_sample_delay_seconds_sum 160000
				cortex_distributor_sample_delay_seconds_count 2
			`,
		},
		"A push to ingesters with a current sample should report the correct metrics with no metadata": {
			numIngesters:   3,
			happyIngesters: 2,
			samples:        samplesIn{num: 1, startTimestampMs: now.UnixMilli() - 1000}, // 1 second old
			metadata:       0,
			metricNames:    []string{distributorSampleDelay},
			expectedMetrics: `
				# HELP cortex_distributor_sample_delay_seconds Number of seconds by which a sample came in late wrt wallclock.
				# TYPE cortex_distributor_sample_delay_seconds histogram
				cortex_distributor_sample_delay_seconds_bucket{le="30"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="60"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="120"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="240"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="480"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="600"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="1800"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="3600"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="7200"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="10800"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="21600"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="86400"} 2
				cortex_distributor_sample_delay_seconds_bucket{le="+Inf"} 2
				cortex_distributor_sample_delay_seconds_sum 2.000
				cortex_distributor_sample_delay_seconds_count 2
			`,
		},
		"A push to ingesters without samples should report the correct metrics": {
			numIngesters:   3,
			happyIngesters: 2,
			samples:        samplesIn{num: 0, startTimestampMs: 123456789000},
			metadata:       1,
			metricNames:    []string{distributorSampleDelay},
			expectedMetrics: `
				# HELP cortex_distributor_sample_delay_seconds Number of seconds by which a sample came in late wrt wallclock.
				# TYPE cortex_distributor_sample_delay_seconds histogram
				cortex_distributor_sample_delay_seconds_bucket{le="30"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="60"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="120"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="240"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="480"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="1800"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="3600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="7200"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="10800"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="21600"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="86400"} 0
				cortex_distributor_sample_delay_seconds_bucket{le="+Inf"} 0
				cortex_distributor_sample_delay_seconds_sum 0
				cortex_distributor_sample_delay_seconds_count 0
			`,
		},
		"A timed out push should fail": {
			numIngesters:   3,
			happyIngesters: 3,
			samples:        samplesIn{num: 10, startTimestampMs: 123456789000},
			timeOut:        true,
			expectedError:  errors.New("exceeded configured distributor remote timeout: failed pushing to ingester: context deadline exceeded"),
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.IngestionRate = 20
			limits.IngestionBurstSize = 20

			ds, _, regs := prepare(t, prepConfig{
				numIngesters:    tc.numIngesters,
				happyIngesters:  tc.happyIngesters,
				numDistributors: 1,
				limits:          limits,
				timeOut:         tc.timeOut,
			})

			request := makeWriteRequest(tc.samples.startTimestampMs, tc.samples.num, tc.metadata, false, true)
			response, err := ds[0].Push(ctx, request)

			if tc.expectedError == nil && tc.expectedGRPCError == nil {
				require.NoError(t, err)
				assert.Equal(t, emptyResponse, response)
			} else {
				assert.Nil(t, response)

				if tc.expectedGRPCError == nil {
					assert.EqualError(t, err, tc.expectedError.Error())
				} else {
					checkGRPCError(t, tc.expectedGRPCError, tc.expectedErrorDetails, err)
				}
			}

			// Check tracked Prometheus metrics. Since the Push() response is sent as soon as the quorum
			// is reached, when we reach this point the 3rd ingester may not have received series/metadata
			// yet. To avoid flaky test we retry metrics assertion until we hit the desired state (no error)
			// within a reasonable timeout.
			if tc.expectedMetrics != "" {
				test.Poll(t, time.Second, nil, func() interface{} {
					return testutil.GatherAndCompare(regs[0], strings.NewReader(tc.expectedMetrics), tc.metricNames...)
				})
			}
		})
	}
}

func TestDistributor_ContextCanceledRequest(t *testing.T) {
	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	ds, ings, _ := prepare(t, prepConfig{
		numIngesters:    3,
		happyIngesters:  3,
		numDistributors: 1,
	})

	// Lock all mockIngester instances, so they will be waiting
	for i := range ings {
		ings[i].Lock()
		defer func(ing *mockIngester) {
			ing.Unlock()
		}(&ings[i])
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	request := makeWriteRequest(123456789000, 1, 1, false, true)
	_, err := ds[0].Push(ctx, request)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDistributor_MetricsCleanup(t *testing.T) {
	dists, _, regs := prepare(t, prepConfig{
		numDistributors: 1,
	})
	d := dists[0]
	reg := regs[0]

	metrics := []string{
		"cortex_distributor_received_samples_total",
		"cortex_distributor_received_exemplars_total",
		"cortex_distributor_received_metadata_total",
		"cortex_distributor_deduped_samples_total",
		"cortex_distributor_samples_in_total",
		"cortex_distributor_exemplars_in_total",
		"cortex_distributor_metadata_in_total",
		"cortex_distributor_non_ha_samples_received_total",
		"cortex_distributor_latest_seen_sample_timestamp_seconds",
	}

	d.receivedSamples.WithLabelValues("userA").Add(5)
	d.receivedSamples.WithLabelValues("userB").Add(10)
	d.receivedExemplars.WithLabelValues("userA").Add(5)
	d.receivedExemplars.WithLabelValues("userB").Add(10)
	d.receivedMetadata.WithLabelValues("userA").Add(5)
	d.receivedMetadata.WithLabelValues("userB").Add(10)
	d.incomingSamples.WithLabelValues("userA").Add(5)
	d.incomingExemplars.WithLabelValues("userA").Add(5)
	d.incomingMetadata.WithLabelValues("userA").Add(5)
	d.nonHASamples.WithLabelValues("userA").Add(5)
	d.dedupedSamples.WithLabelValues("userA", "cluster1").Inc() // We cannot clean this metric
	d.latestSeenSampleTimestampPerUser.WithLabelValues("userA").Set(1111)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter
		cortex_distributor_deduped_samples_total{cluster="cluster1",user="userA"} 1

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
		cortex_distributor_latest_seen_sample_timestamp_seconds{user="userA"} 1111

		# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
		# TYPE cortex_distributor_metadata_in_total counter
		cortex_distributor_metadata_in_total{user="userA"} 5

		# HELP cortex_distributor_non_ha_samples_received_total The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.
		# TYPE cortex_distributor_non_ha_samples_received_total counter
		cortex_distributor_non_ha_samples_received_total{user="userA"} 5

		# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
		# TYPE cortex_distributor_received_metadata_total counter
		cortex_distributor_received_metadata_total{user="userA"} 5
		cortex_distributor_received_metadata_total{user="userB"} 10

		# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
		# TYPE cortex_distributor_received_samples_total counter
		cortex_distributor_received_samples_total{user="userA"} 5
		cortex_distributor_received_samples_total{user="userB"} 10

		# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
		# TYPE cortex_distributor_received_exemplars_total counter
		cortex_distributor_received_exemplars_total{user="userA"} 5
		cortex_distributor_received_exemplars_total{user="userB"} 10

		# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected or deduped samples.
		# TYPE cortex_distributor_samples_in_total counter
		cortex_distributor_samples_in_total{user="userA"} 5

		# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
		# TYPE cortex_distributor_exemplars_in_total counter
		cortex_distributor_exemplars_in_total{user="userA"} 5
		`), metrics...))

	d.cleanupInactiveUser("userA")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_distributor_deduped_samples_total The total number of deduplicated samples.
		# TYPE cortex_distributor_deduped_samples_total counter

		# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
		# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge

		# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
		# TYPE cortex_distributor_metadata_in_total counter

		# HELP cortex_distributor_non_ha_samples_received_total The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels.
		# TYPE cortex_distributor_non_ha_samples_received_total counter

		# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
		# TYPE cortex_distributor_received_metadata_total counter
		cortex_distributor_received_metadata_total{user="userB"} 10

		# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
		# TYPE cortex_distributor_received_samples_total counter
		cortex_distributor_received_samples_total{user="userB"} 10

		# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
		# TYPE cortex_distributor_received_exemplars_total counter
		cortex_distributor_received_exemplars_total{user="userB"} 10

		# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected or deduped samples.
		# TYPE cortex_distributor_samples_in_total counter

		# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
		# TYPE cortex_distributor_exemplars_in_total counter
		`), metrics...))
}

func TestDistributor_PushRequestRateLimiter(t *testing.T) {
	type testPush struct {
		expectedError *status.Status
	}
	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		distributors               int
		requestRate                float64
		requestBurstSize           int
		pushes                     []testPush
		enableServiceOverloadError bool
	}{
		"request limit should be evenly shared across distributors": {
			distributors:     2,
			requestRate:      4,
			requestBurstSize: 2,
			pushes: []testPush{
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: status.New(codes.ResourceExhausted, newRequestRateLimitedError(4, 2).Error())},
			},
		},
		"request limit is disabled when set to 0": {
			distributors:     2,
			requestRate:      0,
			requestBurstSize: 0,
			pushes: []testPush{
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: nil},
			},
		},
		"request burst should set to each distributor": {
			distributors:     2,
			requestRate:      2,
			requestBurstSize: 3,
			pushes: []testPush{
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: status.New(codes.ResourceExhausted, newRequestRateLimitedError(2, 3).Error())},
			},
		},
		"request limit is reached return 529 when enable service overload error set to true": {
			distributors:               2,
			requestRate:                4,
			requestBurstSize:           2,
			enableServiceOverloadError: true,
			pushes: []testPush{
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: status.New(codes.Unavailable, newRequestRateLimitedError(4, 2).Error())},
			},
		},
	}

	expectedDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.RequestRate = testData.requestRate
			limits.RequestBurstSize = testData.requestBurstSize
			limits.ServiceOverloadStatusCodeOnRateLimitEnabled = testData.enableServiceOverloadError

			// Start all expected distributors
			distributors, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: testData.distributors,
				limits:          limits,
			})

			// Send multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, 1, 1, false, true)
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, emptyResponse, response)
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response)
					checkGRPCError(t, push.expectedError, expectedDetails, err)
				}
			}
		})
	}
}

func TestDistributor_PushIngestionRateLimiter(t *testing.T) {
	type testPush struct {
		samples       int
		metadata      int
		expectedError *status.Status
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		distributors       int
		ingestionRate      float64
		ingestionBurstSize int
		pushes             []testPush
	}{
		"evenly share the ingestion limit across distributors": {
			distributors:       2,
			ingestionRate:      10,
			ingestionBurstSize: 5,
			pushes: []testPush{
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: nil},
				{samples: 2, metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 5).Error())},
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 5).Error())},
				{metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 5).Error())},
			},
		},
		"for each distributor, set an ingestion burst limit.": {
			distributors:       2,
			ingestionRate:      10,
			ingestionBurstSize: 20,
			pushes: []testPush{
				{samples: 10, expectedError: nil},
				{samples: 5, expectedError: nil},
				{samples: 5, metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
				{metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
			},
		},
	}

	expectedErrorDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.IngestionRate = testData.ingestionRate
			limits.IngestionBurstSize = testData.ingestionBurstSize

			// Start all expected distributors
			distributors, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: testData.distributors,
				limits:          limits,
			})

			// Push samples in multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, push.samples, push.metadata, false, false)
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, emptyResponse, response)
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response)
					checkGRPCError(t, push.expectedError, expectedErrorDetails, err)
				}
			}
		})
	}
}

func TestDistributor_PushInstanceLimits(t *testing.T) {
	type testPush struct {
		samples       int
		metadata      int
		expectedError error
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		preInflight    int
		preRateSamples int        // initial rate before first push
		pushes         []testPush // rate is recomputed after each push

		// limits
		inflightLimit      int
		inflightBytesLimit int
		ingestionRateLimit float64

		metricNames     []string
		expectedMetrics string
	}{
		"no limits limit": {
			preInflight:    100,
			preRateSamples: 1000,

			pushes: []testPush{
				{samples: 100, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric},
			expectedMetrics: `
				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 0
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 0
		        cortex_distributor_instance_limits{limit="max_inflight_push_requests_bytes"} 0
			`,
		},
		"below inflight limit": {
			preInflight:   100,
			inflightLimit: 101,
			pushes: []testPush{
				{samples: 100, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric, "cortex_distributor_inflight_push_requests"},
			expectedMetrics: `
				# HELP cortex_distributor_inflight_push_requests Current number of inflight push requests in distributor.
				# TYPE cortex_distributor_inflight_push_requests gauge
				cortex_distributor_inflight_push_requests 100

				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 101
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 0
		        cortex_distributor_instance_limits{limit="max_inflight_push_requests_bytes"} 0
			`,
		},
		"hits inflight limit": {
			preInflight:   101,
			inflightLimit: 101,
			pushes: []testPush{
				{samples: 100, expectedError: errMaxInflightRequestsReached},
			},
		},
		"below ingestion rate limit": {
			preRateSamples:     500,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 1000, expectedError: nil},
			},

			metricNames: []string{instanceLimitsMetric, "cortex_distributor_ingestion_rate_samples_per_second"},
			expectedMetrics: `
				# HELP cortex_distributor_ingestion_rate_samples_per_second Current ingestion rate in samples/sec that distributor is using to limit access.
				# TYPE cortex_distributor_ingestion_rate_samples_per_second gauge
				cortex_distributor_ingestion_rate_samples_per_second 600

				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 0
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 1000
		        cortex_distributor_instance_limits{limit="max_inflight_push_requests_bytes"} 0
			`,
		},
		"hits rate limit on first request, but second request can proceed": {
			preRateSamples:     1200,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 100, expectedError: errMaxIngestionRateReached},
				{samples: 100, expectedError: nil},
			},
		},

		"below rate limit on first request, but hits the rate limit afterwards": {
			preRateSamples:     500,
			ingestionRateLimit: 1000,

			pushes: []testPush{
				{samples: 5000, expectedError: nil},                        // after push, rate = 500 + 0.2*(5000-500) = 1400
				{samples: 5000, expectedError: errMaxIngestionRateReached}, // after push, rate = 1400 + 0.2*(0 - 1400) = 1120
				{samples: 5000, expectedError: errMaxIngestionRateReached}, // after push, rate = 1120 + 0.2*(0 - 1120) = 896
				{samples: 5000, expectedError: nil},                        // 896 is below 1000, so this push succeeds, new rate = 896 + 0.2*(5000-896) = 1716.8
			},
		},

		"below inflight size limit": {
			inflightBytesLimit: 5800, // 5800 ~= size of a singe request with 100 samples

			pushes: []testPush{
				{samples: 10, expectedError: nil},
			},
			metricNames: []string{instanceLimitsMetric, "cortex_distributor_inflight_push_requests_bytes"},

			expectedMetrics: `
				# HELP cortex_distributor_inflight_push_requests_bytes Current sum of inflight push requests in distributor in bytes.
				# TYPE cortex_distributor_inflight_push_requests_bytes gauge
				cortex_distributor_inflight_push_requests_bytes 0

				# HELP cortex_distributor_instance_limits Instance limits used by this distributor.
				# TYPE cortex_distributor_instance_limits gauge
				cortex_distributor_instance_limits{limit="max_inflight_push_requests_bytes"} 5800
				cortex_distributor_instance_limits{limit="max_inflight_push_requests"} 0
				cortex_distributor_instance_limits{limit="max_ingestion_rate"} 0
			`,
		},

		"hits inflight size limit": {
			inflightBytesLimit: 5800, // 5800 ~= size of a singe request with 100 samples

			pushes: []testPush{
				{samples: 150, expectedError: errMaxInflightRequestsBytesReached},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)

			// Start all expected distributors
			distributors, _, regs := prepare(t, prepConfig{
				numIngesters:             3,
				happyIngesters:           3,
				numDistributors:          1,
				limits:                   limits,
				maxInflightRequests:      testData.inflightLimit,
				maxInflightRequestsBytes: testData.inflightBytesLimit,
				maxIngestionRate:         testData.ingestionRateLimit,
			})

			d := distributors[0]
			d.inflightPushRequests.Add(int64(testData.preInflight))
			d.ingestionRate.Add(int64(testData.preRateSamples))

			d.ingestionRate.Tick()

			for _, push := range testData.pushes {
				request := makeWriteRequest(0, push.samples, push.metadata, false, false)
				resp, err := d.Push(ctx, request)

				if push.expectedError == nil {
					assert.NoError(t, err)
					assert.Equal(t, emptyResponse, resp)
				} else {
					assert.Error(t, err)
					assert.Nil(t, resp)
					checkGRPCError(t, status.New(codes.Internal, push.expectedError.Error()), nil, err)
				}

				d.ingestionRate.Tick()

				if testData.expectedMetrics != "" {
					// The number of inflight requests is decreased asynchronously once the request to the latest
					// ingester is completed too. To avoid flaky tests, we poll the metrics because what we expect
					// is that metrics reconcile to the expected ones.
					test.Poll(t, 3*time.Second, nil, func() interface{} {
						return testutil.GatherAndCompare(regs[0], strings.NewReader(testData.expectedMetrics), testData.metricNames...)
					})
				}
			}
		})
	}
}

func TestDistributor_PushHAInstances(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	for i, tc := range []struct {
		enableTracker    bool
		acceptedReplica  string
		testReplica      string
		cluster          string
		samples          int
		expectedResponse *mimirpb.WriteResponse
		expectedError    *status.Status
		expectedDetails  *mimirpb.WriteErrorDetails
	}{
		{
			enableTracker:    true,
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponse,
		},
		// The 202 indicates that we didn't accept this sample.
		{
			enableTracker:   true,
			acceptedReplica: "instance2",
			testReplica:     "instance0",
			cluster:         "cluster0",
			samples:         5,
			expectedError:   status.New(codes.AlreadyExists, newReplicasDidNotMatchError("instance0", "instance2").Error()),
			expectedDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
		},
		// If the HA tracker is disabled we should still accept samples that have both labels.
		{
			enableTracker:    false,
			acceptedReplica:  "instance0",
			testReplica:      "instance0",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponse,
		},
		// Using very long replica label value results in validation error.
		{
			enableTracker:   true,
			acceptedReplica: "instance0",
			testReplica:     "instance1234567890123456789012345678901234567890",
			cluster:         "cluster0",
			samples:         5,
			expectedError:   status.New(codes.FailedPrecondition, fmt.Sprintf(labelValueTooLongMsgFormat, "instance1234567890123456789012345678901234567890", formatLabelSet(labelSetGenWithReplicaAndCluster("instance1234567890123456789012345678901234567890", "cluster0")(0)))),
			expectedDetails: &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.AcceptHASamples = true
			limits.MaxLabelValueLength = 15

			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          &limits,
				enableTracker:   tc.enableTracker,
			})

			d := ds[0]

			userID, err := tenant.TenantID(ctx)
			assert.NoError(t, err)
			err = d.HATracker.checkReplica(ctx, userID, tc.cluster, tc.acceptedReplica, time.Now())
			assert.NoError(t, err)

			request := makeWriteRequestForGenerators(tc.samples, labelSetGenWithReplicaAndCluster(tc.testReplica, tc.cluster), nil, nil)
			response, err := d.Push(ctx, request)
			assert.Equal(t, tc.expectedResponse, response)

			if tc.expectedError != nil {
				checkGRPCError(t, tc.expectedError, tc.expectedDetails, err)
			}
		})
	}
}

func TestDistributor_PushQuery(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	barMatcher := mustEqualMatcher("bar", "baz")

	type testcase struct {
		name              string
		numIngesters      int
		happyIngesters    int
		samples           int
		metadata          int
		matchers          []*labels.Matcher
		expectedIngesters int
		expectedResponse  model.Matrix
		expectedError     error
		shuffleShardSize  int
	}

	// We'll programmatically build the test cases now, as we want complete
	// coverage along quite a few different axis.
	testcases := []testcase{}

	// Test with between 2 and 10 ingesters.
	for numIngesters := 2; numIngesters < 10; numIngesters++ {
		// Test with between 0 and numIngesters "happy" ingesters.
		for happyIngesters := 0; happyIngesters <= numIngesters; happyIngesters++ {
			// Test either with shuffle-sharding enabled or disabled.
			for _, shuffleShardSize := range []int{0, 5} {
				scenario := fmt.Sprintf("numIngester=%d, happyIngester=%d, shuffleShardSize=%v)", numIngesters, happyIngesters, shuffleShardSize)

				var expectedIngesters int
				if shuffleShardSize > 0 {
					expectedIngesters = util_math.Min(shuffleShardSize, numIngesters)
				} else {
					expectedIngesters = numIngesters
				}

				// Queriers with more than one failed ingester should fail.
				if numIngesters-happyIngesters > 1 {
					testcases = append(testcases, testcase{
						name:             fmt.Sprintf("ExpectFail(%s)", scenario),
						numIngesters:     numIngesters,
						happyIngesters:   happyIngesters,
						matchers:         []*labels.Matcher{nameMatcher, barMatcher},
						expectedError:    errFail,
						shuffleShardSize: shuffleShardSize,
					})
					continue
				}

				// When we have less ingesters than replication factor, any failed ingester
				// will cause a failure.
				if numIngesters < 3 && happyIngesters < 2 {
					testcases = append(testcases, testcase{
						name:             fmt.Sprintf("ExpectFail(%s)", scenario),
						numIngesters:     numIngesters,
						happyIngesters:   happyIngesters,
						matchers:         []*labels.Matcher{nameMatcher, barMatcher},
						expectedError:    errFail,
						shuffleShardSize: shuffleShardSize,
					})
					continue
				}

				// Reading all the samples back should succeed.
				testcases = append(testcases, testcase{
					name:              fmt.Sprintf("ReadAll(%s)", scenario),
					numIngesters:      numIngesters,
					happyIngesters:    happyIngesters,
					samples:           10,
					matchers:          []*labels.Matcher{nameMatcher, barMatcher},
					expectedResponse:  expectedResponse(0, 10, true),
					expectedIngesters: expectedIngesters,
					shuffleShardSize:  shuffleShardSize,
				})

				// As should reading none of the samples back.
				testcases = append(testcases, testcase{
					name:              fmt.Sprintf("ReadNone(%s)", scenario),
					numIngesters:      numIngesters,
					happyIngesters:    happyIngesters,
					samples:           10,
					matchers:          []*labels.Matcher{nameMatcher, mustEqualMatcher("not", "found")},
					expectedResponse:  expectedResponse(0, 0, true),
					expectedIngesters: expectedIngesters,
					shuffleShardSize:  shuffleShardSize,
				})

				// And reading each sample individually.
				for i := 0; i < 10; i++ {
					testcases = append(testcases, testcase{
						name:              fmt.Sprintf("ReadOne(%s, sample=%d)", scenario, i),
						numIngesters:      numIngesters,
						happyIngesters:    happyIngesters,
						samples:           10,
						matchers:          []*labels.Matcher{nameMatcher, mustEqualMatcher("sample", strconv.Itoa(i))},
						expectedResponse:  expectedResponse(i, i+1, true),
						expectedIngesters: expectedIngesters,
						shuffleShardSize:  shuffleShardSize,
					})
				}
			}
		}
	}

	for _, tc := range testcases {
		// Change scope to ensure it work fine when test cases are executed concurrently.
		tc := tc

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := prepConfig{
				numIngesters:    tc.numIngesters,
				happyIngesters:  tc.happyIngesters,
				numDistributors: 1,
			}

			cfg.shuffleShardSize = tc.shuffleShardSize

			ds, ingesters, reg := prepare(t, cfg)

			request := makeWriteRequest(0, tc.samples, tc.metadata, false, true)
			writeResponse, err := ds[0].Push(ctx, request)
			assert.Equal(t, &mimirpb.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			queryMetrics := stats.NewQueryMetrics(reg[0])
			resp, err := ds[0].QueryStream(ctx, queryMetrics, 0, 10, tc.matchers...)

			if tc.expectedError == nil {
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.expectedError.Error())

				// Assert that downstream gRPC statuses are passed back upstream
				_, ok := httpgrpc.HTTPResponseFromError(err)
				assert.True(t, ok, fmt.Sprintf("expected error to be an httpgrpc error, but got: %T", err))
			}

			var m model.Matrix
			if len(resp.Chunkseries) == 0 {
				m, err = client.TimeSeriesChunksToMatrix(0, 10, nil)
			} else {
				m, err = client.TimeSeriesChunksToMatrix(0, 10, resp.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse.String(), m.String())

			// Check how many ingesters have been queried.
			// Due to the quorum the distributor could cancel the last request towards ingesters
			// if all other ones are successful, so we're good either has been queried X or X-1
			// ingesters.
			if tc.expectedError == nil {
				assert.Contains(t, []int{tc.expectedIngesters, tc.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "QueryStream"))
			}
		})
	}
}

func TestDistributor_Push_LabelRemoval(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		removeReplica  bool
		removeLabels   []string
	}

	cases := []testcase{
		// Remove both cluster and replica label.
		{
			removeReplica:  true,
			removeLabels:   []string{"cluster"},
			inputSeries:    labels.FromStrings("__name__", "some_metric", "cluster", "one", "__replica__", "two"),
			expectedSeries: labels.FromStrings("__name__", "some_metric"),
		},
		// Remove multiple labels and replica.
		{
			removeReplica: true,
			removeLabels:  []string{"foo", "some"},
			inputSeries: labels.FromStrings("__name__", "some_metric", "cluster", "one", "__replica__", "two",
				"foo", "bar", "some", "thing"),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "cluster", "one"),
		},
		// Remove blank labels.
		{
			inputSeries:    labels.FromStrings("__name__", "some_metric", "blank", "", "foo", "bar"),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labels.FromStrings("__name__", "some_metric", "foo", "bar", "zzz_blank", ""),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labels.FromStrings("__blank__", "", "__name__", "some_metric", "foo", "bar"),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labels.FromStrings("__blank__", "", "__name__", "some_metric", "foo", "bar", "zzz_blank", ""),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "foo", "bar"),
		},
		// Don't remove any labels.
		{
			removeReplica:  false,
			inputSeries:    labels.FromStrings("__name__", "some_metric", "__replica__", "two", "cluster", "one"),
			expectedSeries: labels.FromStrings("__name__", "some_metric", "__replica__", "two", "cluster", "one"),
		},
	}

	for _, tc := range cases {
		var err error
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.DropLabels = tc.removeLabels
		limits.AcceptHASamples = tc.removeReplica

		ds, ingesters, _ := prepare(t, prepConfig{
			numIngesters:    2,
			happyIngesters:  2,
			numDistributors: 1,
			limits:          &limits,
		})

		// Push the series to the distributor
		req := mockWriteRequest(tc.inputSeries, 1, 1)
		_, err = ds[0].Push(ctx, req)
		require.NoError(t, err)

		// Since each test pushes only 1 series, we do expect the ingester
		// to have received exactly 1 series
		for i := range ingesters {
			timeseries := ingesters[i].series()
			assert.Equal(t, 1, len(timeseries))
			for _, v := range timeseries {
				assert.Equal(t, tc.expectedSeries, mimirpb.FromLabelAdaptersToLabels(v.Labels))
			}
		}
	}
}

func TestDistributor_Push_ShouldGuaranteeShardingTokenConsistencyOverTheTime(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		inputSeries    labels.Labels
		expectedSeries labels.Labels
		expectedToken  uint32
	}{
		"metric_1 with value_1": {
			inputSeries:    labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedSeries: labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped label due to config": {
			inputSeries: labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1",
				"dropped", "unused"),
			expectedSeries: labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped HA replica label": {
			inputSeries: labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1",
				"__replica__", "replica_1"),
			expectedSeries: labels.FromStrings("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_2 with value_1": {
			inputSeries:    labels.FromStrings("__name__", "metric_2", "key", "value_1"),
			expectedSeries: labels.FromStrings("__name__", "metric_2", "key", "value_1"),
			expectedToken:  0xa60906f2,
		},
		"metric_1 with value_2": {
			inputSeries:    labels.FromStrings("__name__", "metric_1", "key", "value_2"),
			expectedSeries: labels.FromStrings("__name__", "metric_1", "key", "value_2"),
			expectedToken:  0x18abc8a2,
		},
	}

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.DropLabels = []string{"dropped"}
	limits.AcceptHASamples = true

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ds, ingesters, _ := prepare(t, prepConfig{
				numIngesters:    2,
				happyIngesters:  2,
				numDistributors: 1,
				limits:          &limits,
			})

			// Push the series to the distributor
			req := mockWriteRequest(testData.inputSeries, 1, 1)
			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)

			// Since each test pushes only 1 series, we do expect the ingester
			// to have received exactly 1 series
			for i := range ingesters {
				timeseries := ingesters[i].series()
				assert.Equal(t, 1, len(timeseries))

				series, ok := timeseries[testData.expectedToken]
				require.True(t, ok)
				assert.Equal(t, testData.expectedSeries, mimirpb.FromLabelAdaptersToLabels(series.Labels))
			}
		})
	}
}

func TestDistributor_Push_LabelNameValidation(t *testing.T) {
	inputLabels := labels.FromStrings(model.MetricNameLabel, "foo", "999.illegal", "baz")
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		inputLabels                labels.Labels
		skipLabelNameValidationCfg bool
		skipLabelNameValidationReq bool
		errExpected                bool
		errMessage                 string
	}{
		"label name validation is on by default": {
			inputLabels: inputLabels,
			errExpected: true,
			errMessage:  `received a series with an invalid label: '999.illegal' series: 'foo{999.illegal="baz"}' (err-mimir-label-invalid)`,
		},
		"label name validation can be skipped via config": {
			inputLabels:                inputLabels,
			skipLabelNameValidationCfg: true,
			errExpected:                false,
		},
		"label name validation can be skipped via WriteRequest parameter": {
			inputLabels:                inputLabels,
			skipLabelNameValidationReq: true,
			errExpected:                false,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:            2,
				happyIngesters:          2,
				numDistributors:         1,
				shuffleShardSize:        0,
				skipLabelNameValidation: tc.skipLabelNameValidationCfg,
			})
			req := mockWriteRequest(tc.inputLabels, 42, 100000)
			req.SkipLabelNameValidation = tc.skipLabelNameValidationReq
			_, err := ds[0].Push(ctx, req)
			if tc.errExpected {
				fromError, _ := status.FromError(err)
				assert.Equal(t, tc.errMessage, fromError.Message())
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestDistributor_Push_ExemplarValidation(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	manyLabels := []string{model.MetricNameLabel, "test"}
	for i := 1; i < 31; i++ {
		manyLabels = append(manyLabels, fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
	}

	tests := map[string]struct {
		req    *mimirpb.WriteRequest
		errMsg string
		errID  globalerror.ID
	}{
		"valid exemplar": {
			req: makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{"foo", "bar"}),
		},
		"rejects exemplar with no labels": {
			req:    makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{}),
			errMsg: `received an exemplar with no valid labels, timestamp: 1000 series: {__name__="test"} labels: {}`,
			errID:  globalerror.ExemplarLabelsMissing,
		},
		"rejects exemplar with no timestamp": {
			req:    makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 0, []string{"foo", "bar"}),
			errMsg: `received an exemplar with no timestamp, timestamp: 0 series: {__name__="test"} labels: {foo="bar"}`,
			errID:  globalerror.ExemplarTimestampInvalid,
		},
		"rejects exemplar with too long labelset": {
			req:    makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, []string{"foo", strings.Repeat("0", 126)}),
			errMsg: fmt.Sprintf(`received an exemplar where the size of its combined labels exceeds the limit of 128 characters, timestamp: 1000 series: {__name__="test"} labels: {foo="%s"}`, strings.Repeat("0", 126)),
			errID:  globalerror.ExemplarLabelsTooLong,
		},
		"rejects exemplar with too many series labels": {
			req:    makeWriteRequestExemplar(manyLabels, 0, nil),
			errMsg: "received a series whose number of labels exceeds the limit",
			errID:  globalerror.MaxLabelNamesPerSeries,
		},
		"rejects exemplar with duplicate series labels": {
			req:    makeWriteRequestExemplar([]string{model.MetricNameLabel, "test", "foo", "bar", "foo", "bar"}, 0, nil),
			errMsg: "received a series with duplicate label name",
			errID:  globalerror.SeriesWithDuplicateLabelNames,
		},
		"rejects exemplar with empty series label name": {
			req:    makeWriteRequestExemplar([]string{model.MetricNameLabel, "test", "", "bar"}, 0, nil),
			errMsg: "received a series with an invalid label",
			errID:  globalerror.SeriesInvalidLabel,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.MaxGlobalExemplarsPerUser = 10
			ds, _, _ := prepare(t, prepConfig{
				limits:           limits,
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shuffleShardSize: 0,
			})
			_, err := ds[0].Push(ctx, tc.req)
			if tc.errMsg != "" {
				fromError, _ := status.FromError(err)
				assert.Contains(t, fromError.Message(), tc.errMsg)
				assert.Contains(t, fromError.Message(), tc.errID)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}

func TestDistributor_Push_HistogramValidation(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	// For testing bucket limit
	testHistogram := util_test.GenerateTestFloatHistogram(0)
	require.Equal(t, 8, len(testHistogram.PositiveBuckets)+len(testHistogram.NegativeBuckets), "selftest, check generator drift")

	tests := map[string]struct {
		req         *mimirpb.WriteRequest
		expectedErr *status.Status
		errMsg      string
		errID       globalerror.ID
		bucketLimit int
	}{
		"valid histogram": {
			req: makeWriteRequestHistogram([]string{model.MetricNameLabel, "test"}, 1000, generateTestHistogram(0)),
		},
		"too new histogram": {
			req:         makeWriteRequestHistogram([]string{model.MetricNameLabel, "test"}, math.MaxInt64, generateTestHistogram(0)),
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(sampleTimestampTooNewMsgFormat, math.MaxInt64, "test")),
		},
		"valid float histogram": {
			req: makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, 1000, generateTestFloatHistogram(0)),
		},
		"too new float histogram": {
			req:         makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, math.MaxInt64, generateTestFloatHistogram(0)),
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(sampleTimestampTooNewMsgFormat, math.MaxInt64, "test")),
		},
		"buckets at limit": {
			req:         makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, 1000, testHistogram),
			bucketLimit: 8,
		},
		"buckets over limit": {
			req:         makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, 1000, testHistogram),
			bucketLimit: 7,
			errMsg:      "received a native histogram sample with too many buckets, timestamp",
			errID:       globalerror.MaxNativeHistogramBuckets,
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(maxNativeHistogramBucketsMsgFormat, 1000, "{__name__=\"test\"}", 8, 7)),
		},
	}

	expectedDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.CreationGracePeriod = model.Duration(time.Minute)
			limits.MaxNativeHistogramBuckets = tc.bucketLimit

			ds, _, _ := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shuffleShardSize: 0,
				limits:           limits,
			})

			resp, err := ds[0].Push(ctx, tc.req)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
				assert.Equal(t, emptyResponse, resp)
			} else {
				assert.Error(t, err)
				assert.Nil(t, resp)
				checkGRPCError(t, tc.expectedErr, expectedDetails, err)
			}

			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ds[0]))
			})
		})
	}
}

func TestDistributor_ExemplarValidation(t *testing.T) {
	tests := map[string]struct {
		prepareConfig     func(limits *validation.Limits)
		minExemplarTS     int64
		maxExemplarTS     int64
		req               *mimirpb.WriteRequest
		expectedExemplars []mimirpb.PreallocTimeseries
		expectedMetrics   string
	}{
		"disable exemplars": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 0
			},
			minExemplarTS: 0,
			maxExemplarTS: 0,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, []string{"foo", "bar"}),
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:    []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test1"}},
					Exemplars: []mimirpb.Exemplar{},
				}},
			},
		},
		"valid exemplars": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 1
			},
			minExemplarTS: 0,
			maxExemplarTS: math.MaxInt64,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, []string{"foo", "bar"}),
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, []string{"foo", "bar"}),
			},
		},
		"should drop exemplars with timestamp lower than the accepted minimum, when the exemplars are specified in different series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 1
			},
			minExemplarTS: 300000,
			maxExemplarTS: math.MaxInt64,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 1000, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 601000, []string{"foo", "bar"}),
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:    []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{},
				}},
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 601000, []string{"foo", "bar"}),
			},
			expectedMetrics: `
                # HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
                # TYPE cortex_discarded_exemplars_total counter
                cortex_discarded_exemplars_total{reason="exemplar_too_old",user="user"} 1
            `,
		},
		"should drop exemplars with timestamp lower than the accepted minimum, when multiple exemplars are specified for the same series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 2
			},
			minExemplarTS: 300000,
			maxExemplarTS: math.MaxInt64,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			},
			expectedMetrics: `
                # HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
                # TYPE cortex_discarded_exemplars_total counter
                cortex_discarded_exemplars_total{reason="exemplar_too_old",user="user"} 1
            `,
		},
		"should drop exemplars with timestamp lower than the accepted minimum, when multiple exemplars are specified in the same series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 2
			},
			minExemplarTS: 300000,
			maxExemplarTS: math.MaxInt64,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			},
			expectedMetrics: `
                # HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
                # TYPE cortex_discarded_exemplars_total counter
                cortex_discarded_exemplars_total{reason="exemplar_too_old",user="user"} 1
            `,
		},
		"should drop exemplars with timestamp greater than the accepted maximum, when multiple exemplars are specified in the same series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 2
			},
			minExemplarTS: 0,
			maxExemplarTS: 100000,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
						},
					},
				},
			},
			expectedMetrics: `
                # HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
                # TYPE cortex_discarded_exemplars_total counter
                cortex_discarded_exemplars_total{reason="exemplar_too_far_in_future",user="user"} 1
            `,
		},
	}
	now := mtime.Now()
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			tc.prepareConfig(limits)
			ds, _, regs := prepare(t, prepConfig{
				limits:          limits,
				numDistributors: 1,
			})

			// Pre-condition check.
			require.Len(t, ds, 1)
			require.Len(t, regs, 1)

			for _, ts := range tc.req.Timeseries {
				err := ds[0].validateSeries(now, &ts, "user", "test-group", false, tc.minExemplarTS, tc.maxExemplarTS)
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expectedExemplars, tc.req.Timeseries)
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(tc.expectedMetrics), "cortex_discarded_exemplars_total"))
		})
	}
}

func mkLabels(n int, extra ...string) []mimirpb.LabelAdapter {
	ret := make([]mimirpb.LabelAdapter, 1+n+len(extra)/2)
	ret[0] = mimirpb.LabelAdapter{Name: model.MetricNameLabel, Value: "foo"}
	for i := 0; i < n; i++ {
		ret[i+1] = mimirpb.LabelAdapter{Name: fmt.Sprintf("name_%d", i), Value: fmt.Sprintf("value_%d", i)}
	}
	for i := 0; i < len(extra); i += 2 {
		ret[i+n+1] = mimirpb.LabelAdapter{Name: extra[i], Value: extra[i+1]}
	}
	slices.SortFunc(ret, func(a, b mimirpb.LabelAdapter) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
	return ret
}

func BenchmarkDistributor_Push(b *testing.B) {
	const (
		numSeriesPerRequest = 1000
	)
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		prepareConfig func(limits *validation.Limits)
		prepareSeries func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample)
		expectedErr   string
	}{
		"all samples successfully pushed": {
			prepareConfig: func(limits *validation.Limits) {},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					metrics[i] = mkLabels(10)
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "",
		},
		"ingestion rate limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.IngestionRate = 1
				limits.IngestionBurstSize = 1
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					metrics[i] = mkLabels(10)
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "ingestion rate limit",
		},
		"too many labels limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelNamesPerSeries = 30
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					metrics[i] = mkLabels(31)
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "received a series whose number of labels exceeds the limit",
		},
		"max label name length limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelNameLength = 1024
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					// Add a label with a very long name.
					metrics[i] = mkLabels(10, fmt.Sprintf("xxx_%0.2000d", 1), "xxx")
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "received a series whose label name length exceeds the limit",
		},
		"max label value length limit reached": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxLabelValueLength = 1024
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					// Add a label with a very long value.
					metrics[i] = mkLabels(10, "xxx", fmt.Sprintf("xxx_%0.2000d", 1))
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "received a series whose label value length exceeds the limit",
		},
		"timestamp too new": {
			prepareConfig: func(limits *validation.Limits) {
				limits.CreationGracePeriod = model.Duration(time.Minute)
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					metrics[i] = mkLabels(10)
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().Add(time.Hour).UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "received a sample whose timestamp is too far in the future",
		},
		"all samples go to metric_relabel_configs": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MetricRelabelConfigs = []*relabel.Config{
					{
						SourceLabels: []model.LabelName{"__name__"},
						Action:       relabel.DefaultRelabelConfig.Action,
						Regex:        relabel.DefaultRelabelConfig.Regex,
						Replacement:  relabel.DefaultRelabelConfig.Replacement,
						TargetLabel:  "__tmp_name",
					},
				}
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					metrics[i] = mkLabels(10)
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "",
		},
	}

	for testName, testData := range tests {
		b.Run(testName, func(b *testing.B) {
			// Create an in-memory KV store for the ring with 1 ingester registered.
			kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			b.Cleanup(func() { assert.NoError(b, closer.Close()) })

			err := kvStore.CAS(context.Background(), ingester.IngesterRingKey,
				func(_ interface{}) (interface{}, bool, error) {
					d := &ring.Desc{}
					d.AddIngester("ingester-1", "127.0.0.1", "", ring.NewRandomTokenGenerator().GenerateTokens(128, nil), ring.ACTIVE, time.Now())
					return d, true, nil
				},
			)
			require.NoError(b, err)

			ingestersRing, err := ring.New(ring.Config{
				KVStore:           kv.Config{Mock: kvStore},
				HeartbeatTimeout:  60 * time.Minute,
				ReplicationFactor: 1,
			}, ingester.IngesterRingKey, ingester.IngesterRingKey, log.NewNopLogger(), nil)
			require.NoError(b, err)
			require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingestersRing))
			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(context.Background(), ingestersRing))
			})

			test.Poll(b, time.Second, 1, func() interface{} {
				return ingestersRing.InstancesCount()
			})

			// Prepare the distributor configuration.
			var distributorCfg Config
			var clientConfig client.Config
			limits := validation.Limits{}
			flagext.DefaultValues(&distributorCfg, &clientConfig, &limits)
			distributorCfg.DistributorRing.Common.KVStore.Store = "inmemory"

			limits.IngestionRate = float64(rate.Inf) // Unlimited.
			testData.prepareConfig(&limits)

			distributorCfg.IngesterClientFactory = ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
				return &noopIngester{}, nil
			})

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(b, err)

			// Start the distributor.
			distributor, err := New(distributorCfg, clientConfig, overrides, nil, ingestersRing, true, nil, log.NewNopLogger())
			require.NoError(b, err)
			require.NoError(b, services.StartAndAwaitRunning(context.Background(), distributor))

			b.Cleanup(func() {
				require.NoError(b, services.StopAndAwaitTerminated(context.Background(), distributor))
			})

			// Prepare the series to remote write before starting the benchmark.
			metrics, samples := testData.prepareSeries()

			// Run the benchmark.
			b.ReportAllocs()
			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, err := distributor.Push(ctx, mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API))

				if testData.expectedErr == "" && err != nil {
					b.Fatalf("no error expected but got %v", err)
				}
				if testData.expectedErr != "" && (err == nil || !strings.Contains(err.Error(), testData.expectedErr)) {
					b.Fatalf("expected %v error but got %v", testData.expectedErr, err)
				}
			}
		})
	}
}

func TestSlowQueries(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, "foo")
	nIngesters := 3
	for happy := 0; happy <= nIngesters; happy++ {
		t.Run(fmt.Sprintf("%d", happy), func(t *testing.T) {
			var expectedErr error
			if nIngesters-happy > 1 {
				expectedErr = errFail
			}

			ds, _, reg := prepare(t, prepConfig{
				numIngesters:    nIngesters,
				happyIngesters:  happy,
				numDistributors: 1,
				queryDelay:      100 * time.Millisecond,
			})

			queryMetrics := stats.NewQueryMetrics(reg[0])
			_, err := ds[0].QueryStream(ctx, queryMetrics, 0, 10, nameMatcher)
			assert.Equal(t, expectedErr, err)
		})
	}
}

func TestDistributor_MetricsForLabelMatchers(t *testing.T) {
	const numIngesters = 5

	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.FromStrings(labels.MetricName, "fast_fingerprint_collision", "app", "l", "uniq0", "0", "uniq1", "1"), 1, 300000},
		{labels.FromStrings(labels.MetricName, "fast_fingerprint_collision", "app", "m", "uniq0", "1", "uniq1", "1"), 1, 300000},
	}

	tests := map[string]struct {
		shuffleShardSize  int
		matchers          []*labels.Matcher
		expectedResult    []labels.Labels
		expectedIngesters int
	}{
		"should return an empty response if no metric match": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown"),
			},
			expectedResult:    []labels.Labels{},
			expectedIngesters: numIngesters,
		},
		"should filter metrics by single matcher": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []labels.Labels{
				fixtures[0].lbls,
				fixtures[1].lbls,
			},
			expectedIngesters: numIngesters,
		},
		"should filter metrics by multiple matchers": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "status", "200"),
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []labels.Labels{
				fixtures[0].lbls,
			},
			expectedIngesters: numIngesters,
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "fast_fingerprint_collision"),
			},
			expectedResult: []labels.Labels{
				fixtures[3].lbls,
				fixtures[4].lbls,
			},
			expectedIngesters: numIngesters,
		},
		"should query only ingesters belonging to tenant's subring if shuffle shard size is set": {
			shuffleShardSize: 3,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: []labels.Labels{
				fixtures[0].lbls,
				fixtures[1].lbls,
			},
			expectedIngesters: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			now := model.Now()

			// Create distributor
			ds, ingesters, _ := prepare(t, prepConfig{
				numIngesters:     numIngesters,
				happyIngesters:   numIngesters,
				numDistributors:  1,
				shuffleShardSize: testData.shuffleShardSize,
			})

			// Push fixtures
			ctx := user.InjectOrgID(context.Background(), "test")

			for _, series := range fixtures {
				req := mockWriteRequest(series.lbls, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			metrics, err := ds[0].MetricsForLabelMatchers(ctx, now, now, testData.matchers...)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedResult, metrics)

			// Check how many ingesters have been queried.
			// Due to the quorum the distributor could cancel the last request towards ingesters
			// if all other ones are successful, so we're good either has been queried X or X-1
			// ingesters.
			assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "MetricsForLabelMatchers"))
		})
	}
}

func TestDistributor_LabelNames(t *testing.T) {
	const numIngesters = 5

	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	tests := map[string]struct {
		shuffleShardSize  int
		matchers          []*labels.Matcher
		expectedResult    []string
		expectedIngesters int
	}{
		"should return an empty response if no metric match": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown"),
			},
			expectedResult:    []string{},
			expectedIngesters: numIngesters,
		},
		"should filter metrics by single matcher": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult:    []string{labels.MetricName, "reason", "status"},
			expectedIngesters: numIngesters,
		},
		"should filter metrics by multiple matchers": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, "status", "200"),
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult:    []string{labels.MetricName, "status"},
			expectedIngesters: numIngesters,
		},
		"should query only ingesters belonging to tenant's subring if shuffle sharding is enabled": {
			shuffleShardSize: 3,
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult:    []string{labels.MetricName, "reason", "status"},
			expectedIngesters: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			now := model.Now()

			// Create distributor
			ds, ingesters, _ := prepare(t, prepConfig{
				numIngesters:     numIngesters,
				happyIngesters:   numIngesters,
				numDistributors:  1,
				shuffleShardSize: testData.shuffleShardSize,
			})

			// Push fixtures
			ctx := user.InjectOrgID(context.Background(), "test")

			for _, series := range fixtures {
				req := mockWriteRequest(series.lbls, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			names, err := ds[0].LabelNames(ctx, now, now, testData.matchers...)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expectedResult, names)

			// Check how many ingesters have been queried.
			// Due to the quorum the distributor could cancel the last request towards ingesters
			// if all other ones are successful, so we're good either has been queried X or X-1
			// ingesters.
			assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalls(ingesters, "LabelNames"))
		})
	}
}

func TestDistributor_MetricsMetadata(t *testing.T) {
	const numIngesters = 5

	tests := map[string]struct {
		shuffleShardSize  int
		expectedIngesters int
	}{
		"should query all ingesters if shuffle sharding is enabled but shard size is 0": {
			shuffleShardSize:  0,
			expectedIngesters: numIngesters,
		},
		"should query only ingesters belonging to tenant's subring if shuffle sharding is enabled": {
			shuffleShardSize:  3,
			expectedIngesters: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create distributor
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:     numIngesters,
				happyIngesters:   numIngesters,
				numDistributors:  1,
				shuffleShardSize: testData.shuffleShardSize,
				limits:           nil,
			})

			// Push metadata
			ctx := user.InjectOrgID(context.Background(), "test")

			req := makeWriteRequest(0, 0, 10, false, true)
			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)

			// Check how many ingesters are queried as part of the shuffle sharding subring.
			replicationSet, err := ds[0].GetIngesters(ctx)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedIngesters, len(replicationSet.Instances))

			// Assert on metric metadata
			metadata, err := ds[0].MetricsMetadata(ctx, client.DefaultMetricsMetadataRequest())
			require.NoError(t, err)

			expectedMetadata := make([]scrape.MetricMetadata, 0, len(req.Metadata))
			for _, m := range req.Metadata {
				expectedMetadata = append(expectedMetadata, scrape.MetricMetadata{
					Metric: m.MetricFamilyName,
					Type:   mimirpb.MetricMetadataMetricTypeToMetricType(m.Type),
					Help:   m.Help,
					Unit:   m.Unit,
				})
			}

			assert.ElementsMatch(t, metadata, expectedMetadata)
		})
	}
}

func TestDistributor_LabelNamesAndValuesLimitTest(t *testing.T) {
	// distinct values are "__name__", "label_00", "label_01" that is 24 bytes in total
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "label_00"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "label_11"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "label_11"), 2, 200000},
	}
	tests := map[string]struct {
		sizeLimitBytes int
		expectedError  string
	}{
		"expected error if sizeLimit is reached": {
			sizeLimitBytes: 20,
			expectedError:  "size of distinct label names and values is greater than 20 bytes",
		},
		"expected no error if sizeLimit is not reached": {
			sizeLimitBytes: 25,
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "label-names-values")

			// Create distributor
			limits := validation.Limits{}
			flagext.DefaultValues(&limits)
			limits.LabelNamesAndValuesResultsMaxSizeBytes = testData.sizeLimitBytes
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          &limits,
			})
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ds[0]))
			})

			// Push fixtures
			for _, series := range fixtures {
				req := mockWriteRequest(series.lbls, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			_, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{})
			if len(testData.expectedError) == 0 {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, testData.expectedError)
			}
		})
	}
}

func TestDistributor_LabelValuesForLabelName(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "label_0", "status", "200"), 1, 100_000},
		{labels.FromStrings(labels.MetricName, "label_1", "status", "500", "reason", "broken"), 1, 110_000},
		{labels.FromStrings(labels.MetricName, "label_1"), 2, 200_000},
	}
	tests := map[string]struct {
		from, to            model.Time
		expectedLabelValues []string
		matchers            []*labels.Matcher
	}{
		"all time selected, no matchers": {
			from:                0,
			to:                  300_000,
			expectedLabelValues: []string{"label_0", "label_1"},
		},
		"subset of time selected": {
			from:                150_000,
			to:                  300_000,
			expectedLabelValues: []string{"label_1"},
		},
		"matchers provided": {
			from:                0,
			to:                  300_000,
			expectedLabelValues: []string{"label_1"},
			matchers:            []*labels.Matcher{mustNewMatcher(labels.MatchEqual, "reason", "broken")},
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "label-names-values")

			// Create distributor
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:      12,
				happyIngesters:    12,
				numDistributors:   1,
				replicationFactor: 3,
			})
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ds[0]))
			})

			// Push fixtures
			for _, series := range fixtures {
				req := mockWriteRequest(series.lbls, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			response, err := ds[0].LabelValuesForLabelName(ctx, testCase.from, testCase.to, labels.MetricName, testCase.matchers...)
			require.NoError(t, err)
			assert.ElementsMatch(t, response, testCase.expectedLabelValues)
		})
	}
}

func TestDistributor_LabelNamesAndValues(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "label_0", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "label_1", "status", "500", "reason", "broken"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "label_1"), 2, 200000},
	}
	expectedLabelValues := []*client.LabelValues{
		{
			LabelName: labels.MetricName,
			Values:    []string{"label_0", "label_1"},
		},
		{
			LabelName: "reason",
			Values:    []string{"broken"},
		},
		{
			LabelName: "status",
			Values:    []string{"200", "500"},
		},
	}
	tests := map[string]struct {
		zones              []string
		zonesResponseDelay map[string]time.Duration
	}{
		"should group values of labels by label name and return only distinct label values": {},
		"should return the results if zone awareness is enabled and only 2 zones return the results": {
			zones:              []string{"A", "B", "C"},
			zonesResponseDelay: map[string]time.Duration{"C": 10 * time.Second},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "label-names-values")

			// Create distributor
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:                       12,
				happyIngesters:                     12,
				numDistributors:                    1,
				replicationFactor:                  3,
				ingesterZones:                      testData.zones,
				labelNamesStreamZonesResponseDelay: testData.zonesResponseDelay,
			})
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ds[0]))
			})

			// Push fixtures
			for _, series := range fixtures {
				req := mockWriteRequest(series.lbls, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			// Assert on metric metadata
			timeBeforeExecution := time.Now()
			response, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{})
			require.NoError(t, err)
			if len(testData.zonesResponseDelay) > 0 {
				executionDuration := time.Since(timeBeforeExecution)
				require.Less(t, executionDuration, 5*time.Second, "Execution must be completed earlier than in 5 seconds")
			}
			require.Len(t, response.Items, len(expectedLabelValues))

			// sort label values to make stable assertion
			for _, item := range response.Items {
				slices.Sort(item.Values)
			}
			assert.ElementsMatch(t, response.Items, expectedLabelValues)
		})
	}
}

// This test asserts that distributor waits for all ingester responses to be completed even if ZoneAwareness is enabled.
// Also, it simulates delay from zone C to verify that there is no race condition. must be run with `-race` flag (race detection).
func TestDistributor_LabelValuesCardinality_ExpectedAllIngestersResponsesToBeCompleted(t *testing.T) {
	ctx, ds := prepareWithZoneAwarenessAndZoneDelay(t, createSeries(10000))

	names := []model.LabelName{labels.MetricName}
	response, err := ds[0].labelValuesCardinality(ctx, names, []*labels.Matcher{}, cardinality.InMemoryMethod)
	require.NoError(t, err)
	require.Len(t, response.Items, 1)
	// labelValuesCardinality must wait for all responses from all ingesters
	require.Len(t, response.Items[0].LabelValueSeries, 10000)
}

// This test asserts that distributor returns all possible label and values even if results from only two Zones are completed and ZoneAwareness is enabled.
// Also, it simulates delay from zone C to verify that there is no race condition. must be run with `-race` flag (race detection).
func TestDistributor_LabelNamesAndValues_ExpectedAllPossibleLabelNamesAndValuesToBeReturned(t *testing.T) {
	ctx, ds := prepareWithZoneAwarenessAndZoneDelay(t, createSeries(10000))
	response, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{})
	require.NoError(t, err)
	require.Len(t, response.Items, 1)
	require.Equal(t, 10000, len(response.Items[0].Values))
}

func prepareWithZoneAwarenessAndZoneDelay(t *testing.T, fixtures []series) (context.Context, []*Distributor) {
	ctx := user.InjectOrgID(context.Background(), "cardinality-user")

	// Create distributor
	ds, _, _ := prepare(t, prepConfig{
		numIngesters:      150,
		happyIngesters:    150,
		numDistributors:   1,
		replicationFactor: 3,
		ingesterZones:     []string{"ZONE-A", "ZONE-B", "ZONE-C"},
		labelNamesStreamZonesResponseDelay: map[string]time.Duration{
			// ingesters from zones A and B will respond in 1 second but ingesters from zone C will respond in 2 seconds.
			"ZONE-A": 1 * time.Second,
			"ZONE-B": 1 * time.Second,
			"ZONE-C": 2 * time.Second,
		},
	})
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ds[0]))
	})

	// Push fixtures
	for _, series := range fixtures {
		req := mockWriteRequest(series.lbls, series.value, series.timestamp)
		_, err := ds[0].Push(ctx, req)
		require.NoError(t, err)
	}
	return ctx, ds
}

type series struct {
	lbls      labels.Labels
	value     float64
	timestamp int64
}

func createSeries(count int) []series {
	fixtures := make([]series, 0, count)
	for i := 0; i < count; i++ {
		fixtures = append(fixtures, series{
			labels.FromStrings(labels.MetricName, "metric"+strconv.Itoa(i)), 1, int64(100000 + i),
		})
	}
	return fixtures
}

func TestDistributor_LabelValuesCardinality(t *testing.T) {
	const numIngesters = 3
	const replicationFactor = 3

	fixtures := []struct {
		labels    labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	tests := map[string]struct {
		labelNames                []model.LabelName
		matchers                  []*labels.Matcher
		ingestersSeriesCountTotal uint64
		expectedResult            *client.LabelValuesCardinalityResponse
		expectedIngesters         int
		happyIngesters            int
		expectedSeriesCountTotal  uint64
		ingesterZones             []string
	}{
		"should return an empty map if no label names": {
			labelNames:                []model.LabelName{},
			matchers:                  []*labels.Matcher{},
			ingestersSeriesCountTotal: 0,
			expectedResult:            &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
			expectedIngesters:         numIngesters,
			happyIngesters:            numIngesters,
			expectedSeriesCountTotal:  0,
		},
		"should return a map with the label values and series occurrences of a single label name": {
			labelNames:                []model.LabelName{labels.MetricName},
			matchers:                  []*labels.Matcher{},
			ingestersSeriesCountTotal: 100,
			expectedResult: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 2, "test_2": 1},
				}},
			},
			expectedIngesters:        numIngesters - 1,
			happyIngesters:           numIngesters,
			expectedSeriesCountTotal: 100,
			ingesterZones:            []string{"ZONE-A", "ZONE-B", "ZONE-C"},
		},
		"should return a map with the label values and series occurrences of a single label name, during single zone failure": {
			labelNames:                []model.LabelName{labels.MetricName},
			matchers:                  []*labels.Matcher{},
			ingestersSeriesCountTotal: 100,
			expectedResult: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 2, "test_2": 1},
				}},
			},
			expectedIngesters:        numIngesters - 1,
			happyIngesters:           numIngesters - 1,
			expectedSeriesCountTotal: 100,
			ingesterZones:            []string{"ZONE-A", "ZONE-B", "ZONE-C"},
		},
		"should return a map with the label values and series occurrences of all the label names": {
			labelNames:                []model.LabelName{labels.MetricName, "status"},
			matchers:                  []*labels.Matcher{},
			ingestersSeriesCountTotal: 100,
			expectedResult: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{
					{
						LabelName:        labels.MetricName,
						LabelValueSeries: map[string]uint64{"test_1": 2, "test_2": 1},
					},
					{
						LabelName:        "status",
						LabelValueSeries: map[string]uint64{"200": 1, "500": 1},
					},
				},
			},
			expectedIngesters:        numIngesters,
			happyIngesters:           numIngesters,
			expectedSeriesCountTotal: 100,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create distributor
			ds, ingesters, _ := prepare(t, prepConfig{
				numIngesters:              numIngesters,
				happyIngesters:            testData.happyIngesters,
				numDistributors:           1,
				replicationFactor:         replicationFactor,
				ingestersSeriesCountTotal: testData.ingestersSeriesCountTotal,
				ingesterZones:             testData.ingesterZones,
			})

			// Push fixtures
			ctx := user.InjectOrgID(context.Background(), "label-values-cardinality")

			for _, series := range fixtures {
				req := mockWriteRequest(series.labels, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			// Since the Push() response is sent as soon as the quorum is reached, when we reach this point
			// the final ingester may not have received series yet.
			// To avoid flaky test we retry the assertions until we hit the desired state within a reasonable timeout.
			test.Poll(t, time.Second, testData.expectedResult, func() interface{} {
				seriesCountTotal, cardinalityMap, err := ds[0].LabelValuesCardinality(ctx, testData.labelNames, testData.matchers, cardinality.InMemoryMethod)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSeriesCountTotal, seriesCountTotal)
				// Make sure the resultant label names are sorted
				sort.Slice(cardinalityMap.Items, func(l, r int) bool {
					return cardinalityMap.Items[l].LabelName < cardinalityMap.Items[r].LabelName
				})
				return cardinalityMap
			})

			// Make sure enough ingesters were queried
			assert.GreaterOrEqual(t, countMockIngestersCalls(ingesters, "LabelValuesCardinality"), testData.expectedIngesters)
		})
	}
}

func TestDistributor_LabelValuesCardinalityLimit(t *testing.T) {
	fixtures := []struct {
		labels    labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.FromStrings(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labels.FromStrings(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labels.FromStrings(labels.MetricName, "test_2"), 2, 200000},
	}

	tests := map[string]struct {
		labelNames              []model.LabelName
		maxLabelNamesPerRequest int
		expectedHTTPGrpcError   error
	}{
		"should return a httpgrpc error if the maximum number of label names per request is reached": {
			labelNames:              []model.LabelName{labels.MetricName, "status"},
			maxLabelNamesPerRequest: 1,
			expectedHTTPGrpcError: httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
				Code: int32(400),
				Body: []byte("label values cardinality request label names limit (limit: 1 actual: 2) exceeded"),
			}),
		},
		"should succeed if the maximum number of label names per request is not reached": {
			labelNames:              []model.LabelName{labels.MetricName},
			maxLabelNamesPerRequest: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create distributor
			limits := validation.Limits{}
			flagext.DefaultValues(&limits)
			limits.LabelValuesMaxCardinalityLabelNamesPerRequest = testData.maxLabelNamesPerRequest
			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          &limits,
			})

			// Push fixtures
			ctx := user.InjectOrgID(context.Background(), "label-values-cardinality")

			for _, series := range fixtures {
				req := mockWriteRequest(series.labels, series.value, series.timestamp)
				_, err := ds[0].Push(ctx, req)
				require.NoError(t, err)
			}

			_, _, err := ds[0].LabelValuesCardinality(ctx, testData.labelNames, []*labels.Matcher{}, cardinality.InMemoryMethod)
			if testData.expectedHTTPGrpcError == nil {
				require.NoError(t, err)
			} else {
				require.Equal(t, testData.expectedHTTPGrpcError, err)
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality_Concurrency(t *testing.T) {
	const numIngesters = 3

	t.Run("should fail with an error if at least one ingester's LabelValuesCardinality and/or UserStats operations fails", func(t *testing.T) {
		// Create distributor
		ds, ingesters, _ := prepare(t, prepConfig{
			numIngesters:    numIngesters,
			happyIngesters:  numIngesters,
			numDistributors: 1,
		})

		// Push fixtures
		ctx := user.InjectOrgID(context.Background(), "label-values-cardinality")

		// Set the first ingester as unhappy
		ingesters[0].happy = false

		_, _, err := ds[0].LabelValuesCardinality(ctx, []model.LabelName{labels.MetricName}, []*labels.Matcher{}, cardinality.InMemoryMethod)
		require.Error(t, err)
	})
}

func TestHaDedupeMiddleware(t *testing.T) {
	ctxWithUser := user.InjectOrgID(context.Background(), "user")
	const replica1 = "replicaA"
	const replica2 = "replicaB"
	const cluster1 = "clusterA"
	const cluster2 = "clusterB"
	replicasDidNotMatchDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH}
	tooManyClusterDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS}

	type testCase struct {
		name              string
		ctx               context.Context
		enableHaTracker   bool
		acceptHaSamples   bool
		reqs              []*mimirpb.WriteRequest
		expectedReqs      []*mimirpb.WriteRequest
		expectedNextCalls int
		expectErrs        []*status.Status
		expectDetails     []*mimirpb.WriteErrorDetails
	}
	testCases := []testCase{
		{
			name:              "no changes on empty request",
			ctx:               ctxWithUser,
			enableHaTracker:   true,
			acceptHaSamples:   true,
			reqs:              []*mimirpb.WriteRequest{{}},
			expectedReqs:      []*mimirpb.WriteRequest{{}},
			expectedNextCalls: 1,
			expectErrs:        []*status.Status{nil},
		}, {
			name:              "no changes if accept HA samples is false",
			ctx:               ctxWithUser,
			enableHaTracker:   true,
			acceptHaSamples:   false,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil)},
			expectedNextCalls: 1,
			expectErrs:        []*status.Status{nil},
		}, {
			name:              "remove replica label with HA tracker disabled",
			ctx:               ctxWithUser,
			enableHaTracker:   false,
			acceptHaSamples:   true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithCluster(cluster1), nil, nil)},
			expectedNextCalls: 1,
			expectErrs:        []*status.Status{nil},
		}, {
			name:              "do nothing without user in context, don't even call next",
			ctx:               context.Background(),
			enableHaTracker:   true,
			acceptHaSamples:   true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil)},
			expectedReqs:      nil,
			expectedNextCalls: 0,
			expectErrs:        []*status.Status{status.New(codes.Internal, "no org id")},
			expectDetails:     []*mimirpb.WriteErrorDetails{nil},
		}, {
			name:            "perform HA deduplication",
			ctx:             ctxWithUser,
			enableHaTracker: true,
			acceptHaSamples: true,
			reqs: []*mimirpb.WriteRequest{
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica2, cluster1), nil, nil),
			},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithCluster(cluster1), nil, nil)},
			expectedNextCalls: 1,
			expectErrs:        []*status.Status{nil, status.New(codes.AlreadyExists, newReplicasDidNotMatchError(replica2, replica1).Error())},
			expectDetails:     []*mimirpb.WriteErrorDetails{nil, replicasDidNotMatchDetails},
		}, {
			name:            "exceed max ha clusters limit",
			ctx:             ctxWithUser,
			enableHaTracker: true,
			acceptHaSamples: true,
			reqs: []*mimirpb.WriteRequest{
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica2, cluster1), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster2), nil, nil), // HaMaxClusters is set to 1.
				makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica2, cluster2), nil, nil),
			},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithCluster(cluster1), nil, nil)},
			expectedNextCalls: 1,
			expectErrs: []*status.Status{
				nil,
				status.New(codes.AlreadyExists, newReplicasDidNotMatchError(replica2, replica1).Error()),
				status.New(codes.FailedPrecondition, newTooManyClustersError(1).Error()),
				status.New(codes.FailedPrecondition, newTooManyClustersError(1).Error()),
			},
			expectDetails: []*mimirpb.WriteErrorDetails{nil, replicasDidNotMatchDetails, tooManyClusterDetails, tooManyClusterDetails},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupCallCount := 0
			cleanup := func() {
				cleanupCallCount++
			}

			nextCallCount := 0
			var gotReqs []*mimirpb.WriteRequest
			next := func(ctx context.Context, pushReq *Request) error {
				nextCallCount++
				req, err := pushReq.WriteRequest()
				require.NoError(t, err)
				gotReqs = append(gotReqs, req)
				pushReq.CleanUp()
				return nil
			}

			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.AcceptHASamples = tc.acceptHaSamples
			limits.MaxLabelValueLength = 15
			limits.HAMaxClusters = 1

			ds, _, _ := prepare(t, prepConfig{
				numDistributors: 1,
				limits:          &limits,
				enableTracker:   tc.enableHaTracker,
			})

			middleware := ds[0].prePushHaDedupeMiddleware(next)

			var gotErrs []error
			for _, req := range tc.reqs {
				pushReq := NewParsedRequest(req)
				pushReq.AddCleanup(cleanup)
				err := middleware(tc.ctx, pushReq)
				handledErr := err
				if handledErr != nil {
					handledErr = ds[0].handlePushError(tc.ctx, err)
				}
				gotErrs = append(gotErrs, handledErr)
			}

			assert.Equal(t, tc.expectedReqs, gotReqs)
			assert.Len(t, gotErrs, len(tc.expectErrs))
			for errIdx, expectErr := range tc.expectErrs {
				if expectErr == nil {
					assert.NoError(t, gotErrs[errIdx])
				} else {
					checkGRPCError(t, expectErr, tc.expectDetails[errIdx], gotErrs[errIdx])
				}
			}

			// Cleanup must have been called once per request.
			assert.Equal(t, len(tc.reqs), cleanupCallCount)
			assert.Equal(t, tc.expectedNextCalls, nextCallCount)
		})
	}
}

func TestInstanceLimitsBeforeHaDedupe(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	const replica1 = "replicaA"
	const replica2 = "replicaB"
	const cluster1 = "clusterA"

	writeReqReplica1 := makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica1, cluster1), nil, nil)
	writeReqReplica2 := makeWriteRequestForGenerators(5, labelSetGenWithReplicaAndCluster(replica2, cluster1), nil, nil)
	expectedWriteReq := makeWriteRequestForGenerators(5, labelSetGenWithCluster(cluster1), nil, nil)

	// Capture the submitted write requests which the middlewares pass into the mock push function.
	var submittedWriteReqs []*mimirpb.WriteRequest
	mockPush := func(ctx context.Context, pushReq *Request) error {
		defer pushReq.CleanUp()
		writeReq, err := pushReq.WriteRequest()
		require.NoError(t, err)
		submittedWriteReqs = append(submittedWriteReqs, writeReq)
		return nil
	}

	// Setup limits with HA enabled and forwarding rules for the metric "foo".
	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.AcceptHASamples = true
	limits.MaxLabelValueLength = 15

	// Prepare distributor and wrap the mock push function with its middlewares.
	ds, _, _ := prepare(t, prepConfig{
		numDistributors:     1,
		limits:              &limits,
		enableTracker:       true,
		maxInflightRequests: 1,
	})
	wrappedMockPush := ds[0].wrapPushWithMiddlewares(mockPush)

	// Make sure first request hits the limit.
	ds[0].inflightPushRequests.Inc()

	// If we HA deduplication runs before instance limits check,
	// then this would set replica for the cluster.
	err := wrappedMockPush(ctx, NewParsedRequest(writeReqReplica1))
	require.ErrorIs(t, err, errMaxInflightRequestsReached)

	// Simulate no other inflight request.
	ds[0].inflightPushRequests.Dec()

	// We now send request from second replica.
	// If HA deduplication middleware ran before instance limits check, then replica would be already set,
	// and HA deduplication would return 202 status code for this request instead.
	err = wrappedMockPush(ctx, NewParsedRequest(writeReqReplica2))
	require.NoError(t, err)

	// Check that the write requests which have been submitted to the push function look as expected,
	// there should only be one, and it shouldn't have the replica label.
	assert.Equal(t, []*mimirpb.WriteRequest{expectedWriteReq}, submittedWriteReqs)
}

func TestRelabelMiddleware(t *testing.T) {
	ctxWithUser := user.InjectOrgID(context.Background(), "user")

	type testCase struct {
		name           string
		ctx            context.Context
		relabelConfigs []*relabel.Config
		dropLabels     []string
		reqs           []*mimirpb.WriteRequest
		expectedReqs   []*mimirpb.WriteRequest
		expectErrs     []bool
	}
	testCases := []testCase{
		{
			name:           "do nothing",
			ctx:            ctxWithUser,
			relabelConfigs: nil,
			dropLabels:     nil,
			reqs:           []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectedReqs:   []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectErrs:     []bool{false},
		}, {
			name:           "no user in context",
			ctx:            context.Background(),
			relabelConfigs: nil,
			dropLabels:     nil,
			reqs:           []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectedReqs:   nil,
			expectErrs:     []bool{true},
		}, {
			name:           "apply a relabel rule",
			ctx:            ctxWithUser,
			relabelConfigs: nil,
			dropLabels:     []string{"label1", "label3"},
			reqs:           []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "label2", "value2", "label3", "value3"), nil, nil)},
			expectedReqs:   []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label2", "value2"), nil, nil)},
			expectErrs:     []bool{false},
		}, {
			name: "drop two out of three labels",
			ctx:  ctxWithUser,
			relabelConfigs: []*relabel.Config{
				{
					SourceLabels: []model.LabelName{"label1"},
					Action:       relabel.DefaultRelabelConfig.Action,
					Regex:        relabel.DefaultRelabelConfig.Regex,
					TargetLabel:  "target",
					Replacement:  "prefix_$1",
				},
			},
			reqs:         []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1"), nil, nil)},
			expectedReqs: []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "target", "prefix_value1"), nil, nil)},
			expectErrs:   []bool{false},
		}, {
			name:       "drop entire series if they have no labels",
			ctx:        ctxWithUser,
			dropLabels: []string{"__name__", "label2", "label3"},
			reqs: []*mimirpb.WriteRequest{
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1"), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric2", "label2", "value2"), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric3", "label3", "value3"), nil, nil),
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric4", "label4", "value4"), nil, nil),
			},
			expectedReqs: []*mimirpb.WriteRequest{
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "label1", "value1"), nil, nil),
				{Timeseries: []mimirpb.PreallocTimeseries{}},
				{Timeseries: []mimirpb.PreallocTimeseries{}},
				makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "label4", "value4"), nil, nil),
			},
			expectErrs: []bool{false, false, false, false},
		}, {
			name: metaLabelTenantID + " available and cleaned up afterwards",
			ctx:  ctxWithUser,
			relabelConfigs: []*relabel.Config{
				{
					SourceLabels: []model.LabelName{metaLabelTenantID},
					Action:       relabel.DefaultRelabelConfig.Action,
					Regex:        relabel.DefaultRelabelConfig.Regex,
					TargetLabel:  "tenant_id",
					Replacement:  "$1",
				},
			},
			reqs: []*mimirpb.WriteRequest{{
				Timeseries: []mimirpb.PreallocTimeseries{makeWriteRequestTimeseries(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "metric1"},
						{Name: "label1", Value: "value1"},
					},
					123,
					1.23,
				)},
			}},
			expectedReqs: []*mimirpb.WriteRequest{{
				Timeseries: []mimirpb.PreallocTimeseries{makeWriteRequestTimeseries(
					[]mimirpb.LabelAdapter{
						{Name: model.MetricNameLabel, Value: "metric1"},
						{Name: "label1", Value: "value1"},
						{Name: "tenant_id", Value: "user"},
					},
					123,
					1.23,
				)},
			}},
			expectErrs: []bool{false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupCallCount := 0
			cleanup := func() {
				cleanupCallCount++
			}

			var gotReqs []*mimirpb.WriteRequest
			next := func(ctx context.Context, pushReq *Request) error {
				req, err := pushReq.WriteRequest()
				require.NoError(t, err)
				gotReqs = append(gotReqs, req)
				pushReq.CleanUp()
				return nil
			}

			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.MetricRelabelConfigs = tc.relabelConfigs
			limits.DropLabels = tc.dropLabels
			ds, _, _ := prepare(t, prepConfig{
				numDistributors: 1,
				limits:          &limits,
			})
			middleware := ds[0].prePushRelabelMiddleware(next)

			var gotErrs []bool
			for _, req := range tc.reqs {
				pushReq := NewParsedRequest(req)
				pushReq.AddCleanup(cleanup)
				err := middleware(tc.ctx, pushReq)
				gotErrs = append(gotErrs, err != nil)
			}

			assert.Equal(t, tc.expectedReqs, gotReqs)
			assert.Equal(t, tc.expectErrs, gotErrs)

			// Cleanup must have been called once per request.
			assert.Equal(t, len(tc.reqs), cleanupCallCount)
		})
	}
}

func mustNewMatcher(t labels.MatchType, n, v string) *labels.Matcher {
	m, err := labels.NewMatcher(t, n, v)
	if err != nil {
		panic(err)
	}

	return m
}

func mockWriteRequest(lbls labels.Labels, value float64, timestampMs int64) *mimirpb.WriteRequest {
	samples := []mimirpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	return mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)}, samples, nil, nil, mimirpb.API)
}

type prepConfig struct {
	numIngesters, happyIngesters       int
	queryDelay                         time.Duration
	pushDelay                          time.Duration
	shuffleShardSize                   int
	limits                             *validation.Limits
	numDistributors                    int
	skipLabelNameValidation            bool
	maxInflightRequests                int
	maxInflightRequestsBytes           int
	maxIngestionRate                   float64
	replicationFactor                  int
	enableTracker                      bool
	ingestersSeriesCountTotal          uint64
	ingesterZones                      []string
	labelNamesStreamZonesResponseDelay map[string]time.Duration
	preferStreamingChunks              bool
	minimizeIngesterRequests           bool

	timeOut bool
}

func prepare(t *testing.T, cfg prepConfig) ([]*Distributor, []mockIngester, []*prometheus.Registry) {
	ingesters := []mockIngester{}
	for i := 0; i < cfg.happyIngesters; i++ {
		zone := ""
		if len(cfg.ingesterZones) > 0 {
			zone = cfg.ingesterZones[i%len(cfg.ingesterZones)]
		}
		var labelNamesStreamResponseDelay time.Duration
		if len(cfg.labelNamesStreamZonesResponseDelay) > 0 {
			labelNamesStreamResponseDelay = cfg.labelNamesStreamZonesResponseDelay[zone]
		}
		ingesters = append(ingesters, mockIngester{
			happy:                         true,
			queryDelay:                    cfg.queryDelay,
			pushDelay:                     cfg.pushDelay,
			seriesCountTotal:              cfg.ingestersSeriesCountTotal,
			zone:                          zone,
			labelNamesStreamResponseDelay: labelNamesStreamResponseDelay,
			timeOut:                       cfg.timeOut,
		})
	}
	for i := cfg.happyIngesters; i < cfg.numIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			queryDelay:       cfg.queryDelay,
			pushDelay:        cfg.pushDelay,
			seriesCountTotal: cfg.ingestersSeriesCountTotal,
		})
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	ingesterDescs := map[string]ring.InstanceDesc{}
	ingestersByAddr := map[string]*mockIngester{}
	for i := range ingesters {
		addr := fmt.Sprintf("%d", i)
		tokens := []uint32{uint32((math.MaxUint32 / cfg.numIngesters) * i)}
		ingesterDescs[addr] = ring.InstanceDesc{
			Addr:                addr,
			Zone:                ingesters[i].zone,
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              tokens,
		}
		ingestersByAddr[addr] = &ingesters[i]
		ingesters[i].tokens = tokens
	}

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	err := kvStore.CAS(context.Background(), ingester.IngesterRingKey,
		func(_ interface{}) (interface{}, bool, error) {
			return &ring.Desc{
				Ingesters: ingesterDescs,
			}, true, nil
		},
	)
	require.NoError(t, err)

	// Use a default replication factor of 3 if there isn't a provided replication factor.
	rf := cfg.replicationFactor
	if rf == 0 {
		rf = 3
	}

	ingestersRing, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvStore,
		},
		HeartbeatTimeout:     60 * time.Minute,
		ReplicationFactor:    rf,
		ZoneAwarenessEnabled: len(cfg.ingesterZones) > 0,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingestersRing))

	test.Poll(t, time.Second, cfg.numIngesters, func() interface{} {
		return ingestersRing.InstancesCount()
	})

	factory := ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
		return ingestersByAddr[inst.Addr], nil
	})

	distributors := make([]*Distributor, 0, cfg.numDistributors)
	registries := make([]*prometheus.Registry, 0, cfg.numDistributors)
	for i := 0; i < cfg.numDistributors; i++ {
		if cfg.limits == nil {
			cfg.limits = &validation.Limits{}
			flagext.DefaultValues(cfg.limits)
		}

		var distributorCfg Config
		var clientConfig client.Config
		flagext.DefaultValues(&distributorCfg, &clientConfig)

		distributorCfg.IngesterClientFactory = factory
		distributorCfg.DistributorRing.Common.HeartbeatPeriod = 100 * time.Millisecond
		distributorCfg.DistributorRing.Common.InstanceID = strconv.Itoa(i)
		distributorCfg.DistributorRing.Common.KVStore.Mock = kvStore
		distributorCfg.DistributorRing.Common.InstanceAddr = "127.0.0.1"
		distributorCfg.SkipLabelNameValidation = cfg.skipLabelNameValidation
		distributorCfg.DefaultLimits.MaxInflightPushRequests = cfg.maxInflightRequests
		distributorCfg.DefaultLimits.MaxInflightPushRequestsBytes = cfg.maxInflightRequestsBytes
		distributorCfg.DefaultLimits.MaxIngestionRate = cfg.maxIngestionRate
		distributorCfg.ShuffleShardingLookbackPeriod = time.Hour
		distributorCfg.PreferStreamingChunksFromIngesters = cfg.preferStreamingChunks
		distributorCfg.StreamingChunksPerIngesterSeriesBufferSize = 128
		distributorCfg.MinimizeIngesterRequests = cfg.minimizeIngesterRequests

		cfg.limits.IngestionTenantShardSize = cfg.shuffleShardSize

		if cfg.enableTracker {
			codec := GetReplicaDescCodec()
			ringStore, closer := consul.NewInMemoryClient(codec, log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })
			mock := kv.PrefixClient(ringStore, "prefix")
			distributorCfg.HATrackerConfig = HATrackerConfig{
				EnableHATracker: true,
				KVStore:         kv.Config{Mock: mock},
				UpdateTimeout:   100 * time.Millisecond,
				FailoverTimeout: time.Second,
			}
			if cfg.limits.HAMaxClusters == 0 {
				cfg.limits.HAMaxClusters = 100
			}
		}

		overrides, err := validation.NewOverrides(*cfg.limits, nil)
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		d, err := New(distributorCfg, clientConfig, overrides, nil, ingestersRing, true, reg, log.NewNopLogger())
		require.NoError(t, err)

		require.NoError(t, services.StartAndAwaitRunning(context.Background(), d))

		distributors = append(distributors, d)
		registries = append(registries, reg)
	}

	// If the distributors ring is setup, wait until the first distributor
	// updates to the expected size
	if distributors[0].distributorsRing != nil {
		test.Poll(t, time.Second, cfg.numDistributors, func() interface{} {
			return distributors[0].HealthyInstancesCount()
		})
	}

	t.Cleanup(func() { stopAll(distributors, ingestersRing) })

	return distributors, ingesters, registries
}

func stopAll(ds []*Distributor, r *ring.Ring) {
	for _, d := range ds {
		services.StopAndAwaitTerminated(context.Background(), d) //nolint:errcheck
	}

	// Mock consul doesn't stop quickly, so don't wait.
	r.StopAsync()
}

func makeWriteRequest(startTimestampMs int64, samples, metadata int, exemplars, histograms bool, metrics ...string) *mimirpb.WriteRequest {
	request := &mimirpb.WriteRequest{}

	if len(metrics) == 0 {
		metrics = []string{"foo"}
	}

	for _, metric := range metrics {
		for i := 0; i < samples; i++ {
			req := makeWriteRequestTimeseries(
				[]mimirpb.LabelAdapter{
					{Name: model.MetricNameLabel, Value: metric},
					{Name: "bar", Value: "baz"},
					{Name: "sample", Value: fmt.Sprintf("%d", i)},
				},
				startTimestampMs+int64(i),
				float64(i),
			)

			if exemplars {
				req.Exemplars = makeWriteRequestExamplars(
					[]mimirpb.LabelAdapter{
						{Name: "traceID", Value: "123456"},
						{Name: "foo", Value: "bar"},
						{Name: "exemplar", Value: fmt.Sprintf("%d", i)},
					},
					startTimestampMs+int64(i),
					float64(i),
				)
			}

			if histograms {
				if i%2 == 0 {
					req.Histograms = makeWriteRequestHistograms(startTimestampMs+int64(i), generateTestHistogram(i))
				} else {
					req.Histograms = makeWriteRequestFloatHistograms(startTimestampMs+int64(i), generateTestFloatHistogram(i))
				}
			}

			request.Timeseries = append(request.Timeseries, req)
		}

		for i := 0; i < metadata; i++ {
			m := &mimirpb.MetricMetadata{
				MetricFamilyName: fmt.Sprintf("metric_%d", i),
				Type:             mimirpb.COUNTER,
				Help:             fmt.Sprintf("a help for metric_%d", i),
			}
			request.Metadata = append(request.Metadata, m)
		}
	}

	return request
}

func makeWriteRequestTimeseries(labels []mimirpb.LabelAdapter, ts int64, value float64) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: labels,
			Samples: []mimirpb.Sample{
				{
					Value:       value,
					TimestampMs: ts,
				},
			},
		},
	}
}

func makeWriteRequestExamplars(labels []mimirpb.LabelAdapter, ts int64, value float64) []mimirpb.Exemplar {
	return []mimirpb.Exemplar{{
		Labels:      labels,
		Value:       value,
		TimestampMs: ts,
	}}
}

func makeWriteRequestHistograms(ts int64, histogram *histogram.Histogram) []mimirpb.Histogram {
	return []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(ts, histogram)}
}

func makeWriteRequestFloatHistograms(ts int64, histogram *histogram.FloatHistogram) []mimirpb.Histogram {
	return []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(ts, histogram)}
}

// labelSetGenWithReplicaAndCluster returns generator for a label set with the given replica and cluster,
// it can be used with the helper makeWriteRequestForLabelSetGen().
func labelSetGenWithReplicaAndCluster(replica, cluster string) func(int) []mimirpb.LabelAdapter {
	return func(id int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{
			{Name: "__name__", Value: "foo"},
			{Name: "__replica__", Value: replica},
			{Name: "bar", Value: "baz"},
			{Name: "cluster", Value: cluster},
			{Name: "sample", Value: fmt.Sprintf("%d", id)},
		}
	}
}

// labelSetGenWithCluster returns generator for a label set with given cluster but no replica,
// it can be used with the helper makeWriteRequestForLabelSetGen().
func labelSetGenWithCluster(cluster string) func(int) []mimirpb.LabelAdapter {
	return func(id int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{
			{Name: "__name__", Value: "foo"},
			{Name: "bar", Value: "baz"},
			{Name: "cluster", Value: cluster},
			{Name: "sample", Value: fmt.Sprintf("%d", id)},
		}
	}
}

// labelSetGenForStringPairs takes a slice of strings which are interpreted as pairs of label names and values,
// it then returns a label set generator which generates labelsets of the given label names and values.
func labelSetGenForStringPairs(tb testing.TB, namesValues ...string) func(id int) []mimirpb.LabelAdapter {
	return func(id int) []mimirpb.LabelAdapter {
		if len(namesValues)%2 != 0 {
			tb.Fatalf("number of names and values must be even: %q", namesValues)
		}

		labels := make([]mimirpb.LabelAdapter, 0, len(namesValues)/2)
		for idx := 0; idx < len(namesValues)/2; idx++ {
			labels = append(labels, mimirpb.LabelAdapter{
				Name:  namesValues[2*idx],
				Value: namesValues[2*idx+1] + strconv.Itoa(id),
			})
		}

		return labels
	}
}

type labelSetGen func(int) []mimirpb.LabelAdapter
type metaDataGen func(int, string) *mimirpb.MetricMetadata

// makeWriteRequestForGenerators generates a write request based on the given generator functions.
// The label set generator gets called once for each sample, the returned label set gets added to the request with the sample.
// The exemplar label set generator also gets called once for each sample, if it isn't nil.
// The metadata generator gets called once for each unique metric name that has been returned by the label generator, if it isn't nil.
func makeWriteRequestForGenerators(series int, lsg labelSetGen, elsg labelSetGen, mdg metaDataGen) *mimirpb.WriteRequest {
	metricNames := make(map[string]struct{})

	request := &mimirpb.WriteRequest{}
	for i := 0; i < series; i++ {
		labelSet := lsg(i)
		metricName := mimirpb.FromLabelAdaptersToLabels(labelSet).Get(model.MetricNameLabel)
		if metricName != "" {
			metricNames[metricName] = struct{}{}
		}

		ts := mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: labelSet,
			},
		}

		ts.Samples = []mimirpb.Sample{{
			Value:       float64(100 + i),
			TimestampMs: int64(100 + i),
		}}

		if elsg != nil {
			ts.Exemplars = []mimirpb.Exemplar{{
				Labels:      elsg(100 + i),
				Value:       float64(100 + i),
				TimestampMs: int64(100 + i),
			}}
		}
		if i%2 == 0 {
			ts.Histograms = makeWriteRequestHistograms(int64(100+i), generateTestHistogram(i))
		} else {
			ts.Histograms = makeWriteRequestFloatHistograms(int64(100+i), generateTestFloatHistogram(i))
		}
		request.Timeseries = append(request.Timeseries, ts)

	}

	if mdg != nil {
		metricIdx := 0
		for metricName := range metricNames {
			request.Metadata = append(request.Metadata, mdg(metricIdx, metricName))
			metricIdx++
		}
	}

	return request
}

func makeWriteRequestExemplar(seriesLabels []string, timestamp int64, exemplarLabels []string) *mimirpb.WriteRequest {
	return &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeExemplarTimeseries(seriesLabels, timestamp, exemplarLabels),
		},
	}
}

func makeWriteRequestHistogram(seriesLabels []string, timestamp int64, histogram *histogram.Histogram) *mimirpb.WriteRequest {
	return &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeHistogramTimeseries(seriesLabels, timestamp, histogram),
		},
	}
}

func makeWriteRequestFloatHistogram(seriesLabels []string, timestamp int64, histogram *histogram.FloatHistogram) *mimirpb.WriteRequest {
	return &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeFloatHistogramTimeseries(seriesLabels, timestamp, histogram),
		},
	}
}

func makeExemplarTimeseries(seriesLabels []string, timestamp int64, exemplarLabels []string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Exemplars: []mimirpb.Exemplar{
				{
					Labels:      mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(exemplarLabels...)),
					TimestampMs: timestamp,
				},
			},
		},
	}
}

func makeHistogramTimeseries(seriesLabels []string, timestamp int64, histogram *histogram.Histogram) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Histograms: makeWriteRequestHistograms(timestamp, histogram),
		},
	}
}

func makeFloatHistogramTimeseries(seriesLabels []string, timestamp int64, histogram *histogram.FloatHistogram) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Histograms: makeWriteRequestFloatHistograms(timestamp, histogram),
		},
	}
}

func expectedResponse(start, end int, histograms bool) model.Matrix {
	// TODO(histograms): should we modify the tests so it doesn't return both float and histogram for the same timestamp? (but still test sending float alone, histogram alone, and mixed) but might not be worth fixing the mock ingester
	result := model.Matrix{}
	for i := start; i < end; i++ {
		ss := &model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: "foo",
				"bar":                 "baz",
				"sample":              model.LabelValue(fmt.Sprintf("%d", i)),
			},
			Values: []model.SamplePair{
				{
					Value:     model.SampleValue(i),
					Timestamp: model.Time(i),
				},
			},
		}
		if histograms {
			ss.Histograms = []model.SampleHistogramPair{
				{
					Histogram: util_test.GenerateTestSampleHistogram(i),
					Timestamp: model.Time(i),
				},
			}
		}
		result = append(result, ss)
	}
	return result
}

func mustEqualMatcher(k, v string) *labels.Matcher {
	m, err := labels.NewMatcher(labels.MatchEqual, k, v)
	if err != nil {
		panic(err)
	}
	return m
}

type mockIngester struct {
	sync.Mutex
	client.IngesterClient
	grpc_health_v1.HealthClient
	happy                         bool
	stats                         client.UsersStatsResponse
	timeseries                    map[uint32]*mimirpb.PreallocTimeseries
	metadata                      map[uint32]map[mimirpb.MetricMetadata]struct{}
	queryDelay                    time.Duration
	pushDelay                     time.Duration
	calls                         map[string]int
	seriesCountTotal              uint64
	zone                          string
	labelNamesStreamResponseDelay time.Duration
	timeOut                       bool
	tokens                        []uint32
}

func (i *mockIngester) series() map[uint32]*mimirpb.PreallocTimeseries {
	i.Lock()
	defer i.Unlock()

	result := map[uint32]*mimirpb.PreallocTimeseries{}
	for k, v := range i.timeseries {
		result[k] = v
	}
	return result
}

func (i *mockIngester) Check(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("Check")

	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func (i *mockIngester) Close() error {
	return nil
}

func (i *mockIngester) Push(ctx context.Context, req *mimirpb.WriteRequest, _ ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	time.Sleep(i.pushDelay)

	i.Lock()
	defer i.Unlock()

	i.trackCall("Push")

	if !i.happy {
		return nil, errFail
	}

	if i.timeOut {
		return nil, context.DeadlineExceeded
	}

	if len(req.Timeseries) > 0 && i.timeseries == nil {
		i.timeseries = map[uint32]*mimirpb.PreallocTimeseries{}
	}

	if i.metadata == nil {
		i.metadata = map[uint32]map[mimirpb.MetricMetadata]struct{}{}
	}

	orgid, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	for _, series := range req.Timeseries {
		hash := shardByAllLabels(orgid, series.Labels)
		existing, ok := i.timeseries[hash]
		if !ok {
			// Make a copy because the request Timeseries are reused
			item := mimirpb.TimeSeries{
				Labels:     make([]mimirpb.LabelAdapter, len(series.TimeSeries.Labels)),
				Samples:    make([]mimirpb.Sample, len(series.TimeSeries.Samples)),
				Histograms: make([]mimirpb.Histogram, len(series.TimeSeries.Histograms)),
			}

			copy(item.Labels, series.TimeSeries.Labels)
			copy(item.Samples, series.TimeSeries.Samples)
			copy(item.Histograms, series.TimeSeries.Histograms)

			i.timeseries[hash] = &mimirpb.PreallocTimeseries{TimeSeries: &item}
		} else {
			existing.Samples = append(existing.Samples, series.Samples...)
			existing.Histograms = append(existing.Histograms, series.Histograms...)
		}
	}

	for _, m := range req.Metadata {
		hash := shardByMetricName(orgid, m.MetricFamilyName)
		set, ok := i.metadata[hash]
		if !ok {
			set = map[mimirpb.MetricMetadata]struct{}{}
			i.metadata[hash] = set
		}
		set[*m] = struct{}{}
	}

	return &mimirpb.WriteResponse{}, nil
}

func makeWireChunk(c chunk.EncodedChunk) client.Chunk {
	var buf bytes.Buffer
	chunk := client.Chunk{
		Encoding: int32(c.Encoding()),
	}
	if err := c.Marshal(&buf); err != nil {
		panic(err)
	}
	chunk.Data = buf.Bytes()
	return chunk
}

func (i *mockIngester) QueryStream(_ context.Context, req *client.QueryRequest, _ ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
	time.Sleep(i.queryDelay)

	i.Lock()
	defer i.Unlock()

	i.trackCall("QueryStream")

	if !i.happy {
		return nil, errFail
	}

	_, _, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	nonStreamingResponses := []*client.QueryStreamResponse{}
	streamingLabelResponses := []*client.QueryStreamResponse{}
	streamingChunkResponses := []*client.QueryStreamResponse{}

	series := make([]*mimirpb.PreallocTimeseries, 0, len(i.timeseries))

	for _, ts := range i.timeseries {
		if !match(ts.Labels, matchers) {
			continue
		}

		series = append(series, ts)
	}

	slices.SortFunc(series, func(a, b *mimirpb.PreallocTimeseries) int {
		return labels.Compare(mimirpb.FromLabelAdaptersToLabels(a.Labels), mimirpb.FromLabelAdaptersToLabels(b.Labels))
	})

	for seriesIndex, ts := range series {
		c, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
		if err != nil {
			return nil, err
		}

		hc, err := chunk.NewForEncoding(chunk.PrometheusHistogramChunk)
		if err != nil {
			return nil, err
		}

		fhc, err := chunk.NewForEncoding(chunk.PrometheusFloatHistogramChunk)
		if err != nil {
			return nil, err
		}

		chunks := []chunk.EncodedChunk{c}
		for _, sample := range ts.Samples {
			newChunk, err := c.Add(model.SamplePair{
				Timestamp: model.Time(sample.TimestampMs),
				Value:     model.SampleValue(sample.Value),
			})
			if err != nil {
				panic(err)
			}
			if newChunk != nil {
				c = newChunk
				chunks = append(chunks, newChunk)
			}
		}

		hexists := false
		fhexists := false
		hchunks := []chunk.EncodedChunk{hc}
		fhchunks := []chunk.EncodedChunk{fhc}
		for _, h := range ts.Histograms {
			if h.IsFloatHistogram() {
				fhexists = true
				newChunk, err := fhc.AddFloatHistogram(h.Timestamp, mimirpb.FromFloatHistogramProtoToFloatHistogram(&h))
				if err != nil {
					panic(err)
				}
				if newChunk != nil {
					fhc = newChunk
					fhchunks = append(fhchunks, newChunk)
				}
			} else {
				hexists = true
				newChunk, err := hc.AddHistogram(h.Timestamp, mimirpb.FromHistogramProtoToHistogram(&h))
				if err != nil {
					panic(err)
				}
				if newChunk != nil {
					hc = newChunk
					hchunks = append(hchunks, newChunk)
				}
			}
		}

		wireChunks := []client.Chunk{}
		for _, c := range chunks {
			wireChunks = append(wireChunks, makeWireChunk(c))
		}
		if hexists {
			for _, c := range hchunks {
				// panic("got hist")
				wireChunks = append(wireChunks, makeWireChunk(c))
			}
		}
		if fhexists {
			for _, c := range fhchunks {
				// panic("got fhist")
				wireChunks = append(wireChunks, makeWireChunk(c))
			}
		}

		if req.StreamingChunksBatchSize > 0 {
			streamingLabelResponses = append(streamingLabelResponses, &client.QueryStreamResponse{
				StreamingSeries: []client.QueryStreamSeries{
					{
						Labels:     ts.Labels,
						ChunkCount: int64(len(wireChunks)),
					},
				},
			})

			streamingChunkResponses = append(streamingChunkResponses, &client.QueryStreamResponse{
				StreamingSeriesChunks: []client.QueryStreamSeriesChunks{
					{
						SeriesIndex: uint64(seriesIndex),
						Chunks:      wireChunks,
					},
				},
			})
		} else {
			nonStreamingResponses = append(nonStreamingResponses, &client.QueryStreamResponse{
				Chunkseries: []client.TimeSeriesChunk{
					{
						Labels: ts.Labels,
						Chunks: wireChunks,
					},
				},
			})
		}
	}

	var results []*client.QueryStreamResponse

	if req.StreamingChunksBatchSize > 0 {
		endOfLabelsMessage := &client.QueryStreamResponse{
			IsEndOfSeriesStream: true,
		}
		results = append(streamingLabelResponses, endOfLabelsMessage)
		results = append(results, streamingChunkResponses...)
	} else {
		results = nonStreamingResponses
	}

	return &stream{
		results: results,
	}, nil
}

func (i *mockIngester) MetricsForLabelMatchers(_ context.Context, req *client.MetricsForLabelMatchersRequest, _ ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("MetricsForLabelMatchers")

	if !i.happy {
		return nil, errFail
	}

	multiMatchers, err := client.FromMetricsForLabelMatchersRequest(req)
	if err != nil {
		return nil, err
	}

	response := client.MetricsForLabelMatchersResponse{}
	for _, matchers := range multiMatchers {
		for _, ts := range i.timeseries {
			if match(ts.Labels, matchers) {
				response.Metric = append(response.Metric, &mimirpb.Metric{Labels: ts.Labels})
			}
		}
	}
	return &response, nil
}

func (i *mockIngester) LabelValues(_ context.Context, req *client.LabelValuesRequest, _ ...grpc.CallOption) (*client.LabelValuesResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("LabelValues")

	if !i.happy {
		return nil, errFail
	}

	labelName, from, to, matchers, err := client.FromLabelValuesRequest(req)
	if err != nil {
		return nil, err
	}

	response := []string{}
	for _, ts := range i.timeseries {
		if !match(ts.Labels, matchers) {
			continue
		}

		sampleInTimeRange := false

		for _, s := range ts.Samples {
			if s.TimestampMs >= from && s.TimestampMs < to {
				sampleInTimeRange = true
				break
			}
		}

		if !sampleInTimeRange {
			continue
		}

		for _, lbl := range ts.Labels {
			if lbl.Name == labelName {
				response = append(response, lbl.Value)
			}
		}

	}

	slices.Sort(response)

	return &client.LabelValuesResponse{LabelValues: response}, nil
}

func (i *mockIngester) LabelNames(_ context.Context, req *client.LabelNamesRequest, _ ...grpc.CallOption) (*client.LabelNamesResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("LabelNames")

	if !i.happy {
		return nil, errFail
	}

	_, _, matchers, err := client.FromLabelNamesRequest(req)
	if err != nil {
		return nil, err
	}

	response := client.LabelNamesResponse{}
	for _, ts := range i.timeseries {
		if match(ts.Labels, matchers) {
			for _, lbl := range ts.Labels {
				response.LabelNames = append(response.LabelNames, lbl.Name)
			}
		}
	}
	slices.Sort(response.LabelNames)

	return &response, nil
}

func (i *mockIngester) MetricsMetadata(context.Context, *client.MetricsMetadataRequest, ...grpc.CallOption) (*client.MetricsMetadataResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("MetricsMetadata")

	if !i.happy {
		return nil, errFail
	}

	resp := &client.MetricsMetadataResponse{}
	for _, sets := range i.metadata {
		for m := range sets {
			resp.Metadata = append(resp.Metadata, &m)
		}
	}

	return resp, nil
}

func (i *mockIngester) LabelNamesAndValues(_ context.Context, _ *client.LabelNamesAndValuesRequest, _ ...grpc.CallOption) (client.Ingester_LabelNamesAndValuesClient, error) {
	i.Lock()
	defer i.Unlock()
	results := map[string]map[string]struct{}{}
	for _, ts := range i.timeseries {
		for _, lbl := range ts.Labels {
			labelValues, exists := results[lbl.Name]
			if !exists {
				labelValues = map[string]struct{}{}
			}
			labelValues[lbl.Value] = struct{}{}
			results[lbl.Name] = labelValues
		}
	}
	var items []*client.LabelValues
	for labelName, labelValues := range results {
		var values []string
		for val := range labelValues {
			values = append(values, val)
		}
		items = append(items, &client.LabelValues{LabelName: labelName, Values: values})
	}
	resp := &client.LabelNamesAndValuesResponse{Items: items}
	return &labelNamesAndValuesMockStream{responses: []*client.LabelNamesAndValuesResponse{resp}, responseDelay: i.labelNamesStreamResponseDelay}, nil
}

type labelNamesAndValuesMockStream struct {
	grpc.ClientStream
	responses     []*client.LabelNamesAndValuesResponse
	i             int
	responseDelay time.Duration
}

func (*labelNamesAndValuesMockStream) CloseSend() error {
	return nil
}

func (s *labelNamesAndValuesMockStream) Recv() (*client.LabelNamesAndValuesResponse, error) {
	time.Sleep(s.responseDelay)
	if s.i >= len(s.responses) {
		return nil, io.EOF
	}
	result := s.responses[s.i]
	s.i++
	return result, nil
}

func (i *mockIngester) LabelValuesCardinality(_ context.Context, req *client.LabelValuesCardinalityRequest, _ ...grpc.CallOption) (client.Ingester_LabelValuesCardinalityClient, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("LabelValuesCardinality")

	if !i.happy {
		return nil, errFail
	}

	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return nil, err
	}

	labelValuesSeriesCount := map[string]map[string]uint64{}
	for _, ts := range i.timeseries {
		if !match(ts.Labels, matchers) {
			continue
		}
		for _, reqLabelName := range req.LabelNames {
			for _, lbl := range ts.Labels {
				if reqLabelName != lbl.Name {
					continue
				}
				if _, exists := labelValuesSeriesCount[lbl.Name]; !exists {
					labelValuesSeriesCount[lbl.Name] = map[string]uint64{lbl.Value: 1}
				} else {
					labelValuesSeriesCount[lbl.Name][lbl.Value]++
				}
			}
		}
	}

	result := &client.LabelValuesCardinalityResponse{}
	for labelName, labelValuesSeriesCount := range labelValuesSeriesCount {
		labelValueSeriesMap := map[string]uint64{}
		for labelValue, seriesCount := range labelValuesSeriesCount {
			labelValueSeriesMap[labelValue] = seriesCount
		}
		lblCardinality := &client.LabelValueSeriesCount{
			LabelName:        labelName,
			LabelValueSeries: labelValueSeriesMap,
		}

		result.Items = append(result.Items, lblCardinality)
	}

	return &labelValuesCardinalityStream{results: []*client.LabelValuesCardinalityResponse{result}}, nil
}

type labelValuesCardinalityStream struct {
	grpc.ClientStream
	i       int
	results []*client.LabelValuesCardinalityResponse
}

func (*labelValuesCardinalityStream) CloseSend() error {
	return nil
}

func (s *labelValuesCardinalityStream) Recv() (*client.LabelValuesCardinalityResponse, error) {
	if s.i >= len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.i]
	s.i++
	return result, nil
}

func (i *mockIngester) trackCall(name string) {
	if i.calls == nil {
		i.calls = map[string]int{}
	}

	i.calls[name]++
}

func (i *mockIngester) countCalls(name string) int {
	i.Lock()
	defer i.Unlock()

	return i.calls[name]
}

// noopIngester is a mocked ingester which does nothing.
type noopIngester struct {
	client.IngesterClient
	grpc_health_v1.HealthClient
}

func (i *noopIngester) Close() error {
	return nil
}

func (i *noopIngester) Push(context.Context, *mimirpb.WriteRequest, ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	return nil, nil
}

type stream struct {
	grpc.ClientStream
	i       int
	results []*client.QueryStreamResponse
}

func (*stream) CloseSend() error {
	return nil
}

func (s *stream) Recv() (*client.QueryStreamResponse, error) {
	if s.i >= len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.i]
	s.i++
	return result, nil
}

func (s *stream) Context() context.Context {
	return context.Background()
}

func (i *mockIngester) AllUserStats(context.Context, *client.UserStatsRequest, ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}

func (i *mockIngester) UserStats(context.Context, *client.UserStatsRequest, ...grpc.CallOption) (*client.UserStatsResponse, error) {
	if !i.happy {
		return nil, errFail
	}

	return &client.UserStatsResponse{
		IngestionRate:     0,
		NumSeries:         i.seriesCountTotal,
		ApiIngestionRate:  0,
		RuleIngestionRate: 0,
	}, nil
}

func match(labels []mimirpb.LabelAdapter, matchers []*labels.Matcher) bool {
outer:
	for _, matcher := range matchers {
		for _, labels := range labels {
			if matcher.Name == labels.Name && matcher.Matches(labels.Value) {
				continue outer
			}
		}
		return false
	}
	return true
}

func TestDistributorValidation(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "1")
	now := model.Now()
	future, past := now.Add(5*time.Hour), now.Add(-25*time.Hour)
	expectedDetails := &mimirpb.WriteErrorDetails{Cause: mimirpb.BAD_DATA}

	for name, tc := range map[string]struct {
		metadata    []*mimirpb.MetricMetadata
		labels      [][]mimirpb.LabelAdapter
		samples     []mimirpb.Sample
		exemplars   []*mimirpb.Exemplar
		expectedErr *status.Status
	}{
		"validation passes": {
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "testmetric", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			exemplars: []*mimirpb.Exemplar{{
				Labels:      []mimirpb.LabelAdapter{{Name: "traceID", Value: "123abc"}},
				TimestampMs: int64(now),
				Value:       1,
			}},
		},

		"validation passes when labels are unsorted": {
			labels: [][]mimirpb.LabelAdapter{
				{
					{Name: "foo", Value: "bar"},
					{Name: labels.MetricName, Value: "testmetric"},
				}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
		},

		"validation fails for samples from the future": {
			labels: [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(future),
				Value:       4,
			}},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(sampleTimestampTooNewMsgFormat, future, "testmetric")),
		},

		"exceeds maximum labels per series": {
			labels: [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{foo2="bar2", foo="bar"}`, "")),
		},
		"exceeds maximum labels per series with a metric that exceeds 200 characters when formatted": {
			labels: [][]mimirpb.LabelAdapter{{
				{Name: labels.MetricName, Value: "testmetric"},
				{Name: "foo-with-a-long-long-label", Value: "bar-with-a-long-long-value"},
				{Name: "foo2-with-a-long-long-label", Value: "bar2-with-a-long-long-value"},
				{Name: "foo3-with-a-long-long-label", Value: "bar3-with-a-long-long-value"},
				{Name: "foo4-with-a-long-long-label", Value: "bar4-with-a-long-long-value"},
			}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(tooManyLabelsMsgFormat, 5, 2, `testmetric{foo-with-a-long-long-label="bar-with-a-long-long-value", foo2-with-a-long-long-label="bar2-with-a-long-long-value", foo3-with-a-long-long-label="bar3-with-a-long-long-value", foo4-with-a-lo`, "…")),
		},
		"exceeds maximum labels per series with a metric that exceeds 200 bytes when formatted": {
			labels: [][]mimirpb.LabelAdapter{{
				{Name: labels.MetricName, Value: "testmetric"},
				{Name: "foo", Value: "b"},
				{Name: "families", Value: "👩‍👦👨‍👧👨‍👩‍👧👩‍👧👩‍👩‍👦‍👦👨‍👩‍👧‍👦👨‍👧‍👦👨‍👩‍👦👪👨‍👦👨‍👦‍👦👨‍👨‍👧👨‍👧‍👧"},
			}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{families="👩\u200d👦👨\u200d👧👨\u200d👩\u200d👧👩\u200d👧👩\u200d👩\u200d👦\u200d👦👨\u200d👩\u200d👧\u200d👦👨\u200d👧\u200d👦👨\u200d👩\u200d👦👪👨\u200d👦👨\u200d👦\u200d👦👨\u200d👨\u200d👧👨\u200d👧\u200d👧", foo="b"}`, "")),
		},
		"multiple validation failures should return the first failure": {
			labels: [][]mimirpb.LabelAdapter{
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}},
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}},
			},
			samples: []mimirpb.Sample{
				{TimestampMs: int64(now), Value: 2},
				{TimestampMs: int64(past), Value: 2},
			},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{foo2="bar2", foo="bar"}`, "")),
		},
		"metadata validation failure": {
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			expectedErr: status.New(codes.FailedPrecondition, metadataMetricNameMissingMsgFormat),
		},
		"empty exemplar labels": {
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "testmetric", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			exemplars: []*mimirpb.Exemplar{{
				Labels:      nil,
				TimestampMs: int64(now),
				Value:       1,
			}},
			expectedErr: status.New(codes.FailedPrecondition, fmt.Sprintf(exemplarEmptyLabelsMsgFormat, now, labels.FromStrings(labels.MetricName, "testmetric", "foo", "bar"), "{}")),
		},
	} {
		t.Run(name, func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)

			limits.CreationGracePeriod = model.Duration(2 * time.Hour)
			limits.MaxLabelNamesPerSeries = 2
			limits.MaxGlobalExemplarsPerUser = 10

			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          &limits,
			})

			resp, err := ds[0].Push(ctx, mimirpb.ToWriteRequest(tc.labels, tc.samples, tc.exemplars, tc.metadata, mimirpb.API))
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, emptyResponse, resp)
			} else {
				require.Error(t, err)
				require.Nil(t, resp)
				checkGRPCError(t, tc.expectedErr, expectedDetails, err)
			}
		})
	}
}

// This is not great, but we deal with unsorted labels in prePushRelabelMiddleware.
func TestShardByAllLabelsReturnsWrongResultsForUnsortedLabels(t *testing.T) {
	val1 := shardByAllLabels("test", []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "sample", Value: "1"},
	})

	val2 := shardByAllLabels("test", []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "bar", Value: "baz"},
	})

	assert.NotEqual(t, val1, val2)
}

func TestDistributor_Push_Relabel(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries          labels.Labels
		expectedSeries       labels.Labels
		metricRelabelConfigs []*relabel.Config
	}

	cases := []testcase{
		// No relabel config.
		{
			inputSeries:    labels.FromStrings("__name__", "foo", "cluster", "one"),
			expectedSeries: labels.FromStrings("__name__", "foo", "cluster", "one"),
		},
		{
			inputSeries:    labels.FromStrings("__name__", "foo", "cluster", "one"),
			expectedSeries: labels.FromStrings("__name__", "foo", "cluster", "two"),
			metricRelabelConfigs: []*relabel.Config{
				{
					SourceLabels: []model.LabelName{"cluster"},
					Action:       relabel.DefaultRelabelConfig.Action,
					Regex:        relabel.DefaultRelabelConfig.Regex,
					TargetLabel:  "cluster",
					Replacement:  "two",
				},
			},
		},
	}

	for _, tc := range cases {
		var err error
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.MetricRelabelConfigs = tc.metricRelabelConfigs

		ds, ingesters, _ := prepare(t, prepConfig{
			numIngesters:    2,
			happyIngesters:  2,
			numDistributors: 1,
			limits:          &limits,
		})

		// Push the series to the distributor
		req := mockWriteRequest(tc.inputSeries, 1, 1)
		_, err = ds[0].Push(ctx, req)
		require.NoError(t, err)

		// Since each test pushes only 1 series, we do expect the ingester
		// to have received exactly 1 series
		for i := range ingesters {
			timeseries := ingesters[i].series()
			assert.Equal(t, 1, len(timeseries))
			for _, v := range timeseries {
				assert.Equal(t, tc.expectedSeries, mimirpb.FromLabelAdaptersToLabels(v.Labels))
			}
		}
	}
}

func countMockIngestersCalls(ingesters []mockIngester, name string) int {
	count := 0
	for i := 0; i < len(ingesters); i++ {
		if ingesters[i].countCalls(name) > 0 {
			count++
		}
	}
	return count
}

// TestDistributor_MetricsWithRequestModifications tests that the distributor metrics are properly updated when
// requests get modified by the mechanisms that can modify them: relabel rules, drop labels, ha-dedupe, forwarding, limits.
func TestDistributor_MetricsWithRequestModifications(t *testing.T) {
	tenant := "tenant1"
	getCtx := func() context.Context {
		return user.InjectOrgID(context.Background(), tenant)
	}
	getDefaultConfig := func() prepConfig {
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.MaxGlobalExemplarsPerUser = 1000

		return prepConfig{
			limits:            &limits,
			numIngesters:      1,
			happyIngesters:    1,
			replicationFactor: 1,
			numDistributors:   1,
		}
	}
	getDistributor := func(config prepConfig) (*Distributor, *prometheus.Registry) {
		ds, _, regs := prepare(t, config)
		return ds[0], regs[0]
	}
	type expectedMetricsCfg struct {
		requestsIn        int
		samplesIn         int
		exemplarsIn       int
		metadataIn        int
		receivedRequests  int
		receivedSamples   int
		receivedExemplars int
		receivedMetadata  int
	}
	getExpectedMetrics := func(cfg expectedMetricsCfg) (string, []string) {
		return fmt.Sprintf(`
				# HELP cortex_distributor_requests_in_total The total number of requests that have come in to the distributor, including rejected or deduped requests.
				# TYPE cortex_distributor_requests_in_total counter
				cortex_distributor_requests_in_total{user="%s"} %d
				# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected or deduped samples.
				# TYPE cortex_distributor_samples_in_total counter
				cortex_distributor_samples_in_total{user="%s"} %d
				# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
				# TYPE cortex_distributor_exemplars_in_total counter
				cortex_distributor_exemplars_in_total{user="%s"} %d
				# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
				# TYPE cortex_distributor_metadata_in_total counter
				cortex_distributor_metadata_in_total{user="%s"} %d
				# HELP cortex_distributor_received_requests_total The total number of received requests, excluding rejected and deduped requests.
				# TYPE cortex_distributor_received_requests_total counter
				cortex_distributor_received_requests_total{user="%s"} %d
				# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected and deduped samples.
				# TYPE cortex_distributor_received_samples_total counter
				cortex_distributor_received_samples_total{user="%s"} %d
				# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
				# TYPE cortex_distributor_received_exemplars_total counter
				cortex_distributor_received_exemplars_total{user="%s"} %d
				# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
				# TYPE cortex_distributor_received_metadata_total counter
				cortex_distributor_received_metadata_total{user="%s"} %d
	`, tenant, cfg.requestsIn, tenant, cfg.samplesIn, tenant, cfg.exemplarsIn, tenant, cfg.metadataIn, tenant, cfg.receivedRequests, tenant, cfg.receivedSamples, tenant, cfg.receivedExemplars, tenant, cfg.receivedMetadata), []string{
				"cortex_distributor_requests_in_total",
				"cortex_distributor_samples_in_total",
				"cortex_distributor_exemplars_in_total",
				"cortex_distributor_metadata_in_total",
				"cortex_distributor_received_requests_total",
				"cortex_distributor_received_samples_total",
				"cortex_distributor_received_exemplars_total",
				"cortex_distributor_received_metadata_total",
			}
	}
	uniqueMetricsGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", sampleIdx)}}
	}
	exemplarLabelGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "exemplarLabel", Value: fmt.Sprintf("value_%d", sampleIdx)}}
	}
	metaDataGen := func(metricIdx int, metricName string) *mimirpb.MetricMetadata {
		return &mimirpb.MetricMetadata{
			Type:             mimirpb.COUNTER,
			MetricFamilyName: metricName,
			Help:             "test metric",
			Unit:             "unknown",
		}
	}

	t.Run("No modifications", func(t *testing.T) {
		dist, reg := getDistributor(getDefaultConfig())
		req := makeWriteRequestForGenerators(10, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

		_, err := dist.Push(getCtx(), req)
		require.NoError(t, err)

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   20,
			receivedExemplars: 10,
			receivedMetadata:  10})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop samples via relabel rules", func(t *testing.T) {
		cfg := getDefaultConfig()
		cfg.limits.MetricRelabelConfigs = []*relabel.Config{{
			SourceLabels: []model.LabelName{"__name__"},
			Regex:        relabel.MustNewRegexp("^metric_[5-9]$"),
			Action:       relabel.Drop,
		}}
		dist, reg := getDistributor(cfg)
		req := makeWriteRequestForGenerators(10, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

		_, err := dist.Push(getCtx(), req)
		require.NoError(t, err)

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   10,
			receivedExemplars: 5,
			receivedMetadata:  10})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop samples via drop_label rule", func(t *testing.T) {
		cfg := getDefaultConfig()
		cfg.limits.DropLabels = []string{"__name__"}
		dist, reg := getDistributor(cfg)
		req := makeWriteRequestForGenerators(10, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

		dist.Push(getCtx(), req) //nolint:errcheck

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   0,
			receivedExemplars: 0,
			receivedMetadata:  10})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop requests via ha-dedupe", func(t *testing.T) {
		cfg := getDefaultConfig()
		cfg.limits.AcceptHASamples = true
		cfg.limits.MaxLabelValueLength = 15
		cfg.limits.HAMaxClusters = 1
		cfg.enableTracker = true
		dist, reg := getDistributor(cfg)

		uniqueMetricsGenWithReplica := func(replica string) func(sampleIdx int) []mimirpb.LabelAdapter {
			return func(sampleIdx int) []mimirpb.LabelAdapter {
				labels := uniqueMetricsGen(sampleIdx)
				labels = append(labels, mimirpb.LabelAdapter{Name: "__replica__", Value: replica})
				labels = append(labels, mimirpb.LabelAdapter{Name: "cluster", Value: "test_cluster"})
				return labels
			}
		}

		ctx := getCtx()
		_, err := dist.Push(ctx, makeWriteRequestForGenerators(10, uniqueMetricsGenWithReplica("replica1"), exemplarLabelGen, metaDataGen))
		require.NoError(t, err)
		dist.Push(ctx, makeWriteRequestForGenerators(10, uniqueMetricsGenWithReplica("replica2"), exemplarLabelGen, metaDataGen)) //nolint:errcheck

		_, err = dist.Push(ctx, makeWriteRequestForGenerators(10, uniqueMetricsGenWithReplica("replica1"), exemplarLabelGen, metaDataGen))
		require.NoError(t, err)
		dist.Push(ctx, makeWriteRequestForGenerators(10, uniqueMetricsGenWithReplica("replica2"), exemplarLabelGen, metaDataGen)) //nolint:errcheck

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        4,
			samplesIn:         80,
			exemplarsIn:       40,
			metadataIn:        40,
			receivedRequests:  2,
			receivedSamples:   40,
			receivedExemplars: 20,
			receivedMetadata:  20})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop examplars via limit", func(t *testing.T) {
		cfg := getDefaultConfig()
		cfg.limits.MaxGlobalExemplarsPerUser = 0
		dist, reg := getDistributor(cfg)
		req := makeWriteRequestForGenerators(10, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

		_, err := dist.Push(getCtx(), req)
		require.NoError(t, err)

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   20,
			receivedExemplars: 0,
			receivedMetadata:  10})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop half of samples via label value length limit", func(t *testing.T) {
		labelValueLimit := 10
		cfg := getDefaultConfig()
		cfg.limits.MaxLabelValueLength = labelValueLimit
		dist, reg := getDistributor(cfg)

		halfLabelValuesAboveLimit := func(sampleIdx int) []mimirpb.LabelAdapter {
			labels := uniqueMetricsGen(sampleIdx)
			if sampleIdx%2 == 0 {
				labels = append(labels, mimirpb.LabelAdapter{
					Name: "long_label", Value: strings.Repeat("a", labelValueLimit+1),
				})
			}
			return labels
		}

		req := makeWriteRequestForGenerators(10, halfLabelValuesAboveLimit, exemplarLabelGen, metaDataGen)

		dist.Push(getCtx(), req) //nolint:errcheck

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   10,
			receivedExemplars: 5,
			receivedMetadata:  10})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop half of samples via native histogram bucket limit", func(t *testing.T) {
		cfg := getDefaultConfig()
		cfg.limits.MaxNativeHistogramBuckets = 2
		dist, reg := getDistributor(cfg)

		validHistogram := mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 2},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeDeltas: []int64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			PositiveDeltas: []int64{1},
			Timestamp:      100,
		}
		invalidHistogram := mimirpb.Histogram{
			Count:          &mimirpb.Histogram_CountInt{CountInt: 3},
			Sum:            10,
			Schema:         1,
			ZeroThreshold:  0.001,
			ZeroCount:      &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: 0},
			NegativeSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 1}},
			NegativeDeltas: []int64{1},
			PositiveSpans:  []mimirpb.BucketSpan{{Offset: 0, Length: 2}},
			PositiveDeltas: []int64{1, 2},
			Timestamp:      100,
		}

		req := mimirpb.NewWriteRequest(nil, mimirpb.API)
		for sampleIdx := 0; sampleIdx < 20; sampleIdx++ {
			lbs := uniqueMetricsGen(sampleIdx)
			if sampleIdx%2 == 0 {
				req.AddHistogramSeries([][]mimirpb.LabelAdapter{lbs}, []mimirpb.Histogram{validHistogram}, nil)
			} else {
				req.AddHistogramSeries([][]mimirpb.LabelAdapter{lbs}, []mimirpb.Histogram{invalidHistogram}, nil)
			}
		}

		dist.Push(getCtx(), req) //nolint:errcheck

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       0,
			metadataIn:        0,
			receivedRequests:  1,
			receivedSamples:   10,
			receivedExemplars: 0,
			receivedMetadata:  0})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})

	t.Run("Drop metadata via label value length limit, except help", func(t *testing.T) {
		metadataLengthLimit := 20
		cfg := getDefaultConfig()
		cfg.limits.MaxMetadataLength = metadataLengthLimit
		dist, reg := getDistributor(cfg)

		metaDataGen := func(metricIdx int, metricName string) *mimirpb.MetricMetadata {
			if metricIdx%3 == 0 {
				return &mimirpb.MetricMetadata{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: metricName,
					Help:             strings.Repeat("a", metadataLengthLimit+1),
					Unit:             "unknown",
				}
			} else if metricIdx%3 == 1 {
				return &mimirpb.MetricMetadata{
					Type:             mimirpb.COUNTER,
					MetricFamilyName: strings.Repeat("a", metadataLengthLimit+1),
					Help:             strings.Repeat("a", metadataLengthLimit+1),
					Unit:             "unknown",
				}
			}
			return &mimirpb.MetricMetadata{
				Type:             mimirpb.COUNTER,
				MetricFamilyName: metricName,
				Help:             strings.Repeat("a", metadataLengthLimit+1),
				Unit:             strings.Repeat("a", metadataLengthLimit+1),
			}
		}

		req := makeWriteRequestForGenerators(10, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

		dist.Push(getCtx(), req) //nolint:errcheck

		expectedMetrics, metricNames := getExpectedMetrics(expectedMetricsCfg{
			requestsIn:        1,
			samplesIn:         20,
			exemplarsIn:       10,
			metadataIn:        10,
			receivedRequests:  1,
			receivedSamples:   20,
			receivedExemplars: 10,
			receivedMetadata:  4})

		require.NoError(t, testutil.GatherAndCompare(
			reg,
			strings.NewReader(expectedMetrics),
			metricNames...,
		))
	})
}

func TestDistributor_CleanupIsDoneAfterLastIngesterReturns(t *testing.T) {
	// We want to decrement inflight requests and other counters that we use for limits
	// only after the last ingester has returned.
	// Distributor.Push returns after a quorum of ingesters have returned.
	// But there are still resources occupied within the distributor while it's
	// waiting for all ingesters to return. So we want the instance limits to accurately reflect that.

	distributors, ingesters, _ := prepare(t, prepConfig{
		numIngesters:        3,
		happyIngesters:      3,
		numDistributors:     1,
		maxInflightRequests: 1,
		replicationFactor:   3,
		enableTracker:       false,
	})
	ingesters[2].pushDelay = time.Second // give the test enough time to do assertions

	lbls := labels.FromStrings("__name__", "metric_1", "key", "value_1")
	ctx := user.InjectOrgID(context.Background(), "user")

	_, err := distributors[0].Push(ctx, mockWriteRequest(lbls, 1, 1))
	assert.NoError(t, err)

	// First push request returned, but there's still an ingester call inflight.
	// This means that the push request is counted as inflight, so another incoming request should be rejected.
	_, err = distributors[0].Push(ctx, mockWriteRequest(labels.EmptyLabels(), 1, 1))
	checkGRPCError(t, status.New(codes.Internal, errMaxInflightRequestsReached.Error()), nil, err)
}

func TestSeriesAreShardedToCorrectIngesters(t *testing.T) {
	config := prepConfig{
		numIngesters:      5,
		happyIngesters:    5,
		numDistributors:   1,
		replicationFactor: 1, // push each series to single ingester only
	}
	d, ing, _ := prepare(t, config)

	uniqueMetricsGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{
			{Name: "__name__", Value: fmt.Sprintf("%d", sampleIdx)},
			{Name: "x", Value: fmt.Sprintf("%d", sampleIdx)},
		}
	}
	exemplarLabelGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "exemplarLabel", Value: fmt.Sprintf("value_%d", sampleIdx)}}
	}
	metaDataGen := func(metricIdx int, metricName string) *mimirpb.MetricMetadata {
		return &mimirpb.MetricMetadata{
			Type:             mimirpb.COUNTER,
			MetricFamilyName: metricName,
			Help:             "test metric",
			Unit:             "unknown",
		}
	}

	const series = 1000
	const userName = "userName"

	req := makeWriteRequestForGenerators(series, uniqueMetricsGen, exemplarLabelGen, metaDataGen)

	ctx := user.InjectOrgID(context.Background(), userName)
	// skip all the middlewares, just do the push
	distrib := d[0]
	err := distrib.push(ctx, NewParsedRequest(req))
	require.NoError(t, err)

	// Verify that each ingester only received series and metadata that it should receive.
	totalSeries := 0
	totalMetadata := 0
	for ix := range ing {
		totalSeries += len(ing[ix].timeseries)
		totalMetadata += len(ing[ix].metadata)

		for _, ts := range ing[ix].timeseries {
			token := distrib.tokenForLabels(userName, ts.Labels)
			ingIx := getIngesterIndexForToken(token, ing)
			assert.Equal(t, ix, ingIx)
		}

		for _, metadataMap := range ing[ix].metadata {
			for m := range metadataMap {
				token := distrib.tokenForMetadata(userName, m.MetricFamilyName)
				ingIx := getIngesterIndexForToken(token, ing)
				assert.Equal(t, ix, ingIx)
			}
		}
	}

	// Verify that all timeseries were forwarded to ingesters.
	for _, ts := range req.Timeseries {
		token := distrib.tokenForLabels(userName, ts.Labels)
		ingIx := getIngesterIndexForToken(token, ing)

		assert.Equal(t, ts.Labels, ing[ingIx].timeseries[token].Labels)
	}

	assert.Equal(t, series, totalSeries)
	assert.Equal(t, series, totalMetadata) // each series has unique metric name, and each metric name gets metadata
}

func TestHandleIngesterPushError(t *testing.T) {
	testErrorMsg := "this is a test error message"
	outputErrorMsgPrefix := "failed pushing to ingester"
	userID := "test"
	errWithUserID := fmt.Errorf("user=%s: %s", userID, testErrorMsg)
	unavailableErr := status.New(codes.Unavailable, testErrorMsg).Err()
	test := map[string]struct {
		ingesterPushError   error
		expectedOutputError error
	}{
		"no error gives no error": {
			ingesterPushError:   nil,
			expectedOutputError: nil,
		},
		"a 4xx HTTP gRPC error gives a 4xx HTTP gRPC error": {
			ingesterPushError:   httpgrpc.Errorf(http.StatusBadRequest, testErrorMsg),
			expectedOutputError: httpgrpc.Errorf(http.StatusBadRequest, "%s: %s", outputErrorMsgPrefix, testErrorMsg),
		},
		"a 5xx HTTP gRPC error gives a 5xx HTTP gRPC error": {
			ingesterPushError:   httpgrpc.Errorf(http.StatusServiceUnavailable, testErrorMsg),
			expectedOutputError: httpgrpc.Errorf(http.StatusServiceUnavailable, "%s: %s", outputErrorMsgPrefix, testErrorMsg),
		},
		"a random ingester error without status gives the same wrapped error": {
			ingesterPushError:   errWithUserID,
			expectedOutputError: errors.Wrap(errWithUserID, outputErrorMsgPrefix),
		},
		"a gRPC unavailable error gives the same wrapped error": {
			ingesterPushError:   unavailableErr,
			expectedOutputError: errors.Wrap(unavailableErr, outputErrorMsgPrefix),
		},
		"a context cancel error gives the same wrapped error": {
			ingesterPushError:   context.Canceled,
			expectedOutputError: errors.Wrap(context.Canceled, outputErrorMsgPrefix),
		},
	}

	for _, testData := range test {
		err := handleIngesterPushError(testData.ingesterPushError)
		if testData.expectedOutputError == nil {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, testData.expectedOutputError.Error())
		}
	}
}

func TestHandlePushError(t *testing.T) {
	testErrorMsg := "this is a test error message"
	userID := "test"
	errWithUserID := fmt.Errorf("user=%s: %s", userID, testErrorMsg)
	httpGrpc4xxErr := httpgrpc.Errorf(http.StatusBadRequest, testErrorMsg)
	httpGrpc5xxErr := httpgrpc.Errorf(http.StatusServiceUnavailable, testErrorMsg)
	test := map[string]struct {
		pushError          error
		expectedGRPCError  *status.Status
		expectedOtherError error
	}{
		"a context.Canceled error gives context.Canceled": {
			pushError:          context.Canceled,
			expectedOtherError: context.Canceled,
		},
		"a context.DeadlineExceeded error gives context.DeadlineExceeded": {
			pushError:          context.DeadlineExceeded,
			expectedOtherError: context.DeadlineExceeded,
		},
		"a 4xx HTTP gRPC error gives the same 4xx HTTP gRPC error": {
			pushError:          httpGrpc4xxErr,
			expectedOtherError: httpGrpc4xxErr,
		},
		"a 5xx HTTP gRPC error gives the same 5xx HTTP gRPC error": {
			pushError:          httpGrpc5xxErr,
			expectedOtherError: httpGrpc5xxErr,
		},
		"a random ingester error without status gives an Internal gRPC error": {
			pushError:         errWithUserID,
			expectedGRPCError: status.New(codes.Internal, errWithUserID.Error()),
		},
		"a random ingester gRPC error gives the same gRPC errpr": {
			pushError:         status.Error(codes.Unavailable, testErrorMsg),
			expectedGRPCError: status.New(codes.Unavailable, testErrorMsg),
		},
	}

	config := prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		replicationFactor: 1, // push each series to single ingester only
	}
	d, _, _ := prepare(t, config)
	ctx := context.Background()

	for _, testData := range test {
		err := d[0].handlePushError(ctx, testData.pushError)
		if testData.expectedGRPCError == nil {
			require.Equal(t, testData.expectedOtherError, err)
		} else {
			checkGRPCError(t, testData.expectedGRPCError, nil, err)
		}
	}
}

func getIngesterIndexForToken(key uint32, ings []mockIngester) int {
	tokens := []uint32{}
	tokensMap := map[uint32]int{}

	for ix := range ings {
		tokens = append(tokens, ings[ix].tokens...)
		for _, t := range ings[ix].tokens {
			tokensMap[t] = ix
		}
	}

	ix := searchToken(tokens, key)
	t := tokens[ix]
	return tokensMap[t]
}

// copied from vendor/github.com/grafana/dskit/ring/util.go
func searchToken(tokens []uint32, key uint32) int {
	i := sort.Search(len(tokens), func(x int) bool {
		return tokens[x] > key
	})
	if i >= len(tokens) {
		i = 0
	}
	return i
}

func checkGRPCError(t *testing.T, expectedStatus *status.Status, expectedDetails *mimirpb.WriteErrorDetails, err error) {
	stat, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, expectedStatus.Code(), stat.Code())
	require.Equal(t, expectedStatus.Message(), stat.Message())
	if expectedDetails == nil {
		require.Len(t, stat.Details(), 0)
	} else {
		details := stat.Details()
		require.Len(t, details, 1)
		errorDetails, ok := details[0].(*mimirpb.WriteErrorDetails)
		require.True(t, ok)
		require.Equal(t, expectedDetails, errorDetails)
	}
}

func callCount(ingesters []mockIngester, call string) int {
	count := 0

	for i := range ingesters {
		count += ingesters[i].calls[call]
	}

	return count
}
