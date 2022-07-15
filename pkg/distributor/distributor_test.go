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
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/mtime"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/distributor/forwarding"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_math "github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errFail       = fmt.Errorf("Fail")
	emptyResponse = &mimirpb.WriteResponse{}
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

	type samplesIn struct {
		num              int
		startTimestampMs int64
	}
	for name, tc := range map[string]struct {
		metricNames      []string
		numIngesters     int
		happyIngesters   int
		samples          samplesIn
		metadata         int
		expectedResponse *mimirpb.WriteResponse
		expectedError    error
		expectedMetrics  string
	}{
		"A push of no samples shouldn't block or return error, even if ingesters are sad": {
			numIngesters:     3,
			happyIngesters:   0,
			expectedResponse: emptyResponse,
		},
		"A push to 3 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   3,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: emptyResponse,
			metricNames:      []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.004
			`,
		},
		"A push to 2 happy ingesters should succeed": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 5, startTimestampMs: 123456789000},
			metadata:         5,
			expectedResponse: emptyResponse,
			metricNames:      []string{lastSeenTimestamp},
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
			expectedError:  errFail,
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
			expectedError:  errFail,
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
		"A push exceeding burst size should fail": {
			numIngesters:   3,
			happyIngesters: 3,
			samples:        samplesIn{num: 25, startTimestampMs: 123456789000},
			metadata:       5,
			expectedError:  httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(20, 20).Error()),
			metricNames:    []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.024
			`,
		},
		"A push to ingesters with an old sample should report the correct metrics with no metadata": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 1, startTimestampMs: now.UnixMilli() - 80000*1000}, // 80k seconds old
			metadata:         0,
			metricNames:      []string{distributorSampleDelay},
			expectedResponse: emptyResponse,
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
				cortex_distributor_sample_delay_seconds_bucket{le="86400"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="+Inf"} 1
				cortex_distributor_sample_delay_seconds_sum 80000
				cortex_distributor_sample_delay_seconds_count 1
			`,
		},
		"A push to ingesters with a current sample should report the correct metrics with no metadata": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 1, startTimestampMs: now.UnixMilli() - 1000}, // 1 second old
			metadata:         0,
			metricNames:      []string{distributorSampleDelay},
			expectedResponse: emptyResponse,
			expectedMetrics: `
				# HELP cortex_distributor_sample_delay_seconds Number of seconds by which a sample came in late wrt wallclock.
				# TYPE cortex_distributor_sample_delay_seconds histogram
				cortex_distributor_sample_delay_seconds_bucket{le="30"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="60"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="120"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="240"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="480"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="600"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="1800"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="3600"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="7200"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="10800"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="21600"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="86400"} 1
				cortex_distributor_sample_delay_seconds_bucket{le="+Inf"} 1
				cortex_distributor_sample_delay_seconds_sum 1.000
				cortex_distributor_sample_delay_seconds_count 1
			`,
		},
		"A push to ingesters without samples should report the correct metrics": {
			numIngesters:     3,
			happyIngesters:   2,
			samples:          samplesIn{num: 0, startTimestampMs: 123456789000},
			metadata:         1,
			metricNames:      []string{distributorSampleDelay},
			expectedResponse: emptyResponse,
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
			})

			request := makeWriteRequest(tc.samples.startTimestampMs, tc.samples.num, tc.metadata, false)
			response, err := ds[0].Push(ctx, request)
			assert.Equal(t, tc.expectedResponse, response)
			assert.Equal(t, tc.expectedError, err)

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
	request := makeWriteRequest(123456789000, 1, 1, false)
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

		# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
		# TYPE cortex_distributor_received_samples_total counter
		cortex_distributor_received_samples_total{user="userA"} 5
		cortex_distributor_received_samples_total{user="userB"} 10

		# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
		# TYPE cortex_distributor_received_exemplars_total counter
		cortex_distributor_received_exemplars_total{user="userA"} 5
		cortex_distributor_received_exemplars_total{user="userB"} 10

		# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
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

		# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
		# TYPE cortex_distributor_received_samples_total counter
		cortex_distributor_received_samples_total{user="userB"} 10

		# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
		# TYPE cortex_distributor_received_exemplars_total counter
		cortex_distributor_received_exemplars_total{user="userB"} 10

		# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
		# TYPE cortex_distributor_samples_in_total counter

		# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
		# TYPE cortex_distributor_exemplars_in_total counter
		`), metrics...))
}

func TestDistributor_PushRequestRateLimiter(t *testing.T) {
	type testPush struct {
		expectedError error
	}
	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		distributors     int
		requestRate      float64
		requestBurstSize int
		pushes           []testPush
	}{
		"request limit should be evenly shared across distributors": {
			distributors:     2,
			requestRate:      4,
			requestBurstSize: 2,
			pushes: []testPush{
				{expectedError: nil},
				{expectedError: nil},
				{expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewRequestRateLimitedError(4, 2).Error())},
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
				{expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewRequestRateLimitedError(2, 3).Error())},
			},
		},
	}

	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)
			limits.RequestRate = testData.requestRate
			limits.RequestBurstSize = testData.requestBurstSize

			// Start all expected distributors
			distributors, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: testData.distributors,
				limits:          limits,
			})

			// Send multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, 1, 1, false)
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, emptyResponse, response)
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response)
					assert.EqualError(t, err, push.expectedError.Error())
				}
			}
		})
	}
}

func TestDistributor_PushIngestionRateLimiter(t *testing.T) {
	type testPush struct {
		samples       int
		metadata      int
		expectedError error
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
				{samples: 2, metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 5).Error())},
				{samples: 2, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 5).Error())},
				{metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 5).Error())},
			},
		},
		"for each distributor, set an ingestion burst limit.": {
			distributors:       2,
			ingestionRate:      10,
			ingestionBurstSize: 20,
			pushes: []testPush{
				{samples: 10, expectedError: nil},
				{samples: 5, expectedError: nil},
				{samples: 5, metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 20).Error())},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 20).Error())},
				{metadata: 1, expectedError: httpgrpc.Errorf(http.StatusTooManyRequests, validation.NewIngestionRateLimitedError(10, 20).Error())},
			},
		},
	}

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
				request := makeWriteRequest(0, push.samples, push.metadata, false)
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, emptyResponse, response)
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response)
					assert.Equal(t, push.expectedError, err)
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
			inflightBytesLimit: makeWriteRequest(0, 100, 1, false).Size(),

			pushes: []testPush{
				{samples: 10, expectedError: nil},
			},
		},

		"hits inflight size limit": {
			inflightBytesLimit: makeWriteRequest(0, 100, 1, false).Size(),

			pushes: []testPush{
				{samples: 101, expectedError: errMaxInflightRequestsBytesReached},
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
				request := makeWriteRequest(0, push.samples, push.metadata, false)
				_, err := d.Push(ctx, request)

				if push.expectedError == nil {
					assert.Nil(t, err)
				} else {
					assert.Equal(t, push.expectedError, err)
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
		expectedCode     int32
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
			expectedCode:    202,
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
			enableTracker:    true,
			acceptedReplica:  "instance0",
			testReplica:      "instance1234567890123456789012345678901234567890",
			cluster:          "cluster0",
			samples:          5,
			expectedResponse: emptyResponse,
			expectedCode:     400,
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

			request := makeWriteRequestHA(tc.samples, tc.testReplica, tc.cluster)
			response, err := d.Push(ctx, request)
			assert.Equal(t, tc.expectedResponse, response)

			httpResp, ok := httpgrpc.HTTPResponseFromError(err)
			if ok {
				assert.Equal(t, tc.expectedCode, httpResp.Code)
			} else if tc.expectedCode != 0 {
				assert.Fail(t, "expected HTTP status code", tc.expectedCode)
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
					expectedResponse:  expectedResponse(0, 10),
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
					expectedResponse:  expectedResponse(0, 0),
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
						expectedResponse:  expectedResponse(i, i+1),
						expectedIngesters: expectedIngesters,
						shuffleShardSize:  shuffleShardSize,
					})
				}
			}
		}
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := prepConfig{
				numIngesters:    tc.numIngesters,
				happyIngesters:  tc.happyIngesters,
				numDistributors: 1,
			}

			cfg.shuffleShardSize = tc.shuffleShardSize

			ds, ingesters, _ := prepare(t, cfg)

			request := makeWriteRequest(0, tc.samples, tc.metadata, false)
			writeResponse, err := ds[0].Push(ctx, request)
			assert.Equal(t, &mimirpb.WriteResponse{}, writeResponse)
			assert.Nil(t, err)

			series, err := ds[0].QueryStream(ctx, 0, 10, tc.matchers...)
			assert.Equal(t, tc.expectedError, err)

			var response model.Matrix
			if series == nil {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, nil)
			} else {
				response, err = chunkcompat.SeriesChunksToMatrix(0, 10, series.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse.String(), response.String())

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

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxChunksPerQueryLimitIsReached(t *testing.T) {
	const maxChunksLimit = 30 // Chunks are duplicated due to replication factor.

	ctx := user.InjectOrgID(context.Background(), "user")
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	limits.MaxChunksPerQuery = maxChunksLimit

	// Prepare distributors.
	ds, _, _ := prepare(t, prepConfig{
		numIngesters:    3,
		happyIngesters:  3,
		numDistributors: 1,
		limits:          limits,
	})

	ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, maxChunksLimit))

	// Push a number of series below the max chunks limit. Each series has 1 sample,
	// so expect 1 chunk per series when querying back.
	initialSeries := maxChunksLimit / 3
	writeReq := makeWriteRequest(0, initialSeries, 0, false)
	writeRes, err := ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	allSeriesMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
	}

	// Since the number of series (and thus chunks) is equal to the limit (but doesn't
	// exceed it), we expect a query running on all series to succeed.
	queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)
	assert.Len(t, queryRes.Chunkseries, initialSeries)

	// Push more series to exceed the limit once we'll query back all series.
	writeReq = &mimirpb.WriteRequest{}
	for i := 0; i < maxChunksLimit; i++ {
		writeReq.Timeseries = append(writeReq.Timeseries,
			makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: fmt.Sprintf("another_series_%d", i)}}, 0, 0),
		)
	}

	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the number of series (and thus chunks) is exceeding to the limit, we expect
	// a query running on all series to fail.
	_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.Error(t, err)
	assert.ErrorContains(t, err, "the query exceeded the maximum number of chunks")
}

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxSeriesPerQueryLimitIsReached(t *testing.T) {
	const maxSeriesLimit = 10

	ctx := user.InjectOrgID(context.Background(), "user")
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(maxSeriesLimit, 0, 0))

	// Prepare distributors.
	ds, _, _ := prepare(t, prepConfig{
		numIngesters:    3,
		happyIngesters:  3,
		numDistributors: 1,
		limits:          limits,
	})

	// Push a number of series below the max series limit.
	initialSeries := maxSeriesLimit
	writeReq := makeWriteRequest(0, initialSeries, 0, false)
	writeRes, err := ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	allSeriesMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
	}

	// Since the number of series is equal to the limit (but doesn't
	// exceed it), we expect a query running on all series to succeed.
	queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)
	assert.Len(t, queryRes.Chunkseries, initialSeries)

	// Push more series to exceed the limit once we'll query back all series.
	writeReq = &mimirpb.WriteRequest{}
	writeReq.Timeseries = append(writeReq.Timeseries,
		makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0),
	)

	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the number of series is exceeding the limit, we expect
	// a query running on all series to fail.
	_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.Error(t, err)
	assert.ErrorContains(t, err, "the query exceeded the maximum number of series")
}

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxChunkBytesPerQueryLimitIsReached(t *testing.T) {
	const seriesToAdd = 10

	ctx := user.InjectOrgID(context.Background(), "user")
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)

	// Prepare distributors.
	// Use replication factor of 1 so that we always wait the response from all ingesters.
	// This guarantees us to always read the same chunks and have a stable test.
	ds, _, _ := prepare(t, prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		limits:            limits,
		replicationFactor: 1,
	})

	allSeriesMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
	}
	// Push a single series to allow us to calculate the chunk size to calculate the limit for the test.
	writeReq := &mimirpb.WriteRequest{}
	writeReq.Timeseries = append(writeReq.Timeseries,
		makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0),
	)
	writeRes, err := ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)
	chunkSizeResponse, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)

	// Use the resulting chunks size to calculate the limit as (series to add + our test series) * the response chunk size.
	responseChunkSize := chunkSizeResponse.ChunksSize()
	maxBytesLimit := (seriesToAdd) * responseChunkSize

	// Update the limiter with the calculated limits.
	ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, maxBytesLimit, 0))

	// Push a number of series below the max chunk bytes limit. Subtract one for the series added above.
	writeReq = makeWriteRequest(0, seriesToAdd-1, 0, false)
	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the number of chunk bytes is equal to the limit (but doesn't
	// exceed it), we expect a query running on all series to succeed.
	queryRes, err := ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)
	assert.Len(t, queryRes.Chunkseries, seriesToAdd)

	// Push another series to exceed the chunk bytes limit once we'll query back all series.
	writeReq = &mimirpb.WriteRequest{}
	writeReq.Timeseries = append(writeReq.Timeseries,
		makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series_1"}}, 0, 0),
	)

	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the aggregated chunk size is exceeding the limit, we expect
	// a query running on all series to fail.
	_, err = ds[0].QueryStream(ctx, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.Error(t, err)
	assert.ErrorContains(t, err, fmt.Sprintf(limiter.MaxChunkBytesHitMsgFormat, maxBytesLimit))
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
			removeReplica: true,
			removeLabels:  []string{"cluster"},
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
			},
		},
		// Remove multiple labels and replica.
		{
			removeReplica: true,
			removeLabels:  []string{"foo", "some"},
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
				{Name: "__replica__", Value: "two"},
				{Name: "foo", Value: "bar"},
				{Name: "some", Value: "thing"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "cluster", Value: "one"},
			},
		},
		// Don't remove any labels.
		{
			removeReplica: false,
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "__replica__", Value: "two"},
				{Name: "cluster", Value: "one"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "some_metric"},
				{Name: "__replica__", Value: "two"},
				{Name: "cluster", Value: "one"},
			},
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
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped label due to config": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
				{Name: "dropped", Value: "unused"}, // will be dropped, doesn't need to be in correct order
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped HA replica label": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
				{Name: "__replica__", Value: "replica_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "cluster", Value: "cluster_1"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xec0a2e9d,
		},
		"metric_2 with value_1": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_2"},
				{Name: "key", Value: "value_1"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_2"},
				{Name: "key", Value: "value_1"},
			},
			expectedToken: 0xa60906f2,
		},
		"metric_1 with value_2": {
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_2"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "metric_1"},
				{Name: "key", Value: "value_2"},
			},
			expectedToken: 0x18abc8a2,
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
	inputLabels := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
		{Name: "999.illegal", Value: "baz"},
	}
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
			ds, _, _ := prepare(t, prepConfig{
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

func TestDistributor_ExemplarValidation(t *testing.T) {
	tests := map[string]struct {
		minExemplarTS     int64
		req               *mimirpb.WriteRequest
		expectedExemplars []mimirpb.PreallocTimeseries
	}{
		"valid exemplars": {
			minExemplarTS: 0,
			req: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, []string{"foo", "bar"}),
			}},
			expectedExemplars: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, []string{"foo", "bar"}),
			},
		},
		"one old, one new, separate series": {
			minExemplarTS: 300000,
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
		},
		"multi exemplars": {
			minExemplarTS: 300000,
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
		},
		"one old, one new, same series": {
			minExemplarTS: 300000,
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
		},
	}
	ds, _, _ := prepare(t, prepConfig{
		numDistributors: 1,
	})
	now := mtime.Now()
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			err := ds[0].validateSeries(now, tc.req.Timeseries[0], "user", false, tc.minExemplarTS)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedExemplars, tc.req.Timeseries)
		})
	}
}

func BenchmarkDistributor_Push(b *testing.B) {
	const (
		numSeriesPerRequest = 1000
	)
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		prepareConfig func(limits *validation.Limits)
		prepareSeries func() ([]labels.Labels, []mimirpb.Sample)
		expectedErr   string
	}{
		"all samples successfully pushed": {
			prepareConfig: func(limits *validation.Limits) {},
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
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
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
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
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 1; i < 31; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
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
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					// Add a label with a very long name.
					lbls.Set(fmt.Sprintf("xxx_%0.2000d", 1), "xxx")

					metrics[i] = lbls.Labels()
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
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					// Add a label with a very long value.
					lbls.Set("xxx", fmt.Sprintf("xxx_%0.2000d", 1))

					metrics[i] = lbls.Labels()
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
			prepareSeries: func() ([]labels.Labels, []mimirpb.Sample) {
				metrics := make([]labels.Labels, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					lbls := labels.NewBuilder(labels.Labels{{Name: model.MetricNameLabel, Value: "foo"}})
					for i := 0; i < 10; i++ {
						lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
					}

					metrics[i] = lbls.Labels()
					samples[i] = mimirpb.Sample{
						Value:       float64(i),
						TimestampMs: time.Now().Add(time.Hour).UnixNano() / int64(time.Millisecond),
					}
				}

				return metrics, samples
			},
			expectedErr: "received a sample whose timestamp is too far in the future",
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
					d.AddIngester("ingester-1", "127.0.0.1", "", ring.GenerateTokens(128, nil), ring.ACTIVE, time.Now())
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
			distributorCfg.DistributorRing.KVStore.Store = "inmemory"

			limits.IngestionRate = float64(rate.Inf) // Unlimited.
			testData.prepareConfig(&limits)

			distributorCfg.IngesterClientFactory = func(addr string) (ring_client.PoolClient, error) {
				return &noopIngester{}, nil
			}

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(b, err)

			// Start the distributor.
			distributor, err := New(distributorCfg, clientConfig, overrides, ingestersRing, true, nil, log.NewNopLogger())
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

			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    nIngesters,
				happyIngesters:  happy,
				numDistributors: 1,
				queryDelay:      100 * time.Millisecond,
			})

			_, err := ds[0].QueryStream(ctx, 0, 10, nameMatcher)
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
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "fast_fingerprint_collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
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
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "reason", Value: "broken"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
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

			req := makeWriteRequest(0, 0, 10, false)
			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)

			// Check how many ingesters are queried as part of the shuffle sharding subring.
			replicationSet, err := ds[0].GetIngestersForMetadata(ctx)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedIngesters, len(replicationSet.Instances))

			// Assert on metric metadata
			metadata, err := ds[0].MetricsMetadata(ctx)
			require.NoError(t, err)
			assert.Equal(t, 10, len(metadata))
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
		{labels.Labels{{Name: labels.MetricName, Value: "label_00"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "label_11"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "label_11"}}, 2, 200000},
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

func TestDistributor_LabelNamesAndValues(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "label_0"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "label_1"}, {Name: "status", Value: "500"}, {Name: "reason", Value: "broken"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "label_1"}}, 2, 200000},
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
				numIngesters:       12,
				happyIngesters:     12,
				numDistributors:    1,
				replicationFactor:  3,
				ingesterZones:      testData.zones,
				zonesResponseDelay: testData.zonesResponseDelay,
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
				sort.Strings(item.Values)
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
	response, err := ds[0].labelValuesCardinality(ctx, names, []*labels.Matcher{})
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

func TestDistributor_IngestionIsControlledByForwarder(t *testing.T) {
	type testcase struct {
		name                  string
		request               *mimirpb.WriteRequest
		ingestSample          bool
		expectIngestedMetrics []string
		expectedMetrics       string
	}

	metric := "test_metric"
	testcases := []testcase{
		{
			name:                  "do ingest with only samples",
			request:               makeWriteRequest(123456789000, 5, 0, false, metric),
			ingestSample:          true,
			expectIngestedMetrics: []string{metric},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 5
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 0
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 0
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 0
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 0
`,
		}, {
			name:                  "don't ingest with only samples",
			request:               makeWriteRequest(123456789000, 5, 0, false, metric),
			ingestSample:          false,
			expectIngestedMetrics: []string{},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 0
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 0
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 0
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 0
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 0
`,
		}, {
			name:                  "do ingest with metadata",
			request:               makeWriteRequest(123456789000, 5, 5, false, metric),
			ingestSample:          true,
			expectIngestedMetrics: []string{metric},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 5
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 0
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 5
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 0
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 5
`,
		}, {
			name:                  "don't ingest with metadata",
			request:               makeWriteRequest(123456789000, 5, 5, false, metric),
			ingestSample:          false,
			expectIngestedMetrics: []string{},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 0
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 0
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 5
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 0
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 5
`,
		}, {
			name:                  "do ingest with exemplars",
			request:               makeWriteRequest(123456789000, 5, 0, true, metric),
			ingestSample:          true,
			expectIngestedMetrics: []string{metric},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 5
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 5
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 0
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 5
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 0
`,
		}, {
			name:                  "don't ingest with exemplars",
			request:               makeWriteRequest(123456789000, 5, 0, true, metric),
			ingestSample:          false,
			expectIngestedMetrics: []string{},
			expectedMetrics: `
			# HELP cortex_distributor_received_samples_total The total number of received samples, excluding rejected, forwarded and deduped samples.
			# TYPE cortex_distributor_received_samples_total counter
			cortex_distributor_received_samples_total{user="user"} 0
			# HELP cortex_distributor_received_exemplars_total The total number of received exemplars, excluding rejected and deduped exemplars.
			# TYPE cortex_distributor_received_exemplars_total counter
			cortex_distributor_received_exemplars_total{user="user"} 0
			# HELP cortex_distributor_received_metadata_total The total number of received metadata, excluding rejected.
			# TYPE cortex_distributor_received_metadata_total counter
			cortex_distributor_received_metadata_total{user="user"} 0
			# HELP cortex_distributor_samples_in_total The total number of samples that have come in to the distributor, including rejected, forwarded or deduped samples.
			# TYPE cortex_distributor_samples_in_total counter
			cortex_distributor_samples_in_total{user="user"} 5
			# HELP cortex_distributor_exemplars_in_total The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars.
			# TYPE cortex_distributor_exemplars_in_total counter
			cortex_distributor_exemplars_in_total{user="user"} 5
			# HELP cortex_distributor_metadata_in_total The total number of metadata the have come in to the distributor, including rejected.
			# TYPE cortex_distributor_metadata_in_total counter
			cortex_distributor_metadata_in_total{user="user"} 0
`,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "user")
			limits := &validation.Limits{
				ForwardingRules: validation.ForwardingRules{metric: validation.ForwardingRule{}},
			}
			flagext.DefaultValues(limits)
			limits.IngestionRate = 20
			limits.IngestionBurstSize = 20

			distributors, ingesters, regs := prepare(t, prepConfig{
				numIngesters:      1,
				happyIngesters:    1,
				replicationFactor: 1,
				numDistributors:   1,
				limits:            limits,
				forwarding:        true,
			})

			forwarder := setMockForwarder(distributors[0], tc.ingestSample)

			response, err := distributors[0].Push(ctx, tc.request)
			assert.NoError(t, err)
			assert.Equal(t, emptyResponse, response)
			assert.Equal(t, 1, int(forwarder.sendCount.Load()))

			ingestedMetrics := getIngestedMetrics(ctx, t, &ingesters[0])
			assert.Equal(t, tc.expectIngestedMetrics, ingestedMetrics)

			require.NoError(t, testutil.GatherAndCompare(
				regs[0],
				strings.NewReader(tc.expectedMetrics),
				"cortex_distributor_received_samples_total",
				"cortex_distributor_received_exemplars_total",
				"cortex_distributor_received_metadata_total",
				"cortex_distributor_samples_in_total",
				"cortex_distributor_exemplars_in_total",
				"cortex_distributor_metadata_in_total",
			))
		})
	}
}

// getIngestedMetrics takes a mock ingester and returns all the metric names which it has ingested.
func getIngestedMetrics(ctx context.Context, t *testing.T, ingester *mockIngester) []string {
	labelsClient, err := ingester.LabelNamesAndValues(ctx, nil)
	assert.NoError(t, err)

	labels, err := labelsClient.Recv()
	assert.NoError(t, err)

	resultsUniq := make(map[string]struct{}, len(labels.Items))
	for _, label := range labels.Items {
		if label.LabelName == "__name__" {
			for _, value := range label.Values {
				resultsUniq[value] = struct{}{}
			}
		}
	}

	results := make([]string, 0, len(resultsUniq))
	for result := range resultsUniq {
		results = append(results, result)
	}

	return results
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
		zonesResponseDelay: map[string]time.Duration{
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
			labels.Labels{{Name: labels.MetricName, Value: "metric" + strconv.Itoa(i)}}, 1, int64(100000 + i),
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
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "reason", Value: "broken"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	tests := map[string]struct {
		labelNames                []model.LabelName
		matchers                  []*labels.Matcher
		ingestersSeriesCountTotal uint64
		expectedResult            *client.LabelValuesCardinalityResponse
		expectedIngesters         int
		expectedSeriesCountTotal  uint64
		ingesterZones             []string
	}{
		"should return an empty map if no label names": {
			labelNames:                []model.LabelName{},
			matchers:                  []*labels.Matcher{},
			ingestersSeriesCountTotal: 0,
			expectedResult:            &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
			expectedIngesters:         numIngesters,
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
			expectedIngesters:        numIngesters,
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
			expectedSeriesCountTotal: 100,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create distributor
			ds, ingesters, _ := prepare(t, prepConfig{
				numIngesters:              numIngesters,
				happyIngesters:            numIngesters,
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
				seriesCountTotal, cardinalityMap, err := ds[0].LabelValuesCardinality(ctx, testData.labelNames, testData.matchers)
				require.NoError(t, err)
				assert.Equal(t, testData.expectedSeriesCountTotal, seriesCountTotal)
				// Make sure the resultant label names are sorted
				sort.Slice(cardinalityMap.Items, func(l, r int) bool {
					return cardinalityMap.Items[l].LabelName < cardinalityMap.Items[r].LabelName
				})
				return cardinalityMap
			})

			// Make sure all the ingesters have been queried
			assert.Equal(t, testData.expectedIngesters, countMockIngestersCalls(ingesters, "LabelValuesCardinality"))
		})
	}
}

func TestDistributor_LabelValuesCardinalityLimit(t *testing.T) {
	fixtures := []struct {
		labels    labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "reason", Value: "broken"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
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

			_, _, err := ds[0].LabelValuesCardinality(ctx, testData.labelNames, []*labels.Matcher{})
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

		_, _, err := ds[0].LabelValuesCardinality(ctx, []model.LabelName{labels.MetricName}, []*labels.Matcher{})
		require.Error(t, err)
	})
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

	return mimirpb.ToWriteRequest([]labels.Labels{lbls}, samples, nil, nil, mimirpb.API)
}

type prepConfig struct {
	numIngesters, happyIngesters int
	queryDelay                   time.Duration
	shuffleShardSize             int
	limits                       *validation.Limits
	numDistributors              int
	skipLabelNameValidation      bool
	maxInflightRequests          int
	maxInflightRequestsBytes     int
	maxIngestionRate             float64
	replicationFactor            int
	enableTracker                bool
	ingestersSeriesCountTotal    uint64
	ingesterZones                []string
	zonesResponseDelay           map[string]time.Duration
	forwarding                   bool
}

func prepare(t *testing.T, cfg prepConfig) ([]*Distributor, []mockIngester, []*prometheus.Registry) {
	ingesters := []mockIngester{}
	for i := 0; i < cfg.happyIngesters; i++ {
		zone := ""
		if len(cfg.ingesterZones) > 0 {
			zone = cfg.ingesterZones[i%len(cfg.ingesterZones)]
		}
		var responseDelay time.Duration
		if len(cfg.zonesResponseDelay) > 0 {
			responseDelay = cfg.zonesResponseDelay[zone]
		}
		ingesters = append(ingesters, mockIngester{
			happy:            true,
			queryDelay:       cfg.queryDelay,
			seriesCountTotal: cfg.ingestersSeriesCountTotal,
			zone:             zone,
			responseDelay:    responseDelay,
		})
	}
	for i := cfg.happyIngesters; i < cfg.numIngesters; i++ {
		ingesters = append(ingesters, mockIngester{
			queryDelay:       cfg.queryDelay,
			seriesCountTotal: cfg.ingestersSeriesCountTotal,
		})
	}

	// Use a real ring with a mock KV store to test ring RF logic.
	ingesterDescs := map[string]ring.InstanceDesc{}
	ingestersByAddr := map[string]*mockIngester{}
	for i := range ingesters {
		addr := fmt.Sprintf("%d", i)
		ingesterDescs[addr] = ring.InstanceDesc{
			Addr:                addr,
			Zone:                ingesters[i].zone,
			State:               ring.ACTIVE,
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(),
			Tokens:              []uint32{uint32((math.MaxUint32 / cfg.numIngesters) * i)},
		}
		ingestersByAddr[addr] = &ingesters[i]
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

	factory := func(addr string) (ring_client.PoolClient, error) {
		return ingestersByAddr[addr], nil
	}

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
		distributorCfg.DistributorRing.HeartbeatPeriod = 100 * time.Millisecond
		distributorCfg.DistributorRing.InstanceID = strconv.Itoa(i)
		distributorCfg.DistributorRing.KVStore.Mock = kvStore
		distributorCfg.DistributorRing.InstanceAddr = "127.0.0.1"
		distributorCfg.SkipLabelNameValidation = cfg.skipLabelNameValidation
		distributorCfg.InstanceLimits.MaxInflightPushRequests = cfg.maxInflightRequests
		distributorCfg.InstanceLimits.MaxInflightPushRequestsBytes = cfg.maxInflightRequestsBytes
		distributorCfg.InstanceLimits.MaxIngestionRate = cfg.maxIngestionRate
		distributorCfg.ShuffleShardingLookbackPeriod = time.Hour

		if cfg.forwarding {
			distributorCfg.Forwarding.Enabled = true
			distributorCfg.Forwarding.RequestTimeout = 10 * time.Second
		}

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
			cfg.limits.HAMaxClusters = 100
		}

		overrides, err := validation.NewOverrides(*cfg.limits, nil)
		require.NoError(t, err)

		reg := prometheus.NewPedanticRegistry()
		d, err := New(distributorCfg, clientConfig, overrides, ingestersRing, true, reg, log.NewNopLogger())
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

func makeWriteRequest(startTimestampMs int64, samples int, metadata int, exemplars bool, metrics ...string) *mimirpb.WriteRequest {
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

func makeWriteRequestHA(samples int, replica, cluster string) *mimirpb.WriteRequest {
	request := &mimirpb.WriteRequest{}
	for i := 0; i < samples; i++ {
		ts := mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "__name__", Value: "foo"},
					{Name: "__replica__", Value: replica},
					{Name: "bar", Value: "baz"},
					{Name: "cluster", Value: cluster},
					{Name: "sample", Value: fmt.Sprintf("%d", i)},
				},
			},
		}
		ts.Samples = []mimirpb.Sample{
			{
				Value:       float64(i),
				TimestampMs: int64(i),
			},
		}
		request.Timeseries = append(request.Timeseries, ts)
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

func expectedResponse(start, end int) model.Matrix {
	result := model.Matrix{}
	for i := start; i < end; i++ {
		result = append(result, &model.SampleStream{
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
		})
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
	happy            bool
	stats            client.UsersStatsResponse
	timeseries       map[uint32]*mimirpb.PreallocTimeseries
	metadata         map[uint32]map[mimirpb.MetricMetadata]struct{}
	queryDelay       time.Duration
	calls            map[string]int
	seriesCountTotal uint64
	zone             string
	responseDelay    time.Duration
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

func (i *mockIngester) Check(ctx context.Context, in *grpc_health_v1.HealthCheckRequest, opts ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("Check")

	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func (i *mockIngester) Close() error {
	return nil
}

func (i *mockIngester) Push(ctx context.Context, req *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("Push")

	if !i.happy {
		return nil, errFail
	}

	if i.timeseries == nil {
		i.timeseries = map[uint32]*mimirpb.PreallocTimeseries{}
	}

	if i.metadata == nil {
		i.metadata = map[uint32]map[mimirpb.MetricMetadata]struct{}{}
	}

	orgid, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	for j := range req.Timeseries {
		series := req.Timeseries[j]
		hash := shardByAllLabels(orgid, series.Labels)
		existing, ok := i.timeseries[hash]
		if !ok {
			// Make a copy because the request Timeseries are reused
			item := mimirpb.TimeSeries{
				Labels:  make([]mimirpb.LabelAdapter, len(series.TimeSeries.Labels)),
				Samples: make([]mimirpb.Sample, len(series.TimeSeries.Samples)),
			}

			copy(item.Labels, series.TimeSeries.Labels)
			copy(item.Samples, series.TimeSeries.Samples)

			i.timeseries[hash] = &mimirpb.PreallocTimeseries{TimeSeries: &item}
		} else {
			existing.Samples = append(existing.Samples, series.Samples...)
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

func (i *mockIngester) QueryStream(ctx context.Context, req *client.QueryRequest, opts ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
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

	results := []*client.QueryStreamResponse{}
	for _, ts := range i.timeseries {
		if !match(ts.Labels, matchers) {
			continue
		}

		c, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
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

		wireChunks := []client.Chunk{}
		for _, c := range chunks {
			var buf bytes.Buffer
			chunk := client.Chunk{
				Encoding: int32(c.Encoding()),
			}
			if err := c.Marshal(&buf); err != nil {
				panic(err)
			}
			chunk.Data = buf.Bytes()
			wireChunks = append(wireChunks, chunk)
		}

		results = append(results, &client.QueryStreamResponse{
			Chunkseries: []client.TimeSeriesChunk{
				{
					Labels: ts.Labels,
					Chunks: wireChunks,
				},
			},
		})
	}
	return &stream{
		results: results,
	}, nil
}

func (i *mockIngester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest, opts ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	i.Lock()
	defer i.Unlock()

	i.trackCall("MetricsForLabelMatchers")

	if !i.happy {
		return nil, errFail
	}

	_, _, multiMatchers, err := client.FromMetricsForLabelMatchersRequest(req)
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

func (i *mockIngester) LabelNames(ctx context.Context, req *client.LabelNamesRequest, opts ...grpc.CallOption) (*client.LabelNamesResponse, error) {
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
	sort.Strings(response.LabelNames)

	return &response, nil
}

func (i *mockIngester) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest, opts ...grpc.CallOption) (*client.MetricsMetadataResponse, error) {
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
	return &labelNamesAndValuesMockStream{responses: []*client.LabelNamesAndValuesResponse{resp}, responseDelay: i.responseDelay}, nil
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

func (i *mockIngester) LabelValuesCardinality(ctx context.Context, req *client.LabelValuesCardinalityRequest, opts ...grpc.CallOption) (client.Ingester_LabelValuesCardinalityClient, error) {
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

func (i *noopIngester) Push(ctx context.Context, req *mimirpb.WriteRequest, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
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

func (i *mockIngester) AllUserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}

func (i *mockIngester) UserStats(ctx context.Context, in *client.UserStatsRequest, opts ...grpc.CallOption) (*client.UserStatsResponse, error) {
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

func setMockForwarder(distributor *Distributor, ingest bool) *mockForwarder {
	forwarder := &mockForwarder{ingest: ingest}
	distributor.forwarder = forwarder
	return forwarder
}

type mockForwarder struct {
	ingest    bool
	sendCount atomic.Uint32
}

func (m *mockForwarder) NewRequest(ctx context.Context, tenant string, _ validation.ForwardingRules) forwarding.Request {
	return &mockForwardingRequest{forwarder: m}
}

type mockForwardingRequest struct {
	forwarder *mockForwarder
}

func (m *mockForwardingRequest) Add(sample mimirpb.PreallocTimeseries) bool {
	return m.forwarder.ingest
}

func (m *mockForwardingRequest) Send(ctx context.Context) <-chan error {
	m.forwarder.sendCount.Inc()

	errCh := make(chan error)
	close(errCh)
	return errCh
}

func TestDistributorValidation(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "1")
	now := model.Now()
	future, past := now.Add(5*time.Hour), now.Add(-25*time.Hour)

	for i, tc := range []struct {
		metadata           []*mimirpb.MetricMetadata
		labels             []labels.Labels
		samples            []mimirpb.Sample
		exemplars          []*mimirpb.Exemplar
		expectedStatusCode int32
		expectedErr        string
	}{
		// Test validation passes.
		{
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "testmetric", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
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

		// Test validation fails for samples from the future.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(future),
				Value:       4,
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        fmt.Sprintf(`received a sample whose timestamp is too far in the future, timestamp: %d series: 'testmetric' (err-mimir-too-far-in-future)`, future),
		},

		// Test maximum labels names per series.
		{
			labels: []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        `received a series whose number of labels exceeds the limit (actual: 3, limit: 2) series: 'testmetric{foo2="bar2", foo="bar"}'`,
		},
		// Test multiple validation fails return the first one.
		{
			labels: []labels.Labels{
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}},
				{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}},
			},
			samples: []mimirpb.Sample{
				{TimestampMs: int64(now), Value: 2},
				{TimestampMs: int64(past), Value: 2},
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        `received a series whose number of labels exceeds the limit (actual: 3, limit: 2) series: 'testmetric{foo2="bar2", foo="bar"}'`,
		},
		// Test metadata validation fails
		{
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        `received a metric metadata with no metric name`,
		},
		// Test empty exemplar labels fails.
		{
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "testmetric", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   []labels.Labels{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			exemplars: []*mimirpb.Exemplar{{
				Labels:      nil,
				TimestampMs: int64(now),
				Value:       1,
			}},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        fmt.Sprintf("received an exemplar with no valid labels, timestamp: %d series: %+v labels: {}", now, labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}),
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)

			limits.CreationGracePeriod = model.Duration(2 * time.Hour)
			limits.MaxLabelNamesPerSeries = 2

			ds, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          &limits,
			})

			_, err := ds[0].Push(ctx, mimirpb.ToWriteRequest(tc.labels, tc.samples, tc.exemplars, tc.metadata, mimirpb.API))
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				res, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok)
				require.Equal(t, tc.expectedStatusCode, res.Code)
				require.Contains(t, string(res.GetBody()), tc.expectedErr)
			}
		})
	}
}

func TestRemoveReplicaLabel(t *testing.T) {
	replicaLabel := "replica"
	clusterLabel := "cluster"
	cases := []struct {
		labelsIn  []mimirpb.LabelAdapter
		labelsOut []mimirpb.LabelAdapter
	}{
		// Replica label is present
		{
			labelsIn: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "replica", Value: replicaLabel},
			},
			labelsOut: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
			},
		},
		// Replica label is not present
		{
			labelsIn: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "cluster", Value: clusterLabel},
			},
			labelsOut: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: "foo"},
				{Name: "bar", Value: "baz"},
				{Name: "sample", Value: "1"},
				{Name: "cluster", Value: clusterLabel},
			},
		},
	}

	for _, c := range cases {
		removeLabel(replicaLabel, &c.labelsIn)
		assert.Equal(t, c.labelsOut, c.labelsIn)
	}
}

// This is not great, but we deal with unsorted labels when validating labels.
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

func TestSortLabels(t *testing.T) {
	sorted := []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "bar", Value: "baz"},
		{Name: "cluster", Value: "cluster"},
		{Name: "sample", Value: "1"},
	}

	// no allocations if input is already sorted
	require.Equal(t, 0.0, testing.AllocsPerRun(100, func() {
		sortLabelsIfNeeded(sorted)
	}))

	unsorted := []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "foo"},
		{Name: "sample", Value: "1"},
		{Name: "cluster", Value: "cluster"},
		{Name: "bar", Value: "baz"},
	}

	sortLabelsIfNeeded(unsorted)

	sort.SliceIsSorted(unsorted, func(i, j int) bool {
		return unsorted[i].Name < unsorted[j].Name
	})
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
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "foo"},
				{Name: "cluster", Value: "one"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "foo"},
				{Name: "cluster", Value: "one"},
			},
		},
		{
			inputSeries: labels.Labels{
				{Name: "__name__", Value: "foo"},
				{Name: "cluster", Value: "one"},
			},
			expectedSeries: labels.Labels{
				{Name: "__name__", Value: "foo"},
				{Name: "cluster", Value: "two"},
			},
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
