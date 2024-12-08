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
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcutil"
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
	promtestutil "github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	util_math "github.com/grafana/mimir/pkg/util/math"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
)

var (
	errFail                    = status.Error(codes.Internal, "Fail")
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
	ctx := user.InjectOrgID(context.Background(), "user")

	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	type samplesIn struct {
		num              int
		startTimestampMs int64
	}
	for name, tc := range map[string]struct {
		metricNames           []string
		numIngesters          int
		happyIngesters        int
		samples               samplesIn
		metadata              int
		expectedErrorContains []string
		expectedGRPCError     *status.Status
		expectedErrorDetails  *mimirpb.ErrorDetails
		expectedMetrics       string
		timeOut               bool
		configure             func(*Config)
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
			numIngesters:          3,
			happyIngesters:        1,
			samples:               samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedErrorContains: []string{"Internal", "failed pushing to ingester"},
			metricNames:           []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
		"A push to 0 happy ingesters should fail": {
			numIngesters:          3,
			happyIngesters:        0,
			samples:               samplesIn{num: 10, startTimestampMs: 123456789000},
			expectedErrorContains: []string{"Internal", "failed pushing to ingester"},
			metricNames:           []string{lastSeenTimestamp},
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
			expectedErrorDetails: &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED},
			metricNames:          []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.024
			`,
		},
		"A timed out push should fail": {
			numIngesters:          3,
			happyIngesters:        3,
			samples:               samplesIn{num: 10, startTimestampMs: 123456789000},
			timeOut:               true,
			expectedErrorContains: []string{"exceeded configured distributor remote timeout", "failed pushing to ingester", "context deadline exceeded"},
			metricNames:           []string{lastSeenTimestamp},
			expectedMetrics: `
				# HELP cortex_distributor_latest_seen_sample_timestamp_seconds Unix timestamp of latest received sample per user.
				# TYPE cortex_distributor_latest_seen_sample_timestamp_seconds gauge
				cortex_distributor_latest_seen_sample_timestamp_seconds{user="user"} 123456789.009
			`,
		},
		"A push to 3 happy ingesters using batch worker gouroutines should succeed": {
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
			configure: func(cfg *Config) {
				// 2 workers, so 1 push would need to spawn a new goroutine.
				cfg.ReusableIngesterPushWorkers = 2
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.IngestionRate = 20
			limits.IngestionBurstSize = 20

			ds, _, regs, _ := prepare(t, prepConfig{
				numIngesters:    tc.numIngesters,
				happyIngesters:  tc.happyIngesters,
				numDistributors: 1,
				limits:          limits,
				timeOut:         tc.timeOut,
				configure:       tc.configure,
			})

			request := makeWriteRequest(tc.samples.startTimestampMs, tc.samples.num, tc.metadata, false, true, "foo")
			response, err := ds[0].Push(ctx, request)

			if tc.expectedErrorContains == nil && tc.expectedGRPCError == nil {
				require.NoError(t, err)
				assert.Equal(t, emptyResponse, response)
			} else {
				assert.Nil(t, response)

				if tc.expectedGRPCError == nil {
					for _, msg := range tc.expectedErrorContains {
						assert.ErrorContains(t, err, msg)
					}
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

func TestDistributor_PushWithDoBatchWorkers(t *testing.T) {
	limits := prepareDefaultLimits()
	limits.IngestionRate = 20
	limits.IngestionBurstSize = 20

	ds, _, _, _ := prepare(t, prepConfig{
		numIngesters:    3,
		happyIngesters:  3,
		numDistributors: 1,
		limits:          limits,
		configure: func(cfg *Config) {
			// 2 workers, so 1 push would need to spawn a new goroutine.
			cfg.ReusableIngesterPushWorkers = 2
		},
	})
	require.Len(t, ds, 1)
	distributor := ds[0]

	require.NotNil(t, distributor.doBatchPushWorkers)
	counter := atomic.NewInt64(0)
	originalIngesterDoBatchPushWorkers := distributor.doBatchPushWorkers
	distributor.doBatchPushWorkers = func(f func()) {
		counter.Inc()
		originalIngesterDoBatchPushWorkers(f)
	}

	request := makeWriteRequest(123456789000, 3, 5, false, false, "foo")
	ctx := user.InjectOrgID(context.Background(), "user")
	response, err := distributor.Push(ctx, request)

	require.NoError(t, err)
	require.Equal(t, emptyResponse, response)
	require.GreaterOrEqual(t, counter.Load(), int64(3))
}

func TestDistributor_ContextCanceledRequest(t *testing.T) {
	now := time.Now()
	mtime.NowForce(now)
	t.Cleanup(mtime.NowReset)

	ds, ings, _, _ := prepare(t, prepConfig{
		numIngesters:    3,
		happyIngesters:  3,
		numDistributors: 1,
	})

	// Lock all mockIngester instances, so they will be waiting
	for _, ing := range ings {
		ing.Lock()
		defer func(ing *mockIngester) {
			ing.Unlock()
		}(ing)
	}

	ctx := user.InjectOrgID(context.Background(), "user")
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	request := makeWriteRequest(123456789000, 1, 1, false, true, "foo")
	_, err := ds[0].Push(ctx, request)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestDistributor_MetricsCleanup(t *testing.T) {
	dists, _, regs, _ := prepare(t, prepConfig{
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
		"cortex_distributor_label_values_with_newlines_total",
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
	d.labelValuesWithNewlinesPerUser.WithLabelValues("userA").Inc()

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

		# HELP cortex_distributor_label_values_with_newlines_total Total number of label values with newlines seen at ingestion time.
		# TYPE cortex_distributor_label_values_with_newlines_total counter
		cortex_distributor_label_values_with_newlines_total{user="userA"} 1
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

		# HELP cortex_distributor_label_values_with_newlines_total Total number of label values with newlines seen at ingestion time.
		# TYPE cortex_distributor_label_values_with_newlines_total counter
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

	expectedDetails := &mimirpb.ErrorDetails{Cause: mimirpb.REQUEST_RATE_LIMITED}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.RequestRate = testData.requestRate
			limits.RequestBurstSize = testData.requestBurstSize
			limits.ServiceOverloadStatusCodeOnRateLimitEnabled = testData.enableServiceOverloadError

			// Start all expected distributors
			distributors, _, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: testData.distributors,
				limits:          limits,
			})

			// Send multiple requests to the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, 1, 1, false, true, "foo")
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
		distributors         int
		ingestionRate        float64
		ingestionBurstSize   int
		ingestionBurstFactor float64
		pushes               []testPush
	}{
		"evenly share the ingestion limit across distributors": {
			distributors:         2,
			ingestionRate:        10,
			ingestionBurstSize:   5,
			ingestionBurstFactor: 0,
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
			distributors:         2,
			ingestionRate:        10,
			ingestionBurstSize:   20,
			ingestionBurstFactor: 0,
			pushes: []testPush{
				{samples: 10, expectedError: nil},
				{samples: 5, expectedError: nil},
				{samples: 5, metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
				{metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
			},
		},
		"evenly share the ingestion burst limit across distributors": {
			distributors:         2,
			ingestionRate:        10,
			ingestionBurstFactor: 4,
			// This is equivalent to the test above because the ingestion rate and burst are per distributor with the burst factor meaning the burst would be:
			// (10 (ingest rate) / 2 (number of distributors)) * 4 (burst factor)= 20 burst per distributor
			pushes: []testPush{
				{samples: 10, expectedError: nil},
				{samples: 5, expectedError: nil},
				{samples: 5, metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 40).Error())},
				{samples: 5, expectedError: nil},
				{samples: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 40).Error())},
				{metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 40).Error())},
			},
		},
		"Test burstFactor burst limit in one burst": {
			distributors:         2,
			ingestionRate:        10,
			ingestionBurstFactor: 2,
			pushes: []testPush{
				// Burst is 10 for the distributor (10/2)*2 = 10
				{samples: 10, expectedError: nil},
				// We've drained the pool so this should fail until the bucket re-fills in a few seconds
				{samples: 1, metadata: 1, expectedError: status.New(codes.ResourceExhausted, newIngestionRateLimitedError(10, 20).Error())},
			},
		},
	}

	expectedErrorDetails := &mimirpb.ErrorDetails{Cause: mimirpb.INGESTION_RATE_LIMITED}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.IngestionRate = testData.ingestionRate
			limits.IngestionBurstSize = testData.ingestionBurstSize
			limits.IngestionBurstFactor = testData.ingestionBurstFactor

			// Start all expected distributors
			distributors, _, _, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: testData.distributors,
				limits:          limits,
			})

			// Push samples in multiple requests to only the first distributor
			for _, push := range testData.pushes {
				request := makeWriteRequest(0, push.samples, push.metadata, false, false, "foo")
				response, err := distributors[0].Push(ctx, request)

				if push.expectedError == nil {
					assert.Equal(t, emptyResponse, response, "Received error when a successful write was expected.")
					assert.Nil(t, err)
				} else {
					assert.Nil(t, response, "Received successful write response when an error was expected.")
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
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()

			// Start all expected distributors
			distributors, _, regs, _ := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          limits,
				configure: func(config *Config) {
					config.DefaultLimits.MaxIngestionRate = testData.ingestionRateLimit
					config.DefaultLimits.MaxInflightPushRequests = testData.inflightLimit
					config.DefaultLimits.MaxInflightPushRequestsBytes = testData.inflightBytesLimit
				},
			})

			d := distributors[0]
			d.inflightPushRequests.Add(int64(testData.preInflight))
			d.ingestionRate.Add(int64(testData.preRateSamples))

			d.ingestionRate.Tick()

			for _, push := range testData.pushes {
				request := makeWriteRequest(0, push.samples, push.metadata, false, false, "foo")
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
		expectedDetails  *mimirpb.ErrorDetails
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
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH},
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
			expectedError:   status.New(codes.InvalidArgument, fmt.Sprintf(labelValueTooLongMsgFormat, "__replica__", "instance1234567890123456789012345678901234567890", mimirpb.FromLabelAdaptersToString(labelSetGenWithReplicaAndCluster("instance1234567890123456789012345678901234567890", "cluster0")(0)))),
			expectedDetails: &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.AcceptHASamples = true
			limits.MaxLabelValueLength = 15

			ds, _, _, _ := prepare(t, prepConfig{
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
	const metricName = "foo"
	ctx := user.InjectOrgID(context.Background(), "user")
	nameMatcher := mustEqualMatcher(model.MetricNameLabel, metricName)
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
					expectedResponse:  expectedResponse(0, 10, true, metricName),
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
					expectedResponse:  expectedResponse(0, 0, true, metricName),
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
						expectedResponse:  expectedResponse(i, i+1, true, metricName),
						expectedIngesters: expectedIngesters,
						shuffleShardSize:  shuffleShardSize,
					})
				}
			}
		}
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := prepConfig{
				numIngesters:    tc.numIngesters,
				happyIngesters:  tc.happyIngesters,
				numDistributors: 1,
			}

			cfg.shuffleShardSize = tc.shuffleShardSize

			ds, ingesters, reg, _ := prepare(t, cfg)

			request := makeWriteRequest(0, tc.samples, tc.metadata, false, true, metricName)
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
				_, ok := grpcutil.ErrorToStatus(err)
				assert.True(t, ok, fmt.Sprintf("expected error to be a status error, but got: %T", err))
			}

			var m model.Matrix
			if len(resp.Chunkseries) == 0 {
				m, err = client.StreamingSeriesToMatrix(0, 10, resp.StreamingSeries)
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
				assert.Contains(t, []int{tc.expectedIngesters, tc.expectedIngesters - 1}, countMockIngestersCalled(ingesters, "QueryStream"))
			}
		})
	}
}

// Helper function to generate LabelAdapter slice from string pairs.
func labelAdapters(ss ...string) []mimirpb.LabelAdapter {
	if len(ss)%2 != 0 {
		panic("invalid number of strings")
	}
	res := make([]mimirpb.LabelAdapter, 0, len(ss)/2)
	for i := 0; i < len(ss); i += 2 {
		res = append(res, mimirpb.LabelAdapter{Name: ss[i], Value: ss[i+1]})
	}
	slices.SortFunc(res, func(a, b mimirpb.LabelAdapter) int { return strings.Compare(a.Name, b.Name) })
	return res
}

func TestDistributor_Push_LabelRemoval(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries    []mimirpb.LabelAdapter
		expectedSeries []mimirpb.LabelAdapter
		removeReplica  bool
		removeLabels   []string
	}

	cases := []testcase{
		// Remove both cluster and replica label.
		{
			removeReplica:  true,
			removeLabels:   []string{"cluster"},
			inputSeries:    labelAdapters("__name__", "some_metric", "cluster", "one", "__replica__", "two"),
			expectedSeries: labelAdapters("__name__", "some_metric"),
		},
		// Remove multiple labels and replica.
		{
			removeReplica: true,
			removeLabels:  []string{"foo", "some"},
			inputSeries: labelAdapters("__name__", "some_metric", "cluster", "one", "__replica__", "two",
				"foo", "bar", "some", "thing"),
			expectedSeries: labelAdapters("__name__", "some_metric", "cluster", "one"),
		},
		// Remove blank labels.
		{
			inputSeries:    labelAdapters("__name__", "some_metric", "blank", "", "foo", "bar"),
			expectedSeries: labelAdapters("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labelAdapters("__name__", "some_metric", "foo", "bar", "zzz_blank", ""),
			expectedSeries: labelAdapters("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labelAdapters("__blank__", "", "__name__", "some_metric", "foo", "bar"),
			expectedSeries: labelAdapters("__name__", "some_metric", "foo", "bar"),
		},
		{
			inputSeries:    labelAdapters("__blank__", "", "__name__", "some_metric", "foo", "bar", "zzz_blank", ""),
			expectedSeries: labelAdapters("__name__", "some_metric", "foo", "bar"),
		},
		// Don't remove any labels.
		{
			removeReplica:  false,
			inputSeries:    labelAdapters("__name__", "some_metric", "__replica__", "two", "cluster", "one"),
			expectedSeries: labelAdapters("__name__", "some_metric", "__replica__", "two", "cluster", "one"),
		},
	}

	for _, tc := range cases {
		var err error
		var limits validation.Limits
		flagext.DefaultValues(&limits)
		limits.DropLabels = tc.removeLabels
		limits.AcceptHASamples = tc.removeReplica

		ds, ingesters, _, _ := prepare(t, prepConfig{
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
				assert.Equal(t, tc.expectedSeries, v.Labels)
			}
		}
	}
}

func TestDistributor_Push_ShouldGuaranteeShardingTokenConsistencyOverTheTime(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")
	tests := map[string]struct {
		inputSeries    []mimirpb.LabelAdapter
		expectedSeries []mimirpb.LabelAdapter
		expectedToken  uint32
	}{
		"metric_1 with value_1": {
			inputSeries:    labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedSeries: labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped label due to config": {
			inputSeries: labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1",
				"dropped", "unused"),
			expectedSeries: labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_1 with value_1 and dropped HA replica label": {
			inputSeries: labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1",
				"__replica__", "replica_1"),
			expectedSeries: labelAdapters("__name__", "metric_1", "cluster", "cluster_1", "key", "value_1"),
			expectedToken:  0xec0a2e9d,
		},
		"metric_2 with value_1": {
			inputSeries:    labelAdapters("__name__", "metric_2", "key", "value_1"),
			expectedSeries: labelAdapters("__name__", "metric_2", "key", "value_1"),
			expectedToken:  0xa60906f2,
		},
		"metric_1 with value_2": {
			inputSeries:    labelAdapters("__name__", "metric_1", "key", "value_2"),
			expectedSeries: labelAdapters("__name__", "metric_1", "key", "value_2"),
			expectedToken:  0x18abc8a2,
		},
	}

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.DropLabels = []string{"dropped"}
	limits.AcceptHASamples = true

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			ds, ingesters, _, _ := prepare(t, prepConfig{
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
				assert.Equal(t, testData.expectedSeries, series.Labels)
			}
		})
	}
}

func TestDistributor_Push_LabelNameValidation(t *testing.T) {
	inputLabels := labelAdapters(model.MetricNameLabel, "foo", "999.illegal", "baz")
	ctx := user.InjectOrgID(context.Background(), "user")

	tests := map[string]struct {
		inputLabels                []mimirpb.LabelAdapter
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
		"UTF-8 characters are not accepted": {
			inputLabels: labelAdapters(model.MetricNameLabel, "foo", "😊", "smile!"),
			errExpected: true,
			errMessage:  `received a series with an invalid label: '😊' series: 'foo{😊="smile!"}' (err-mimir-label-invalid)`,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			ds, _, _, _ := prepare(t, prepConfig{
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shuffleShardSize: 0,
				configure: func(config *Config) {
					config.SkipLabelValidation = tc.skipLabelNameValidationCfg
				},
			})
			req := mockWriteRequest(tc.inputLabels, 42, 100000)
			req.SkipLabelValidation = tc.skipLabelNameValidationReq
			_, err := ds[0].Push(ctx, req)
			if tc.errExpected {
				fromError, _ := grpcutil.ErrorToStatus(err)
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
		req            *mimirpb.WriteRequest
		expectedDrop   bool
		expectedErrMsg string
		expectedErrID  globalerror.ID
	}{
		"valid exemplar": {
			req: makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, 1, []string{"foo", "bar"}),
		},
		"drops exemplar with no labels": {
			req:          makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, 1, []string{}),
			expectedDrop: true,
		},
		"drops exemplar with no timestamp": {
			req:          makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 0, 1, []string{"foo", "bar"}),
			expectedDrop: true,
		},
		"drops exemplar with too long labelset": {
			req:          makeWriteRequestExemplar([]string{model.MetricNameLabel, "test"}, 1000, 1, []string{"foo", strings.Repeat("0", 126)}),
			expectedDrop: true,
		},
		"rejects exemplar with too many series labels": {
			req:            makeWriteRequestExemplar(manyLabels, 0, 1, nil),
			expectedErrMsg: "received a series whose number of labels exceeds the limit",
			expectedErrID:  globalerror.MaxLabelNamesPerSeries,
		},
		"rejects exemplar with duplicate series labels": {
			req:            makeWriteRequestExemplar([]string{model.MetricNameLabel, "test", "foo", "bar", "foo", "bar"}, 0, 1, nil),
			expectedErrMsg: "received a series with duplicate label name",
			expectedErrID:  globalerror.SeriesWithDuplicateLabelNames,
		},
		"rejects exemplar with empty series label name": {
			req:            makeWriteRequestExemplar([]string{model.MetricNameLabel, "test", "", "bar"}, 0, 1, nil),
			expectedErrMsg: "received a series with an invalid label",
			expectedErrID:  globalerror.SeriesInvalidLabel,
		},
	}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			// Pass a copy of the reference request, since Push may modify it during cleanup.
			reqBytes, err := tc.req.Marshal()
			require.NoError(t, err)
			reqCopy := &mimirpb.WriteRequest{}
			require.NoError(t, reqCopy.Unmarshal(reqBytes))

			expectedSamples := tc.req.Timeseries[0].Samples
			expectedExemplars := tc.req.Timeseries[0].Exemplars

			limits := prepareDefaultLimits()
			limits.MaxGlobalExemplarsPerUser = 10
			ds, ingesters, _, _ := prepare(t, prepConfig{
				limits:           limits,
				numIngesters:     2,
				happyIngesters:   2,
				numDistributors:  1,
				shuffleShardSize: 0,
			})
			_, err = ds[0].Push(ctx, reqCopy)
			if tc.expectedErrMsg != "" {
				require.Error(t, err)
				fromError, _ := grpcutil.ErrorToStatus(err)
				assert.Contains(t, fromError.Message(), tc.expectedErrMsg)
				assert.Contains(t, fromError.Message(), tc.expectedErrID)
				return
			}

			require.NoError(t, err)
			for _, i := range ingesters {
				ss := i.series()
				require.Len(t, ss, 1)
				for _, s := range ss {
					require.Equal(t, expectedSamples, s.Samples)
					if !tc.expectedDrop {
						require.Equal(t, expectedExemplars, s.Exemplars)
					} else {
						require.Empty(t, s.Exemplars)
					}
				}
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(sampleTimestampTooNewMsgFormat, math.MaxInt64, "test")),
		},
		"valid float histogram": {
			req: makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, 1000, generateTestFloatHistogram(0)),
		},
		"too new float histogram": {
			req:         makeWriteRequestFloatHistogram([]string{model.MetricNameLabel, "test"}, math.MaxInt64, generateTestFloatHistogram(0)),
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(sampleTimestampTooNewMsgFormat, math.MaxInt64, "test")),
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(maxNativeHistogramBucketsMsgFormat, 1000, "test", 8, 7)),
		},
	}

	expectedDetails := &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA}

	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()
			limits.CreationGracePeriod = model.Duration(time.Minute)
			limits.MaxNativeHistogramBuckets = tc.bucketLimit
			limits.ReduceNativeHistogramOverMaxBuckets = false

			ds, _, _, _ := prepare(t, prepConfig{
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
		})
	}
}

func TestDistributor_SampleDuplicateTimestamp(t *testing.T) {
	const (
		metricName         = "series"
		ts1        int64   = 10
		ts2        int64   = 20
		ts3        int64   = 30
		ts4        int64   = 40
		v1         float64 = 1
		v2         float64 = 2
		v3         float64 = 3
		v4         float64 = 4
	)

	labels := []string{labels.MetricName, metricName, "job", "job", "service", "service"}

	testCases := map[string]struct {
		req             *mimirpb.WriteRequest
		expectedSamples []mimirpb.PreallocTimeseries
		expectedErrors  []error
		expectedMetrics string
	}{
		"do not deduplicate if there are no duplicated timestamps": {
			req: makeWriteRequestWith(
				makeTimeseries(labels, makeSamples(ts1, v1), nil, nil),
				makeTimeseries(labels, makeSamples(ts2, v2), nil, nil),
			),
			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, makeSamples(ts1, v1), nil, nil),
				makeTimeseries(labels, makeSamples(ts2, v2), nil, nil),
			},
		},
		"deduplicate duplicated timestamps within a single timeseries, and return the first error encountered": {
			req: makeWriteRequestWith(
				makeTimeseries(labels, append(makeSamples(ts1, v1), append(makeSamples(ts2, v2), append(makeSamples(ts1, v3), makeSamples(ts2, v4)...)...)...), nil, nil),
				makeTimeseries(labels, nil, append(makeHistograms(ts3, generateTestHistogram(0)), append(makeHistograms(ts4, generateTestHistogram(1)), append(makeHistograms(ts4, generateTestHistogram(2)), makeHistograms(ts3, generateTestHistogram(3))...)...)...), nil),
			),
			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, append(makeSamples(ts1, v1), makeSamples(ts2, v2)...), nil, nil),
				makeTimeseries(labels, nil, append(makeHistograms(ts3, generateTestHistogram(0)), makeHistograms(ts4, generateTestHistogram(1))...), nil),
			},
			expectedErrors: []error{
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 2, metricName),
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 2, metricName),
			},
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="test-group",reason="sample_duplicate_timestamp",user="user"} 4
			`,
		},
		"do not deduplicate duplicated timestamps in different timeseries": {
			req: makeWriteRequestWith(
				makeTimeseries(labels, append(makeSamples(ts1, v1), makeSamples(ts1, v2)...), makeHistograms(ts3, generateTestHistogram(0)), nil),
				makeTimeseries(labels, makeSamples(ts1, v3), append(makeHistograms(ts2, generateTestHistogram(1)), makeHistograms(ts2, generateTestHistogram(2))...), nil),
				makeTimeseries(labels, makeSamples(ts1, v4), append(makeHistograms(ts2, generateTestHistogram(3)), makeHistograms(ts3, generateTestHistogram(4))...), nil),
			),
			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, makeSamples(ts1, v1), makeHistograms(ts3, generateTestHistogram(0)), nil),
				makeTimeseries(labels, makeSamples(ts1, v3), makeHistograms(ts2, generateTestHistogram(1)), nil),
				makeTimeseries(labels, makeSamples(ts1, v4), append(makeHistograms(ts2, generateTestHistogram(3)), makeHistograms(ts3, generateTestHistogram(4))...), nil),
			},
			expectedErrors: []error{
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 1, metricName),
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 1, metricName),
				nil,
			},
			expectedMetrics: `
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{group="test-group",reason="sample_duplicate_timestamp",user="user"} 2
			`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			limits := prepareDefaultLimits()
			ds, _, regs, _ := prepare(t, prepConfig{
				limits:          limits,
				numDistributors: 1,
			})

			// Pre-condition check.
			require.Len(t, ds, 1)
			require.Len(t, regs, 1)

			now := mtime.Now()
			for i, ts := range tc.req.Timeseries {
				shouldRemove, err := ds[0].validateSeries(now, &ts, "user", "test-group", true, true, 0, 0)
				require.False(t, shouldRemove)
				if len(tc.expectedErrors) == 0 {
					require.NoError(t, err)
				} else {
					if tc.expectedErrors[i] == nil {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
						require.Equal(t, tc.expectedErrors[i], err)
					}
				}
			}

			assert.Equal(t, tc.expectedSamples, tc.req.Timeseries)
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(tc.expectedMetrics), "cortex_discarded_samples_total"))
		})
	}
}

func BenchmarkDistributor_SampleDuplicateTimestamp(b *testing.B) {
	const (
		metricName              = "series"
		testSize                = 80_000
		numberOfDifferentValues = 40_000
	)
	labels := []string{labels.MetricName, metricName, "job", "job", "service", "service"}

	now := mtime.Now()
	timestamp := now.UnixMilli()

	testCases := map[string]struct {
		setup           func() []mimirpb.PreallocTimeseries
		expectedSamples []mimirpb.PreallocTimeseries
		expectedErrors  []error
	}{
		"one timeseries with 80_000 samples with duplicated timestamps": {
			setup: func() []mimirpb.PreallocTimeseries {
				samples := make([]mimirpb.Sample, testSize)
				value := 0
				for i := 0; i < testSize; i++ {
					if i < numberOfDifferentValues {
						value++
					}
					samples[i].TimestampMs = timestamp
					samples[i].Value = float64(value)
				}
				timeseries := []mimirpb.PreallocTimeseries{
					makeTimeseries(labels, samples, nil, nil),
				}
				return timeseries
			},

			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, makeSamples(timestamp, 1.0), nil, nil),
			},
			expectedErrors: []error{
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 79_999, metricName),
			},
		},
		"one timeseries with 80_000 histograms with duplicated timestamps": {
			setup: func() []mimirpb.PreallocTimeseries {
				histograms := make([]mimirpb.Histogram, testSize)
				value := 0
				for i := 0; i < testSize; i++ {
					if i < numberOfDifferentValues {
						value++
					}
					histograms[i] = makeHistograms(timestamp, generateTestHistogram(value))[0]
				}
				timeseries := []mimirpb.PreallocTimeseries{
					makeTimeseries(labels, nil, histograms, nil),
				}
				return timeseries
			},

			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, nil, makeHistograms(timestamp, generateTestHistogram(1)), nil),
			},
			expectedErrors: []error{
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 79_999, metricName),
			},
		},
		"one timeseries with 80_000 samples and 80_000 histograms with duplicated timestamps": {
			setup: func() []mimirpb.PreallocTimeseries {
				samples := make([]mimirpb.Sample, testSize)
				histograms := make([]mimirpb.Histogram, testSize)
				value := 0
				for i := 0; i < testSize; i++ {
					if i < numberOfDifferentValues {
						value++
					}
					samples[i].TimestampMs = timestamp
					samples[i].Value = float64(value)
					histograms[i] = makeHistograms(timestamp, generateTestHistogram(value))[0]
				}

				timeseries := []mimirpb.PreallocTimeseries{
					makeTimeseries(labels, samples, histograms, nil),
				}
				return timeseries
			},

			expectedSamples: []mimirpb.PreallocTimeseries{
				makeTimeseries(labels, makeSamples(timestamp, 1.0), makeHistograms(timestamp, generateTestHistogram(1)), nil),
			},
			expectedErrors: []error{
				fmt.Errorf("samples with duplicated timestamps have been discarded, discarded samples: %d series: '%.200s' (err-mimir-sample-duplicate-timestamp)", 159_998, metricName),
			},
		},
	}

	limits := prepareDefaultLimits()
	ds, _, regs, _ := prepare(b, prepConfig{
		limits:          limits,
		numDistributors: 1,
	})

	// Pre-condition check.
	require.Len(b, ds, 1)
	require.Len(b, regs, 1)

	for name, tc := range testCases {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				b.StopTimer()
				timeseries := tc.setup()
				b.StartTimer()
				for i, ts := range timeseries {
					shouldRemove, err := ds[0].validateSeries(now, &ts, "user", "test-group", true, true, 0, 0)
					require.False(b, shouldRemove)
					if len(tc.expectedErrors) == 0 {
						require.NoError(b, err)
					} else {
						if tc.expectedErrors[i] == nil {
							require.NoError(b, err)
						} else {
							require.Error(b, err)
							require.Equal(b, tc.expectedErrors[i], err)
						}
					}
				}
				assert.Equal(b, tc.expectedSamples, timeseries)
			}
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
			req: makeWriteRequestWith(
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, 0, []string{"foo", "bar"}),
			),
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
			req: makeWriteRequestWith(
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, 0, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, 0, []string{"foo", "bar"}),
			),
			expectedExemplars: []mimirpb.PreallocTimeseries{
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test1"}, 1000, 0, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test2"}, 1000, 0, []string{"foo", "bar"}),
			},
		},
		"should drop exemplars with timestamp lower than the accepted minimum, when the exemplars are specified in different series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 1
			},
			minExemplarTS: 300000,
			maxExemplarTS: math.MaxInt64,
			req: makeWriteRequestWith(
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 1000, 0, []string{"foo", "bar"}),
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 601000, 0, []string{"foo", "bar"}),
			),
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{TimeSeries: &mimirpb.TimeSeries{
					Labels:    []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{},
				}},
				makeExemplarTimeseries([]string{model.MetricNameLabel, "test"}, 601000, 0, []string{"foo", "bar"}),
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
			req: makeWriteRequestWith(mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
					},
				},
			}),
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
			req: makeWriteRequestWith(mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
					},
				},
			}),
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
			req: makeWriteRequestWith(mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 1000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
					},
				},
			}),
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
		"should drop exemplars above the allowed exemplars per series limit, when multiple exemplars are specified in the same series": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 2
				limits.MaxExemplarsPerSeriesPerRequest = 2
			},
			minExemplarTS: 300000,
			maxExemplarTS: math.MaxInt64,
			req: makeWriteRequestWith(mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 600000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar3"}}, TimestampMs: 602000},
					},
				},
			}),
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 600000},
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
						},
					},
				},
			},
			expectedMetrics: `
                # HELP cortex_discarded_exemplars_total The total number of exemplars that were discarded.
                # TYPE cortex_discarded_exemplars_total counter
                cortex_discarded_exemplars_total{reason="too_many_exemplars_per_series_per_request",user="user"} 1
            `,
		},
		"should sort exemplars if they are not sorted": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxGlobalExemplarsPerUser = 3
			},
			minExemplarTS: 600000,
			maxExemplarTS: math.MaxInt64,
			req: makeWriteRequestWith(mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
					Exemplars: []mimirpb.Exemplar{
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 602000},
						{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
					},
				},
			}),
			expectedExemplars: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "test"}},
						Exemplars: []mimirpb.Exemplar{
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar2"}}, TimestampMs: 601000},
							{Labels: []mimirpb.LabelAdapter{{Name: "foo", Value: "bar1"}}, TimestampMs: 602000},
						},
					},
				},
			},
		},
	}
	now := mtime.Now()
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()
			tc.prepareConfig(limits)
			ds, _, regs, _ := prepare(t, prepConfig{
				limits:          limits,
				numDistributors: 1,
			})

			// Pre-condition check.
			require.Len(t, ds, 1)
			require.Len(t, regs, 1)

			for _, ts := range tc.req.Timeseries {
				shouldRemove, err := ds[0].validateSeries(now, &ts, "user", "test-group", false, false, tc.minExemplarTS, tc.maxExemplarTS)
				assert.NoError(t, err)
				assert.False(t, shouldRemove)
			}

			assert.Equal(t, tc.expectedExemplars, tc.req.Timeseries)
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(tc.expectedMetrics), "cortex_discarded_exemplars_total"))
		})
	}
}

func TestDistributor_HistogramReduction(t *testing.T) {
	h := &histogram.Histogram{
		Count:         12,
		ZeroCount:     2,
		ZeroThreshold: 0.001,
		Sum:           18.4,
		Schema:        0,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		PositiveBuckets: []int64{1, 1, -1, 0}, // 1 in 0(0.5, 1], 2 in 1(1, 2], 1 in 3(4, 8], 1 in 4(8, 16]
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 2},
			{Offset: 1, Length: 2},
		},
		NegativeBuckets: []int64{1, 1, -1, 0}, // 1 in -4[-16, -8), 1 in -3[-8, -4), 2 in -1[-2, -1), 1 in -0[-1, -0.5)
	}

	reducedH := &histogram.Histogram{
		Count:         12,
		ZeroCount:     2,
		ZeroThreshold: 0.001,
		Sum:           18.4,
		Schema:        -1,
		PositiveSpans: []histogram.Span{
			{Offset: 0, Length: 3},
		},
		PositiveBuckets: []int64{1, 1, 0}, // 1 in 0(0.25, 1], 2 in 1(1, 4], 2 in 2(4, 16]
		NegativeSpans: []histogram.Span{
			{Offset: 0, Length: 3},
		},
		NegativeBuckets: []int64{1, 1, 0},
	}

	hugeH := &histogram.Histogram{
		Count:         12,
		ZeroCount:     2,
		ZeroThreshold: 0.001,
		Sum:           18.4,
		Schema:        -3,
		PositiveSpans: []histogram.Span{
			{Offset: -1e6, Length: 1},
			{Offset: 2e6, Length: 1}, // Further apart than the min schema of -4 with a bucket width of 64K.
		},
		PositiveBuckets: []int64{1, 1},
	}

	tests := map[string]struct {
		prepareConfig      func(limits *validation.Limits)
		req                *mimirpb.WriteRequest
		expectedError      error
		expectedTimeSeries []mimirpb.PreallocTimeseries
	}{
		"should not reduce histogram under bucket limit": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxNativeHistogramBuckets = 8
			},
			req: makeWriteRequestHistogram([]string{model.MetricNameLabel, "test"}, 1000, h),
			expectedTimeSeries: []mimirpb.PreallocTimeseries{
				makeHistogramTimeseries([]string{model.MetricNameLabel, "test"}, 1000, h),
			},
		},
		"should reduce histogram over bucket limit": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxNativeHistogramBuckets = 7
			},
			req: makeWriteRequestHistogram([]string{model.MetricNameLabel, "test"}, 1000, h),
			expectedTimeSeries: []mimirpb.PreallocTimeseries{
				makeHistogramTimeseries([]string{model.MetricNameLabel, "test"}, 1000, reducedH),
			},
		},
		"should fail if not possible to reduce": {
			prepareConfig: func(limits *validation.Limits) {
				limits.MaxNativeHistogramBuckets = 1
			},
			req:                makeWriteRequestHistogram([]string{model.MetricNameLabel, "test"}, 1000, hugeH),
			expectedError:      fmt.Errorf("received a native histogram sample with too many buckets and cannot reduce, timestamp: 1000 series: test, buckets: 2, limit: 1 (not-reducible-native-histogram)"),
			expectedTimeSeries: []mimirpb.PreallocTimeseries{},
		},
	}
	now := mtime.Now()
	for testName, tc := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := prepareDefaultLimits()
			tc.prepareConfig(limits)
			limits.ReduceNativeHistogramOverMaxBuckets = true
			ds, _, regs, _ := prepare(t, prepConfig{
				limits:          limits,
				numDistributors: 1,
			})

			// Pre-condition check.
			require.Len(t, ds, 1)
			require.Len(t, regs, 1)

			for _, ts := range tc.req.Timeseries {
				shouldRemove, err := ds[0].validateSeries(now, &ts, "user", "test-group", false, false, 0, 0)
				if tc.expectedError != nil {
					require.ErrorAs(t, err, &tc.expectedError)
					require.True(t, shouldRemove)
				} else {
					assert.NoError(t, err)
					assert.False(t, shouldRemove)
				}
			}
			if tc.expectedError == nil {
				assert.Equal(t, tc.expectedTimeSeries, tc.req.Timeseries)
			}
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
			prepareConfig: func(*validation.Limits) {},
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
				limits.MaxLabelNameLength = 200
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					// Add a label with a very long name.
					metrics[i] = mkLabels(10, fmt.Sprintf("xxx_%0.200d", 1), "xxx")
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
				limits.MaxLabelValueLength = 200
			},
			prepareSeries: func() ([][]mimirpb.LabelAdapter, []mimirpb.Sample) {
				metrics := make([][]mimirpb.LabelAdapter, numSeriesPerRequest)
				samples := make([]mimirpb.Sample, numSeriesPerRequest)

				for i := 0; i < numSeriesPerRequest; i++ {
					// Add a label with a very long value.
					metrics[i] = mkLabels(10, "xxx", fmt.Sprintf("xxx_%0.200d", 1))
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
					d.AddIngester("ingester-1", "127.0.0.1", "", ring.NewRandomTokenGenerator().GenerateTokens(128, nil), ring.ACTIVE, time.Now(), false, time.Time{})
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

			distributorCfg.IngesterClientFactory = ring_client.PoolInstFunc(func(ring.InstanceDesc) (ring_client.PoolClient, error) {
				return &noopIngester{}, nil
			})

			overrides, err := validation.NewOverrides(limits, nil)
			require.NoError(b, err)

			// Start the distributor.
			distributor, err := New(distributorCfg, clientConfig, overrides, nil, ingestersRing, nil, true, nil, log.NewNopLogger())
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

			ds, _, reg, _ := prepare(t, prepConfig{
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
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "status", "500"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labelAdapters(labels.MetricName, "fast_fingerprint_collision", "app", "l", "uniq0", "0", "uniq1", "1"), 1, 300000},
		{labelAdapters(labels.MetricName, "fast_fingerprint_collision", "app", "m", "uniq0", "1", "uniq1", "1"), 1, 300000},
	}

	tests := map[string]struct {
		shuffleShardSize  int
		matchers          []*labels.Matcher
		maxSeriesPerQuery int
		expectedResult    [][]mimirpb.LabelAdapter
		expectedIngesters int
		expectedError     error
	}{
		"should return an empty response if no metric match": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown"),
			},
			expectedResult:    nil,
			expectedIngesters: numIngesters,
		},
		"should filter metrics by single matcher": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			expectedResult: [][]mimirpb.LabelAdapter{
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
			expectedResult: [][]mimirpb.LabelAdapter{
				fixtures[0].lbls,
			},
			expectedIngesters: numIngesters,
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "fast_fingerprint_collision"),
			},
			expectedResult: [][]mimirpb.LabelAdapter{
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
			expectedResult: [][]mimirpb.LabelAdapter{
				fixtures[0].lbls,
				fixtures[1].lbls,
			},
			expectedIngesters: 3,
		},
		"should error out if max series per query is reached": {
			matchers: []*labels.Matcher{
				mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1"),
			},
			maxSeriesPerQuery: 1,
			expectedError:     validation.NewLimitError("the query exceeded the maximum number of series (limit: 1 series) (err-mimir-max-series-per-query). Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query. Otherwise, to adjust the related per-tenant limit, configure -querier.max-fetched-series-per-query, or contact your service administrator."),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, ingestStorageEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
					t.Parallel()

					now := model.Now()

					testConfig := prepConfig{
						numIngesters:    numIngesters,
						happyIngesters:  numIngesters,
						numDistributors: 1,
					}

					if ingestStorageEnabled {
						testConfig.ingestStorageEnabled = true
						testConfig.limits = prepareDefaultLimits()
						testConfig.limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize
					} else {
						testConfig.shuffleShardSize = testData.shuffleShardSize
					}

					// Create distributor
					ds, ingesters, _, _ := prepare(t, testConfig)

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "test")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Push fixtures
					for _, series := range fixtures {
						req := mockWriteRequest(series.lbls, series.value, series.timestamp)
						_, err := ds[0].Push(ctx, req)
						require.NoError(t, err)
					}

					// Set up limiter
					ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(testData.maxSeriesPerQuery, 0, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))

					metrics, err := ds[0].MetricsForLabelMatchers(ctx, now, now, testData.matchers...)
					if testData.expectedError != nil {
						require.ErrorIs(t, err, testData.expectedError)
						return
					}

					require.NoError(t, err)
					requireLabelAdaptersMatchLabels(t, testData.expectedResult, metrics)

					// Check how many ingesters have been queried.
					if ingestStorageEnabled {
						// When ingest storage is enabled, we request quorum 1 for each partition.
						// In this test each ingester owns a different partition, so we expect all
						// ingesters to be queried.
						assert.Equal(t, testData.expectedIngesters, countMockIngestersCalled(ingesters, "MetricsForLabelMatchers"))
					} else {
						// Due to the quorum the distributor could cancel the last request towards ingesters
						// if all other ones are successful, so we're good either has been queried X or X-1
						// ingesters.
						assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalled(ingesters, "MetricsForLabelMatchers"))
					}
				})
			}
		})
	}
}

func TestDistributor_ActiveSeries(t *testing.T) {
	const numIngesters = 5
	const responseSizeLimitBytes = 1024

	collision1, collision2 := labelsWithHashCollision()

	pushedData := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "team", "a"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "team", "b"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
		{collision1, 3, 300000},
		{collision2, 4, 300000},
		{labelAdapters(labels.MetricName, "large_metric", "label", strings.Repeat("1", 2*responseSizeLimitBytes)), 5, 400000},
	}

	tests := map[string]struct {
		shuffleShardSize            int
		requestMatchers             []*labels.Matcher
		expectError                 error
		expectedSeries              [][]mimirpb.LabelAdapter
		expectedNumQueriedIngesters int
	}{
		"should return an empty response if no metric match": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown")},
			expectedSeries:              nil,
			expectedNumQueriedIngesters: numIngesters,
		},
		"should return all matching metrics": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1")},
			expectedSeries:              [][]mimirpb.LabelAdapter{pushedData[0].lbls, pushedData[1].lbls},
			expectedNumQueriedIngesters: numIngesters,
		},
		"should honour shuffle shard size": {
			shuffleShardSize:            3,
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_2")},
			expectedSeries:              [][]mimirpb.LabelAdapter{pushedData[2].lbls},
			expectedNumQueriedIngesters: 3,
		},
		"should return all matching series even if their hash collides": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric")},
			expectedSeries:              [][]mimirpb.LabelAdapter{collision1, collision2},
			expectedNumQueriedIngesters: numIngesters,
		},
		"aborts if response is too large": {
			requestMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "large_metric")},
			expectError:     ErrResponseTooLarge,
		},
	}

	// Programmatically build different scenarios under which run the tests.
	type scenario struct {
		ingestStorageEnabled     bool
		minimizeIngesterRequests bool
	}

	scenarios := map[string]scenario{}
	for _, minimizeIngesterRequests := range []bool{false, true} {
		for _, ingestStorageEnabled := range []bool{false, true} {
			name := fmt.Sprintf("minimize ingester requests: %t, ingest storage enabled: %t", minimizeIngesterRequests, ingestStorageEnabled)

			scenarios[name] = scenario{
				ingestStorageEnabled:     ingestStorageEnabled,
				minimizeIngesterRequests: minimizeIngesterRequests,
			}
		}
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for scenarioName, scenarioData := range scenarios {
				t.Run(scenarioName, func(t *testing.T) {
					t.Parallel()

					testConfig := prepConfig{
						numIngesters:         numIngesters,
						happyIngesters:       numIngesters,
						numDistributors:      1,
						ingestStorageEnabled: scenarioData.ingestStorageEnabled,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = scenarioData.minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.ActiveSeriesResultsMaxSizeBytes = responseSizeLimitBytes
							return limits
						}(),
					}

					if scenarioData.ingestStorageEnabled {
						testConfig.limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize
					} else {
						testConfig.shuffleShardSize = testData.shuffleShardSize
					}

					// Create distributor and ingesters.
					distributors, ingesters, _, _ := prepare(t, testConfig)
					d := distributors[0]

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "test")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Push test data.
					for _, series := range pushedData {
						req := mockWriteRequest(series.lbls, series.value, series.timestamp)
						_, err := d.Push(ctx, req)
						require.NoError(t, err)
					}

					// Prepare empty query stats.
					qStats, ctx := stats.ContextWithEmptyStats(ctx)

					// Query active series.
					series, err := d.ActiveSeries(ctx, testData.requestMatchers)
					if testData.expectError != nil {
						require.ErrorIs(t, err, testData.expectError)
						return
					}

					require.NoError(t, err)
					requireLabelAdaptersMatchLabels(t, testData.expectedSeries, series)

					// Check that query stats are set correctly.
					assert.Equal(t, uint64(len(testData.expectedSeries)), qStats.GetFetchedSeriesCount())

					// Check how many ingesters have been queried.
					if scenarioData.ingestStorageEnabled {
						// When ingest storage is enabled, we request quorum 1 for each partition.
						// In this test each ingester owns a different partition, so we expect all
						// ingesters to be queried.
						assert.Equal(t, testData.expectedNumQueriedIngesters, countMockIngestersCalled(ingesters, "ActiveSeries"))
					} else {
						// Due to the quorum the distributor could cancel the last request towards ingesters
						// if all other ones are successful, so we're good either has been queried X or X-1
						// ingesters.
						assert.Contains(t, []int{testData.expectedNumQueriedIngesters, testData.expectedNumQueriedIngesters - 1}, countMockIngestersCalled(ingesters, "ActiveSeries"))
					}
				})
			}
		})
	}
}

// Check that all the LabelAdaptors match all the Labels. Assume LabelAdaptors are already sorted.
func requireLabelAdaptersMatchLabels(tb testing.TB, a [][]mimirpb.LabelAdapter, b []labels.Labels) {
	tb.Helper()
	if len(a) == 0 && len(b) == 0 {
		return
	}
	bAsLabelAdapters := make([][]mimirpb.LabelAdapter, len(b))
	for i, s := range b {
		bAsLabelAdapters[i] = mimirpb.FromLabelsToLabelAdapters(s)
	}
	slices.SortFunc(bAsLabelAdapters, mimirpb.CompareLabelAdapters)

	promtestutil.RequireEqual(tb, a, bAsLabelAdapters)
}

func TestDistributor_ActiveNativeHistogramSeries(t *testing.T) {
	const numIngesters = 5
	const responseSizeLimitBytes = 1024

	collision1, collision2 := labelsWithHashCollision()

	pushedData := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "team", "a"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "team", "b"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
		{collision1, 3, 300000},
		{collision2, 4, 300000},
		{labelAdapters(labels.MetricName, "large_metric", "label", strings.Repeat("1", 2*responseSizeLimitBytes)), 5, 400000},
	}

	tests := map[string]struct {
		shuffleShardSize            int
		requestMatchers             []*labels.Matcher
		expectedFetchedSeries       uint64
		expectedMetrics             []cardinality.ActiveMetricWithBucketCount
		expectedNumQueriedIngesters int
		expectedError               error
	}{
		"should return an empty response if no metric match": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "unknown")},
			expectedMetrics:             []cardinality.ActiveMetricWithBucketCount{},
			expectedNumQueriedIngesters: numIngesters,
		},
		"should return all matching metrics": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_1")},
			expectedFetchedSeries:       2,
			expectedMetrics:             []cardinality.ActiveMetricWithBucketCount{{Metric: "test_1", SeriesCount: 2, BucketCount: 16, MaxBucketCount: 8, MinBucketCount: 8, AvgBucketCount: 8.0}},
			expectedNumQueriedIngesters: numIngesters,
		},
		"should honour shuffle shard size": {
			shuffleShardSize:            3,
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test_2")},
			expectedFetchedSeries:       1,
			expectedMetrics:             []cardinality.ActiveMetricWithBucketCount{{Metric: "test_2", SeriesCount: 1, BucketCount: 8, MaxBucketCount: 8, MinBucketCount: 8, AvgBucketCount: 8.0}},
			expectedNumQueriedIngesters: 3,
		},
		"should return all matching series even if their hash collides": {
			requestMatchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "metric")},
			expectedFetchedSeries:       2,
			expectedMetrics:             []cardinality.ActiveMetricWithBucketCount{{Metric: "metric", SeriesCount: 2, BucketCount: 16, MaxBucketCount: 8, MinBucketCount: 8, AvgBucketCount: 8.0}},
			expectedNumQueriedIngesters: numIngesters,
		},
		"aborts if response is too large": {
			requestMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "large_metric")},
			expectedError:   ErrResponseTooLarge,
		},
	}

	// Programmatically build different scenarios under which run the tests.
	type scenario struct {
		ingestStorageEnabled     bool
		minimizeIngesterRequests bool
	}

	scenarios := map[string]scenario{}
	for _, minimizeIngesterRequests := range []bool{false, true} {
		for _, ingestStorageEnabled := range []bool{false, true} {
			name := fmt.Sprintf("minimize ingester requests: %t, ingest storage enabled: %t", minimizeIngesterRequests, ingestStorageEnabled)
			scenarios[name] = scenario{
				ingestStorageEnabled:     ingestStorageEnabled,
				minimizeIngesterRequests: minimizeIngesterRequests,
			}
		}
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for scenarioName, scenarioData := range scenarios {
				t.Run(scenarioName, func(t *testing.T) {
					t.Parallel()

					testConfig := prepConfig{
						numIngesters:         numIngesters,
						happyIngesters:       numIngesters,
						numDistributors:      1,
						ingestStorageEnabled: scenarioData.ingestStorageEnabled,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = scenarioData.minimizeIngesterRequests
						},
						limits: func() *validation.Limits {
							limits := prepareDefaultLimits()
							limits.ActiveSeriesResultsMaxSizeBytes = responseSizeLimitBytes
							limits.NativeHistogramsIngestionEnabled = true
							return limits
						}(),
					}

					if scenarioData.ingestStorageEnabled {
						testConfig.limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize
					} else {
						testConfig.shuffleShardSize = testData.shuffleShardSize
					}

					// Create distributor and ingesters.
					distributors, ingesters, _, _ := prepare(t, testConfig)
					d := distributors[0]

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "test")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Push test data.
					for _, series := range pushedData {
						req := mockWriteHistogramRequest(series.lbls, series.value, series.timestamp)
						_, err := d.Push(ctx, req)
						require.NoError(t, err)
					}

					// Prepare empty query stats.
					qStats, ctx := stats.ContextWithEmptyStats(ctx)

					// Query active native histogram metric stats.
					series, err := d.ActiveNativeHistogramMetrics(ctx, testData.requestMatchers)
					if testData.expectedError != nil {
						require.ErrorIs(t, err, testData.expectedError)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, testData.expectedMetrics, series.Data)

					// Check that query stats are set correctly.
					assert.Equal(t, testData.expectedFetchedSeries, qStats.GetFetchedSeriesCount())

					// Check how many ingesters have been queried.
					if scenarioData.ingestStorageEnabled {
						// When ingest storage is enabled, we request quorum 1 for each partition.
						// In this test each ingester owns a different partition, so we expect all
						// ingesters to be queried.
						assert.Equal(t, testData.expectedNumQueriedIngesters, countMockIngestersCalled(ingesters, "ActiveSeries"))
					} else {
						// Due to the quorum the distributor could cancel the last request towards ingesters
						// if all other ones are successful, so we're good either has been queried X or X-1
						// ingesters.
						assert.Contains(t, []int{testData.expectedNumQueriedIngesters, testData.expectedNumQueriedIngesters - 1}, countMockIngestersCalled(ingesters, "ActiveSeries"))
					}
				})
			}
		})
	}
}

func TestDistributor_ActiveSeries_AvailabilityAndConsistency(t *testing.T) {
	// In this test we run all queries with a matcher which matches all series.
	reqMatchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+")}

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeriesCount int
		expectedErr         error
	}{
		"single zone, 3 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"single zone, 3 ingesters, every series successfully replicated only to 2 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_2", "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"single zone, 6 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
				},
			},
			expectedSeriesCount: 9,
		},
		"single zone, 6 ingesters, most series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8"), // series_9 has not been replicated here.
				},
			},
			expectedSeriesCount: 9,
		},
		"single zone, 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
			},
			expectedSeriesCount: 6,
		},
		"single zone, 6 ingesters, 2 ingesters in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"single zone, 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 3 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"multi zone, 3 ingesters, every series successfully replicated only to 2 ingesters across 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-c": {
					nil,
				},
			},
			expectedSeriesCount: 3,
		},
		"multi zone, 3 ingesters, every series successfully replicated only to 2 ingesters across 3 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_3"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_2", "series_3"),
				},
			},
			expectedSeriesCount: 3,
		},
		"multi zone, 6 ingesters, every series successfully replicated to 1 ingester per zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeriesCount: 6,
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeriesCount: 6,
		},
		"multi zone, 6 ingesters, all ingesters in 2 zones are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeriesCount: 6,
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester in 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 1},
				"zone-b": {numIngesters: 2, happyIngesters: 1},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 6 ingesters, 1 LEAVING ingester per zone, but the LEAVING ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
			},
			shardSize:           1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
			expectedSeriesCount: 5,
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester per zone, but the UNHEALTHY ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-c": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
			},
			shardSize:           1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
			expectedSeriesCount: 5,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor.
					distributors, _, _, _ := prepare(t, prepConfig{
						ingesterStateByZone: testData.ingesterStateByZone,
						ingesterDataByZone:  testData.ingesterDataByZone,
						numDistributors:     1,
						shuffleShardSize:    testData.shardSize,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
					})

					ctx := user.InjectOrgID(context.Background(), "test")
					qStats, ctx := stats.ContextWithEmptyStats(ctx)

					// Query active series.
					series, err := distributors[0].ActiveSeries(ctx, reqMatchers)
					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, testData.expectedSeriesCount, len(series))

					// Check that query stats are set correctly.
					assert.Equal(t, testData.expectedSeriesCount, int(qStats.GetFetchedSeriesCount()))
				})
			}
		})
	}
}

func BenchmarkDistributor_ActiveSeries(b *testing.B) {
	const numIngesters = 3
	const numSeries = 10e3

	// Create distributor and ingesters.
	distributors, _, _, _ := prepare(b, prepConfig{
		numIngesters:    numIngesters,
		happyIngesters:  numIngesters,
		numDistributors: 1,
	})

	ctx := user.InjectOrgID(context.Background(), "user")

	// Push test data.
	metrics := make([][]mimirpb.LabelAdapter, numSeries)
	samples := make([]mimirpb.Sample, numSeries)

	for i := 0; i < numSeries; i++ {
		metrics[i] = mkLabels(10)
		metrics[i] = append(metrics[i], mimirpb.LabelAdapter{Name: "series_no", Value: fmt.Sprintf("series_%d", i)})
		samples[i] = mimirpb.Sample{TimestampMs: time.Now().UnixNano() / int64(time.Millisecond), Value: 1}
	}

	_, err := distributors[0].Push(
		ctx,
		mimirpb.ToWriteRequest(metrics, samples, nil, nil, mimirpb.API),
	)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	// Run the benchmark.
	for n := 0; n < b.N; n++ {
		_, err := distributors[0].ActiveSeries(
			ctx,
			[]*labels.Matcher{mustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "foo")},
		)
		require.NoError(b, err)
	}
}

func TestDistributor_LabelNames(t *testing.T) {
	const numIngesters = 5

	fixtures := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
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
			t.Parallel()

			for _, ingestStorageEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
					t.Parallel()

					now := model.Now()

					testConfig := prepConfig{
						numIngesters:    numIngesters,
						happyIngesters:  numIngesters,
						numDistributors: 1,
					}

					if ingestStorageEnabled {
						testConfig.ingestStorageEnabled = true

						testConfig.limits = prepareDefaultLimits()
						testConfig.limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize
					} else {
						testConfig.shuffleShardSize = testData.shuffleShardSize
					}

					// Create distributor
					ds, ingesters, _, _ := prepare(t, testConfig)

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "test")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Push fixtures
					for _, series := range fixtures {
						req := mockWriteRequest(series.lbls, series.value, series.timestamp)
						_, err := ds[0].Push(ctx, req)
						require.NoError(t, err)
					}

					names, err := ds[0].LabelNames(ctx, now, now, testData.matchers...)
					require.NoError(t, err)
					assert.ElementsMatch(t, testData.expectedResult, names)

					// Check how many ingesters have been queried.
					if ingestStorageEnabled {
						// When ingest storage is enabled, we request quorum 1 for each partition.
						// In this test each ingester owns a different partition, so we expect all
						// ingesters to be queried.
						assert.Equal(t, testData.expectedIngesters, countMockIngestersCalled(ingesters, "LabelNames"))
					} else {
						// Due to the quorum the distributor could cancel the last request towards ingesters
						// if all other ones are successful, so we're good either has been queried X or X-1
						// ingesters.
						assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalled(ingesters, "LabelNames"))
					}
				})
			}
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
			t.Parallel()

			for _, ingestStorageEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
					t.Parallel()

					testConfig := prepConfig{
						numIngesters:    numIngesters,
						happyIngesters:  numIngesters,
						numDistributors: 1,
					}

					if ingestStorageEnabled {
						testConfig.ingestStorageEnabled = true
						testConfig.limits = prepareDefaultLimits()
						testConfig.limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize
					} else {
						testConfig.shuffleShardSize = testData.shuffleShardSize
					}

					// Create distributor
					ds, ingesters, _, _ := prepare(t, testConfig)

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "test")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Push metadata
					req := makeWriteRequest(0, 0, 10, false, true, "foo")
					_, err := ds[0].Push(ctx, req)
					require.NoError(t, err)

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

					// Check how many ingesters have been queried.
					if ingestStorageEnabled {
						// When ingest storage is enabled, we request quorum 1 for each partition.
						// In this test each ingester owns a different partition, so we expect all
						// ingesters to be queried.
						assert.Equal(t, testData.expectedIngesters, countMockIngestersCalled(ingesters, "MetricsMetadata"))
					} else {
						// Due to the quorum the distributor could cancel the last request towards ingesters
						// if all other ones are successful, so we're good either has been queried X or X-1
						// ingesters.
						assert.Contains(t, []int{testData.expectedIngesters, testData.expectedIngesters - 1}, countMockIngestersCalled(ingesters, "MetricsMetadata"))
					}
				})
			}
		})
	}
}

func TestDistributor_LabelNamesAndValuesLimitTest(t *testing.T) {
	// distinct values are "__name__", "label_00", "label_01" that is 24 bytes in total
	fixtures := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "label_00"), 1, 100000},
		{labelAdapters(labels.MetricName, "label_11"), 1, 110000},
		{labelAdapters(labels.MetricName, "label_11"), 2, 200000},
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
			t.Parallel()

			for _, ingestStorageEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
					t.Parallel()

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "label-names-values")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Create distributor
					limits := validation.Limits{}
					flagext.DefaultValues(&limits)
					limits.LabelNamesAndValuesResultsMaxSizeBytes = testData.sizeLimitBytes
					ds, _, _, _ := prepare(t, prepConfig{
						numIngesters:         3,
						happyIngesters:       3,
						numDistributors:      1,
						limits:               &limits,
						ingestStorageEnabled: ingestStorageEnabled,
					})

					// Push fixtures
					for _, series := range fixtures {
						req := mockWriteRequest(series.lbls, series.value, series.timestamp)
						_, err := ds[0].Push(ctx, req)
						require.NoError(t, err)
					}

					_, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{}, cardinality.InMemoryMethod)
					if len(testData.expectedError) == 0 {
						require.NoError(t, err)
					} else {
						require.EqualError(t, err, testData.expectedError)
					}
				})
			}
		})
	}
}

func TestDistributor_LabelValuesForLabelName(t *testing.T) {
	fixtures := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "label_0", "status", "200"), 1, 100_000},
		{labelAdapters(labels.MetricName, "label_1", "status", "500", "reason", "broken"), 1, 110_000},
		{labelAdapters(labels.MetricName, "label_1"), 2, 200_000},
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
			t.Parallel()

			for _, ingestStorageEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
					t.Parallel()

					// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
					ctx := user.InjectOrgID(context.Background(), "label-names-values")
					ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

					// Create distributor
					ds, _, _, _ := prepare(t, prepConfig{
						numIngesters:         12,
						happyIngesters:       12,
						numDistributors:      1,
						replicationFactor:    3,
						ingestStorageEnabled: ingestStorageEnabled,
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
		})
	}
}

func TestDistributor_LabelNamesAndValues(t *testing.T) {
	fixtures := []struct {
		lbls      []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "label_0", "status", "200"), 1, 100000},
		{labelAdapters(labels.MetricName, "label_1", "status", "500", "reason", "broken"), 1, 110000},
		{labelAdapters(labels.MetricName, "label_1"), 2, 200000},
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

	t.Run("should group values of labels by label name and return only distinct label values", func(t *testing.T) {
		for _, ingestStorageEnabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("ingest storage enabled: %t", ingestStorageEnabled), func(t *testing.T) {
				t.Parallel()

				// Ensure strong read consistency, required to have no flaky tests when ingest storage is enabled.
				ctx := user.InjectOrgID(context.Background(), "label-names-values")
				ctx = api.ContextWithReadConsistencyLevel(ctx, api.ReadConsistencyStrong)

				// Create distributor
				ds, _, _, _ := prepare(t, prepConfig{
					numIngesters:         12,
					happyIngesters:       12,
					numDistributors:      1,
					replicationFactor:    3,
					ingestStorageEnabled: ingestStorageEnabled,
				})

				// Push fixtures
				for _, series := range fixtures {
					req := mockWriteRequest(series.lbls, series.value, series.timestamp)
					_, err := ds[0].Push(ctx, req)
					require.NoError(t, err)
				}

				response, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{}, cardinality.InMemoryMethod)
				require.NoError(t, err)
				require.Len(t, response.Items, len(expectedLabelValues))

				// sort label values to make stable assertion
				for _, item := range response.Items {
					slices.Sort(item.Values)
				}
				assert.ElementsMatch(t, response.Items, expectedLabelValues)
			})
		}
	})

	t.Run("should return the results if zone awareness is enabled and only 2 zones return the results", func(t *testing.T) {
		ctx := user.InjectOrgID(context.Background(), "label-names-values")
		slowZoneDelay := 10 * time.Second

		// Create distributor
		ds, _, _, _ := prepare(t, prepConfig{
			numIngesters:                       12,
			happyIngesters:                     12,
			numDistributors:                    1,
			replicationFactor:                  3,
			ingesterZones:                      []string{"A", "B", "C"},
			labelNamesStreamZonesResponseDelay: map[string]time.Duration{"C": slowZoneDelay},
		})

		// Push fixtures
		for _, series := range fixtures {
			req := mockWriteRequest(series.lbls, series.value, series.timestamp)
			_, err := ds[0].Push(ctx, req)
			require.NoError(t, err)
		}

		// Assert on metric metadata
		timeBeforeExecution := time.Now()
		response, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{}, cardinality.InMemoryMethod)
		require.NoError(t, err)
		require.Less(t, time.Since(timeBeforeExecution), slowZoneDelay/2, "Execution must be completed before the slow zone ingesters respond")
		require.Len(t, response.Items, len(expectedLabelValues))

		// sort label values to make stable assertion
		for _, item := range response.Items {
			slices.Sort(item.Values)
		}
		assert.ElementsMatch(t, response.Items, expectedLabelValues)
	})
}

// This test asserts that distributor waits for all ingester responses to be completed even if ZoneAwareness is enabled.
// Also, it simulates delay from zone C to verify that there is no race condition. must be run with `-race` flag (race detection).
func TestDistributor_LabelValuesCardinality_ExpectedAllIngestersResponsesToBeCompleted(t *testing.T) {
	ctx, ds := prepareWithZoneAwarenessAndZoneDelay(t, 10000)

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
	ctx, ds := prepareWithZoneAwarenessAndZoneDelay(t, 10000)
	response, err := ds[0].LabelNamesAndValues(ctx, []*labels.Matcher{}, cardinality.InMemoryMethod)
	require.NoError(t, err)
	require.Len(t, response.Items, 1)
	require.Equal(t, 10000, len(response.Items[0].Values))
}

// adapted from pkg/ingester/activeseries/active_series_test.go
func labelsWithHashCollision() ([]mimirpb.LabelAdapter, []mimirpb.LabelAdapter) {
	// These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
	ls1 := labelAdapters("__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y")
	ls2 := labelAdapters("__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF")

	if mimirpb.FromLabelAdaptersToLabels(ls1).Hash() != mimirpb.FromLabelAdaptersToLabels(ls2).Hash() {
		// These ones are the same when using -tags stringlabels
		ls1 = labelAdapters("__name__", "metric", "lbl", "HFnEaGl")
		ls2 = labelAdapters("__name__", "metric", "lbl", "RqcXatm")
	}

	if mimirpb.FromLabelAdaptersToLabels(ls1).Hash() != mimirpb.FromLabelAdaptersToLabels(ls2).Hash() {
		panic("This code needs to be updated: find new labels with colliding hash values.")
	}

	return ls1, ls2
}

func prepareWithZoneAwarenessAndZoneDelay(t *testing.T, count int) (context.Context, []*Distributor) {
	ctx := user.InjectOrgID(context.Background(), "cardinality-user")

	// Create distributor
	ds, _, _, _ := prepare(t, prepConfig{
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

	// Push test series.
	for i := 0; i < count; i++ {
		req := mockWriteRequest(labelAdapters(labels.MetricName, "metric"+strconv.Itoa(i)), 1, int64(100000+i))
		_, err := ds[0].Push(ctx, req)
		require.NoError(t, err)
	}
	return ctx, ds
}

func TestDistributor_UserStats(t *testing.T) {
	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedSeries      uint64
		expectedErr         error
	}{
		"single zone, 3 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			expectedSeries: 3,
		},
		"single zone, 3 ingesters, every series successfully replicated only to 2 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_2", "series_3"),
				},
			},
			// We pushed 3 series, but the estimated count is 2 because every series has been
			// successfully replicated only to 2 ingesters.
			expectedSeries: 2,
		},
		"single zone, 6 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
				},
			},
			expectedSeries: 9,
		},
		"single zone, 6 ingesters, most series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8", "series_9"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7", "series_8"), // series_9 has not been replicated here.
				},
			},
			expectedSeries: 9,
		},
		"single zone, 6 ingesters, 1 ingester in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
			},
			// We pushed 6 series but the LEAVING ingester isn't queried so its series are not counted
			// in the estimation. The actual estimation is computed as: (3 series * 5 ingesters) / 3 RF = 5.
			expectedSeries: 5,
		},
		"single zone, 6 ingesters, 2 ingesters in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"single zone, 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 3 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
			},
			expectedSeries: 3,
		},
		"multi zone, 3 ingesters, every series successfully replicated only to 2 ingesters across 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-c": {
					nil,
				},
			},
			expectedSeries: 3,
		},
		"multi zone, 3 ingesters, every series successfully replicated only to 2 ingesters across 3 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_3"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_2", "series_3"),
				},
			},
			// We pushed 3 series but every series has been successfully replicated only to 2 ingesters (in different zones).
			expectedSeries: 2,
		},
		"multi zone, 6 ingesters, every series successfully replicated to 1 ingester per zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeries: 6,
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeries: 6,
		},
		"multi zone, 6 ingesters, all ingesters in 2 zones are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_6"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedSeries: 6,
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester in 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 1},
				"zone-b": {numIngesters: 2, happyIngesters: 1},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
					nil,
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4"),
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_6"),
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 6 ingesters, 1 LEAVING ingester per zone, but the LEAVING ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					makeWriteRequest(0, 1, 0, false, false, "series_6", "series_7"), // Not belonging to tenant's shard.
				},
			},
			shardSize:      1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
			expectedSeries: 5,
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester per zone, but the UNHEALTHY ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-c": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3", "series_4", "series_5"),
					nil,
				},
			},
			shardSize:      1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
			expectedSeries: 5,
		},
		"multi zone, 5 ingesters, every series successfully replicated to 3 ingesters across 5 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
				"zone-d": {numIngesters: 1, happyIngesters: 1},
				"zone-e": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "series_1", "series_2", "series_3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "series_2", "series_3", "series_4"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "series_3", "series_4", "series_5"),
				},
				"zone-d": {
					makeWriteRequest(0, 1, 0, false, false, "series_4", "series_5", "series_1"),
				},
				"zone-e": {
					makeWriteRequest(0, 1, 0, false, false, "series_5", "series_1", "series_2"),
				},
			},
			// We pushed 5 series and every series has been replicated to 3 ingesters (in different zones).
			expectedSeries: 5,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					distributors, _, _, _ := prepare(t, prepConfig{
						numDistributors:     1,
						replicationFactor:   3,
						ingesterStateByZone: testData.ingesterStateByZone,
						ingesterDataByZone:  testData.ingesterDataByZone,
						shuffleShardSize:    testData.shardSize,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
					})

					// Fetch user stats.
					ctx := user.InjectOrgID(context.Background(), "test")
					res, err := distributors[0].UserStats(ctx, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.Equal(t, int(testData.expectedSeries), int(res.NumSeries))
				})
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality(t *testing.T) {
	const numIngesters = 3
	const replicationFactor = 3

	fixtures := []struct {
		labels    []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
	}

	tests := map[string]struct {
		labelNames               []model.LabelName
		matchers                 []*labels.Matcher
		expectedResult           *client.LabelValuesCardinalityResponse
		expectedIngesters        int
		happyIngesters           int
		expectedSeriesCountTotal uint64
		ingesterZones            []string
	}{
		"should return an empty map on no label names provided": {
			labelNames:               []model.LabelName{},
			matchers:                 []*labels.Matcher{},
			expectedResult:           &client.LabelValuesCardinalityResponse{Items: []*client.LabelValueSeriesCount{}},
			expectedIngesters:        numIngesters,
			happyIngesters:           numIngesters,
			expectedSeriesCountTotal: 3,
		},
		"should return a map with the label values and series occurrences of a single label name": {
			labelNames: []model.LabelName{labels.MetricName},
			matchers:   []*labels.Matcher{},
			expectedResult: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 2, "test_2": 1},
				}},
			},
			expectedIngesters:        numIngesters - 1,
			happyIngesters:           numIngesters,
			expectedSeriesCountTotal: 3,
			ingesterZones:            []string{"ZONE-A", "ZONE-B", "ZONE-C"},
		},
		"should return a map with the label values and series occurrences of a single label name, during single zone failure": {
			labelNames: []model.LabelName{labels.MetricName},
			matchers:   []*labels.Matcher{},
			expectedResult: &client.LabelValuesCardinalityResponse{
				Items: []*client.LabelValueSeriesCount{{
					LabelName:        labels.MetricName,
					LabelValueSeries: map[string]uint64{"test_1": 2, "test_2": 1},
				}},
			},
			expectedIngesters:        numIngesters - 1,
			happyIngesters:           numIngesters - 1,
			expectedSeriesCountTotal: 3,
			ingesterZones:            []string{"ZONE-A", "ZONE-B", "ZONE-C"},
		},
		"should return a map with the label values and series occurrences of all the label names": {
			labelNames: []model.LabelName{labels.MetricName, "status"},
			matchers:   []*labels.Matcher{},
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
			expectedSeriesCountTotal: 3,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor
					ds, ingesters, _, _ := prepare(t, prepConfig{
						numIngesters:      numIngesters,
						happyIngesters:    testData.happyIngesters,
						numDistributors:   1,
						replicationFactor: replicationFactor,
						ingesterZones:     testData.ingesterZones,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
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
					assert.GreaterOrEqual(t, countMockIngestersCalled(ingesters, "LabelValuesCardinality"), testData.expectedIngesters)
				})
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality_AvailabilityAndConsistency(t *testing.T) {
	var (
		// Define fixtures used in tests.
		series1 = makeTimeseries([]string{labels.MetricName, "series_1", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series2 = makeTimeseries([]string{labels.MetricName, "series_2", "job", "job-b", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series3 = makeTimeseries([]string{labels.MetricName, "series_3", "job", "job-c", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series4 = makeTimeseries([]string{labels.MetricName, "series_4", "job", "job-a", "service", "service-1"}, makeSamples(0, 0), nil, nil)
		series5 = makeTimeseries([]string{labels.MetricName, "series_5", "job", "job-a", "service", "service-2"}, makeSamples(0, 0), nil, nil)
		series6 = makeTimeseries([]string{labels.MetricName, "series_6", "job", "job-b" /* no service label */}, makeSamples(0, 0), nil, nil)
		other1  = makeTimeseries([]string{labels.MetricName, "other_1", "job", "job-1", "service", "service-1"}, makeSamples(0, 0), nil, nil)

		// To keep assertions simple, all tests push all series, and then request the cardinality of the same label names,
		// so we expect the same response from each successful test.
		reqLabelNames = []model.LabelName{"job", "service"}
		expectedRes   = []*client.LabelValueSeriesCount{
			{
				LabelName:        "job",
				LabelValueSeries: map[string]uint64{"job-a": 3, "job-b": 2, "job-c": 1},
			}, {
				LabelName:        "service",
				LabelValueSeries: map[string]uint64{"service-1": 4, "service-2": 1},
			},
		}
	)

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		ingesterDataByZone  map[string][]*mimirpb.WriteRequest
		shardSize           int
		expectedErr         error
	}{
		"single zone, 3 ingesters, series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 3, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
			},
		},
		"single zone, 6 ingesters, series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
					makeWriteRequestWith(series4, series5, series6),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
		},
		"single zone, 6 ingesters, 2 ingesters in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 6, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
					makeWriteRequestWith(series4, series5, series6),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"single zone, 6 ingesters, 1 ingester is UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"single-zone": {numIngesters: 6, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"single-zone": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
					makeWriteRequestWith(series4, series5, series6),
					nil,
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 3 ingesters, every series successfully replicated to 3 ingesters": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
			},
		},
		"multi zone, 3 ingesters, every series successfully replicated only to 2 ingesters across 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 1, happyIngesters: 1},
				"zone-b": {numIngesters: 1, happyIngesters: 1},
				"zone-c": {numIngesters: 1, happyIngesters: 1},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
				},
				"zone-c": {
					nil,
				},
			},
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5),
					makeWriteRequestWith(series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
		},
		"multi zone, 6 ingesters, all ingesters in 2 zones are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.LEAVING, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5),
					makeWriteRequestWith(series6),
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
			expectedErr: ring.ErrTooManyUnhealthyInstances,
		},
		"multi zone, 6 ingesters, all ingesters in 1 zone are UNHEALTHY": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 0},
				"zone-b": {numIngesters: 2, happyIngesters: 2},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					makeWriteRequestWith(series5, series6),
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester in 2 zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 1},
				"zone-b": {numIngesters: 2, happyIngesters: 1},
				"zone-c": {numIngesters: 2, happyIngesters: 2},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5),
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4),
					nil,
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3),
					makeWriteRequestWith(series4, series5, series6),
				},
			},
			expectedErr: errFail,
		},
		"multi zone, 6 ingesters, 1 LEAVING ingester per zone, but the LEAVING ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-b": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
				"zone-c": {numIngesters: 2, happyIngesters: 2, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					makeWriteRequestWith(other1), // Not belonging to tenant's shard.
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					makeWriteRequestWith(other1), // Not belonging to tenant's shard.
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					makeWriteRequestWith(other1), // Not belonging to tenant's shard.
				},
			},
			shardSize: 1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
		},
		"multi zone, 6 ingesters, 1 UNHEALTHY ingester per zone, but the UNHEALTHY ingesters are not part of tenant's shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
				"zone-c": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					nil,
				},
				"zone-b": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					nil,
				},
				"zone-c": {
					makeWriteRequestWith(series1, series2, series3, series4, series5, series6),
					nil,
				},
			},
			shardSize: 1, // Tenant's shard made of: ingester-zone-a-0, ingester-zone-b-0 and ingester-zone-c-0.
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			for _, minimizeIngesterRequests := range []bool{false, true} {
				t.Run(fmt.Sprintf("minimize ingester requests: %t", minimizeIngesterRequests), func(t *testing.T) {
					t.Parallel()

					// Create distributor.
					distributors, _, _, _ := prepare(t, prepConfig{
						ingesterStateByZone: testData.ingesterStateByZone,
						ingesterDataByZone:  testData.ingesterDataByZone,
						numDistributors:     1,
						shuffleShardSize:    testData.shardSize,
						configure: func(config *Config) {
							config.MinimizeIngesterRequests = minimizeIngesterRequests
						},
					})

					// Fetch label values cardinality.
					ctx := user.InjectOrgID(context.Background(), "test")
					_, res, err := distributors[0].LabelValuesCardinality(ctx, reqLabelNames, nil, cardinality.InMemoryMethod)

					if testData.expectedErr != nil {
						require.ErrorIs(t, err, testData.expectedErr)
						return
					}

					require.NoError(t, err)
					assert.ElementsMatch(t, expectedRes, res.Items)
				})
			}
		})
	}
}

func TestDistributor_LabelValuesCardinality_Limit(t *testing.T) {
	fixtures := []struct {
		labels    []mimirpb.LabelAdapter
		value     float64
		timestamp int64
	}{
		{labelAdapters(labels.MetricName, "test_1", "status", "200"), 1, 100000},
		{labelAdapters(labels.MetricName, "test_1", "status", "500", "reason", "broken"), 1, 110000},
		{labelAdapters(labels.MetricName, "test_2"), 2, 200000},
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
			ds, _, _, _ := prepare(t, prepConfig{
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
		ds, ingesters, _, _ := prepare(t, prepConfig{
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
	replicasDidNotMatchDetails := &mimirpb.ErrorDetails{Cause: mimirpb.REPLICAS_DID_NOT_MATCH}
	tooManyClusterDetails := &mimirpb.ErrorDetails{Cause: mimirpb.TOO_MANY_CLUSTERS}

	type testCase struct {
		name              string
		ctx               context.Context
		enableHaTracker   bool
		acceptHaSamples   bool
		reqs              []*mimirpb.WriteRequest
		expectedReqs      []*mimirpb.WriteRequest
		expectedNextCalls int
		expectErrs        []*status.Status
		expectDetails     []*mimirpb.ErrorDetails
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
			expectDetails:     []*mimirpb.ErrorDetails{nil},
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
			expectDetails:     []*mimirpb.ErrorDetails{nil, replicasDidNotMatchDetails},
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
			expectDetails: []*mimirpb.ErrorDetails{nil, replicasDidNotMatchDetails, tooManyClusterDetails, tooManyClusterDetails},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupCallCount := 0
			cleanup := func() {
				cleanupCallCount++
			}

			duplicateCleanup := func() {
				// If we get here, that means the middleware called `next`
				// (which will call `CleanUp`) and then called `CleanUp` again.
				assert.Fail(t, "cleanup called twice")
			}

			nextCallCount := 0
			var gotReqs []*mimirpb.WriteRequest
			next := func(_ context.Context, pushReq *Request) error {
				nextCallCount++
				req, err := pushReq.WriteRequest()
				require.NoError(t, err)
				gotReqs = append(gotReqs, req)
				pushReq.CleanUp()
				pushReq.AddCleanup(duplicateCleanup)
				return nil
			}

			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.AcceptHASamples = tc.acceptHaSamples
			limits.MaxLabelValueLength = 15
			limits.HAMaxClusters = 1

			ds, _, _, _ := prepare(t, prepConfig{
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
	mockPush := func(_ context.Context, pushReq *Request) error {
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
	ds, _, _, _ := prepare(t, prepConfig{
		numDistributors: 1,
		limits:          &limits,
		enableTracker:   true,
		configure: func(config *Config) {
			config.DefaultLimits.MaxInflightPushRequests = 1
		},
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
		name              string
		ctx               context.Context
		relabelConfigs    []*relabel.Config
		dropLabels        []string
		relabelingEnabled bool
		reqs              []*mimirpb.WriteRequest
		expectedReqs      []*mimirpb.WriteRequest
		expectErrs        []bool
	}
	testCases := []testCase{
		{
			name:              "do nothing",
			ctx:               ctxWithUser,
			relabelConfigs:    nil,
			dropLabels:        nil,
			relabelingEnabled: true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectErrs:        []bool{false},
		}, {
			name:              "no user in context",
			ctx:               context.Background(),
			relabelConfigs:    nil,
			dropLabels:        nil,
			relabelingEnabled: true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label", "value_%d"), nil, nil)},
			expectedReqs:      nil,
			expectErrs:        []bool{true},
		}, {
			name:              "apply a relabel rule",
			ctx:               ctxWithUser,
			relabelConfigs:    nil,
			dropLabels:        []string{"label1", "label3"},
			relabelingEnabled: true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "label2", "value2", "label3", "value3"), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label2", "value2"), nil, nil)},
			expectErrs:        []bool{false},
		}, {
			name:              "relabeling disabled",
			ctx:               ctxWithUser,
			relabelConfigs:    nil,
			dropLabels:        []string{"label1", "label3"},
			relabelingEnabled: false,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "label2", "value2", "label3", "value3"), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "label2", "value2", "label3", "value3"), nil, nil)},
			expectErrs:        []bool{false},
		}, {}, {
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
			relabelingEnabled: true,
			reqs:              []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1"), nil, nil)},
			expectedReqs:      []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "label1", "value1", "target", "prefix_value1"), nil, nil)},
			expectErrs:        []bool{false},
		}, {
			name:              "drop entire series if they have no labels",
			ctx:               ctxWithUser,
			dropLabels:        []string{"__name__", "label2", "label3"},
			relabelingEnabled: true,
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
			relabelingEnabled: true,
			reqs: []*mimirpb.WriteRequest{{
				Timeseries: []mimirpb.PreallocTimeseries{makeTimeseries(
					[]string{
						model.MetricNameLabel, "metric1",
						"label1", "value1",
					},
					makeSamples(123, 1.23),
					nil,
					nil,
				)},
			}},
			expectedReqs: []*mimirpb.WriteRequest{{
				Timeseries: []mimirpb.PreallocTimeseries{makeTimeseries(
					[]string{
						model.MetricNameLabel, "metric1",
						"label1", "value1",
						"tenant_id", "user",
					},
					makeSamples(123, 1.23),
					nil,
					nil,
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

			duplicateCleanup := func() {
				// If we get here, that means the middleware called `next`
				// (which will call `CleanUp`) and then called `CleanUp` again.
				assert.Fail(t, "cleanup called twice")
			}

			var gotReqs []*mimirpb.WriteRequest
			next := func(_ context.Context, pushReq *Request) error {
				req, err := pushReq.WriteRequest()
				require.NoError(t, err)
				gotReqs = append(gotReqs, req)
				pushReq.CleanUp()
				pushReq.AddCleanup(duplicateCleanup)
				return nil
			}

			var limits validation.Limits
			flagext.DefaultValues(&limits)
			limits.MetricRelabelConfigs = tc.relabelConfigs
			limits.DropLabels = tc.dropLabels
			limits.MetricRelabelingEnabled = tc.relabelingEnabled
			ds, _, _, _ := prepare(t, prepConfig{
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

func TestSortAndFilterMiddleware(t *testing.T) {
	ctxWithUser := user.InjectOrgID(context.Background(), "user")

	type testCase struct {
		name         string
		ctx          context.Context
		reqs         []*mimirpb.WriteRequest
		expectedReqs []*mimirpb.WriteRequest
		expectErrs   []bool
	}
	testCases := []testCase{
		{
			name:         "unsorted labels",
			ctx:          ctxWithUser,
			reqs:         []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "labelb", "valueb", "labela", "valuea"), nil, nil)},
			expectedReqs: []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenForStringPairs(t, "__name__", "metric1", "labela", "valuea", "labelb", "valueb"), nil, nil)},
			expectErrs:   []bool{false},
		},
		{
			name:         "empty labels",
			ctx:          ctxWithUser,
			reqs:         []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithEmptyLabels("metric1", "empty"), nil, nil)},
			expectedReqs: []*mimirpb.WriteRequest{makeWriteRequestForGenerators(5, labelSetGenWithEmptyLabels("metric1"), nil, nil)},
			expectErrs:   []bool{false},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cleanupCallCount := 0
			cleanup := func() {
				cleanupCallCount++
			}

			duplicateCleanup := func() {
				// If we get here, that means the middleware called `next`
				// (which will call `CleanUp`) and then called `CleanUp` again.
				assert.Fail(t, "cleanup called twice")
			}

			var gotReqs []*mimirpb.WriteRequest
			next := func(_ context.Context, pushReq *Request) error {
				req, err := pushReq.WriteRequest()
				require.NoError(t, err)
				gotReqs = append(gotReqs, req)
				pushReq.CleanUp()
				pushReq.AddCleanup(duplicateCleanup)
				return nil
			}

			var limits validation.Limits
			flagext.DefaultValues(&limits)
			ds, _, _, _ := prepare(t, prepConfig{
				numDistributors: 1,
				limits:          &limits,
			})
			middleware := ds[0].prePushSortAndFilterMiddleware(next)

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

func mockWriteRequest(la []mimirpb.LabelAdapter, value float64, timestampMs int64) *mimirpb.WriteRequest {
	samples := []mimirpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	return mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{la}, samples, nil, nil, mimirpb.API)
}

func mockWriteHistogramRequest(lbls []mimirpb.LabelAdapter, value float64, timestampMs int64) *mimirpb.WriteRequest {
	histograms := []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(timestampMs, util_test.GenerateTestHistogram(int(value)))}

	req := mimirpb.NewWriteRequest(nil, mimirpb.API)
	return req.AddHistogramSeries([][]mimirpb.LabelAdapter{lbls}, histograms, nil)
}

type ingesterState int

const (
	ingesterStateHappy = ingesterState(iota)
	ingesterStateFailed
)

type ingesterZoneState struct {
	// numIngesters and happyIngesters are superseded by states
	numIngesters, happyIngesters int

	// states explicitly sets the state of each ingester.
	// states takes precedence over numIngesters and happyIngesters
	states []ingesterState

	// ringStates explicitly sets the state of each ingester in the ring.
	// If unset, instances will be in ring.ACTIVE state.
	ringStates []ring.InstanceState
}

type ingesterIngestionType string

const (
	ingesterIngestionTypeGRPC  = "grpc"
	ingesterIngestionTypeKafka = "kafka"
)

type prepConfig struct {
	// numIngesters, happyIngesters, and ingesterZones are superseded by ingesterStateByZone
	numIngesters, happyIngesters int
	ingesterZones                []string

	// ingesterStateByZone supersedes numIngesters, happyIngesters, and ingesterZones
	ingesterStateByZone map[string]ingesterZoneState

	// ingesterDataByZone:
	//   map[zone-a][0] -> ingester-zone-a-0 write request
	//   map[zone-a][1] -> ingester-zone-a-1 write request
	// Each zone in ingesterDataByZone can be shorter than the actual number of ingesters for the zone, but it cannot be longer.
	// If a request is nil, sending a request to the ingester is skipped.
	ingesterDataByZone map[string][]*mimirpb.WriteRequest

	// ingesterDataTenantID is the tenant under which ingesterDataByZone is pushed
	ingesterDataTenantID string

	// ingesterIngestionType allows to override how the ingester is expected to receive data.
	// If empty, it defaults to the ingestion storage type configured in the test.
	ingesterIngestionType ingesterIngestionType

	queryDelay       time.Duration
	pushDelay        time.Duration
	shuffleShardSize int
	limits           *validation.Limits
	numDistributors  int

	replicationFactor                  int
	enableTracker                      bool
	labelNamesStreamZonesResponseDelay map[string]time.Duration

	configure func(*Config)

	timeOut bool

	// Ingest storage specific configuration.
	ingestStorageEnabled          bool
	ingestStorageMigrationEnabled bool
	ingestStoragePartitions       int32 // Number of partitions. Auto-detected from configured ingesters if not explicitly set.
	ingestStorageKafka            *kfake.Cluster

	// We need this setting to simulate a response from ingesters that didn't support responding
	// with a stream of chunks, and were responding with chunk series instead. This is needed to
	// ensure backwards compatibility, i.e., that queriers can still correctly handle both types
	// or responses.
	disableStreamingResponse bool
}

// totalIngesters takes into account ingesterStateByZone and numIngesters.
func (c prepConfig) totalIngesters() int {
	if len(c.ingesterStateByZone) == 0 {
		return c.numIngesters
	}
	n := 0
	for _, s := range c.ingesterStateByZone {
		if s.states != nil {
			n += len(s.states)
		} else {
			n += s.numIngesters
		}
	}
	return n
}

// totalZones takes into account ingesterStateByZone and ingesterZones.
// If there were no explicit zones, then totalZones returns 1.
func (c prepConfig) totalZones() int {
	if len(c.ingesterStateByZone) == 0 {
		evenlyAssignedZones := len(c.ingesterZones)
		if evenlyAssignedZones == 0 {
			return 1
		}
		return evenlyAssignedZones
	}
	return len(c.ingesterStateByZone)
}

// maxIngestersPerZone returns the max number of ingester per zone. For example,
// if a zone has 2 ingesters and another zone has 3 ingesters, this function will
// return 3.
func (c prepConfig) maxIngestersPerZone() int {
	maxIngestersPerZone := c.numIngesters

	for _, state := range c.ingesterStateByZone {
		maxIngestersPerZone = max(maxIngestersPerZone, max(state.numIngesters, len(state.states)))
	}

	return maxIngestersPerZone
}

func (c prepConfig) ingesterRingState(zone string, id int) ring.InstanceState {
	if len(c.ingesterStateByZone[zone].ringStates) == 0 {
		return ring.ACTIVE
	}
	return c.ingesterStateByZone[zone].ringStates[id]
}

func (c prepConfig) ingesterShouldConsumeFromKafka() bool {
	if c.ingesterIngestionType == "" {
		// If the ingestion type has not been overridden then consume from Kafka when ingest storage is enabled.
		return c.ingestStorageEnabled
	}

	return c.ingesterIngestionType == ingesterIngestionTypeKafka
}

func (c prepConfig) validate(t testing.TB) {
	if len(c.ingesterStateByZone) != 0 {
		require.Zero(t, c.numIngesters, "ingesterStateByZone and numIngesters/happyIngesters are exclusive")
		require.Zero(t, c.happyIngesters, "ingesterStateByZone and numIngesters/happyIngesters are exclusive")
		require.Nil(t, c.ingesterZones, "ingesterStateByZone and numIngesters/happyIngesters are exclusive")

		for zone, state := range c.ingesterStateByZone {
			ingestersInZone := state.numIngesters
			if state.states != nil {
				require.Zero(t, state.numIngesters, "ingesterStateByZone's states and numIngesters/happyIngesters are exclusive")
				require.Zero(t, state.happyIngesters, "ingesterStateByZone's states and numIngesters/happyIngesters are exclusive")
				ingestersInZone = len(state.states)
			}
			if len(state.ringStates) > 0 {
				require.Len(t, state.ringStates, ingestersInZone, "ringStates cannot be longer than the number of ingesters in the zone")
			}
			require.LessOrEqual(t, len(c.ingesterDataByZone[zone]), ingestersInZone, "ingesterDataPerZone cannot be longer than the number of ingesters in the zone")
		}
	}
}

func prepareIngesters(t testing.TB, cfg prepConfig) []*mockIngester {
	if len(cfg.ingesterStateByZone) != 0 {
		ingesters := []*mockIngester(nil)
		for zone, state := range cfg.ingesterStateByZone {
			ingesters = append(ingesters, prepareIngesterZone(t, zone, state, cfg)...)
		}
		return ingesters
	}
	ingesters := []*mockIngester(nil)
	numZones := len(cfg.ingesterZones)
	if numZones == 0 {
		return prepareIngesterZone(t, "", ingesterZoneState{numIngesters: cfg.numIngesters, happyIngesters: cfg.happyIngesters}, cfg)
	}
	for zoneIdx, zone := range cfg.ingesterZones {
		state := ingesterZoneState{
			numIngesters:   cfg.numIngesters / numZones,
			happyIngesters: cfg.happyIngesters / numZones,
		}
		if zoneIdx < cfg.happyIngesters%numZones {
			// Account for cases where the number of happy ingesters isn't divisible by numZones.
			// For example, when there are 3 zones, 9 ingesters, and 8 happy ingesters.
			// In this case ingester-zone-c-2 (the last ingester) is the unhappy ingester.
			state.happyIngesters++
		}

		if zoneIdx < cfg.numIngesters%numZones {
			// Account for cases where the number of ingesters isn't divisible by numZones.
			// For example, when there are 3 zones and 11 ingesters.
			// In this case the distribution of replicas is zone-a: 4, zone-b: 4, zone-c: 3.
			state.numIngesters++
		}

		ingesters = append(ingesters, prepareIngesterZone(t, zone, state, cfg)...)
	}
	return ingesters

}

func prepareIngesterZone(t testing.TB, zone string, state ingesterZoneState, cfg prepConfig) []*mockIngester {
	ingesters := []*mockIngester(nil)

	if state.states == nil {
		state.states = make([]ingesterState, state.numIngesters)
		for i := 0; i < state.happyIngesters; i++ {
			state.states[i] = ingesterStateHappy
		}
		for i := state.happyIngesters; i < state.numIngesters; i++ {
			state.states[i] = ingesterStateFailed
		}
	}

	for i, s := range state.states {
		var labelNamesStreamResponseDelay time.Duration
		if len(cfg.labelNamesStreamZonesResponseDelay) > 0 {
			labelNamesStreamResponseDelay = cfg.labelNamesStreamZonesResponseDelay[zone]
		}

		ingester := &mockIngester{
			id:                            i,
			happy:                         s == ingesterStateHappy,
			queryDelay:                    cfg.queryDelay,
			pushDelay:                     cfg.pushDelay,
			zone:                          zone,
			labelNamesStreamResponseDelay: labelNamesStreamResponseDelay,
			timeOut:                       cfg.timeOut,
			disableStreamingResponse:      cfg.disableStreamingResponse,
		}

		// Init the partition reader if the ingester should consume from Kafka.
		// This is required to let each mocked ingester to consume their own partition.
		if cfg.ingesterShouldConsumeFromKafka() {
			var err error

			kafkaCfg := ingest.KafkaConfig{}
			flagext.DefaultValues(&kafkaCfg)
			kafkaCfg.Address = cfg.ingestStorageKafka.ListenAddrs()[0]
			kafkaCfg.Topic = kafkaTopic
			kafkaCfg.LastProducedOffsetPollInterval = 100 * time.Millisecond
			kafkaCfg.LastProducedOffsetRetryTimeout = 100 * time.Millisecond

			ingester.partitionReader, err = ingest.NewPartitionReaderForPusher(kafkaCfg, ingester.partitionID(), ingester.instanceID(), newMockIngesterPusherAdapter(ingester), log.NewNopLogger(), nil)
			require.NoError(t, err)

			// We start it async, and then we wait until running in a defer so that multiple partition
			// readers will be started concurrently.
			require.NoError(t, ingester.partitionReader.StartAsync(context.Background()))

			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester.partitionReader))
			})
		}

		ingesters = append(ingesters, ingester)
	}

	// Wait until all partition readers have started.
	if cfg.ingesterShouldConsumeFromKafka() {
		for _, ingester := range ingesters {
			require.NoError(t, ingester.partitionReader.AwaitRunning(context.Background()))
		}
	}

	return ingesters
}

func prepareRingInstances(cfg prepConfig, ingesters []*mockIngester) *ring.Desc {
	ingesterDescs := map[string]ring.InstanceDesc{}

	for i := range ingesters {
		addr := ingesters[i].address()
		tokens := []uint32{uint32((math.MaxUint32 / len(ingesters)) * i)}
		ingesterDescs[addr] = ring.InstanceDesc{
			Addr:                addr,
			Zone:                ingesters[i].zone,
			State:               cfg.ingesterRingState(ingesters[i].zone, ingesters[i].id),
			Timestamp:           time.Now().Unix(),
			RegisteredTimestamp: time.Now().Add(-2 * time.Hour).Unix(), // registered before the shuffle sharding lookback period, so we don't start including other ingesters
			Tokens:              tokens,
		}
		ingesters[i].tokens = tokens
	}
	return &ring.Desc{Ingesters: ingesterDescs}
}

func preparePartitionsRing(cfg prepConfig, ingesters []*mockIngester) *ring.PartitionRingDesc {
	desc := ring.NewPartitionRingDesc()

	// When we add partitions we simulate the case they were switched to ACTIVE before
	// the shuffle sharding lookback period, so that we don't extend partitions
	// when shuffle sharding is in use.
	timeBeforeShuffleShardingLookbackPeriod := time.Now().Add(-2 * time.Hour)

	// Add all partitions.
	for partitionID := int32(0); partitionID < cfg.ingestStoragePartitions; partitionID++ {
		desc.AddPartition(partitionID, ring.PartitionActive, timeBeforeShuffleShardingLookbackPeriod)
	}

	// Add all ingesters are partition owners.
	for _, ingester := range ingesters {
		desc.AddOrUpdateOwner(ingester.instanceID(), ring.OwnerActive, ingester.partitionID(), timeBeforeShuffleShardingLookbackPeriod)
	}

	return desc
}

func prepareDefaultLimits() *validation.Limits {
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)
	return limits
}

func prepare(t testing.TB, cfg prepConfig) ([]*Distributor, []*mockIngester, []*prometheus.Registry, *kfake.Cluster) {
	ctx := context.Background()

	// Apply default config.
	if cfg.replicationFactor == 0 {
		cfg.replicationFactor = 3
	}
	if cfg.ingestStorageEnabled && cfg.ingestStoragePartitions == 0 {
		cfg.ingestStoragePartitions = int32(cfg.maxIngestersPerZone())
	}

	cfg.validate(t)

	logger := log.NewNopLogger()

	// Init a fake Kafka cluster if ingest storage is enabled.
	if cfg.ingestStorageEnabled && cfg.ingestStorageKafka == nil {
		cfg.ingestStorageKafka, _ = testkafka.CreateCluster(t, cfg.ingestStoragePartitions, kafkaTopic)
	}

	// Create the mocked ingesters.
	ingesters := prepareIngesters(t, cfg)

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Add ingesters to the ring.
	err := kvStore.CAS(ctx, ingester.IngesterRingKey,
		func(_ interface{}) (interface{}, bool, error) {
			return prepareRingInstances(cfg, ingesters), true, nil
		},
	)
	require.NoError(t, err)

	ingestersHeartbeatTimeout := 60 * time.Minute
	ingestersRing, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvStore,
		},
		HeartbeatTimeout:     ingestersHeartbeatTimeout,
		ReplicationFactor:    cfg.replicationFactor,
		ZoneAwarenessEnabled: cfg.totalZones() > 1,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, logger, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingestersRing))

	// The ring client doesn't stop quickly, so we don't wait.
	t.Cleanup(ingestersRing.StopAsync)

	test.Poll(t, time.Second, cfg.totalIngesters(), func() interface{} {
		return ingestersRing.InstancesCount()
	})

	// Mock the ingester clients pool in order to return our mocked ingester instance instead of a
	// real gRPC client.
	factory := ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
		for _, ing := range ingesters {
			if ing.address() == inst.Addr {
				return ing, nil
			}
		}
		return nil, fmt.Errorf("ingester with address %s not found", inst.Addr)
	})

	// Initialize the ingest storage's partitions ring.
	var partitionsRing *ring.PartitionInstanceRing
	if cfg.ingestStorageEnabled {
		// Init the partitions ring.
		partitionsStore := kvStore.WithCodec(ring.GetPartitionRingCodec())
		require.NoError(t, partitionsStore.CAS(ctx, ingester.PartitionRingKey, func(_ interface{}) (interface{}, bool, error) {
			return preparePartitionsRing(cfg, ingesters), true, nil
		}))

		// Init the watcher.
		watcher := ring.NewPartitionRingWatcher(ingester.PartitionRingName, ingester.PartitionRingKey, partitionsStore, logger, prometheus.NewPedanticRegistry())
		require.NoError(t, services.StartAndAwaitRunning(ctx, watcher))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, watcher))
		})

		partitionsRing = ring.NewPartitionInstanceRing(watcher, ingestersRing, ingestersHeartbeatTimeout)
	}

	if cfg.limits == nil {
		cfg.limits = prepareDefaultLimits()
	}
	cfg.limits.IngestionTenantShardSize = cfg.shuffleShardSize

	// Create distributors.
	distributors := make([]*Distributor, 0, cfg.numDistributors)
	distributorRegistries := make([]*prometheus.Registry, 0, cfg.numDistributors)
	for i := 0; i < cfg.numDistributors; i++ {
		var (
			distributorCfg Config
			clientConfig   client.Config
			ingestCfg      ingest.Config
		)
		flagext.DefaultValues(&distributorCfg, &clientConfig, &ingestCfg)

		ingestCfg.Enabled = cfg.ingestStorageEnabled
		if cfg.ingestStorageEnabled {
			ingestCfg.KafkaConfig.Topic = kafkaTopic
			ingestCfg.KafkaConfig.Address = cfg.ingestStorageKafka.ListenAddrs()[0]
			ingestCfg.KafkaConfig.LastProducedOffsetPollInterval = 100 * time.Millisecond
			ingestCfg.Migration.DistributorSendToIngestersEnabled = cfg.ingestStorageMigrationEnabled
		}

		distributorCfg.IngesterClientFactory = factory
		distributorCfg.DistributorRing.Common.HeartbeatPeriod = 100 * time.Millisecond
		distributorCfg.DistributorRing.Common.InstanceID = strconv.Itoa(i)
		distributorCfg.DistributorRing.Common.KVStore.Mock = kvStore
		distributorCfg.DistributorRing.Common.InstanceAddr = "127.0.0.1"
		distributorCfg.ShuffleShardingLookbackPeriod = time.Hour
		distributorCfg.StreamingChunksPerIngesterSeriesBufferSize = 128
		distributorCfg.IngestStorageConfig = ingestCfg

		if cfg.configure != nil {
			cfg.configure(&distributorCfg)
		}

		if cfg.enableTracker {
			codec := GetReplicaDescCodec()
			ringStore, closer := consul.NewInMemoryClient(codec, logger, nil)
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
		d, err := New(distributorCfg, clientConfig, overrides, nil, ingestersRing, partitionsRing, true, reg, log.NewNopLogger())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(ctx, d))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), d))
		})

		distributors = append(distributors, d)
		distributorRegistries = append(distributorRegistries, reg)
	}

	// If the distributors ring is setup, wait until the first distributor
	// updates to the expected size
	if distributors[0].distributorsRing != nil {
		test.Poll(t, time.Second, cfg.numDistributors, func() interface{} {
			return distributors[0].HealthyInstancesCount()
		})
	}

	if len(cfg.ingesterDataByZone) != 0 {
		populateIngestersData(t, ingesters, cfg.ingesterDataByZone, cfg.ingesterDataTenantID)
	}

	return distributors, ingesters, distributorRegistries, cfg.ingestStorageKafka
}

func populateIngestersData(t testing.TB, ingesters []*mockIngester, dataPerZone map[string][]*mimirpb.WriteRequest, tenantID string) {
	ctx := user.InjectOrgID(context.Background(), tenantID)

	findIngester := func(zone string, id int) *mockIngester {
		for _, ing := range ingesters {
			if ing.zone == zone && ing.id == id {
				return ing
			}
		}
		panic("pushing to non-existent ingester; prepConfig.validate() should have caught this")
	}

	for zone, requests := range dataPerZone {
		for ingesterID, request := range requests {
			if request == nil {
				continue
			}
			ing := findIngester(zone, ingesterID)
			_, err := ing.Push(ctx, request)
			require.NoErrorf(t, err, "pushing to ingester %s", ing.address())
		}
	}
}

func makeWriteRequest(startTimestampMs int64, samples, metadata int, exemplars, histograms bool, metrics ...string) *mimirpb.WriteRequest {
	request := &mimirpb.WriteRequest{}
	for _, metric := range metrics {
		for i := 0; i < samples; i++ {
			req := makeTimeseries(
				[]string{
					model.MetricNameLabel, metric,
					"bar", "baz",
					"sample", fmt.Sprintf("%d", i),
				},
				makeSamples(startTimestampMs+int64(i), float64(i)),
				nil,
				nil,
			)

			if exemplars {
				req.Exemplars = makeExemplars(
					[]string{
						"traceID", "123456",
						"foo", "bar",
						"exemplar", fmt.Sprintf("%d", i),
					},
					startTimestampMs+int64(i),
					float64(i),
				)
			}

			if histograms {
				if i%2 == 0 {
					req.Histograms = makeHistograms(startTimestampMs+int64(i), generateTestHistogram(i))
				} else {
					req.Histograms = makeFloatHistograms(startTimestampMs+int64(i), generateTestFloatHistogram(i))
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

func makeWriteRequestWith(series ...mimirpb.PreallocTimeseries) *mimirpb.WriteRequest {
	return &mimirpb.WriteRequest{Timeseries: series}
}

func makeTimeseries(seriesLabels []string, samples []mimirpb.Sample, histograms []mimirpb.Histogram, exemplars []mimirpb.Exemplar) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Samples:    samples,
			Histograms: histograms,
			Exemplars:  exemplars,
		},
	}
}

func makeSamples(ts int64, value float64) []mimirpb.Sample {
	return []mimirpb.Sample{{
		Value:       value,
		TimestampMs: ts,
	}}
}

func makeExemplars(exemplarLabels []string, ts int64, value float64) []mimirpb.Exemplar {
	return []mimirpb.Exemplar{{
		Labels:      mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(exemplarLabels...)),
		Value:       value,
		TimestampMs: ts,
	}}
}

func makeHistograms(ts int64, histogram *histogram.Histogram) []mimirpb.Histogram {
	return []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(ts, histogram)}
}

func makeFloatHistograms(ts int64, histogram *histogram.FloatHistogram) []mimirpb.Histogram {
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

// labelSetGenWithEmptyLabels takes a slice of label names, it then returns a label set generator which generates
// labelsets of the given label names with empty values, plus a metric name and unique id label which are non-empty.
func labelSetGenWithEmptyLabels(metric string, names ...string) func(id int) []mimirpb.LabelAdapter {
	return func(id int) []mimirpb.LabelAdapter {
		labels := make([]mimirpb.LabelAdapter, 0, len(names)+2)
		labels = append(labels, mimirpb.LabelAdapter{
			Name:  model.MetricNameLabel,
			Value: metric,
		})

		labels = append(labels, mimirpb.LabelAdapter{
			Name:  "id",
			Value: fmt.Sprintf("%d", id),
		})

		for _, name := range names {
			labels = append(labels, mimirpb.LabelAdapter{
				Name:  name,
				Value: "",
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
			ts.Histograms = makeHistograms(int64(100+i), generateTestHistogram(i))
		} else {
			ts.Histograms = makeFloatHistograms(int64(100+i), generateTestFloatHistogram(i))
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

func makeWriteRequestExemplar(seriesLabels []string, timestamp int64, samples int, exemplarLabels []string) *mimirpb.WriteRequest {
	return makeWriteRequestWith(makeExemplarTimeseries(seriesLabels, timestamp, samples, exemplarLabels))
}

func makeWriteRequestHistogram(seriesLabels []string, timestamp int64, histogram *histogram.Histogram) *mimirpb.WriteRequest {
	return makeWriteRequestWith(makeHistogramTimeseries(seriesLabels, timestamp, histogram))
}

func makeWriteRequestFloatHistogram(seriesLabels []string, timestamp int64, histogram *histogram.FloatHistogram) *mimirpb.WriteRequest {
	return makeWriteRequestWith(makeFloatHistogramTimeseries(seriesLabels, timestamp, histogram))
}

func makeExemplarTimeseries(seriesLabels []string, timestamp int64, numSamples int, exemplarLabels []string) mimirpb.PreallocTimeseries {
	var samples []mimirpb.Sample
	for i := 0; i < numSamples; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       1,
			TimestampMs: timestamp,
		})
	}
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:  mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Samples: samples,
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
			Histograms: makeHistograms(timestamp, histogram),
		},
	}
}

func makeFloatHistogramTimeseries(seriesLabels []string, timestamp int64, histogram *histogram.FloatHistogram) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:     mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(seriesLabels...)),
			Histograms: makeFloatHistograms(timestamp, histogram),
		},
	}
}

func expectedResponse(start, end int, histograms bool, metrics ...string) model.Matrix {
	// TODO(histograms): should we modify the tests so it doesn't return both float and histogram for the same timestamp? (but still test sending float alone, histogram alone, and mixed) but might not be worth fixing the mock ingester
	result := model.Matrix{}
	for _, metricName := range metrics {
		for i := start; i < end; i++ {
			ss := &model.SampleStream{
				Metric: model.Metric{
					model.MetricNameLabel: model.LabelValue(metricName),
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
	zone                          string
	labelNamesStreamResponseDelay time.Duration
	timeOut                       bool
	tokens                        []uint32
	id                            int
	disableStreamingResponse      bool

	// partitionReader is responsible to consume a partition from Kafka when the
	// ingest storage is enabled. This field is nil if the ingest storage is disabled.
	partitionReader *ingest.PartitionReader

	// Hooks.
	hooksMx        sync.Mutex
	beforePushHook func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool)
}

func (i *mockIngester) registerBeforePushHook(fn func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool)) {
	i.hooksMx.Lock()
	defer i.hooksMx.Unlock()
	i.beforePushHook = fn
}

func (i *mockIngester) getBeforePushHook() func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
	i.hooksMx.Lock()
	defer i.hooksMx.Unlock()
	return i.beforePushHook
}

func (i *mockIngester) instanceID() string {
	if i.zone != "" {
		return fmt.Sprintf("ingester-%s-%d", i.zone, i.id)
	}

	return fmt.Sprintf("ingester-%d", i.id)
}

// partitionID returns the partition ID owned by this ingester when ingest storage is used.
func (i *mockIngester) partitionID() int32 {
	id, err := ingest.IngesterPartitionID(i.instanceID())
	if err != nil {
		panic(err)
	}

	return id
}

func (i *mockIngester) address() string {
	return i.instanceID()
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

// metricNames return a sorted list of ingested metric names.
func (i *mockIngester) metricNames() []string {
	i.Lock()
	defer i.Unlock()

	var out []string

	for _, series := range i.timeseries {
		if metricName, err := extract.UnsafeMetricNameFromLabelAdapters(series.Labels); err == nil && !slices.Contains(out, metricName) {
			out = append(out, metricName)
		}
	}

	slices.Sort(out)

	return out
}

func (i *mockIngester) Check(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	i.trackCall("Check")

	return &grpc_health_v1.HealthCheckResponse{}, nil
}

func (i *mockIngester) Close() error {
	return nil
}

func (i *mockIngester) Push(ctx context.Context, req *mimirpb.WriteRequest, _ ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	i.trackCall("Push")

	time.Sleep(i.pushDelay)

	if hook := i.getBeforePushHook(); hook != nil {
		if res, err, handled := hook(ctx, req); handled {
			return res, err
		}
	}

	i.Lock()
	defer i.Unlock()

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

	for _, unsafeSeries := range req.Timeseries {
		// Make a copy because the request Timeseries are reused. It's important to make a copy
		// even if the timeseries already exists in our local map, because we may still retain
		// some request's memory when merging exemplars.
		series, err := clonePreallocTimeseries(unsafeSeries)
		if err != nil {
			return nil, err
		}

		hash := mimirpb.ShardByAllLabelAdapters(orgid, series.Labels)
		existing, ok := i.timeseries[hash]
		if !ok {
			i.timeseries[hash] = &series
		} else {
			existing.Samples = append(existing.Samples, series.Samples...)
			existing.Histograms = append(existing.Histograms, series.Histograms...)
			existing.Exemplars = append(existing.Exemplars, series.Exemplars...)
		}
	}

	for _, m := range req.Metadata {
		hash := mimirpb.ShardByMetricName(orgid, m.MetricFamilyName)
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

func (i *mockIngester) QueryStream(ctx context.Context, req *client.QueryRequest, _ ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
	i.trackCall("QueryStream")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	if err := i.enforceQueryDelay(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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
				wireChunks = append(wireChunks, makeWireChunk(c))
			}
		}
		if fhexists {
			for _, c := range fhchunks {
				wireChunks = append(wireChunks, makeWireChunk(c))
			}
		}

		if i.disableStreamingResponse || req.StreamingChunksBatchSize == 0 {
			nonStreamingResponses = append(nonStreamingResponses, &client.QueryStreamResponse{
				Chunkseries: []client.TimeSeriesChunk{
					{
						Labels: ts.Labels,
						Chunks: wireChunks,
					},
				},
			})
		} else {
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
		}
	}

	var results []*client.QueryStreamResponse

	if i.disableStreamingResponse {
		results = nonStreamingResponses
	} else {
		endOfLabelsMessage := &client.QueryStreamResponse{
			IsEndOfSeriesStream: true,
		}
		results = append(streamingLabelResponses, endOfLabelsMessage)
		results = append(results, streamingChunkResponses...)
	}

	return &stream{
		ctx:     ctx,
		results: results,
	}, nil
}

func (i *mockIngester) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest, _ ...grpc.CallOption) (*client.ExemplarQueryResponse, error) {
	i.trackCall("QueryExemplars")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	from, through, multiMatchers, err := client.FromExemplarQueryRequest(req)
	if err != nil {
		return nil, err
	}

	res := &client.ExemplarQueryResponse{}

	for _, series := range i.timeseries {
		seriesMatches := false
		seriesLabels := mimirpb.FromLabelAdaptersToLabels(series.Labels)

		// Check if the series matches any of the matchers in the request.
		for _, matchers := range multiMatchers {
			matcherMatches := true

			for _, matcher := range matchers {
				if !matcher.Matches(seriesLabels.Get(matcher.Name)) {
					matcherMatches = false
					break
				}
			}

			if matcherMatches {
				seriesMatches = true
				break
			}
		}

		if !seriesMatches {
			continue
		}

		// Filter exemplars by time range.
		var exemplars []mimirpb.Exemplar
		for _, exemplar := range series.Exemplars {
			if exemplar.TimestampMs >= from && exemplar.TimestampMs <= through {
				exemplars = append(exemplars, exemplar)
			}
		}

		if len(exemplars) > 0 {
			res.Timeseries = append(res.Timeseries, mimirpb.TimeSeries{
				Labels:    series.Labels,
				Exemplars: exemplars,
			})
		}
	}

	// Sort series by labels because the real ingester returns sorted ones.
	slices.SortFunc(res.Timeseries, func(a, b mimirpb.TimeSeries) int {
		aKey := mimirpb.FromLabelAdaptersToKeyString(a.Labels)
		bKey := mimirpb.FromLabelAdaptersToKeyString(b.Labels)
		return strings.Compare(aKey, bKey)
	})

	return res, nil
}

func (i *mockIngester) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest, _ ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	i.trackCall("MetricsForLabelMatchers")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) LabelValues(ctx context.Context, req *client.LabelValuesRequest, _ ...grpc.CallOption) (*client.LabelValuesResponse, error) {
	i.trackCall("LabelValues")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) LabelNames(ctx context.Context, req *client.LabelNamesRequest, _ ...grpc.CallOption) (*client.LabelNamesResponse, error) {
	i.trackCall("LabelNames")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) MetricsMetadata(ctx context.Context, _ *client.MetricsMetadataRequest, _ ...grpc.CallOption) (*client.MetricsMetadataResponse, error) {
	i.trackCall("MetricsMetadata")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) LabelNamesAndValues(ctx context.Context, _ *client.LabelNamesAndValuesRequest, _ ...grpc.CallOption) (client.Ingester_LabelNamesAndValuesClient, error) {
	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

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

func (i *mockIngester) LabelValuesCardinality(ctx context.Context, req *client.LabelValuesCardinalityRequest, _ ...grpc.CallOption) (client.Ingester_LabelValuesCardinalityClient, error) {
	i.trackCall("LabelValuesCardinality")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) ActiveSeries(ctx context.Context, req *client.ActiveSeriesRequest, _ ...grpc.CallOption) (client.Ingester_ActiveSeriesClient, error) {
	i.trackCall("ActiveSeries")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	matchers, err := client.FromLabelMatchers(req.GetMatchers())
	if err != nil {
		return nil, err
	}

	var results []*client.ActiveSeriesResponse

	resp := &client.ActiveSeriesResponse{}

	for _, series := range i.timeseries {
		if match(series.Labels, matchers) {
			lbls := series.Labels
			if req.Type == client.NATIVE_HISTOGRAM_SERIES {
				if len(series.Histograms) == 0 {
					continue
				}
				h := series.Histograms[len(series.Histograms)-1]
				bucketCount := len(h.NegativeCounts) + len(h.NegativeDeltas) + len(h.PositiveCounts) + len(h.PositiveDeltas)
				resp.BucketCount = append(resp.BucketCount, uint64(bucketCount))
			}
			resp.Metric = append(resp.Metric, &mimirpb.Metric{Labels: lbls})
		}
		if len(resp.Metric) > 1 {
			results = append(results, resp)
			resp = &client.ActiveSeriesResponse{}
		}
	}
	if len(resp.Metric) > 0 {
		results = append(results, resp)
	}

	return &activeSeriesStream{results: results}, nil
}

type activeSeriesStream struct {
	grpc.ClientStream
	next    int
	results []*client.ActiveSeriesResponse
}

func (s *activeSeriesStream) Recv() (*client.ActiveSeriesResponse, error) {
	if s.next >= len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.next]
	s.next++
	return result, nil
}

func (s *activeSeriesStream) CloseSend() error {
	return nil
}

func (i *mockIngester) trackCall(name string) {
	i.Lock()
	defer i.Unlock()

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

func (i *mockIngester) enforceReadConsistency(ctx context.Context) error {
	// Strong read consistency is required to be enforced only if ingest storage is enabled.
	if i.partitionReader == nil {
		return nil
	}

	level, ok := api.ReadConsistencyLevelFromContext(ctx)
	if !ok || level != api.ReadConsistencyStrong {
		return nil
	}

	return i.partitionReader.WaitReadConsistencyUntilLastProducedOffset(ctx)
}

func (i *mockIngester) enforceQueryDelay(ctx context.Context) error {
	select {
	case <-time.After(i.queryDelay):
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

type mockIngesterPusherAdapter struct {
	ingester *mockIngester
}

func newMockIngesterPusherAdapter(ingester *mockIngester) *mockIngesterPusherAdapter {
	return &mockIngesterPusherAdapter{
		ingester: ingester,
	}
}

// PushToStorage implements ingest.Pusher.
func (c *mockIngesterPusherAdapter) PushToStorage(ctx context.Context, req *mimirpb.WriteRequest) error {
	_, err := c.ingester.Push(ctx, req)
	return err
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

	// The mocked gRPC client's context.
	ctx context.Context

	i       int
	results []*client.QueryStreamResponse
}

func (*stream) CloseSend() error {
	return nil
}

func (s *stream) Recv() (*client.QueryStreamResponse, error) {
	// Check whether the context has been canceled, so that we can test the case the context
	// gets cancelled while reading messages from gRPC client.
	if s.ctx.Err() != nil {
		return nil, s.ctx.Err()
	}

	if s.i >= len(s.results) {
		return nil, io.EOF
	}
	result := s.results[s.i]
	s.i++
	return result, nil
}

func (s *stream) Context() context.Context {
	return s.ctx
}

func (i *mockIngester) AllUserStats(context.Context, *client.UserStatsRequest, ...grpc.CallOption) (*client.UsersStatsResponse, error) {
	return &i.stats, nil
}

func (i *mockIngester) UserStats(ctx context.Context, _ *client.UserStatsRequest, _ ...grpc.CallOption) (*client.UserStatsResponse, error) {
	i.trackCall("UserStats")

	if err := i.enforceReadConsistency(ctx); err != nil {
		return nil, err
	}

	i.Lock()
	defer i.Unlock()

	if !i.happy {
		return nil, errFail
	}

	return &client.UserStatsResponse{
		IngestionRate:     0,
		NumSeries:         uint64(len(i.timeseries)),
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
	expectedDetails := &mimirpb.ErrorDetails{Cause: mimirpb.BAD_DATA}

	for name, tc := range map[string]struct {
		metadata    []*mimirpb.MetricMetadata
		labels      [][]mimirpb.LabelAdapter
		samples     []mimirpb.Sample
		exemplars   []*mimirpb.Exemplar
		limits      func(limits *validation.Limits)
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(sampleTimestampTooNewMsgFormat, future, "testmetric")),
		},

		"validation does not fail for samples from the past without past_grace_period setting": {
			labels:  [][]mimirpb.LabelAdapter{{{Name: "foo", Value: "bar"}, {Name: labels.MetricName, Value: "testmetric"}}},
			samples: []mimirpb.Sample{{TimestampMs: int64(past), Value: 1}},
		},

		"validation fails for samples from the past": {
			labels:      [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples:     []mimirpb.Sample{{TimestampMs: int64(past), Value: 4}},
			limits:      func(limits *validation.Limits) { limits.PastGracePeriod = model.Duration(now.Sub(past) / 2) },
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(sampleTimestampTooOldMsgFormat, past, "testmetric")),
		},

		"exceeds maximum labels per series": {
			labels: [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}, {Name: "foo2", Value: "bar2"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       2,
			}},
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{foo="bar", foo2="bar2"}`, "")),
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(tooManyLabelsMsgFormat, 5, 2, `testmetric{foo-with-a-long-long-label="bar-with-a-long-long-value", foo2-with-a-long-long-label="bar2-with-a-long-long-value", foo3-with-a-long-long-label="bar3-with-a-long-long-value", foo4-with-a-lo`, "…")),
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{families="👩\u200d👦👨\u200d👧👨\u200d👩\u200d👧👩\u200d👧👩\u200d👩\u200d👦\u200d👦👨\u200d👩\u200d👧\u200d👦👨\u200d👧\u200d👦👨\u200d👩\u200d👦👪👨\u200d👦👨\u200d👦\u200d👦👨\u200d👨\u200d👧👨\u200d👧\u200d👧", foo="b"}`, "")),
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
			expectedErr: status.New(codes.InvalidArgument, fmt.Sprintf(tooManyLabelsMsgFormat, 3, 2, `testmetric{foo="bar", foo2="bar2"}`, "")),
		},
		"metadata validation failure": {
			metadata: []*mimirpb.MetricMetadata{{MetricFamilyName: "", Help: "a test metric.", Unit: "", Type: mimirpb.COUNTER}},
			labels:   [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}},
			samples: []mimirpb.Sample{{
				TimestampMs: int64(now),
				Value:       1,
			}},
			expectedErr: status.New(codes.InvalidArgument, metadataMetricNameMissingMsgFormat),
		},
		// Validation passes for empty exemplar labels, since we just want to skip the exemplars and not fail the time series as a whole.
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
		},
	} {
		t.Run(name, func(t *testing.T) {
			var limits validation.Limits
			flagext.DefaultValues(&limits)

			limits.CreationGracePeriod = model.Duration(2 * time.Hour)
			limits.MaxLabelNamesPerSeries = 2
			limits.MaxGlobalExemplarsPerUser = 10
			if tc.limits != nil {
				tc.limits(&limits)
			}

			ds, _, _, _ := prepare(t, prepConfig{
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

func TestDistributor_Push_Relabel(t *testing.T) {
	ctx := user.InjectOrgID(context.Background(), "user")

	type testcase struct {
		inputSeries          []mimirpb.LabelAdapter
		expectedSeries       []mimirpb.LabelAdapter
		metricRelabelConfigs []*relabel.Config
	}

	cases := []testcase{
		// No relabel config.
		{
			inputSeries:    labelAdapters("__name__", "foo", "cluster", "one"),
			expectedSeries: labelAdapters("__name__", "foo", "cluster", "one"),
		},
		{
			inputSeries:    labelAdapters("__name__", "foo", "cluster", "one"),
			expectedSeries: labelAdapters("__name__", "foo", "cluster", "two"),
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

		ds, ingesters, _, _ := prepare(t, prepConfig{
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
				assert.Equal(t, tc.expectedSeries, v.Labels)
			}
		}
	}
}

func countMockIngestersCalled(ingesters []*mockIngester, name string) int {
	count := 0
	for _, i := range ingesters {
		if i.countCalls(name) > 0 {
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
		ds, _, regs, _ := prepare(t, config)
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
	metaDataGen := func(_ int, metricName string) *mimirpb.MetricMetadata {
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

func TestDistributor_StorageConfigMetrics(t *testing.T) {
	t.Run("classic storage", func(t *testing.T) {
		t.Parallel()
		_, _, regs, _ := prepare(t, prepConfig{
			numDistributors:   1,
			numIngesters:      3,
			happyIngesters:    3,
			replicationFactor: 3,
		})
		assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(`
			# HELP cortex_distributor_replication_factor The configured replication factor.
			# TYPE cortex_distributor_replication_factor gauge
			cortex_distributor_replication_factor 3

			# HELP cortex_distributor_ingest_storage_enabled Whether writes are being processed via ingest storage. Equal to 1 if ingest storage is enabled, 0 if disabled.
			# TYPE cortex_distributor_ingest_storage_enabled gauge
			cortex_distributor_ingest_storage_enabled 0
		`), "cortex_distributor_replication_factor", "cortex_distributor_ingest_storage_enabled"))
	})

	t.Run("migration to ingest storage", func(t *testing.T) {
		t.Parallel()
		_, _, regs, _ := prepare(t, prepConfig{
			ingestStorageEnabled:          true,
			ingestStorageMigrationEnabled: true,
			numDistributors:               1,
			numIngesters:                  3,
			happyIngesters:                3,
			replicationFactor:             3,
		})
		assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(`
			# HELP cortex_distributor_replication_factor The configured replication factor.
			# TYPE cortex_distributor_replication_factor gauge
			cortex_distributor_replication_factor 3

			# HELP cortex_distributor_ingest_storage_enabled Whether writes are being processed via ingest storage. Equal to 1 if ingest storage is enabled, 0 if disabled.
			# TYPE cortex_distributor_ingest_storage_enabled gauge
			cortex_distributor_ingest_storage_enabled 1
		`), "cortex_distributor_replication_factor", "cortex_distributor_ingest_storage_enabled"))
	})

	t.Run("ingest storage", func(t *testing.T) {
		t.Parallel()
		_, _, regs, _ := prepare(t, prepConfig{
			ingestStorageEnabled: true,
			numDistributors:      1,
			numIngesters:         3,
			happyIngesters:       3,
			replicationFactor:    3,
		})
		assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(`
			# HELP cortex_distributor_ingest_storage_enabled Whether writes are being processed via ingest storage. Equal to 1 if ingest storage is enabled, 0 if disabled.
			# TYPE cortex_distributor_ingest_storage_enabled gauge
			cortex_distributor_ingest_storage_enabled 1
		`), "cortex_distributor_replication_factor", "cortex_distributor_ingest_storage_enabled"))
	})
}

func TestDistributor_CleanupIsDoneAfterLastIngesterReturns(t *testing.T) {
	// We want to decrement inflight requests and other counters that we use for limits
	// only after the last ingester has returned.
	// Distributor.Push returns after a quorum of ingesters have returned.
	// But there are still resources occupied within the distributor while it's
	// waiting for all ingesters to return. So we want the instance limits to accurately reflect that.

	distributors, ingesters, _, _ := prepare(t, prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		replicationFactor: 3,
		enableTracker:     false,
		configure: func(config *Config) {
			config.DefaultLimits.MaxInflightPushRequests = 1
		},
	})
	ingesters[2].pushDelay = time.Second // give the test enough time to do assertions

	lbls := labelAdapters("__name__", "metric_1", "key", "value_1")
	ctx := user.InjectOrgID(context.Background(), "user")

	_, err := distributors[0].Push(ctx, mockWriteRequest(lbls, 1, 1))
	assert.NoError(t, err)

	// First push request returned, but there's still an ingester call inflight.
	// This means that the push request is counted as inflight, so another incoming request should be rejected.
	_, err = distributors[0].Push(ctx, mockWriteRequest(nil, 1, 1))
	checkGRPCError(t, status.New(codes.Internal, errMaxInflightRequestsReached.Error()), nil, err)
}

func TestSeriesAreShardedToCorrectIngesters(t *testing.T) {
	config := prepConfig{
		numIngesters:      5,
		happyIngesters:    5,
		numDistributors:   1,
		replicationFactor: 1, // push each series to single ingester only
	}
	d, ingesters, _, _ := prepare(t, config)

	uniqueMetricsGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{
			{Name: "__name__", Value: fmt.Sprintf("%d", sampleIdx)},
			{Name: "x", Value: fmt.Sprintf("%d", sampleIdx)},
		}
	}
	exemplarLabelGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "exemplarLabel", Value: fmt.Sprintf("value_%d", sampleIdx)}}
	}
	metaDataGen := func(_ int, metricName string) *mimirpb.MetricMetadata {
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
	for ix, ing := range ingesters {
		totalSeries += len(ing.timeseries)
		totalMetadata += len(ing.metadata)

		for _, ts := range ing.timeseries {
			token := tokenForLabels(userName, ts.Labels)
			ingIx := getIngesterIndexForToken(token, ingesters)
			assert.Equal(t, ix, ingIx)
		}

		for _, metadataMap := range ing.metadata {
			for m := range metadataMap {
				token := tokenForMetadata(userName, m.MetricFamilyName)
				ingIx := getIngesterIndexForToken(token, ingesters)
				assert.Equal(t, ix, ingIx)
			}
		}
	}

	// Verify that all timeseries were forwarded to ingesters.
	for _, ts := range req.Timeseries {
		token := tokenForLabels(userName, ts.Labels)
		ingIx := getIngesterIndexForToken(token, ingesters)

		assert.Equal(t, ts.Labels, ingesters[ingIx].timeseries[token].Labels)
	}

	assert.Equal(t, series, totalSeries)
	assert.Equal(t, series, totalMetadata) // each series has unique metric name, and each metric name gets metadata
}

func TestHandlePushError(t *testing.T) {
	testErrorMsg := "this is a test error message"
	userID := "test"
	errWithUserID := fmt.Errorf("user=%s: %s", userID, testErrorMsg)
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
		"an Error gives the error returned by toErrorWithGRPCStatus()": {
			pushError:         mockDistributorErr(testErrorMsg),
			expectedGRPCError: status.Convert(toErrorWithGRPCStatus(mockDistributorErr(testErrorMsg), false)),
		},
		"a random error without status gives an Internal gRPC error": {
			pushError:         errWithUserID,
			expectedGRPCError: status.New(codes.Internal, errWithUserID.Error()),
		},
	}

	config := prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		replicationFactor: 1, // push each series to single ingester only
	}
	d, _, _, _ := prepare(t, config)
	ctx := context.Background()

	for testName, testData := range test {
		t.Run(testName, func(t *testing.T) {
			err := d[0].handlePushError(ctx, testData.pushError)
			if testData.expectedGRPCError == nil {
				require.Equal(t, testData.expectedOtherError, err)
			} else {
				var expectedDetails *mimirpb.ErrorDetails
				if distributorErr, ok := testData.pushError.(Error); ok {
					expectedDetails = &mimirpb.ErrorDetails{Cause: distributorErr.Cause()}
				}
				checkGRPCError(t, testData.expectedGRPCError, expectedDetails, err)
			}
		})
	}
}

func getIngesterIndexForToken(key uint32, ings []*mockIngester) int {
	tokens := []uint32{}
	tokensMap := map[uint32]int{}

	for ix, ing := range ings {
		tokens = append(tokens, ing.tokens...)
		for _, t := range ing.tokens {
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

func checkGRPCError(t *testing.T, expectedStatus *status.Status, expectedDetails *mimirpb.ErrorDetails, err error) {
	stat, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok)
	require.Equal(t, expectedStatus.Code(), stat.Code())
	require.Equal(t, expectedStatus.Message(), stat.Message())
	if expectedDetails == nil {
		require.Len(t, stat.Details(), 0)
	} else {
		details := stat.Details()
		require.Len(t, details, 1)
		errorDetails, ok := details[0].(*mimirpb.ErrorDetails)
		require.True(t, ok)
		require.Equal(t, expectedDetails, errorDetails)
	}
}

func createStatusWithDetails(t *testing.T, code codes.Code, message string, cause mimirpb.ErrorCause) *status.Status {
	stat := status.New(code, message)
	statWithDetails, err := stat.WithDetails(&mimirpb.ErrorDetails{Cause: cause})
	require.NoError(t, err)
	return statWithDetails
}

func countCalls(ingesters []*mockIngester, name string) int {
	count := 0

	for _, ing := range ingesters {
		count += ing.countCalls(name)
	}

	return count
}

func TestStartFinishRequest(t *testing.T) {
	uniqueMetricsGen := func(sampleIdx int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{
			{Name: "__name__", Value: fmt.Sprintf("metric_%d", sampleIdx)},
		}
	}

	type ctxType string
	const (
		distributorKey              ctxType = "dist"
		expectedInflightRequestsKey ctxType = "req"
		expectedInflightBytesKey    ctxType = "bytes"
	)

	// Pretend push went OK, make sure to call CleanUp. Also check for expected values of inflight requests and inflight request size.
	finishPush := func(ctx context.Context, pushReq *Request) error {
		defer pushReq.CleanUp()

		distrib := ctx.Value(distributorKey).(*Distributor)
		expReq := ctx.Value(expectedInflightRequestsKey).(int64)
		expBytes := ctx.Value(expectedInflightBytesKey).(int64)

		reqs := distrib.inflightPushRequests.Load()
		if expReq != reqs {
			return errors.Errorf("unexpected number of inflight requests: %d, expected: %d", reqs, expReq)
		}

		bs := distrib.inflightPushRequestsBytes.Load()
		if expBytes != bs {
			return errors.Errorf("unexpected number of inflight request bytes: %d, expected: %d", bs, expBytes)
		}
		return nil
	}

	type testCase struct {
		externalCheck       bool  // Start request "externally", from outside of distributor.
		httpgrpcRequestSize int64 // only used for external check.

		inflightRequestsBeforePush     int
		inflightRequestsSizeBeforePush int64
		addIngestionRateBeforePush     int64

		expectedStartError error
		expectedPushError  error
	}

	const (
		inflightLimit      = 5
		inflightBytesLimit = 1024
		ingestionRateLimit = 100
	)

	testcases := map[string]testCase{
		"request succeeds, internal": {
			expectedStartError: nil,
			expectedPushError:  nil,
		},

		"request succeeds, external": {
			externalCheck:      true,
			expectedStartError: nil,
			expectedPushError:  nil,
		},

		"request succeeds, external, with httpgrpc size": {
			externalCheck:       true,
			httpgrpcRequestSize: 100,
			expectedStartError:  nil,
			expectedPushError:   nil,
		},

		"too many inflight requests, internal": {
			inflightRequestsBeforePush:     inflightLimit,
			inflightRequestsSizeBeforePush: 0,
			expectedStartError:             errMaxInflightRequestsReached,
			expectedPushError:              errMaxInflightRequestsReached,
		},

		"too many inflight requests, external": {
			externalCheck:                  true,
			inflightRequestsBeforePush:     inflightLimit,
			inflightRequestsSizeBeforePush: 0,
			expectedStartError:             errMaxInflightRequestsReached,
			expectedPushError:              errMaxInflightRequestsReached,
		},

		"too many inflight bytes requests, internal": {
			inflightRequestsBeforePush:     1,
			inflightRequestsSizeBeforePush: 2 * inflightBytesLimit,
			expectedStartError:             errMaxInflightRequestsBytesReached,
			expectedPushError:              errMaxInflightRequestsBytesReached,
		},

		"too many inflight bytes requests, external": {
			externalCheck:                  true,
			inflightRequestsBeforePush:     1,
			inflightRequestsSizeBeforePush: 2 * inflightBytesLimit,
			expectedStartError:             nil, // httpgrpc request size is not set when calling StartPushRequest, so it's not checked.
			expectedPushError:              errMaxInflightRequestsBytesReached,
		},

		"too many inflight bytes requests, external with httpgrpc size within limit": {
			externalCheck:                  true,
			httpgrpcRequestSize:            500,
			inflightRequestsBeforePush:     1,
			inflightRequestsSizeBeforePush: inflightBytesLimit - 500,
			expectedStartError:             nil, // httpgrpc request size fits into inflight request size limit.
			expectedPushError:              errMaxInflightRequestsBytesReached,
		},

		"too many inflight bytes requests, external with httpgrpc size outside limit": {
			externalCheck:                  true,
			httpgrpcRequestSize:            500,
			inflightRequestsBeforePush:     1,
			inflightRequestsSizeBeforePush: inflightBytesLimit,
			expectedStartError:             errMaxInflightRequestsBytesReached,
			expectedPushError:              errMaxInflightRequestsBytesReached,
		},

		"high ingestion rate, internal": {
			addIngestionRateBeforePush: 100 * ingestionRateLimit,
			expectedStartError:         errMaxIngestionRateReached,
			expectedPushError:          errMaxIngestionRateReached,
		},

		"high ingestion rate, external": {
			externalCheck:              true,
			addIngestionRateBeforePush: 100 * ingestionRateLimit,
			expectedStartError:         errMaxIngestionRateReached,
			expectedPushError:          errMaxIngestionRateReached,
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			pushReq := makeWriteRequestForGenerators(1, uniqueMetricsGen, nil, nil)

			var limits validation.Limits
			flagext.DefaultValues(&limits)

			// Prepare distributor and wrap the mock push function with its middlewares.
			ds, _, _, _ := prepare(t, prepConfig{
				numDistributors: 1,
				limits:          &limits,
				enableTracker:   true,
				configure: func(config *Config) {
					config.DefaultLimits.MaxIngestionRate = ingestionRateLimit
					config.DefaultLimits.MaxInflightPushRequests = inflightLimit
					config.DefaultLimits.MaxInflightPushRequestsBytes = inflightBytesLimit
				},
			})
			wrappedPush := ds[0].wrapPushWithMiddlewares(finishPush)

			// Setup inflight values before calling push.
			ds[0].inflightPushRequests.Add(int64(tc.inflightRequestsBeforePush))
			ds[0].inflightPushRequestsBytes.Add(tc.inflightRequestsSizeBeforePush)
			ds[0].ingestionRate.Add(tc.addIngestionRateBeforePush)
			ds[0].ingestionRate.Tick()

			ctx := user.InjectOrgID(context.Background(), "user")

			// Set values that are checked by test handler.
			ctx = context.WithValue(ctx, distributorKey, ds[0])
			ctx = context.WithValue(ctx, expectedInflightRequestsKey, int64(tc.inflightRequestsBeforePush)+1)
			ctx = context.WithValue(ctx, expectedInflightBytesKey, tc.inflightRequestsSizeBeforePush+tc.httpgrpcRequestSize+int64(pushReq.Size()))

			if tc.externalCheck {
				var err error
				ctx, err = ds[0].StartPushRequest(ctx, tc.httpgrpcRequestSize)

				if tc.expectedStartError == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tc.expectedStartError)

					// Verify that errors returned by StartPushRequest method are NOT gRPC status errors.
					// They will be converted to gRPC status by grpcInflightMethodLimiter.
					require.Error(t, err)
					_, ok := grpcutil.ErrorToStatus(err)
					require.False(t, ok)
				}
			}

			err := wrappedPush(ctx, NewParsedRequest(pushReq))
			if tc.expectedPushError == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.expectedPushError)
			}

			if tc.externalCheck {
				ds[0].FinishPushRequest(ctx)
			}

			// Verify that inflight metrics are the same as before the request.
			require.Equal(t, int64(tc.inflightRequestsBeforePush), ds[0].inflightPushRequests.Load())
			require.Equal(t, tc.inflightRequestsSizeBeforePush, ds[0].inflightPushRequestsBytes.Load())
		})
	}
}

func TestDistributor_Push_SendMessageMetadata(t *testing.T) {
	const userID = "test"

	distributors, ingesters, _, _ := prepare(t, prepConfig{
		numIngesters:      1,
		happyIngesters:    1,
		numDistributors:   1,
		replicationFactor: 1,
	})

	require.Len(t, distributors, 1)
	require.Len(t, ingesters, 1)

	ctx := context.Background()
	ctx = user.InjectOrgID(ctx, userID)

	req := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries([]string{model.MetricNameLabel, "test1"}, makeSamples(time.Now().UnixMilli(), 1), nil, nil),
		},
		Source: mimirpb.API,
	}

	// Register a hook in the ingester's Push() to check whether the context contains the expected gRPC metadata.
	ingesters[0].registerBeforePushHook(func(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error, bool) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{strconv.Itoa(req.Size())}, md[grpcutil.MetadataMessageSize])

		return nil, nil, false
	})

	_, err := distributors[0].Push(ctx, req)
	require.NoError(t, err)

	// Ensure the ingester's Push() has been called.
	require.Equal(t, 1, ingesters[0].countCalls("Push"))
}

func TestQueryIngestersRingZoneSorter(t *testing.T) {
	testCases := map[string]struct {
		instances []ring.InstanceDesc
		verify    func(t *testing.T, sortedZones []string)
	}{
		"no instances": {
			instances: []ring.InstanceDesc{},
			verify: func(t *testing.T, sortedZones []string) {
				require.Empty(t, sortedZones)
			},
		},
		"one zone": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				require.Equal(t, []string{"zone-a"}, sortedZones)
			},
		},
		"many zones, all instances active": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.ACTIVE},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.ACTIVE},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.ACTIVE},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				// We don't care about the order.
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c"}, sortedZones)
			},
		},
		"many zones, one instance in one zone not active": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.ACTIVE},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.ACTIVE},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c"}, sortedZones, "all zones should be present")
				require.Equal(t, "zone-b", sortedZones[2], "zone with inactive instance should be last, but got %v", sortedZones)
			},
		},
		"many zones, one instance in multiple zones not active": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.PENDING},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.ACTIVE},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.ACTIVE},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c"}, sortedZones, "all zones should be present")

				// We don't care about the order of A and B, just that they're last.
				require.ElementsMatch(t, []string{"zone-a", "zone-b"}, sortedZones[1:], "zones with inactive instance should be last, but got %v", sortedZones)
			},
		},
		"many zones, each with one instance not active": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.PENDING},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.ACTIVE},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.PENDING},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				// We don't care about the order.
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c"}, sortedZones)
			},
		},
		"many zones, some with more inactive instances than others": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.ACTIVE},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.PENDING},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.ACTIVE},
			},
			verify: func(t *testing.T, sortedZones []string) {
				require.Equal(t, []string{"zone-a", "zone-c", "zone-b"}, sortedZones, "expected zones with the least number of inactive instances to be first")
			},
		},
		"many zones, all instances inactive": {
			instances: []ring.InstanceDesc{
				{Addr: "zone-a-instance-1", Zone: "zone-a", State: ring.PENDING},
				{Addr: "zone-a-instance-2", Zone: "zone-a", State: ring.PENDING},
				{Addr: "zone-b-instance-1", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-b-instance-2", Zone: "zone-b", State: ring.PENDING},
				{Addr: "zone-c-instance-1", Zone: "zone-c", State: ring.PENDING},
				{Addr: "zone-c-instance-2", Zone: "zone-c", State: ring.PENDING},
			},
			verify: func(t *testing.T, sortedZones []string) {
				// We don't care about the order.
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c"}, sortedZones)
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			replicationSet := ring.ReplicationSet{
				Instances:            testCase.instances,
				ZoneAwarenessEnabled: true,
			}

			sorted := queryIngestersRingZoneSorter(replicationSet)(uniqueZones(testCase.instances))
			testCase.verify(t, sorted)
		})
	}
}

func TestQueryIngesterPartitionsRingZoneSorter(t *testing.T) {
	testCases := map[string]struct {
		zones         []string
		preferredZone string
		verify        func(t *testing.T, sortedZones []string)
	}{
		"no zones": {
			zones: []string{},
			verify: func(t *testing.T, sortedZones []string) {
				require.Empty(t, sortedZones)
			},
		},
		"one zone, without preferred zone": {
			zones: []string{"zone-a"},
			verify: func(t *testing.T, sortedZones []string) {
				require.Equal(t, []string{"zone-a"}, sortedZones)
			},
		},
		"one zone, with preferred zone": {
			zones:         []string{"zone-a"},
			preferredZone: "zone-a",
			verify: func(t *testing.T, sortedZones []string) {
				require.Equal(t, []string{"zone-a"}, sortedZones)
			},
		},
		"two zones, without preferred zone": {
			zones: []string{"zone-a", "zone-b"},
			verify: func(t *testing.T, sortedZones []string) {
				require.ElementsMatch(t, []string{"zone-a", "zone-b"}, sortedZones)
			},
		},
		"two zones, with preferred zone": {
			zones:         []string{"zone-a", "zone-b"},
			preferredZone: "zone-b",
			verify: func(t *testing.T, sortedZones []string) {
				require.Equal(t, []string{"zone-b", "zone-a"}, sortedZones)
			},
		},
		"many zones, without preferred zone": {
			zones: []string{"zone-a", "zone-b", "zone-c", "zone-d"},
			verify: func(t *testing.T, sortedZones []string) {
				require.ElementsMatch(t, []string{"zone-a", "zone-b", "zone-c", "zone-d"}, sortedZones)
			},
		},
		"many zones, with preferred zone": {
			zones:         []string{"zone-a", "zone-b", "zone-c", "zone-d"},
			preferredZone: "zone-b",
			verify: func(t *testing.T, sortedZones []string) {
				require.Len(t, sortedZones, 4)
				require.Equal(t, "zone-b", sortedZones[0])
				require.ElementsMatch(t, []string{"zone-a", "zone-c", "zone-d"}, sortedZones[1:])
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			sorted := queryIngesterPartitionsRingZoneSorter(testCase.preferredZone)(testCase.zones)
			testCase.verify(t, sorted)
		})
	}
}

func uniqueZones(instances []ring.InstanceDesc) []string {
	var zones []string

	for _, i := range instances {
		if !slices.Contains(zones, i.Zone) {
			zones = append(zones, i.Zone)
		}
	}

	// Randomly shuffle the zones to ensure that the test case doesn't pass by coincidence.
	rand.Shuffle(len(zones), func(i, j int) {
		zones[i], zones[j] = zones[j], zones[i]
	})

	return zones
}

func cloneTimeseries(orig *mimirpb.TimeSeries) (*mimirpb.TimeSeries, error) {
	data, err := orig.Marshal()
	if err != nil {
		return nil, err
	}

	cloned := &mimirpb.TimeSeries{}
	err = cloned.Unmarshal(data)
	return cloned, err
}

func clonePreallocTimeseries(orig mimirpb.PreallocTimeseries) (mimirpb.PreallocTimeseries, error) {
	clonedSeries, err := cloneTimeseries(orig.TimeSeries)
	if err != nil {
		return mimirpb.PreallocTimeseries{}, err
	}

	return mimirpb.PreallocTimeseries{TimeSeries: clonedSeries}, nil
}

func TestCheckStartedMiddleware(t *testing.T) {
	// Create an in-memory KV store for the ring with 1 ingester registered.
	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })

	err := kvStore.CAS(context.Background(), ingester.IngesterRingKey,
		func(_ interface{}) (interface{}, bool, error) {
			d := &ring.Desc{}
			d.AddIngester("ingester-1", "127.0.0.1", "", ring.NewRandomTokenGenerator().GenerateTokens(128, nil), ring.ACTIVE, time.Now(), false, time.Time{})
			return d, true, nil
		},
	)
	require.NoError(t, err)

	ingestersRing, err := ring.New(ring.Config{
		KVStore:           kv.Config{Mock: kvStore},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: 1,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingestersRing))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingestersRing))
	})

	test.Poll(t, time.Second, 1, func() interface{} {
		return ingestersRing.InstancesCount()
	})

	var distributorConfig Config
	var clientConfig client.Config
	limits := validation.Limits{}
	flagext.DefaultValues(&distributorConfig, &clientConfig, &limits)
	distributorConfig.DistributorRing.Common.KVStore.Store = "inmemory"

	limits.IngestionRate = float64(rate.Inf) // Unlimited.

	distributorConfig.IngesterClientFactory = ring_client.PoolInstFunc(func(ring.InstanceDesc) (ring_client.PoolClient, error) {
		return &noopIngester{}, nil
	})

	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	distributor, err := New(distributorConfig, clientConfig, overrides, nil, ingestersRing, nil, true, nil, log.NewNopLogger())
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "user")
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_, err = distributor.Push(ctx, mimirpb.ToWriteRequest(
		[][]mimirpb.LabelAdapter{
			{
				{
					Name:  "__name__",
					Value: "foobar",
				},
			},
		},
		[]mimirpb.Sample{
			{
				TimestampMs: 1000,
				Value:       100,
			},
		},
		nil,
		nil,
		mimirpb.API,
	))

	// We expect the push request to be rejected with an unavailable error.
	require.NotNil(t, err)
	require.ErrorContains(t, err, "rpc error: code = Internal desc = distributor is unavailable (current state: New)")
}

func Test_outerMaybeDelayMiddleware(t *testing.T) {
	tests := []struct {
		name          string
		userID        string
		delay         time.Duration
		pushDuration  time.Duration
		expectedSleep time.Duration
	}{
		{
			name:          "No delay configured",
			userID:        "user1",
			delay:         0,
			pushDuration:  500 * time.Millisecond,
			expectedSleep: 0,
		},
		{
			name:          "Delay configured but request took longer than delay",
			userID:        "user2",
			delay:         500 * time.Millisecond,
			pushDuration:  1 * time.Second,
			expectedSleep: 0,
		},
		{
			name:          "Delay configured and request took less than delay",
			userID:        "user3",
			delay:         500 * time.Millisecond,
			pushDuration:  50 * time.Millisecond,
			expectedSleep: 450 * time.Millisecond,
		},
		{
			name:          "Failed to extract a tenantID",
			userID:        "",
			delay:         500 * time.Millisecond,
			pushDuration:  50 * time.Millisecond,
			expectedSleep: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limits := validation.NewMockTenantLimits(map[string]*validation.Limits{
				tc.userID: {
					IngestionArtificialDelay: model.Duration(tc.delay),
				},
			})
			overrides, err := validation.NewOverrides(*prepareDefaultLimits(), limits)
			require.NoError(t, err)

			// Mock to capture sleep and advance time.
			timeSource := &MockTimeSource{CurrentTime: time.Now()}

			distributor := &Distributor{
				log:    log.NewNopLogger(),
				limits: overrides,
				sleep:  timeSource.Sleep,
				now:    timeSource.Now,
			}

			// fake push just adds time to the mocked time to make it seem like time has moved forward.
			p := func(_ context.Context, _ *Request) error {
				timeSource.Add(tc.pushDuration)
				return nil
			}

			ctx := context.Background()
			if tc.userID != "" {
				ctx = user.InjectOrgID(ctx, tc.userID)
			}
			wrappedPush := distributor.outerMaybeDelayMiddleware(p)
			err = wrappedPush(ctx, NewParsedRequest(&mimirpb.WriteRequest{}))
			require.NoError(t, err)

			// Due to the 10% jitter we need to take into account that the number will not be deterministic in tests.
			difference := timeSource.Slept - tc.expectedSleep
			require.LessOrEqual(t, difference.Abs(), tc.expectedSleep/10)
		})
	}
}

type MockTimeSource struct {
	CurrentTime time.Time
	Slept       time.Duration
}

func (m *MockTimeSource) Now() time.Time {
	return m.CurrentTime
}

func (m *MockTimeSource) Sleep(d time.Duration) {
	if d > 0 {
		m.Slept += d
	}
}

func (m *MockTimeSource) Add(d time.Duration) {
	m.CurrentTime = m.CurrentTime.Add(d)
}
