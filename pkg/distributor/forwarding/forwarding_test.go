// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

var testConfig = Config{
	Enabled:            true,
	RequestTimeout:     time.Second,
	RequestConcurrency: 5,
	PropagateErrors:    true,
}

func TestForwardingSamplesSuccessfullyToCorrectTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()
	forwarder, reg := newForwarder(t, testConfig, true)

	url1, reqs1, bodies1, close1 := newTestServer(t, 200, true)
	defer close1()

	url2, reqs2, bodies2, close2 := newTestServer(t, 200, true)
	defer close2()

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Endpoint: url1, Ingest: false},
		"metric2": validation.ForwardingRule{Endpoint: url2, Ingest: true},
	}

	ts := []mimirpb.PreallocTimeseries{
		newSample(t, now, 1, 100, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, 200, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, 300, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, 400, "__name__", "metric2", "some_label", "bar"),
	}
	tsToIngest, errCh := forwarder.Forward(ctx, "", 0, rules, ts)

	// The metric2 should be returned by the forwarding because the matching rule has ingest set to "true".
	require.Len(t, tsToIngest, 2)
	requireLabelsEqual(t, tsToIngest[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, tsToIngest[0].Samples, now, 3)
	requireExemplarsEqual(t, tsToIngest[0].Exemplars, now, 300)
	requireLabelsEqual(t, tsToIngest[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, tsToIngest[1].Samples, now, 4)
	requireExemplarsEqual(t, tsToIngest[1].Exemplars, now, 400)

	// Loop until errCh gets closed, errCh getting closed indicates that all forwarding requests have completed.
	for err := range errCh {
		require.NoError(t, err)
	}

	for _, req := range append(reqs1(), reqs2()...) {
		require.Equal(t, "snappy", req.Header.Get("Content-Encoding"))
		require.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
	}

	bodies := bodies1()
	require.Len(t, bodies, 1)
	receivedReq1 := decodeBody(t, bodies[0])
	require.Len(t, receivedReq1.Timeseries, 2)
	requireLabelsEqual(t, receivedReq1.Timeseries[0].Labels, "__name__", "metric1", "some_label", "foo")
	requireSamplesEqual(t, receivedReq1.Timeseries[0].Samples, now, 1)
	require.Empty(t, receivedReq1.Timeseries[0].Exemplars)
	requireLabelsEqual(t, receivedReq1.Timeseries[1].Labels, "__name__", "metric1", "some_label", "bar")
	requireSamplesEqual(t, receivedReq1.Timeseries[1].Samples, now, 2)
	require.Empty(t, receivedReq1.Timeseries[1].Exemplars)

	bodies = bodies2()
	require.Len(t, bodies, 1)
	receivedReq2 := decodeBody(t, bodies[0])
	require.Len(t, receivedReq2.Timeseries, 2)
	requireLabelsEqual(t, receivedReq2.Timeseries[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, receivedReq2.Timeseries[0].Samples, now, 3)
	require.Empty(t, receivedReq2.Timeseries[0].Exemplars)
	requireLabelsEqual(t, receivedReq2.Timeseries[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, receivedReq2.Timeseries[1].Samples, now, 4)
	require.Empty(t, receivedReq2.Timeseries[1].Exemplars)

	expectedMetrics := `
	# HELP cortex_distributor_forward_requests_total The total number of requests the Distributor made to forward samples.
	# TYPE cortex_distributor_forward_requests_total counter
	cortex_distributor_forward_requests_total{} 2
	# HELP cortex_distributor_forward_samples_total The total number of samples the Distributor forwarded.
	# TYPE cortex_distributor_forward_samples_total counter
	cortex_distributor_forward_samples_total{} 4
`

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(expectedMetrics),
		"cortex_distributor_forward_requests_total",
		"cortex_distributor_forward_samples_total",
	))
}

func TestForwardingSamplesSuccessfullyToSingleTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()
	forwarder, reg := newForwarder(t, testConfig, true)

	url, reqs, bodiesFn, closeFn := newTestServer(t, 200, true)
	defer closeFn()

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Ingest: false, Endpoint: "not used"},
		"metric2": validation.ForwardingRule{Ingest: true, Endpoint: ""},
	}

	ts := []mimirpb.PreallocTimeseries{
		newSample(t, now, 1, 100, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, 200, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, 300, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, 400, "__name__", "metric2", "some_label", "bar"),
	}
	tsToIngest, errCh := forwarder.Forward(ctx, url, 0, rules, ts)

	// The metric2 should be returned by the forwarding because the matching rule has ingest set to "true".
	require.Len(t, tsToIngest, 2)
	requireLabelsEqual(t, tsToIngest[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, tsToIngest[0].Samples, now, 3)
	requireExemplarsEqual(t, tsToIngest[0].Exemplars, now, 300)
	requireLabelsEqual(t, tsToIngest[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, tsToIngest[1].Samples, now, 4)
	requireExemplarsEqual(t, tsToIngest[1].Exemplars, now, 400)

	// Loop until errCh gets closed, errCh getting closed indicates that all forwarding requests have completed.
	for err := range errCh {
		require.NoError(t, err)
	}

	for _, req := range reqs() {
		require.Equal(t, "snappy", req.Header.Get("Content-Encoding"))
		require.Equal(t, "application/x-protobuf", req.Header.Get("Content-Type"))
	}

	bodies := bodiesFn()
	// Single request, with all the metrics.
	require.Len(t, bodies, 1)

	receivedReq1 := decodeBody(t, bodies[0])
	require.Len(t, receivedReq1.Timeseries, 4)
	requireLabelsEqual(t, receivedReq1.Timeseries[0].Labels, "__name__", "metric1", "some_label", "foo")
	requireSamplesEqual(t, receivedReq1.Timeseries[0].Samples, now, 1)
	require.Empty(t, receivedReq1.Timeseries[0].Exemplars)

	requireLabelsEqual(t, receivedReq1.Timeseries[1].Labels, "__name__", "metric1", "some_label", "bar")
	requireSamplesEqual(t, receivedReq1.Timeseries[1].Samples, now, 2)
	require.Empty(t, receivedReq1.Timeseries[1].Exemplars)

	requireLabelsEqual(t, receivedReq1.Timeseries[2].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, receivedReq1.Timeseries[2].Samples, now, 3)
	require.Empty(t, receivedReq1.Timeseries[2].Exemplars)

	requireLabelsEqual(t, receivedReq1.Timeseries[3].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, receivedReq1.Timeseries[3].Samples, now, 4)
	require.Empty(t, receivedReq1.Timeseries[3].Exemplars)

	expectedMetrics := `
	# HELP cortex_distributor_forward_requests_total The total number of requests the Distributor made to forward samples.
	# TYPE cortex_distributor_forward_requests_total counter
	cortex_distributor_forward_requests_total{} 1
	# HELP cortex_distributor_forward_samples_total The total number of samples the Distributor forwarded.
	# TYPE cortex_distributor_forward_samples_total counter
	cortex_distributor_forward_samples_total{} 4
`

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(expectedMetrics),
		"cortex_distributor_forward_requests_total",
		"cortex_distributor_forward_samples_total",
	))
}

func TestForwardingOmitOldSamples(t *testing.T) {
	ctx := context.Background()

	now := time.Now()
	mockTime := func() time.Time {
		return now
	}
	dontForwardOlderThan := 10 * time.Minute
	cutOffTs := now.UnixMilli() - dontForwardOlderThan.Milliseconds()

	type testCase struct {
		name        string
		inputTs     []mimirpb.PreallocTimeseries
		expectedTs  []mimirpb.PreallocTimeseries
		expectError error
	}

	testCases := []testCase{
		{
			name: "drop half of the samples and half of the exemplars",
			inputTs: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Samples: []mimirpb.Sample{
							{TimestampMs: cutOffTs - 100, Value: 1}, // too old
							{TimestampMs: cutOffTs - 50, Value: 1},  // too old
						},
						Exemplars: []mimirpb.Exemplar{
							{TimestampMs: cutOffTs - 100, Value: 1}, // too old
							{TimestampMs: cutOffTs - 50, Value: 1},  // too old
						},
					},
				}, {
					TimeSeries: &mimirpb.TimeSeries{
						Samples: []mimirpb.Sample{
							{TimestampMs: cutOffTs, Value: 1},      // too old
							{TimestampMs: cutOffTs + 50, Value: 1}, // ok
						},
						Exemplars: []mimirpb.Exemplar{
							{TimestampMs: cutOffTs, Value: 1},      // too old
							{TimestampMs: cutOffTs + 50, Value: 1}, // ok
						},
					},
				}, {
					TimeSeries: &mimirpb.TimeSeries{
						Samples: []mimirpb.Sample{
							{TimestampMs: cutOffTs + 100, Value: 1}, // ok
							{TimestampMs: cutOffTs + 150, Value: 1}, // ok
						},
						Exemplars: []mimirpb.Exemplar{
							{TimestampMs: cutOffTs + 100, Value: 1}, // ok
							{TimestampMs: cutOffTs + 150, Value: 1}, // ok
						},
					},
				},
			},
			expectedTs: []mimirpb.PreallocTimeseries{
				{
					TimeSeries: &mimirpb.TimeSeries{
						Samples: []mimirpb.Sample{
							{TimestampMs: cutOffTs, Value: 1},
						},
						Exemplars: []mimirpb.Exemplar{
							{TimestampMs: cutOffTs, Value: 1},
						},
					},
				}, {
					TimeSeries: &mimirpb.TimeSeries{
						Samples: []mimirpb.Sample{
							{TimestampMs: cutOffTs + 100, Value: 1},
							{TimestampMs: cutOffTs + 150, Value: 1},
						},
						Exemplars: []mimirpb.Exemplar{
							{TimestampMs: cutOffTs + 100, Value: 1},
							{TimestampMs: cutOffTs + 150, Value: 1},
						},
					},
				},
			},
			expectError: errSamplesTooOld,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := newForwarder(t, testConfig, true)
			f.(*forwarder).timeNow = mockTime

			url, _, bodiesFn, closeFn := newTestServer(t, 200, true)
			defer closeFn()

			testMetric := "metric1"
			rules := validation.ForwardingRules{
				testMetric: validation.ForwardingRule{Ingest: true, Endpoint: "not used"},
			}

			for tsIdx := range tc.inputTs {
				setLabels(t, tc.inputTs[tsIdx].TimeSeries, "__name__", testMetric, "some_label", "bar")
			}

			toIngest, errCh := f.Forward(ctx, url, dontForwardOlderThan, rules, tc.inputTs)

			// All samples should be ingested, including the ones that were dropped by the forwarding.
			require.Equal(t, tc.inputTs, toIngest)

			if tc.expectError == nil {
				require.NoError(t, <-errCh)
			} else {
				err := <-errCh
				require.Equal(t, tc.expectError, err)
			}

			bodies := bodiesFn()
			// Expecting single request, with all the metrics.
			require.Len(t, bodies, 1)

			receivedReq1 := decodeBody(t, bodies[0])
			require.Equal(t, tc.expectedTs, receivedReq1.Timeseries)
		})
	}
}

func TestForwardingSamplesWithDifferentErrorsWithPropagation(t *testing.T) {
	type status uint16
	const (
		statusOk                status = 200
		statusErrNonRecoverable status = 400
		statusErrRecoverable    status = 500
	)

	type testCase struct {
		name              string
		config            Config
		remoteStatusCodes []int
		expectStatusCodes []status
	}

	tcs := []testCase{
		{
			name:              "one non-recoverable between two successful",
			config:            testConfig,
			remoteStatusCodes: []int{200, 400, 200},
			expectStatusCodes: []status{statusErrNonRecoverable},
		}, {
			name:              "one of each (1)",
			config:            testConfig,
			remoteStatusCodes: []int{200, 400, 500},
			expectStatusCodes: []status{statusErrNonRecoverable, statusErrRecoverable},
		}, {
			name:              "one of each (2)",
			config:            testConfig,
			remoteStatusCodes: []int{500, 400, 200},
			expectStatusCodes: []status{statusErrNonRecoverable, statusErrRecoverable},
		}, {
			name:              "successful codes should result in no error",
			config:            testConfig,
			remoteStatusCodes: []int{200, 200},
			expectStatusCodes: []status{},
		}, {
			name:              "codes which are not divisible by 100 (1)",
			config:            testConfig,
			remoteStatusCodes: []int{204, 401, 200},
			expectStatusCodes: []status{statusErrNonRecoverable},
		}, {
			name:              "codes which are not divisible by 100 (2)",
			config:            testConfig,
			remoteStatusCodes: []int{202, 403, 502},
			expectStatusCodes: []status{statusErrNonRecoverable, statusErrRecoverable},
		}, {
			name:              "codes which are not divisible by 100 (3)",
			config:            testConfig,
			remoteStatusCodes: []int{504, 404, 201},
			expectStatusCodes: []status{statusErrNonRecoverable, statusErrRecoverable},
		}, {
			name: "errors dont get propagated",
			config: Config{
				Enabled:            testConfig.Enabled,
				RequestTimeout:     testConfig.RequestTimeout,
				RequestConcurrency: testConfig.RequestConcurrency,
				PropagateErrors:    false,
			},
			remoteStatusCodes: []int{504, 404, 201},
			expectStatusCodes: []status{},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			forwarder, reg := newForwarder(t, tc.config, true)
			urls := make([]string, len(tc.remoteStatusCodes))
			closers := make([]func(), len(tc.remoteStatusCodes))
			expectedErrorsByStatusCode := make(map[int]int)
			for i, code := range tc.remoteStatusCodes {
				urls[i], _, _, closers[i] = newTestServer(t, code, false)
				defer closers[i]()

				if code/100 == 2 {
					continue
				}
				expectedErrorsByStatusCode[code]++
			}

			rules := make(validation.ForwardingRules)
			metrics := make([]string, 0, len(tc.remoteStatusCodes))
			for i, url := range urls {
				metric := fmt.Sprintf("metric%d", i)
				metrics = append(metrics, metric)
				rules[metric] = validation.ForwardingRule{Endpoint: url}
			}

			now := time.Now().UnixMilli()
			var ts []mimirpb.PreallocTimeseries
			for _, metric := range metrics {
				ts = append(ts, newSample(t, now, 1, 100, "__name__", metric))
			}
			_, errCh := forwarder.Forward(context.Background(), "", 0, rules, ts)

			gotStatusCodes := []status{}
			for err := range errCh {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok)
				gotStatusCodes = append(gotStatusCodes, status(resp.Code))
			}

			sortStatusCodes := func(statusCodes []status) {
				sort.Slice(statusCodes, func(i, j int) bool { return statusCodes[i] < statusCodes[j] })
			}
			sortStatusCodes(gotStatusCodes)
			sortStatusCodes(tc.expectStatusCodes)
			require.Equal(t, tc.expectStatusCodes, gotStatusCodes)

			var expectedMetrics strings.Builder
			if len(expectedErrorsByStatusCode) > 0 {
				expectedMetrics.WriteString(`
				# TYPE cortex_distributor_forward_errors_total counter
				# HELP cortex_distributor_forward_errors_total The total number of errors that the distributor received from forwarding targets when trying to send samples to them.`)
			}
			for statusCode, count := range expectedErrorsByStatusCode {
				expectedMetrics.WriteString(fmt.Sprintf(`
					cortex_distributor_forward_errors_total{status_code="%d"} %d
	`, statusCode, count))
			}

			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics.String()),
				"cortex_distributor_forward_errors_total",
			))
		})
	}
}

func TestForwardingEnsureThatPooledObjectsGetReturned(t *testing.T) {
	sampleCount := 20

	const forwardingTarget1 = "target1"
	const forwardingTarget2 = "target2"
	const ingested = "ingested"

	type testCase struct {
		name          string
		sendSamples   map[string]int
		rules         validation.ForwardingRules
		expectSamples map[string]int
	}

	testCases := []testCase{
		{
			name: "forward samples of one metric to one target and don't ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount,
			},
		}, {
			name: "forward samples of one metric to one target and ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: true},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount,
				ingested:          sampleCount,
			},
		}, {
			name: "forward samples of four metrics to two targets and don't ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: false},
				"metric2": {Endpoint: forwardingTarget2, Ingest: false},
				"metric3": {Endpoint: forwardingTarget1, Ingest: false},
				"metric4": {Endpoint: forwardingTarget2, Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount / 2,
				forwardingTarget2: sampleCount / 2,
			},
		}, {
			name: "forward samples of four metrics to two targets and ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: true},
				"metric2": {Endpoint: forwardingTarget1, Ingest: true},
				"metric3": {Endpoint: forwardingTarget2, Ingest: true},
				"metric4": {Endpoint: forwardingTarget2, Ingest: true},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount / 2,
				forwardingTarget2: sampleCount / 2,
				ingested:          sampleCount,
			},
		}, {
			name: "forward samples of four metrics to two targets and ingest half of them (1)",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: true},
				"metric2": {Endpoint: forwardingTarget2, Ingest: true},
				"metric3": {Endpoint: forwardingTarget1, Ingest: false},
				"metric4": {Endpoint: forwardingTarget2, Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount / 2,
				forwardingTarget2: sampleCount / 2,
				ingested:          sampleCount / 2,
			},
		}, {
			name: "forward samples of four metrics to two targets and ingest half of them (2)",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Endpoint: forwardingTarget1, Ingest: true},
				"metric2": {Endpoint: forwardingTarget2, Ingest: false},
				"metric3": {Endpoint: forwardingTarget1, Ingest: true},
				"metric4": {Endpoint: forwardingTarget2, Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget1: sampleCount / 2,
				forwardingTarget2: sampleCount / 2,
				ingested:          sampleCount / 2,
			},
		}, {
			name: "don't forward samples, just ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount / 2,
				"metric2": sampleCount / 2,
			},
			rules: validation.ForwardingRules{},
			expectSamples: map[string]int{
				ingested: sampleCount,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			url1, _, bodies1, closer1 := newTestServer(t, 200, true)
			defer closer1()
			url2, _, bodies2, closer2 := newTestServer(t, 200, true)
			defer closer2()

			// Replace forwarding target strings with URLs of test servers.
			for metric, rule := range tc.rules {
				if rule.Endpoint == forwardingTarget1 {
					rule.Endpoint = url1
				} else if rule.Endpoint == forwardingTarget2 {
					rule.Endpoint = url2
				}
				tc.rules[metric] = rule
			}

			// bodiesByForwardingTarget maps the forwarding targets to the methods to obtain the bodies that they received.
			bodiesByForwardingTarget := map[string]func() [][]byte{
				forwardingTarget1: bodies1,
				forwardingTarget2: bodies2,
			}

			// Instantiate the forwarding with validating pools.
			forwarder, validatePoolUsage := getForwarderWithValidatingPools(t, sampleCount, 1000, 1000)

			// Generate the TimeSeries slice to submit to the forwarder.
			ts := forwarder.pools.getTsSlice()
			for metric, sampleCount := range tc.sendSamples {
				// Ensure that "append" doesn't create a new slice.
				require.True(t, cap(ts) > len(ts), "timeseries slice should have enough capacity to append the test data to it")

				tsWriteIdx := len(ts)
				ts = append(ts, mimirpb.PreallocTimeseries{TimeSeries: forwarder.pools.getTs()})
				ts[tsWriteIdx].TimeSeries.Labels = []mimirpb.LabelAdapter{
					{Name: "__name__", Value: metric},
					{Name: "foo", Value: "bar"},
				}
				for sampleIdx := 0; sampleIdx < sampleCount; sampleIdx++ {
					sample := mimirpb.Sample{
						TimestampMs: int64(sampleIdx),
						Value:       42,
					}
					ts[tsWriteIdx].TimeSeries.Samples = append(ts[tsWriteIdx].TimeSeries.Samples, sample)
				}
			}

			// Perform the forwarding operation.
			toIngest, errCh := forwarder.Forward(context.Background(), "", 0, tc.rules, ts)
			require.NoError(t, <-errCh)

			// receivedSamples counts the number of samples that each forwarding target has received.
			receivedSamples := make(map[string]int)
			for forwardingTarget, bodies := range bodiesByForwardingTarget {
				for _, body := range bodies() {
					writeReq := decodeBody(t, body)
					for _, ts := range writeReq.Timeseries {
						receivedForTarget := receivedSamples[forwardingTarget]
						receivedForTarget += len(ts.Samples)
						receivedSamples[forwardingTarget] = receivedForTarget
					}
				}
			}

			// "ingested" is a special target which indicates that a sample should be ingested into the ingesters,
			// for the sake of this test we treat it like any other forwarding target.
			for _, sample := range toIngest {
				receivedSamples[ingested] += len(sample.Samples)
			}

			// Check that each forwarding target has received the correct number of samples.
			require.Len(t, receivedSamples, len(tc.expectSamples))
			for forwardingTarget, expectedSampleCount := range tc.expectSamples {
				require.Equal(t, expectedSampleCount, receivedSamples[forwardingTarget])
			}

			// Return the slice with the samples to ingest to the pool, the distributor would do this in its request cleanup.
			forwarder.pools.putTsSlice(toIngest)

			// Validate pool usage.
			validatePoolUsage()
		})
	}
}

// getForwarderWithValidatingPools returns a forwarder with validating pools, so the pool usage can be validated to be correct.
// The specified caps must be large enough to hold all the data that will be stored in the respective slices because
// otherwise any "append()" will replace the slice which will result in a test failure because the original slice
// won't be returned to the pool.
func getForwarderWithValidatingPools(t *testing.T, tsSliceCap, protobufCap, snappyCap int) (*forwarder, func()) {
	t.Helper()

	var validateUsage func()
	f, _ := newForwarder(t, testConfig, false)
	fTyped := f.(*forwarder)
	fTyped.pools, validateUsage = validatingPools(t, tsSliceCap, protobufCap, snappyCap)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), f))

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), f))
	})

	return fTyped, validateUsage
}

func newForwarder(tb testing.TB, cfg Config, start bool) (Forwarder, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	log := log.NewNopLogger()

	forwarder := NewForwarder(cfg, reg, log)

	if start {
		require.NoError(tb, services.StartAndAwaitRunning(context.Background(), forwarder))

		tb.Cleanup(func() {
			require.NoError(tb, services.StopAndAwaitTerminated(context.Background(), forwarder))
		})
	}

	return forwarder, reg
}

func newSample(tb testing.TB, time int64, value float64, exemplarValue float64, labelValuePairs ...string) mimirpb.PreallocTimeseries {
	tb.Helper()

	ts := &mimirpb.TimeSeries{
		Samples: []mimirpb.Sample{
			{TimestampMs: time, Value: value},
		},
		Exemplars: []mimirpb.Exemplar{
			{TimestampMs: time, Value: exemplarValue},
		},
	}

	setLabels(tb, ts, labelValuePairs...)

	return mimirpb.PreallocTimeseries{
		TimeSeries: ts,
	}
}

func newSampleFromPool(tb testing.TB, time int64, value float64, labelValuePairs ...string) mimirpb.PreallocTimeseries {
	tb.Helper()

	ts := mimirpb.TimeseriesFromPool()

	setLabels(tb, ts, labelValuePairs...)

	return mimirpb.PreallocTimeseries{
		TimeSeries: ts,
	}
}

func setLabels(tb testing.TB, ts *mimirpb.TimeSeries, labelValuePairs ...string) {
	require.Zero(tb, len(labelValuePairs)%2)

	for i := 0; i < len(labelValuePairs)/2; i++ {
		ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
			Name:  labelValuePairs[i*2],
			Value: labelValuePairs[i*2+1],
		})
	}
}

func requireLabelsEqual(t *testing.T, gotLabels []mimirpb.LabelAdapter, wantLabelValuePairs ...string) {
	t.Helper()

	require.Zero(t, len(wantLabelValuePairs)%2)
	require.Equal(t, len(wantLabelValuePairs)/2, len(gotLabels))

	for i := range gotLabels {
		require.Equal(t, wantLabelValuePairs[i*2], gotLabels[i].Name)
		require.Equal(t, wantLabelValuePairs[i*2+1], gotLabels[i].Value)
	}
}

func requireSamplesEqual(t *testing.T, gotSamples []mimirpb.Sample, wantTimeValuePairs ...int64) {
	t.Helper()

	require.Zero(t, len(wantTimeValuePairs)%2)
	require.Equal(t, len(wantTimeValuePairs)/2, len(gotSamples))

	for i := range gotSamples {
		require.Equal(t, wantTimeValuePairs[i*2], gotSamples[i].TimestampMs)
		require.Equal(t, float64(wantTimeValuePairs[i*2+1]), gotSamples[i].Value)
	}
}

func requireExemplarsEqual(t *testing.T, gotExemplars []mimirpb.Exemplar, wantTimeValuePairs ...int64) {
	t.Helper()

	require.Zero(t, len(wantTimeValuePairs)%2)
	require.Equal(t, len(wantTimeValuePairs)/2, len(gotExemplars))

	for i := range gotExemplars {
		require.Equal(t, wantTimeValuePairs[i*2], gotExemplars[i].TimestampMs)
		require.Equal(t, float64(wantTimeValuePairs[i*2+1]), gotExemplars[i].Value)
	}
}

func newTestServer(tb testing.TB, status int, record bool) (string, func() []*http.Request, func() [][]byte, func()) {
	tb.Helper()

	var requests []*http.Request
	var bodies [][]byte
	var mtx sync.Mutex

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if record {
				reqBody, err := io.ReadAll(req.Body)
				require.NoError(tb, err)

				mtx.Lock()
				defer mtx.Unlock()
				requests = append(requests, req)
				bodies = append(bodies, reqBody)
			}

			http.Error(w, "", status)
		}),
	)

	getRequests := func() []*http.Request {
		mtx.Lock()
		defer mtx.Unlock()

		return requests
	}

	getBodies := func() [][]byte {
		mtx.Lock()
		defer mtx.Unlock()

		return bodies
	}

	return srv.URL, getRequests, getBodies, srv.Close
}

func decodeBody(t *testing.T, body []byte) mimirpb.WriteRequest {
	t.Helper()

	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)

	var req mimirpb.WriteRequest
	err = req.Unmarshal(decompressed)
	require.NoError(t, err)

	return req
}

func BenchmarkRemoteWriteForwarding(b *testing.B) {
	const samplesPerReq = 1000
	now := time.Now().UnixMilli()
	ctx := context.Background()

	metrics := make([]string, samplesPerReq)
	for metricIdx := 0; metricIdx < samplesPerReq; metricIdx++ {
		metrics[metricIdx] = "metric" + strconv.Itoa(metricIdx)
	}

	rulesWithIngest := make(validation.ForwardingRules)
	rulesWithoutIngest := make(validation.ForwardingRules)
	rulesWithoutForwarding := make(validation.ForwardingRules)

	for metricIdx := 0; metricIdx < samplesPerReq; metricIdx++ {
		rulesWithIngest[metrics[metricIdx]] = validation.ForwardingRule{Endpoint: "http://localhost/", Ingest: true}
		rulesWithoutIngest[metrics[metricIdx]] = validation.ForwardingRule{Endpoint: "http://localhost/", Ingest: false}
	}
	rulesWithoutForwarding["non-existent-metric"] = validation.ForwardingRule{Endpoint: "http://localhost/", Ingest: false}

	f, _ := newForwarder(b, testConfig, true)

	// No-op http client because we don't want the benchmark to be skewed by TCP performance.
	fTyped := f.(*forwarder)
	fTyped.client = http.Client{Transport: &noopRoundTripper{}}

	errChs := make([]chan error, testConfig.RequestConcurrency)
	var errChIdx int

	type testCase struct {
		name  string
		rules validation.ForwardingRules
	}

	testCases := []testCase{
		{name: "forwarding and ingesting", rules: rulesWithIngest},
		{name: "forwarding and no ingesting", rules: rulesWithoutIngest},
		{name: "no forwarding and only ingesting", rules: rulesWithoutForwarding},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {

			b.ResetTimer()
			b.ReportAllocs()

			for runIdx := 0; runIdx < b.N; runIdx++ {
				samples := mimirpb.PreallocTimeseriesSliceFromPool()
				for sampleIdx := 0; sampleIdx < samplesPerReq; sampleIdx++ {
					samples = append(samples, newSampleFromPool(b, now, 1, "__name__", metrics[sampleIdx]))
				}

				if errChs[errChIdx] != nil {
					require.NoError(b, <-errChs[errChIdx])
				}

				samples, errChs[errChIdx] = f.Forward(ctx, "", 0, tc.rules, samples)
				errChIdx = (errChIdx + 1) % len(errChs)

				mimirpb.ReuseSlice(samples)
			}
		})
	}
}

func TestForwardingToHTTPGrpcTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()

	cfg := testConfig
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("ignored", flag.NewFlagSet("ignored", flag.ContinueOnError))

	forwarder, reg := newForwarder(t, cfg, true)

	url1, reqs1 := newTestHttpgrpcServer(t, 200)
	url2, reqs2 := newTestHttpgrpcServer(t, 200)

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Endpoint: httpGrpcPrefix + url1, Ingest: false},
		"metric2": validation.ForwardingRule{Endpoint: httpGrpcPrefix + url2, Ingest: true},
	}

	ts := []mimirpb.PreallocTimeseries{
		newSample(t, now, 1, 100, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, 200, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, 300, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, 400, "__name__", "metric2", "some_label", "bar"),
	}
	tsToIngest, errCh := forwarder.Forward(ctx, "", 0, rules, ts)

	// The metric2 should be returned by the forwarding because the matching rule has ingest set to "true".
	require.Len(t, tsToIngest, 2)
	requireLabelsEqual(t, tsToIngest[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, tsToIngest[0].Samples, now, 3)
	requireExemplarsEqual(t, tsToIngest[0].Exemplars, now, 300)
	requireLabelsEqual(t, tsToIngest[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, tsToIngest[1].Samples, now, 4)
	requireExemplarsEqual(t, tsToIngest[1].Exemplars, now, 400)

	// Loop until errCh gets closed, errCh getting closed indicates that all forwarding requests have completed.
	for err := range errCh {
		require.NoError(t, err)
	}

	for _, req := range append(reqs1(), reqs2()...) {
		ce := ""
		ct := ""

		for _, h := range req.Headers {
			if h.Key == "Content-Encoding" {
				ce = h.Values[0]
			}
			if h.Key == "Content-Type" {
				ct = h.Values[0]
			}
		}
		require.Equal(t, "snappy", ce)
		require.Equal(t, "application/x-protobuf", ct)
	}

	bodies := reqs1()
	require.Len(t, bodies, 1)
	receivedReq1 := decodeBody(t, bodies[0].Body)
	require.Len(t, receivedReq1.Timeseries, 2)
	requireLabelsEqual(t, receivedReq1.Timeseries[0].Labels, "__name__", "metric1", "some_label", "foo")
	requireSamplesEqual(t, receivedReq1.Timeseries[0].Samples, now, 1)
	require.Empty(t, receivedReq1.Timeseries[0].Exemplars)
	requireLabelsEqual(t, receivedReq1.Timeseries[1].Labels, "__name__", "metric1", "some_label", "bar")
	requireSamplesEqual(t, receivedReq1.Timeseries[1].Samples, now, 2)
	require.Empty(t, receivedReq1.Timeseries[1].Exemplars)

	bodies = reqs2()
	require.Len(t, bodies, 1)
	receivedReq2 := decodeBody(t, bodies[0].Body)
	require.Len(t, receivedReq2.Timeseries, 2)
	requireLabelsEqual(t, receivedReq2.Timeseries[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, receivedReq2.Timeseries[0].Samples, now, 3)
	require.Empty(t, receivedReq2.Timeseries[0].Exemplars)
	requireLabelsEqual(t, receivedReq2.Timeseries[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, receivedReq2.Timeseries[1].Samples, now, 4)
	require.Empty(t, receivedReq2.Timeseries[1].Exemplars)

	expectedMetrics := `
	# HELP cortex_distributor_forward_requests_total The total number of requests the Distributor made to forward samples.
	# TYPE cortex_distributor_forward_requests_total counter
	cortex_distributor_forward_requests_total{} 2
	# HELP cortex_distributor_forward_samples_total The total number of samples the Distributor forwarded.
	# TYPE cortex_distributor_forward_samples_total counter
	cortex_distributor_forward_samples_total{} 4
	# TYPE cortex_distributor_forward_grpc_clients gauge
	# HELP cortex_distributor_forward_grpc_clients Number of gRPC clients used by Distributor forwarder.
	cortex_distributor_forward_grpc_clients 2
`

	require.NoError(t, testutil.GatherAndCompare(
		reg,
		strings.NewReader(expectedMetrics),
		"cortex_distributor_forward_requests_total",
		"cortex_distributor_forward_samples_total",
		"cortex_distributor_forward_grpc_clients",
	))
}

func newTestHttpgrpcServer(tb testing.TB, status int) (string, func() []*httpgrpc.HTTPRequest) {
	tb.Helper()

	server := grpc.NewServer()

	// Register handler for httpgrpc
	hs := &httpgrpcServer{status: int32(status)}
	httpgrpc.RegisterHTTPServer(server, hs)

	// Register health server (pool requires health checks to pass).
	grpc_health_v1.RegisterHealthServer(server, healthServer{})

	l, err := net.Listen("tcp", "")
	require.NoError(tb, err)

	go func() {
		_ = server.Serve(l)
	}()
	tb.Cleanup(func() {
		_ = l.Close()
		server.Stop()
	})

	return l.Addr().String(), hs.getRequests
}

type httpgrpcServer struct {
	mu       sync.Mutex
	requests []*httpgrpc.HTTPRequest
	status   int32
}

func (h *httpgrpcServer) Handle(_ context.Context, httpRequest *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	h.mu.Lock()
	h.requests = append(h.requests, httpRequest)
	h.mu.Unlock()

	return &httpgrpc.HTTPResponse{
		Code: h.status,
	}, nil
}

func (h *httpgrpcServer) getRequests() []*httpgrpc.HTTPRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*httpgrpc.HTTPRequest(nil), h.requests...)
}

type healthServer struct{}

func (healthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}
func (healthServer) Watch(*grpc_health_v1.HealthCheckRequest, grpc_health_v1.Health_WatchServer) error {
	return status.Errorf(codes.Unimplemented, "method Watch not implemented")
}

type noopRoundTripper struct{}

var resp = http.Response{
	Status:     "ok",
	StatusCode: 200,
}

func (n *noopRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &resp, nil
}
