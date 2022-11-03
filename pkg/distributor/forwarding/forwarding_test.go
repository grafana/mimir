// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
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
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
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

func TestForwardingSamplesSuccessfullyToSingleTarget(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UnixMilli()
	forwarder, reg := newForwarder(t, testConfig, true)

	url, reqs, bodiesFn := newTestServer(t, 200, true)

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Ingest: false},
		"metric2": validation.ForwardingRule{Ingest: true},
	}

	ts := []mimirpb.PreallocTimeseries{
		newSample(t, now, 1, 100, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, 200, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, 300, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, 400, "__name__", "metric2", "some_label", "bar"),
	}
	tsToIngest, errCh := forwarder.Forward(ctx, url, 0, rules, ts, "user")

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
		require.Equal(t, "user", req.Header.Get(user.OrgIDHeaderName))
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
	dontForwardOlderThan := 10 * time.Minute
	cutOffTs := now.UnixMilli() - dontForwardOlderThan.Milliseconds()

	type testCase struct {
		name            string
		inputSamples    [][]mimirpb.Sample
		disableIngest   bool // default = false, ie. ingestion is enabled
		expectedSamples [][]mimirpb.Sample
		expectError     error

		checkMetricNames []string
		expectedMetrics  string
	}

	testCases := []testCase{
		{
			name: "drop nothing",
			inputSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs, Value: 1},      // ok
					{TimestampMs: cutOffTs + 50, Value: 1}, // ok
				}, {
					{TimestampMs: cutOffTs + 100, Value: 1}, // ok
					{TimestampMs: cutOffTs + 150, Value: 1}, // ok
				}, {
					{TimestampMs: cutOffTs + 200, Value: 1}, // ok
					{TimestampMs: cutOffTs + 250, Value: 1}, // ok
				},
			},
			expectedSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs, Value: 1},
					{TimestampMs: cutOffTs + 50, Value: 1},
				}, {
					{TimestampMs: cutOffTs + 100, Value: 1},
					{TimestampMs: cutOffTs + 150, Value: 1},
				}, {
					{TimestampMs: cutOffTs + 200, Value: 1},
					{TimestampMs: cutOffTs + 250, Value: 1},
				},
			},
			expectError: nil, // Expecting no error because nothing was dropped.
		}, {
			name: "drop some of the samples, ingest true",
			inputSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old
					{TimestampMs: cutOffTs - 50, Value: 1},  // too old
				}, {
					{TimestampMs: cutOffTs, Value: 1},      // ok
					{TimestampMs: cutOffTs + 50, Value: 1}, // ok
				}, {
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old
					{TimestampMs: cutOffTs + 100, Value: 1}, // ok
					{TimestampMs: cutOffTs + 150, Value: 1}, // ok
				},
			},
			expectedSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs, Value: 1},
					{TimestampMs: cutOffTs + 50, Value: 1},
				}, {
					{TimestampMs: cutOffTs + 100, Value: 1},
					{TimestampMs: cutOffTs + 150, Value: 1},
				},
			},
			expectError:      errSamplesTooOld,
			checkMetricNames: []string{"cortex_discarded_samples_total"},
			expectedMetrics:  ``, // nothing is reported
		}, {
			name: "drop some of the samples, ingest false",
			inputSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old
					{TimestampMs: cutOffTs - 50, Value: 1},  // too old
				}, {
					{TimestampMs: cutOffTs, Value: 1},      // ok
					{TimestampMs: cutOffTs + 50, Value: 1}, // ok
				}, {
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old
					{TimestampMs: cutOffTs + 100, Value: 1}, // ok
					{TimestampMs: cutOffTs + 150, Value: 1}, // ok
				},
			},
			disableIngest: true,
			expectedSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs, Value: 1},
					{TimestampMs: cutOffTs + 50, Value: 1},
				}, {
					{TimestampMs: cutOffTs + 100, Value: 1},
					{TimestampMs: cutOffTs + 150, Value: 1},
				},
			},
			expectError:      errSamplesTooOld,
			checkMetricNames: []string{"cortex_discarded_samples_total"},
			expectedMetrics: `
			# HELP cortex_discarded_samples_total The total number of samples that were discarded.
			# TYPE cortex_discarded_samples_total counter
			cortex_discarded_samples_total{reason="forwarded-sample-too-old",user="user"} 3
`,
		}, {
			name: "split one sample slice in the middle",
			inputSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs - 150, Value: 1}, // too old
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old

				}, {
					{TimestampMs: cutOffTs - 50, Value: 1}, // too old
					{TimestampMs: cutOffTs, Value: 1},      // ok

				}, {
					{TimestampMs: cutOffTs + 50, Value: 1},  // ok
					{TimestampMs: cutOffTs + 100, Value: 1}, // ok

				},
			},
			expectedSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs, Value: 1},
				}, {
					{TimestampMs: cutOffTs + 50, Value: 1},
					{TimestampMs: cutOffTs + 100, Value: 1},
				},
			},
			expectError: errSamplesTooOld,
		}, {
			name: "drop all of the samples",
			inputSamples: [][]mimirpb.Sample{
				{
					{TimestampMs: cutOffTs - 200, Value: 1}, // too old
					{TimestampMs: cutOffTs - 150, Value: 1}, // too old

				}, {
					{TimestampMs: cutOffTs - 100, Value: 1}, // too old
					{TimestampMs: cutOffTs - 50, Value: 1},  // too old

				},
			},
			expectedSamples: [][]mimirpb.Sample{},
			expectError:     errSamplesTooOld,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f, reg := newForwarder(t, testConfig, true)

			url, _, bodiesFn := newTestServer(t, 200, true)

			testMetric := "metric1"
			rules := validation.ForwardingRules{
				testMetric: validation.ForwardingRule{Ingest: !tc.disableIngest},
			}

			inputTs := make([]mimirpb.PreallocTimeseries, 0, len(tc.inputSamples))
			for tsIdx := range tc.inputSamples {
				inputTs = append(inputTs, mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels: []mimirpb.LabelAdapter{
							{Name: "__name__", Value: testMetric},
							{Name: "unique_series_label", Value: strconv.Itoa(tsIdx)},
						},
						Samples: tc.inputSamples[tsIdx],
					},
				})
			}

			toIngest, errCh := f.Forward(ctx, url, cutOffTs, rules, inputTs, "user")

			// If ingestion is enabled, all samples should be ingested, including the ones that were dropped by the forwarding.
			if !tc.disableIngest {
				require.Equal(t, len(tc.inputSamples), len(toIngest))
				for tsIdx := range tc.inputSamples {
					require.Equal(t, tc.inputSamples[tsIdx], toIngest[tsIdx].Samples)
				}
			}

			if tc.expectError == nil {
				require.NoError(t, <-errCh)
			} else {
				err := <-errCh
				require.Equal(t, tc.expectError, err)
			}

			// Wait for forwarding requests to complete.
			for range errCh {
			}

			bodies := bodiesFn()

			if len(tc.expectedSamples) > 0 {
				// Expecting single request, with all the metrics.
				require.Len(t, bodies, 1)

				receivedReq := decodeBody(t, bodies[0])

				require.Equal(t, len(tc.expectedSamples), len(receivedReq.Timeseries))
				for tsIdx := range tc.expectedSamples {
					require.Equal(t, tc.expectedSamples[tsIdx], receivedReq.Timeseries[tsIdx].Samples)
				}
			} else {
				// Expecting no requests if there were no samples to forward.
				require.Len(t, bodies, 0)
			}

			if len(tc.checkMetricNames) > 0 {
				require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(tc.expectedMetrics), tc.checkMetricNames...))
			}
		})
	}
}

func TestForwardingEnsureThatPooledObjectsGetReturned(t *testing.T) {
	sampleCount := 20

	const forwardingTarget = "target1"
	const ingested = "ingested"

	type testCase struct {
		name          string
		sendSamples   map[string]int
		rules         validation.ForwardingRules
		expectSamples map[string]int
	}

	testCases := []testCase{
		{
			name: "forward samples of one metric and don't ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
			},
		}, {
			name: "forward samples of one metric and ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: true},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
				ingested:         sampleCount,
			},
		}, {
			name: "forward samples of four metrics and don't ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: false},
				"metric2": {Ingest: false},
				"metric3": {Ingest: false},
				"metric4": {Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
			},
		}, {
			name: "forward samples of four metrics to and ingest them",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: true},
				"metric2": {Ingest: true},
				"metric3": {Ingest: true},
				"metric4": {Ingest: true},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
				ingested:         sampleCount,
			},
		}, {
			name: "forward samples of four metrics to and ingest half of them (1)",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: true},
				"metric2": {Ingest: true},
				"metric3": {Ingest: false},
				"metric4": {Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
				ingested:         sampleCount / 2,
			},
		}, {
			name: "forward samples of four metrics and ingest half of them (2)",
			sendSamples: map[string]int{
				"metric1": sampleCount / 4,
				"metric2": sampleCount / 4,
				"metric3": sampleCount / 4,
				"metric4": sampleCount / 4,
			},
			rules: validation.ForwardingRules{
				"metric1": {Ingest: true},
				"metric2": {Ingest: false},
				"metric3": {Ingest: true},
				"metric4": {Ingest: false},
			},
			expectSamples: map[string]int{
				forwardingTarget: sampleCount,
				ingested:         sampleCount / 2,
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
			url, _, bodies := newTestServer(t, 200, true)

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
			toIngest, errCh := forwarder.Forward(context.Background(), url, 0, tc.rules, ts, "user")
			require.NoError(t, <-errCh)

			// receivedSamples counts the number of samples that each forwarding target has received.
			receivedSamples := make(map[string]int)
			for _, body := range bodies() {
				writeReq := decodeBody(t, body)
				for _, ts := range writeReq.Timeseries {
					receivedForTarget := receivedSamples[forwardingTarget]
					receivedForTarget += len(ts.Samples)
					receivedSamples[forwardingTarget] = receivedForTarget
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

func TestDropSamplesBefore(t *testing.T) {
	type testCase struct {
		name     string
		cutoff   int64
		input    []mimirpb.Sample
		expected []mimirpb.Sample
	}

	testCases := []testCase{
		{
			name:     "no samples",
			cutoff:   100,
			input:    []mimirpb.Sample{},
			expected: []mimirpb.Sample{},
		}, {
			name:   "all samples before cutoff",
			cutoff: 100,
			input: []mimirpb.Sample{
				{TimestampMs: 97},
				{TimestampMs: 98},
				{TimestampMs: 99},
			},
			expected: []mimirpb.Sample{},
		}, {
			name:   "all samples after cutoff",
			cutoff: 100,
			input: []mimirpb.Sample{
				{TimestampMs: 100},
				{TimestampMs: 101},
				{TimestampMs: 102},
			},
			expected: []mimirpb.Sample{
				{TimestampMs: 100},
				{TimestampMs: 101},
				{TimestampMs: 102},
			},
		}, {
			name:   "cut some samples off",
			cutoff: 100,
			input: []mimirpb.Sample{
				{TimestampMs: 97},
				{TimestampMs: 98},
				{TimestampMs: 99},
				{TimestampMs: 100},
				{TimestampMs: 101},
				{TimestampMs: 102},
			},
			expected: []mimirpb.Sample{
				{TimestampMs: 100},
				{TimestampMs: 101},
				{TimestampMs: 102},
			},
		}, {
			name:   "unsorted sample slice",
			cutoff: 100,
			input: []mimirpb.Sample{
				{TimestampMs: 98},
				{TimestampMs: 101},
				{TimestampMs: 99},
				{TimestampMs: 100},
				{TimestampMs: 97},
				{TimestampMs: 102},
			},
			expected: []mimirpb.Sample{
				{TimestampMs: 102},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := dropSamplesBefore(tc.input, tc.cutoff)
			require.Equal(t, tc.expected, actual)
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

func newTestServer(tb testing.TB, status int, record bool) (string, func() []*http.Request, func() [][]byte) {
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

	tb.Cleanup(srv.Close)

	return srv.URL, getRequests, getBodies
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
		rulesWithIngest[metrics[metricIdx]] = validation.ForwardingRule{Ingest: true}
		rulesWithoutIngest[metrics[metricIdx]] = validation.ForwardingRule{Ingest: false}
	}
	rulesWithoutForwarding["non-existent-metric"] = validation.ForwardingRule{Ingest: false}

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

				samples, errChs[errChIdx] = f.Forward(ctx, "http://localhost/", 0, tc.rules, samples, "user")
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

	url, reqs1 := newTestHttpgrpcServer(t, 200)

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Ingest: false},
		"metric2": validation.ForwardingRule{Ingest: true},
	}

	ts := []mimirpb.PreallocTimeseries{
		newSample(t, now, 1, 100, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, 200, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, 300, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, 400, "__name__", "metric2", "some_label", "bar"),
	}

	tsToIngest, errCh := forwarder.Forward(ctx, httpGrpcPrefix+url, 0, rules, ts, "user")

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

	for _, req := range reqs1() {
		ce := ""
		ct := ""
		us := ""

		for _, h := range req.Headers {
			if h.Key == "Content-Encoding" {
				ce = h.Values[0]
			}
			if h.Key == "Content-Type" {
				ct = h.Values[0]
			}
			if h.Key == textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName) {
				us = h.Values[0]
			}
		}
		require.Equal(t, "snappy", ce)
		require.Equal(t, "application/x-protobuf", ct)
		require.Equal(t, "user", us)
	}

	bodies := reqs1()
	require.Len(t, bodies, 1)
	receivedReq1 := decodeBody(t, bodies[0].Body)
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
	# TYPE cortex_distributor_forward_grpc_clients gauge
	# HELP cortex_distributor_forward_grpc_clients Number of gRPC clients used by Distributor forwarder.
	cortex_distributor_forward_grpc_clients 1
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
