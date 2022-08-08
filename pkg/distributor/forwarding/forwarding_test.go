// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"
	"fmt"
	"io/ioutil"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/dskit/services"

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
		newSample(t, now, 1, "__name__", "metric1", "some_label", "foo"),
		newSample(t, now, 2, "__name__", "metric1", "some_label", "bar"),
		newSample(t, now, 3, "__name__", "metric2", "some_label", "foo"),
		newSample(t, now, 4, "__name__", "metric2", "some_label", "bar"),
	}
	notIngestedCounts, tsToIngest, errCh := forwarder.Forward(ctx, rules, ts)
	require.Equal(t,
		TimeseriesCounts{
			SampleCount:   2,
			ExemplarCount: 0,
		},
		notIngestedCounts,
	)

	// The metric2 should be returned by the forwarding because the matching rule has ingest set to "true".
	require.Len(t, tsToIngest, 2)
	requireLabelsEqual(t, tsToIngest[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, tsToIngest[0].Samples, now, 3)
	requireLabelsEqual(t, tsToIngest[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, tsToIngest[1].Samples, now, 4)

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
	requireLabelsEqual(t, receivedReq1.Timeseries[1].Labels, "__name__", "metric1", "some_label", "bar")
	requireSamplesEqual(t, receivedReq1.Timeseries[1].Samples, now, 2)

	bodies = bodies2()
	require.Len(t, bodies, 1)
	receivedReq2 := decodeBody(t, bodies[0])
	require.Len(t, receivedReq2.Timeseries, 2)
	requireLabelsEqual(t, receivedReq2.Timeseries[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, receivedReq2.Timeseries[0].Samples, now, 3)
	requireLabelsEqual(t, receivedReq2.Timeseries[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, receivedReq2.Timeseries[1].Samples, now, 4)

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
				ts = append(ts, newSample(t, now, 1, "__name__", metric))
			}
			notIngestedCounts, _, errCh := forwarder.Forward(context.Background(), rules, ts)
			require.Equal(t,
				TimeseriesCounts{SampleCount: len(metrics)},
				notIngestedCounts,
			)

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
			notIngestedCounts, toIngest, errCh := forwarder.Forward(context.Background(), tc.rules, ts)
			require.Equal(t,
				TimeseriesCounts{
					SampleCount: sampleCount - tc.expectSamples[ingested],
				},
				notIngestedCounts,
			)
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

func newSample(tb testing.TB, time int64, value float64, labelValuePairs ...string) mimirpb.PreallocTimeseries {
	tb.Helper()

	ts := &mimirpb.TimeSeries{
		Samples: []mimirpb.Sample{
			{TimestampMs: time, Value: value},
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

func newTestServer(tb testing.TB, status int, record bool) (string, func() []*http.Request, func() [][]byte, func()) {
	tb.Helper()

	var requests []*http.Request
	var bodies [][]byte
	var mtx sync.Mutex

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if record {
				reqBody, err := ioutil.ReadAll(req.Body)
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

				_, samples, errChs[errChIdx] = f.Forward(ctx, tc.rules, samples)
				errChIdx = (errChIdx + 1) % len(errChs)

				mimirpb.ReuseSlice(samples)
			}
		})
	}
}

type noopRoundTripper struct{}

var resp = http.Response{
	Status:     "ok",
	StatusCode: 200,
}

func (n *noopRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	return &resp, nil
}
