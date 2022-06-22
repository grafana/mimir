// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

var testConfig = Config{
	Enabled:         true,
	RequestTimeout:  time.Second,
	PropagateErrors: true,
}

func TestForwardingSamplesSuccessfully(t *testing.T) {
	const tenant = "tenant"
	now := time.Now().UnixMilli()

	url1, reqs1, bodies1, close1 := newTestServer(t, 200, true)
	defer close1()

	url2, reqs2, bodies2, close2 := newTestServer(t, 200, true)
	defer close2()

	reg := prometheus.NewPedanticRegistry()
	forwarder := NewForwarder(reg, testConfig)

	rules := validation.ForwardingRules{
		"metric1": validation.ForwardingRule{Endpoint: url1, Ingest: false},
		"metric2": validation.ForwardingRule{Endpoint: url2, Ingest: true},
	}

	forwardingReq := forwarder.NewRequest(context.Background(), tenant, rules)

	ingesterPush := forwardingReq.Add(newSample(t, now, 1, "__name__", "metric1", "some_label", "foo"))
	require.False(t, ingesterPush)

	ingesterPush = forwardingReq.Add(newSample(t, now, 2, "__name__", "metric1", "some_label", "bar"))
	require.False(t, ingesterPush)

	ingesterPush = forwardingReq.Add(newSample(t, now, 3, "__name__", "metric2", "some_label", "foo"))
	require.True(t, ingesterPush)

	ingesterPush = forwardingReq.Add(newSample(t, now, 4, "__name__", "metric2", "some_label", "bar"))
	require.True(t, ingesterPush)

	errCh := forwardingReq.Send(context.Background())
	require.NoError(t, <-errCh)

	for _, req := range append(*reqs1, *reqs2...) {
		require.Equal(t, req.Header.Get("Content-Encoding"), "snappy")
		require.Equal(t, req.Header.Get("Content-Type"), "application/x-protobuf")
	}

	require.Len(t, *bodies1, 1)
	receivedReq := decodeBody(t, (*bodies1)[0])
	require.Len(t, receivedReq.Timeseries, 2)
	requireLabelsEqual(t, receivedReq.Timeseries[0].Labels, "__name__", "metric1", "some_label", "foo")
	requireSamplesEqual(t, receivedReq.Timeseries[0].Samples, now, 1)
	requireLabelsEqual(t, receivedReq.Timeseries[1].Labels, "__name__", "metric1", "some_label", "bar")
	requireSamplesEqual(t, receivedReq.Timeseries[1].Samples, now, 2)

	require.Len(t, *bodies2, 1)
	receivedReq = decodeBody(t, (*bodies2)[0])
	require.Len(t, receivedReq.Timeseries, 2)
	requireLabelsEqual(t, receivedReq.Timeseries[0].Labels, "__name__", "metric2", "some_label", "foo")
	requireSamplesEqual(t, receivedReq.Timeseries[0].Samples, now, 3)
	requireLabelsEqual(t, receivedReq.Timeseries[1].Labels, "__name__", "metric2", "some_label", "bar")
	requireSamplesEqual(t, receivedReq.Timeseries[1].Samples, now, 4)

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
	type errorType uint16
	const (
		errNone           errorType = 0
		errNonRecoverable errorType = 400
		errRecoverable    errorType = 500
	)

	type testcase struct {
		name              string
		config            Config
		remoteStatusCodes []int
		expectedError     errorType
	}

	tcs := []testcase{
		{
			name:              "non-recoverable and successful codes should result in non-recoverable",
			config:            testConfig,
			remoteStatusCodes: []int{200, 400, 200},
			expectedError:     errNonRecoverable,
		}, {
			name:              "recoverable, non-recoverable and successful codes should result in recoverable (1)",
			config:            testConfig,
			remoteStatusCodes: []int{200, 400, 500},
			expectedError:     errRecoverable,
		}, {
			name:              "recoverable, non-recoverable and successful codes should result in recoverable (2)",
			config:            testConfig,
			remoteStatusCodes: []int{500, 400, 200},
			expectedError:     errRecoverable,
		}, {
			name:              "successful codes should result in no error",
			config:            testConfig,
			remoteStatusCodes: []int{200, 200},
			expectedError:     errNone,
		}, {
			name:              "codes which are not divisible by 100 (1)",
			config:            testConfig,
			remoteStatusCodes: []int{204, 401, 200},
			expectedError:     errNonRecoverable,
		}, {
			name:              "codes which are not divisible by 100 (2)",
			config:            testConfig,
			remoteStatusCodes: []int{202, 403, 502},
			expectedError:     errRecoverable,
		}, {
			name:              "codes which are not divisible by 100 (3)",
			config:            testConfig,
			remoteStatusCodes: []int{504, 404, 201},
			expectedError:     errRecoverable,
		}, {
			name: "errors dont get propagated",
			config: Config{
				Enabled:         testConfig.Enabled,
				RequestTimeout:  testConfig.RequestTimeout,
				PropagateErrors: false,
			},
			remoteStatusCodes: []int{504, 404, 201},
			expectedError:     errNone,
		},
	}

	const tenant = "tenant"
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			forwarder := NewForwarder(reg, tc.config)
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
			metrics := make([]string, len(tc.remoteStatusCodes))
			for i, url := range urls {
				metric := fmt.Sprintf("metric%d", i)
				metrics = append(metrics, metric)
				rules[metric] = validation.ForwardingRule{Endpoint: url}
			}

			now := time.Now().UnixMilli()
			forwardingReq := forwarder.NewRequest(context.Background(), tenant, rules)

			for _, metric := range metrics {
				forwardingReq.Add(newSample(t, now, 1, "__name__", metric))
			}

			errCh := forwardingReq.Send(context.Background())
			switch tc.expectedError {
			case errNone:
				require.Nil(t, <-errCh)
			case errRecoverable, errNonRecoverable:
				err := <-errCh
				require.NotNil(t, err)
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok)
				require.Equal(t, tc.expectedError, errorType(resp.Code))
			}

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

func newSample(tb testing.TB, time int64, value float64, labelValuePairs ...string) mimirpb.PreallocTimeseries {
	require.Zero(tb, len(labelValuePairs)%2)

	ts := mimirpb.TimeSeries{
		Samples: []mimirpb.Sample{
			{TimestampMs: time, Value: value},
		},
	}

	for i := 0; i < len(labelValuePairs)/2; i++ {
		ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
			Name:  labelValuePairs[i*2],
			Value: labelValuePairs[i*2+1],
		})
	}

	return mimirpb.PreallocTimeseries{
		TimeSeries: &ts,
	}
}

func requireLabelsEqual(t *testing.T, gotLabels []mimirpb.LabelAdapter, wantLabelValuePairs ...string) {
	require.Zero(t, len(wantLabelValuePairs)%2)
	require.Equal(t, len(wantLabelValuePairs)/2, len(gotLabels))

	for i := range gotLabels {
		require.Equal(t, wantLabelValuePairs[i*2], gotLabels[i].Name)
		require.Equal(t, wantLabelValuePairs[i*2+1], gotLabels[i].Value)
	}
}

func requireSamplesEqual(t *testing.T, gotSamples []mimirpb.Sample, wantTimeValuePairs ...int64) {
	require.Zero(t, len(wantTimeValuePairs)%2)
	require.Equal(t, len(wantTimeValuePairs)/2, len(gotSamples))

	for i := range gotSamples {
		require.Equal(t, wantTimeValuePairs[i*2], gotSamples[i].TimestampMs)
		require.Equal(t, float64(wantTimeValuePairs[i*2+1]), gotSamples[i].Value)
	}
}

func newTestServer(tb testing.TB, status int, record bool) (string, *[]*http.Request, *[][]byte, func()) {
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

	return srv.URL, &requests, &bodies, srv.Close
}

func decodeBody(t *testing.T, body []byte) mimirpb.WriteRequest {
	decompressed, err := snappy.Decode(nil, body)
	require.NoError(t, err)

	var req mimirpb.WriteRequest
	err = req.Unmarshal(decompressed)
	require.NoError(t, err)

	return req
}

func BenchmarkRemoteWriteForwarding(b *testing.B) {
	const tenant = "tenant"
	const samplesPerReq = 1000
	now := time.Now().UnixMilli()
	ctx := context.Background()

	samples := make([]mimirpb.PreallocTimeseries, samplesPerReq)
	rules := make(validation.ForwardingRules)
	for i := 0; i < samplesPerReq; i++ {
		metric := fmt.Sprintf("metric%03d", i)
		samples[i] = newSample(b, now, 1, "__name__", metric)
		rules[metric] = validation.ForwardingRule{Endpoint: "http://localhost/"}
	}

	forwarder := NewForwarder(nil, testConfig).(*forwarder)

	// No-op client, we don't want the benchmark to be skewed by TCP performance
	forwarder.client = http.Client{Transport: &noopRoundTripper{}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		req := forwarder.NewRequest(ctx, tenant, rules)

		for _, sample := range samples {
			req.Add(sample)
		}

		errCh := req.Send(ctx)
		require.Nil(b, <-errCh)
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
